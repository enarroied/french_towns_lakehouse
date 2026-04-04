from flows.staging.staging_current_demographics import staging_current_demographics
from flows.staging.staging_current_geography import staging_current_geography
from flows.staging.staging_current_labels import staging_current_labels
from flows.transformation.transformation_current_dim_geography import (
    transformation_current_dim_geography,
)
from flows.transformation.transformation_current_fact_demographics import (
    transformation_current_fact_demographics,
)
from flows.transformation.transformation_current_labels import (
    transformation_current_labels,
)
from prefect import flow
from prefect import task
from scrapers import run_all_scrapers
from scripts.download import main as download_files


load_dotenv(find_dotenv())

with Path("config.yaml").open() as f:
    config = yaml.safe_load(f)

PATHS = config["paths"]
DIRECTORIES = config.get("directories", [])
DBT_PROJECT_DIR = Path("french_towns_dbt")
DBT_PROFILES_ARGS = ["--profiles-dir", "."]
MINIO_BUCKET = "validated"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")


def _run_dbt_command(args: list[str], failure_message: str) -> None:
    result = subprocess.run(
        ["dbt"] + args + DBT_PROFILES_ARGS,
        cwd=DBT_PROJECT_DIR,
        check=False,
        capture_output=False,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(failure_message)


@task
def create_required_dirs() -> None:
    for dir_key in DIRECTORIES:
        Path(config["paths"][dir_key]).mkdir(exist_ok=True, parents=True)


@task
def download_all_files() -> None:
    asyncio.run(download_files())


@task
def run_scrapers() -> None:
    results = asyncio.run(run_all_scrapers(config))

    succeeded = [r for r in results if r.success]

    width = 50
    print("\n" + "=" * width)
    print("  SCRAPER RUN SUMMARY")
    print("=" * width)
    for r in results:
        status = "OK" if r.success else "FAILED"
        print(f"  {r.name:<30} {status}")
        if not r.success:
            short = (r.error or "unknown error").splitlines()[0][:60]
            print(f"      - {short}")
    print("-" * width)
    print(f"  {len(succeeded)}/{len(results)} scrapers succeeded.")
    print("=" * width + "\n")


@task
def run_custom_parsers() -> None:
    run_all_custom_parsers(config)


@task
def run_dbt() -> None:
    _run_dbt_command(
        ["run-operation", "stage_external_sources"],
        "dbt stage_external_sources failed — check logs above",
    )
    _run_dbt_command(
        ["run"],
        "dbt run failed — check logs above",
    )
    _run_dbt_command(
        ["test"],
        "dbt test failed — check logs above",
    )


@task
def upload_to_minio() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        use_ssl=False,
    )

    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            client.create_bucket(Bucket=MINIO_BUCKET)
        else:
            raise

    for file_path in Path(PATHS["output_dir"]).glob("*.parquet"):
        client.upload_file(
            Filename=str(file_path),
            Bucket=MINIO_BUCKET,
            Key=file_path.name,
        )


@flow
def french_towns_pipeline() -> None:
    """
    Unified pipeline for testing all child flows end-to-end.
    In production, each child flow is deployed and scheduled independently.
    """
    print("\n" + "=" * 60)
    print(" STAGING PHASE")
    print("=" * 60)

    staging_current_geography()
    staging_current_demographics()
    staging_current_labels()

    print("\n" + "=" * 60)
    print(" TRANSFORMATION PHASE")
    print("=" * 60)

    transformation_current_dim_geography()
    transformation_current_fact_demographics()
    transformation_current_labels()

    print("\n" + "=" * 60)
    print(" PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    french_towns_pipeline()
