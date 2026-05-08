from flows_staging.shared import get_latest_hashes
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from flows_staging.shared.dbt import run_and_test
from flows_staging.shared.dbt import stage_external_sources
from flows_transformation.shared.database import ensure_work_database_exists
from flows_transformation.shared.output import handle_outputs
from flows_transformation.shared.validation import validate_inputs
from prefect import flow
from prefect import task


MODEL_SELECTOR = "dim_communes_france"


INPUT_SOURCES = ["french_communes", "arrondissements", "departements", "zip_codes"]


@task
def run_stage_external_sources() -> None:
    stage_external_sources()


@task
def run_dim_communes() -> None:
    run_and_test(MODEL_SELECTOR)


@task
def check_skip(input_sources: list[str], known_hashes: dict) -> bool:
    return any(source in known_hashes for source in input_sources)


@flow(name="transformation_current_dim_geography")
def transformation_current_dim_geography() -> None:
    preflight()
    run_id = init_run(domain="geography", layer="TRANSFORMATION", technical_type="DBT")

    try:
        ensure_work_database_exists()
        known_hashes = get_latest_hashes()

        if check_skip(INPUT_SOURCES, known_hashes):
            finalize_run(run_id=run_id, status="SUCCESS", number_files=0)
            return

        validate_inputs(source_names=INPUT_SOURCES)
        run_stage_external_sources()
        run_dim_communes()
        handle_outputs(model_names=["dim_communes_france"], run_id=run_id)
        finalize_run(run_id=run_id, status="SUCCESS", number_files=1)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED")
        raise


if __name__ == "__main__":
    transformation_current_dim_geography()
