import subprocess
import sys
from pathlib import Path

from prefect import flow
from prefect import task

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flows.shared import DBT_PROJECT_DIR, DBT_PROFILES_ARGS


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
def stage_external_sources() -> None:
    _run_dbt_command(
        ["run-operation", "stage_external_sources"],
        "dbt stage_external_sources failed — check logs above",
    )


@task
def run_dim_communes_model() -> None:
    _run_dbt_command(
        ["run", "--select", "validated_dim_dim_communes_france"],
        "dbt run dim_communes_france failed — check logs above",
    )


@task
def test_dim_communes_model() -> None:
    _run_dbt_command(
        ["test", "--select", "validated_dim_dim_communes_france"],
        "dbt test dim_communes_france failed — check logs above",
    )


@flow(name="transformation_current_dim_geography")
def transformation_current_dim_geography() -> None:
    stage_external_sources()
    run_dim_communes_model()
    test_dim_communes_model()


if __name__ == "__main__":
    transformation_current_dim_geography()
