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
def run_label_models() -> None:
    _run_dbt_command(
        ["run", "--select", "validated_dim_dim_labels,validated_fact_fact_labels"],
        "dbt run label models failed — check logs above",
    )


@task
def test_label_models() -> None:
    _run_dbt_command(
        ["test", "--select", "validated_dim_dim_labels,validated_fact_fact_labels"],
        "dbt test label models failed — check logs above",
    )


@flow(name="transformation_current_labels")
def transformation_current_labels() -> None:
    stage_external_sources()
    run_label_models()
    test_label_models()


if __name__ == "__main__":
    transformation_current_labels()
