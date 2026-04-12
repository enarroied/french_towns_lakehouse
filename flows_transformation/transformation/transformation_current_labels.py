from flows_staging.shared.dbt import run_and_test
from flows_staging.shared.dbt import stage_external_sources
from flows_transformation.shared.database import ensure_work_database_exists
from prefect import flow
from prefect import task


MODEL_SELECTOR = "dim_labels fact_labels"


@task
def run_stage_external_sources() -> None:
    stage_external_sources()


@task
def run_label_models() -> None:
    run_and_test(MODEL_SELECTOR)


@flow(name="transformation_current_labels")
def transformation_current_labels() -> None:
    ensure_work_database_exists()
    run_stage_external_sources()
    run_label_models()


if __name__ == "__main__":
    transformation_current_labels()
