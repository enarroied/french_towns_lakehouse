from flows_staging.shared.dbt import run_and_test
from flows_staging.shared.dbt import stage_external_sources
from prefect import flow
from prefect import task


MODEL_SELECTOR = "fact_population fact_salaries"


@task
def run_stage_external_sources() -> None:
    stage_external_sources()


@task
def run_fact_models() -> None:
    run_and_test(MODEL_SELECTOR)


@flow(name="transformation_current_fact_demographics")
def transformation_current_fact_demographics() -> None:
    run_stage_external_sources()
    run_fact_models()


if __name__ == "__main__":
    transformation_current_fact_demographics()
