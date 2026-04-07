from flows_staging.shared.dbt import run_and_test
from flows_staging.shared.dbt import stage_external_sources
from prefect import flow
from prefect import task


MODEL_SELECTOR = "validated_dim_dim_communes_france"


@task
def run_stage_external_sources() -> None:
    stage_external_sources()


@task
def run_dim_communes() -> None:
    run_and_test(MODEL_SELECTOR)


@flow(name="transformation_current_dim_geography")
def transformation_current_dim_geography() -> None:
    run_stage_external_sources()
    run_dim_communes()


if __name__ == "__main__":
    transformation_current_dim_geography()
