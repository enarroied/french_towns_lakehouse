import sys
from pathlib import Path

from prefect import flow
from prefect import task


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flows.shared.dbt import run_and_test
from flows.shared.dbt import stage_external_sources


MODEL_SELECTOR = "validated_dim_dim_labels,validated_fact_fact_labels"


@task
def run_stage_external_sources() -> None:
    stage_external_sources()


@task
def run_label_models() -> None:
    run_and_test(MODEL_SELECTOR)


@flow(name="transformation_current_labels")
def transformation_current_labels() -> None:
    run_stage_external_sources()
    run_label_models()


if __name__ == "__main__":
    transformation_current_labels()
