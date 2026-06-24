from flows.shared import log
from flows_transformation.transformation.transformation_current_dim_geography import (
    transformation_current_dim_geography,
)
from flows_transformation.transformation.transformation_current_fact_demographics import (
    transformation_current_fact_demographics,
)
from flows_transformation.transformation.transformation_current_fact_unemployment import (
    transformation_current_fact_unemployment,
)
from flows_transformation.transformation.transformation_current_labels import (
    transformation_current_labels,
)
from prefect import flow


@flow(name="transformation_pipeline")
def transformation_pipeline() -> None:
    """Run all transformation pipelines to test them end-to-end."""
    log("Starting Transformation Pipeline")

    log("=== GEOGRAPHY ===")
    transformation_current_dim_geography()

    log("=== DEMOGRAPHICS ===")
    transformation_current_fact_demographics()
    transformation_current_fact_unemployment()

    log("=== LABELS ===")
    transformation_current_labels()

    log("Transformation Pipeline complete")


if __name__ == "__main__":
    transformation_pipeline()
