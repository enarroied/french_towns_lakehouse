from flows.shared import log
from flows_staging.staging.staging_current_geography import staging_current_geography
from flows_staging.staging.staging_current_labels import staging_current_labels
from flows_staging.staging.staging_historical_population import (
    staging_historical_population,
)
from flows_transformation.transformation.transformation_current_dim_geography import (
    transformation_current_dim_geography,
)
from flows_transformation.transformation.transformation_current_fact_demographics import (
    transformation_current_fact_demographics,
)
from flows_transformation.transformation.transformation_current_labels import (
    transformation_current_labels,
)
from prefect import flow


@flow(name="french_towns_pipeline")
def french_towns_pipeline() -> None:
    """
    Unified pipeline for testing all child flows end-to-end.
    In production, each child flow is deployed and scheduled independently.
    """
    log("Starting French Towns Pipeline")

    log("=== STAGING PHASE ===")
    staging_current_geography()
    staging_historical_population()
    staging_current_labels()

    log("=== TRANSFORMATION PHASE ===")
    transformation_current_dim_geography()
    transformation_current_fact_demographics()
    transformation_current_labels()

    log("Pipeline complete")


if __name__ == "__main__":
    french_towns_pipeline()
