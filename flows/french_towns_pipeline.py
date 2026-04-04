import logging

from prefect import flow
from prefect.logging import get_logger

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


logger = get_logger(__name__)


@flow(name="french_towns_pipeline")
def french_towns_pipeline() -> None:
    """
    Unified pipeline for testing all child flows end-to-end.
    In production, each child flow is deployed and scheduled independently.
    """
    logger.info("Starting French Towns Pipeline")

    logger.info("=== STAGING PHASE ===")
    staging_current_geography()
    staging_current_demographics()
    staging_current_labels()

    logger.info("=== TRANSFORMATION PHASE ===")
    transformation_current_dim_geography()
    transformation_current_fact_demographics()
    transformation_current_labels()

    logger.info("Pipeline complete")


if __name__ == "__main__":
    french_towns_pipeline()
