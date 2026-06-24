from flows.shared import log
from flows_integration.integration.integration_current_dim_geography import (
    integration_current_dim_geography,
)
from flows_integration.integration.integration_current_fact_demographics import (
    integration_current_fact_demographics,
)
from flows_integration.integration.integration_current_fact_unemployment import (
    integration_current_fact_unemployment,
)
from flows_integration.integration.integration_current_labels import (
    integration_current_labels,
)
from flows_staging.staging.staging_cog_ensemble import staging_cog_ensemble
from flows_staging.staging.staging_current_labels import staging_current_labels
from flows_staging.staging.staging_french_communes import staging_french_communes
from flows_staging.staging.staging_historical_population import (
    staging_historical_population,
)
from flows_staging.staging.staging_salaries import staging_salaries
from flows_staging.staging.staging_unemployment import staging_unemployment
from flows_staging.staging.staging_zip_codes import staging_zip_codes
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


@flow(name="french_towns_pipeline")
def french_towns_pipeline() -> None:
    """
    Unified pipeline for testing all child flows end-to-end.
    In production, each child flow is deployed and scheduled independently.
    """
    log("Starting French Towns Pipeline")

    log("=== STAGING PHASE ===")
    staging_historical_population()
    staging_salaries()
    staging_unemployment()
    staging_french_communes()
    staging_cog_ensemble()
    staging_zip_codes()
    staging_current_labels()

    log("=== TRANSFORMATION PHASE ===")
    transformation_current_dim_geography()
    transformation_current_fact_demographics()
    transformation_current_fact_unemployment()
    transformation_current_labels()

    log("=== INTEGRATION PHASE ===")
    integration_current_dim_geography()
    integration_current_fact_demographics()
    integration_current_fact_unemployment()
    integration_current_labels()

    log("Pipeline complete")


if __name__ == "__main__":
    french_towns_pipeline()
