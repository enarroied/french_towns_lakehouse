from flows.shared import log
from flows_staging.staging.staging_cog_ensemble import staging_cog_ensemble
from flows_staging.staging.staging_current_labels import staging_current_labels
from flows_staging.staging.staging_french_communes import staging_french_communes
from flows_staging.staging.staging_historical_population import (
    staging_historical_population,
)
from flows_staging.staging.staging_salaries import staging_salaries
from flows_staging.staging.staging_zip_codes import staging_zip_codes
from prefect import flow


@flow(name="staging_pipeline")
def staging_pipeline() -> None:
    """Run all staging pipelines to test them end-to-end."""
    log("Starting Staging Pipeline")

    log("=== DEMOGRAPHICS ===")
    staging_historical_population()
    staging_salaries()

    log("=== GEOGRAPHY ===")
    staging_french_communes()
    staging_cog_ensemble()
    staging_zip_codes()

    log("=== LABELS ===")
    staging_current_labels()

    log("Staging Pipeline complete")


if __name__ == "__main__":
    staging_pipeline()
