from flows.shared import log
from flows_staging.staging.staging_arrondissements import staging_arrondissements
from flows_staging.staging.staging_current_labels import staging_current_labels
from flows_staging.staging.staging_departements import staging_departements
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
    staging_arrondissements()
    staging_departements()
    staging_zip_codes()

    log("=== LABELS ===")
    staging_current_labels()

    log("Staging Pipeline complete")


if __name__ == "__main__":
    staging_pipeline()
