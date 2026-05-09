"""Staging pipeline for arrondissements data — downloads and uploads to MinIO."""

from flows_staging.shared.models import StagingFlowParams
from flows_staging.shared.staging_base import run_staging_flow
from prefect import flow


DOMAIN_DOWNLOAD = "arrondissements"


@flow(name=f"staging_{DOMAIN_DOWNLOAD}")
def staging_arrondissements() -> None:
    """Download arrondissements CSV and stage it in MinIO."""
    run_staging_flow(
        StagingFlowParams(domain="geography", domain_download=DOMAIN_DOWNLOAD)
    )


if __name__ == "__main__":
    staging_arrondissements()
