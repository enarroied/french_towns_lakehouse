from flows_staging.shared.models import StagingFlowParams
from flows_staging.shared.staging_base import run_staging_flow
from prefect import flow


DOMAIN_DOWNLOAD = "cog_ensemble"


@flow(name=f"staging_{DOMAIN_DOWNLOAD}")
def staging_cog_ensemble() -> None:
    run_staging_flow(
        StagingFlowParams(domain="geography", domain_download=DOMAIN_DOWNLOAD)
    )


if __name__ == "__main__":
    staging_cog_ensemble()
