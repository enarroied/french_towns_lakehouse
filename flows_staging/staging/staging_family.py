from flows_staging.shared.models import StagingFlowParams
from flows_staging.shared.staging_base import run_staging_flow
from prefect import flow


DOMAIN_DOWNLOAD = "family"


@flow(name=f"staging_{DOMAIN_DOWNLOAD}")
def staging_family() -> None:
    run_staging_flow(
        StagingFlowParams(domain="demographics", domain_download=DOMAIN_DOWNLOAD)
    )


if __name__ == "__main__":
    staging_family()
