from flows_staging.shared import run_staging_flow
from flows_staging.shared.models import StagingFlowParams
from prefect import flow


DOMAIN_DOWNLOADS = ["populations_historiques", "salaries"]


@flow(name="staging_current_demographics")
def staging_current_demographics() -> None:
    params = StagingFlowParams(
        domain="demographics",
        domain_downloads=DOMAIN_DOWNLOADS,
        technical_type="DOWNLOAD",
    )
    run_staging_flow(params)


if __name__ == "__main__":
    staging_current_demographics()
