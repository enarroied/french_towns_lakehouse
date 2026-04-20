from flows_staging.shared import run_staging_flow
from flows_staging.shared.models import StagingFlowParams
from prefect import flow


DOMAIN_DOWNLOADS = ["french_communes", "arrondissements", "departements", "zip_codes"]


@flow(name="staging_current_geography")
def staging_current_geography() -> None:
    params = StagingFlowParams(
        domain="geography",
        domain_downloads=DOMAIN_DOWNLOADS,
        technical_type="DOWNLOAD",
    )
    run_staging_flow(params)


if __name__ == "__main__":
    staging_current_geography()
