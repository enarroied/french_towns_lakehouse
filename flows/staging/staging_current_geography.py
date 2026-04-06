from flows.shared import run_staging_flow
from prefect import flow


DOMAIN_DOWNLOADS = ["french_communes", "arrondissements", "departements", "zip_codes"]


@flow(name="staging_current_geography")
def staging_current_geography() -> None:
    run_staging_flow(domain="geography", domain_downloads=DOMAIN_DOWNLOADS)


if __name__ == "__main__":
    staging_current_geography()
