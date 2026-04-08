from flows_staging.shared import run_staging_flow
from prefect import flow


DOMAIN_DOWNLOADS = ["populations_historiques", "salaries"]


@flow(name="staging_current_demographics")
def staging_current_demographics() -> None:
    run_staging_flow(domain="demographics", domain_downloads=DOMAIN_DOWNLOADS)


if __name__ == "__main__":
    staging_current_demographics()
