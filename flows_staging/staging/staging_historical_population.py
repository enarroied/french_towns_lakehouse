from flows_staging.shared.audit import RUN_STATUS_FAILED
from flows_staging.shared.audit import RUN_STATUS_SUCCESS
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from flows_staging.shared.staging_base import get_specific_config
from flows_staging.shared.staging_base import stage_files
from prefect import flow


DOMAIN_DOWNLOAD = "historical_population"


@flow(name="staging_historical_population")
def staging_historical_population() -> None:
    """Main Prefect flow for staging historical population data.

    Downloads the populations_historiques dataset, extracts and renames files,
    uploads to MinIO staging bucket, and records metadata in DuckDB.
    Always calls finalize_run — with SUCCESS or FAILED — so the run is never
    left open in the database.
    """
    preflight()
    run_id = init_run(domain="demographics", technical_type="DOWNLOAD")
    config = get_specific_config(DOMAIN_DOWNLOAD, run_id)

    try:
        number_files = stage_files(config)
        finalize_run(
            run_id=run_id, status=RUN_STATUS_SUCCESS, number_files=number_files
        )
    except Exception:
        finalize_run(run_id=run_id, status=RUN_STATUS_FAILED, number_files=0)
        raise


if __name__ == "__main__":
    staging_historical_population()
