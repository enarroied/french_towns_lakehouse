import logging
from importlib import import_module

from flows_staging.scrapers.models import FileMetadata
from flows_staging.shared import get_config
from flows_staging.shared import get_scrapers
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import get_latest_hashes
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import log_upload
from flows_staging.shared.audit import preflight
from flows_staging.shared.minio import STAGING_BUCKET
from prefect import flow
from prefect import task


logger = logging.getLogger(__name__)


@task(retries=2, retry_delay_seconds=30)
async def run_single_scraper(
    scraper: dict, known_hashes: dict
) -> tuple[str, FileMetadata | str | None]:
    """Run a single scraper and return (scraper_name, result).

    Returns:
        - (name, FileMetadata) on success
        - (name, None) if skipped (no changes)
        - (name, "error message") on failure
    """
    name = scraper["name"]
    module = import_module(scraper["module"])
    config = get_config()

    try:
        metadata = await module.run(config, known_hashes)
        if metadata is None:
            logger.info("⏭️ %s skipped (no changes)", name)
            return (name, None)
        logger.info("✅ %s → %s", name, metadata.key)
        return (name, metadata)
    except Exception as exc:
        logger.error("❌ %s failed: %s", name, exc)
        return (name, str(exc))


@flow(name="staging_current_labels")
def staging_current_labels() -> list[tuple[str, FileMetadata | str | None]]:
    """Run all enabled scrapers and log results to audit DB."""
    preflight()
    run_id = init_run(domain="labels", technical_type="SCRAPER")

    try:
        known_hashes = get_latest_hashes()
        all_scrapers = get_scrapers()
        enabled = [s for s in all_scrapers if s.get("enabled", True)]
        disabled = [s for s in all_scrapers if not s.get("enabled", True)]

        for s in disabled:
            logger.info("Skipping %s (disabled)", s["name"])

        if not enabled:
            logger.warning("No scrapers enabled")
            finalize_run(run_id=run_id, status="SUCCESS", number_files=0)
            return []

        futures = [run_single_scraper.submit(s, known_hashes) for s in enabled]
        results = [f.result() for f in futures]  # type: ignore[misc]

        upload_futures = []
        file_count = 0
        failed = 0

        for name, result in results:  # type: ignore[union-attr]
            if isinstance(result, str):
                logger.error("❌ %s: %s", name, result)
                failed += 1
            elif result is None:
                logger.info("⏭️ %s skipped (no changes)", name)
            else:
                logger.info("📝 Logging %s to audit DB", name)
                upload_futures.append(
                    log_upload.submit(
                        run_id=run_id,
                        name=result.base_name,
                        filename_timestamp=result.filename_timestamp,
                        file_location=result.key,
                        source_url=None,
                        size_mb=result.size_mb,
                        md5_hash=result.md5,
                        bucket=STAGING_BUCKET,
                    )
                )
                file_count += 1

        [f.result() for f in upload_futures]

        status = "FAILED" if failed == len(results) else "SUCCESS"
        finalize_run(run_id=run_id, status=status, number_files=file_count)

        if failed > 0:
            logger.warning("%d scraper(s) failed", failed)

        return results

    except Exception as exc:
        logger.exception("Flow failed: %s", exc)
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise


if __name__ == "__main__":
    staging_current_labels()
