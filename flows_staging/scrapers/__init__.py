import asyncio
import logging
from importlib import import_module
from typing import Any

from flows_staging.scrapers.models import FileMetadata
from flows_staging.shared import get_config
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import get_latest_hashes
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import log_upload
from flows_staging.shared.audit import preflight
from flows_staging.shared.minio import STAGING_BUCKET
from prefect import flow
from prefect import task


logger = logging.getLogger(__name__)


async def run_all_scrapers(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Run all enabled scrapers from config."""
    results = []
    enabled = [s for s in config.get("scrapers", []) if s.get("enabled", True)]
    known_hashes = get_latest_hashes()

    for scraper in enabled:
        result = await run_single_scraper_async(scraper, known_hashes)
        results.append(result)

    return results


async def run_single_scraper_async(scraper: dict, known_hashes: dict) -> dict[str, Any]:
    """Run a single scraper asynchronously and return result dict."""
    name = scraper["name"]
    module = import_module(scraper["module"])
    config = get_config()

    try:
        metadata = await module.run(config, known_hashes)
        if metadata is None:
            logger.info("⏭️ %s skipped (no changes)", name)
            return {"name": name, "success": True, "result": None}
        logger.info("✅ %s → %s", name, metadata.key)
        return {"name": name, "success": True, "result": metadata}
    except Exception as exc:
        logger.error("❌ %s failed: %s", name, exc)
        return {"name": name, "success": False, "error": str(exc)}


@task
def run_single_scraper(
    scraper: dict, known_hashes: dict
) -> tuple[str, FileMetadata | str | None]:
    """Run a single scraper and return (scraper_name, error_or_metadata)."""
    name = scraper["name"]
    module = import_module(scraper["module"])
    config = get_config()

    try:
        metadata = asyncio.run(module.run(config, known_hashes))
        if metadata is None:
            logger.info("⏭️ %s skipped (no changes)", name)
            return (name, None)
        logger.info("✅ %s → %s", name, metadata.key)
        return (name, metadata)
    except Exception as exc:
        logger.error("❌ %s failed: %s", name, exc)
        return (name, str(exc))


@flow(name="run_scraper")
def run_scraper(scraper_name: str) -> dict:
    """Run a single scraper by name."""
    preflight()
    run_id = init_run(domain="labels", technical_type="SCRAPER")

    try:
        config = get_config()
        scrapers = config.get("scrapers", [])
        scraper = next((s for s in scrapers if s["name"] == scraper_name), None)

        if not scraper:
            logger.error("Scraper '%s' not found in config", scraper_name)
            finalize_run(run_id=run_id, status="FAILED", number_files=0)
            return {"name": scraper_name, "success": False, "error": "Not found"}

        known_hashes = get_latest_hashes()
        name, result = run_single_scraper(scraper, known_hashes)

        if isinstance(result, Exception) or (isinstance(result, str) and result):
            logger.error("❌ %s: %s", name, result)
            finalize_run(run_id=run_id, status="FAILED", number_files=0)
            return {"name": name, "success": False, "error": str(result)}

        if result is None:
            logger.info("⏭️ %s skipped (no changes)", name)
            finalize_run(run_id=run_id, status="SUCCESS", number_files=0)
            return {"name": name, "success": True, "result": None}

        if isinstance(result, str):
            logger.error("❌ %s: %s", name, result)
            finalize_run(run_id=run_id, status="FAILED", number_files=0)
            return {"name": name, "success": False, "error": result}

        log_upload(
            run_id=run_id,
            name=result.base_name,
            filename_timestamp=result.filename_timestamp,
            file_location=result.key,
            source_url=None,
            size_mb=result.size_mb,
            md5_hash=result.md5,
            bucket=STAGING_BUCKET,
        )

        finalize_run(run_id=run_id, status="SUCCESS", number_files=1)
        return {"name": name, "success": True, "result": result}

    except Exception as exc:
        logger.exception("Flow failed: %s", exc)
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m flows_staging.scrapers <scraper_name>")
    run_scraper(sys.argv[1])
