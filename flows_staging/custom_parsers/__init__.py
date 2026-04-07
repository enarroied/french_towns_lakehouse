import logging

from importlib import import_module

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


@task
def run_single_parser(parser: dict, known_hashes: dict) -> tuple[str, str | None]:
    """Run a single parser and return (parser_name, error_or_metadata)."""
    name = parser["name"]
    module = import_module(parser["module"])
    config = get_config()

    try:
        metadata = module.run(config, known_hashes)
        if metadata is None:
            logger.info("⏭️ %s skipped (no changes)", name)
            return (name, None)
        logger.info("✅ %s → %s", name, metadata.key)
        return (name, metadata)
    except Exception as exc:
        logger.error("❌ %s failed: %s", name, exc)
        return (name, str(exc))


@flow(name="run_parser")
def run_parser(parser_name: str) -> dict:
    """Run a single custom parser by name."""
    preflight()
    run_id = init_run(domain="labels", technical_type="CUSTOM_PARSER")

    try:
        config = get_config()
        custom_parsers = config.get("custom_parsers", [])
        parser = next((p for p in custom_parsers if p["name"] == parser_name), None)

        if not parser:
            logger.error("Parser '%s' not found in config", parser_name)
            finalize_run(run_id=run_id, status="FAILED", number_files=0)
            return {"name": parser_name, "success": False, "error": "Not found"}

        known_hashes = get_latest_hashes()
        name, result = run_single_parser(parser, known_hashes)

        if isinstance(result, Exception) or (isinstance(result, str) and result):
            logger.error("❌ %s: %s", name, result)
            finalize_run(run_id=run_id, status="FAILED", number_files=0)
            return {"name": name, "success": False, "error": str(result)}

        if result is None:
            logger.info("⏭️ %s skipped (no changes)", name)
            finalize_run(run_id=run_id, status="SUCCESS", number_files=0)
            return {"name": name, "success": True, "result": None}

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
        raise SystemExit("Usage: python -m custom_parsers <parser_name>")
    run_parser(sys.argv[1])
