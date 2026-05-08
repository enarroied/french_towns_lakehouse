import asyncio
import logging
from importlib import import_module

from flows_staging.shared.audit import RUN_STATUS_FAILED
from flows_staging.shared.audit import RUN_STATUS_SUCCESS
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from flows_staging.shared.config import get_config
from flows_staging.shared.config import get_scrapers
from prefect import flow


logger = logging.getLogger(__name__)


@flow(name="staging_current_labels")
def staging_current_labels() -> None:
    """Run all enabled scrapers and log results to audit DB."""
    asyncio.run(_staging_current_labels_impl())


async def _staging_current_labels_impl() -> None:
    preflight()
    run_id = init_run(domain="labels", technical_type="SCRAPER")

    try:
        config = get_config()
        all_scrapers = get_scrapers()
        enabled = [s for s in all_scrapers if s.get("enabled", True)]
        disabled = [s for s in all_scrapers if not s.get("enabled", True)]

        for s in disabled:
            logger.info("Skipping %s (disabled)", s["name"])

        if not enabled:
            logger.warning("No scrapers enabled")
            finalize_run(run_id=run_id, status=RUN_STATUS_SUCCESS, number_files=0)
            return

        number_files = 0
        for scraper in enabled:
            module = import_module(scraper["module"])
            if await module.run(config, run_id):
                number_files += 1

        finalize_run(
            run_id=run_id, status=RUN_STATUS_SUCCESS, number_files=number_files
        )
    except Exception as exc:
        logger.exception("Flow failed: %s", exc)
        finalize_run(run_id=run_id, status=RUN_STATUS_FAILED, number_files=0)
        raise


if __name__ == "__main__":
    staging_current_labels()
