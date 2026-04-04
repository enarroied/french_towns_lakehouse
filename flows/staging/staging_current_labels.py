import asyncio
import logging
from importlib import import_module

from flows.scrapers import SCRAPER_MODULE_MAP
from flows.shared import get_config
from flows.shared import get_scrapers
from prefect import flow
from prefect import task


logger = logging.getLogger(__name__)


@task
def run_single_scraper(scraper_name: str) -> dict:
    """Run a single scraper and return a result dict for the summary."""
    module_path = SCRAPER_MODULE_MAP[scraper_name]
    module = import_module(module_path)
    config = get_config()
    try:
        result = asyncio.run(module.run(config))
        return {"name": scraper_name, "success": True, "result": result}
    except Exception as exc:
        logger.error("Scraper %s failed: %s", scraper_name, exc)
        return {"name": scraper_name, "success": False, "error": str(exc)}


def _print_success(succeeded, results, width=50):
    return f"""{"-" * width}
    {len(succeeded)}/{len(results)} scrapers succeeded.
    {"=" * width}
    """


def _print_summary_header(width=50):
    return (
        "\n"
        + f""" {"=" * width}
    SCRAPER RUN SUMMARY
    {"=" * width}

    """
    )


@flow(name="staging_current_labels")
def staging_current_labels() -> list[dict]:
    """Run all enabled scrapers concurrently and log a summary."""
    all_scrapers = get_scrapers()
    enabled = [s for s in all_scrapers if s.get("enabled", True)]
    disabled = [s for s in all_scrapers if not s.get("enabled", True)]

    for s in disabled:
        logger.info("Skipping %s (disabled)", s["name"])

    futures = [run_single_scraper.submit(s["name"]) for s in enabled]
    results = [f.result() for f in futures]

    succeeded = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    logger.info(_print_summary_header())
    for r in results:
        status = "OK" if r["success"] else "FAILED"
        result_info = f" → {r.get('result', '')}" if r["success"] else ""
        logger.info(f" {r['name']:<30} {status}{result_info}")
        if not r["success"]:
            logger.info(f"     - {(r.get('error') or 'unknown error')[:60]}")
    logger.info(_print_success(succeeded, results))

    if failed:
        logger.warning("%d scraper(s) failed", len(failed))

    if len(failed) == len(results):
        raise RuntimeError(f"All {len(results)} scrapers failed")

    return results


if __name__ == "__main__":
    staging_current_labels()
