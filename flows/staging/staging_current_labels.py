import asyncio
import logging
from importlib import import_module
from pathlib import Path

from flows.shared import get_config
from flows.shared import get_scrapers
from prefect import flow
from prefect import task


logger = logging.getLogger(__name__)


SCRAPER_MODULE_MAP = {
    "petites_cites": "scrapers.scrape_petites_cites",
    "villes_fleuries": "scrapers.scrape_villes_fleuries",
    "plus_beaux_villages": "scrapers.scrape_plus_beaux_villages",
    "villes_prudentes": "scrapers.scrape_villes_prudentes",
    "village_etape": "scrapers.scrape_village_etape",
    "famille_plus": "scrapers.scrape_famille_plus",
}


@task
def setup_logs() -> None:
    Path("logs").mkdir(exist_ok=True, parents=True)


@task
def run_single_scraper(scraper_name: str) -> dict:
    module_path = SCRAPER_MODULE_MAP[scraper_name]
    module = import_module(module_path)
    config = get_config()
    try:
        result = asyncio.run(module.run(config))
        return {"name": scraper_name, "success": True, "result": result}
    except Exception as exc:
        logger.error("Scraper %s failed: %s", scraper_name, exc)
        return {"name": scraper_name, "success": False, "error": str(exc)}


@flow(name="staging_current_labels")
def staging_current_labels() -> list[dict]:
    setup_logs()

    enabled_scrapers = [s for s in get_scrapers() if s.get("enabled", True)]
    disabled_scrapers = [s for s in get_scrapers() if not s.get("enabled", True)]

    for s in disabled_scrapers:
        logger.info("Skipping %s (disabled)", s["name"])

    results = []
    for scraper in enabled_scrapers:
        result = run_single_scraper(scraper["name"])
        results.append(result)

    succeeded = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    width = 50
    print("\n" + "=" * width)
    print(" SCRAPER RUN SUMMARY")
    print("=" * width)
    for r in results:
        status = "OK" if r["success"] else "FAILED"
        result_info = f" → {r.get('result', '')}" if r["success"] else ""
        print(f" {r['name']:<30} {status}{result_info}")
        if not r["success"]:
            short = (r.get("error") or "unknown error")[:60]
            print(f"     - {short}")
    print("-" * width)
    print(f" {len(succeeded)}/{len(results)} scrapers succeeded.")
    print("=" * width + "\n")

    if failed:
        logger.warning("%d scrapers failed", len(failed))

    if len(failed) == len(results):
        raise RuntimeError(f"All {len(results)} scrapers failed")

    return results


if __name__ == "__main__":
    staging_current_labels()
