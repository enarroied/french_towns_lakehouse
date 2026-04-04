import asyncio
import sys
from importlib import import_module

from flows.shared import get_config
from prefect import flow


SCRAPER_MODULE_MAP = {
    "petites_cites": "scrapers.scrape_petites_cites",
    "villes_fleuries": "scrapers.scrape_villes_fleuries",
    "plus_beaux_villages": "scrapers.scrape_plus_beaux_villages",
    "villes_prudentes": "scrapers.scrape_villes_prudentes",
    "village_etape": "scrapers.scrape_village_etape",
    "famille_plus": "scrapers.scrape_famille_plus",
}


@flow(name="scraper")
def run_scraper(scraper_name: str) -> str:
    """Run a single scraper by name."""
    module_path = SCRAPER_MODULE_MAP[scraper_name]
    module = import_module(module_path)
    config = get_config()
    return asyncio.run(module.run(config))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        msg = "Usage: python -m flows.scrapers.scraper <scraper_name>"
        raise SystemExit(msg)
    run_scraper(sys.argv[1])
