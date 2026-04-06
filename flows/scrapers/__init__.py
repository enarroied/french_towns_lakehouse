import asyncio
import sys

from flows.shared import get_config
from flows.shared import get_scrapers


def run_scraper(scraper_name: str) -> None:
    """Run a single scraper by name from config."""
    config = get_config()
    scrapers = get_scrapers()

    scraper = next((s for s in scrapers if s["name"] == scraper_name), None)
    if not scraper:
        raise SystemExit(f"No scraper named '{scraper_name}' found in config.")

    module = __import__(scraper["module"], fromlist=["run"])
    asyncio.run(module.run(config))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python -m flows.scrapers <scraper_name>")
    run_scraper(sys.argv[1])
