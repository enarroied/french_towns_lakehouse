import logging

import aiohttp
from bs4 import BeautifulSoup
from flows_staging.scrapers.models import FileMetadata
from flows_staging.scrapers.utils import get_scraper_config
from flows_staging.shared.download import upload_scraper_output


logger = logging.getLogger(__name__)

MODULE = "flows_staging.scrapers.scrape_plus_beaux_villages"
FIELDNAMES = ["city", "department"]


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def parse_villages(html: str) -> list[dict]:
    """Parse village entries from the Les Plus Beaux Villages listing page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for div in soup.find_all("div", class_="result"):
        name_div = div.find("div", class_="name")
        locality_div = div.find("div", class_="locality")

        if not name_div or not locality_div:
            continue

        city = name_div.get_text(strip=True).lower()
        department = locality_div.get_text(strip=True).lower().split()[-1]

        if city and department:
            results.append({"city": city, "department": department})

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def run(config: dict, known_hashes: dict | None = None) -> FileMetadata | None:
    """Scrape Les Plus Beaux Villages de France and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    known_hashes = known_hashes or {}
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        async with session.get(scraper.url) as resp:
            resp.raise_for_status()
            villages = parse_villages(await resp.text())

        if not villages:
            logger.warning("%s: no villages scraped", scraper.name)
            return None

        logger.info("%s: scraped %d villages", scraper.name, len(villages))

        return upload_scraper_output(
            data=villages,
            fieldnames=FIELDNAMES,
            scraper_name=scraper.name,
            target_folder=scraper.target_folder,
            known_hashes=known_hashes,
        )
