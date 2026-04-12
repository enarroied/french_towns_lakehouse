import logging

import aiohttp
from bs4 import BeautifulSoup
from flows_staging.scrapers.models import FileMetadata
from flows_staging.scrapers.utils import get_scraper_config
from flows_staging.shared.download import upload_scraper_output


logger = logging.getLogger(__name__)

MODULE = "flows_staging.scrapers.scrape_villes_prudentes"
FIELDNAMES = ["city", "department"]


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def parse_table(html: str) -> list[dict]:
    """Parse the labeled communes table from a Ville Prudente page."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="ea-advanced-data-table")
    if not table:
        return []

    results = []
    tbody = table.find("tbody")
    if not tbody:
        return []
    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        city = cells[1].get_text(strip=True).lower()
        dept_raw = cells[4].get_text(strip=True)
        department = dept_raw.zfill(2) if dept_raw.isdigit() else dept_raw

        if city and department:
            results.append({"city": city, "department": department})

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def run(config: dict, known_hashes: dict | None = None) -> FileMetadata | None:
    """Scrape Villes Prudentes labeled communes and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    known_hashes = known_hashes or {}
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        async with session.get(scraper.url) as resp:
            resp.raise_for_status()
            communes = parse_table(await resp.text())

        if not communes:
            logger.warning("%s: no communes scraped", scraper.name)
            return None

        logger.info("%s: scraped %d communes", scraper.name, len(communes))

        return upload_scraper_output(
            data=communes,
            fieldnames=FIELDNAMES,
            scraper_name=scraper.name,
            target_folder=scraper.target_folder,
            known_hashes=known_hashes,
        )
