import logging

import aiohttp
from bs4 import BeautifulSoup
from flows_staging.scrapers.utils import get_scraper_config
from flows_staging.scrapers.utils import stage_scraper_output


logger = logging.getLogger(__name__)

MODULE = "flows_staging.scrapers.scrape_villes_prudentes"
FIELDNAMES = ["city", "department"]
EXTENSION = ".csv"


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


async def run(config: dict, run_id: str) -> bool:
    """Scrape Villes Prudentes labeled communes and stage via the shared pipeline.

    Scrapes the listing page, writes output to a temporary CSV, then hands off
    to `_process_single_file` which handles MD5 comparison, archiving the old
    version, uploading, and writing metadata.

    Args:
        config: Full config dict (from config.yaml scrapers section).
        run_id: Unique flow run identifier.

    Returns:
        True if a file was staged, False if skipped or failed.
    """
    scraper = get_scraper_config(config, MODULE)
    logger.info("Starting %s", scraper.name)

    async with (
        aiohttp.ClientSession(headers=scraper.headers) as session,
        session.get(scraper.url) as resp,
    ):
        resp.raise_for_status()
        communes = parse_table(await resp.text())

    if not communes:
        logger.warning("%s: no communes scraped", scraper.name)
        return False

    logger.info("%s: scraped %d communes", scraper.name, len(communes))

    return stage_scraper_output(scraper, run_id, communes, FIELDNAMES, EXTENSION)
