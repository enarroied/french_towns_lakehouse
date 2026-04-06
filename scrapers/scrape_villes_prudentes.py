import logging

import aiohttp
from bs4 import BeautifulSoup
from flows.shared.minio import write_csv_to_staging
from scrapers.utils import get_scraper_config


logger = logging.getLogger(__name__)

MODULE = "scrapers.scrape_villes_prudentes"
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
    for row in table.find("tbody").find_all("tr"):
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


async def run(config: dict) -> str:
    """Scrape Villes Prudentes labeled communes and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        async with session.get(scraper.url) as resp:
            resp.raise_for_status()
            communes = parse_table(await resp.text())

        key = write_csv_to_staging(
            data=communes,
            fieldnames=FIELDNAMES,
            filename=f"{scraper.name}.csv",
            subfolder=scraper.target_folder,
            metadata={"source_url": scraper.url},
            pipeline_name="staging_current_labels",
        )

    logger.info("%s: scraped %d communes → %s", scraper.name, len(communes), key)
    return key
