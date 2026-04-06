import logging

import aiohttp
from bs4 import BeautifulSoup
from flows.shared.minio import write_csv_to_staging
from scrapers.utils import get_scraper_config


logger = logging.getLogger(__name__)

MODULE = "scrapers.scrape_plus_beaux_villages"
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


async def run(config: dict) -> str:
    """Scrape Les Plus Beaux Villages de France and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        async with session.get(scraper.url) as resp:
            resp.raise_for_status()
            villages = parse_villages(await resp.text())

        key = write_csv_to_staging(
            data=villages,
            fieldnames=FIELDNAMES,
            filename=f"{scraper.name}.csv",
            subfolder=scraper.target_folder,
            metadata={"source_url": scraper.url},
            pipeline_name="staging_current_labels",
        )

    logger.info("%s: scraped %d villages → %s", scraper.name, len(villages), key)
    return key
