import logging
import re

import aiohttp
from bs4 import BeautifulSoup
from flows.shared.minio import write_csv_to_staging

from scrapers.utils import get_scraper_config

logger = logging.getLogger(__name__)

MODULE = "scrapers.scrape_famille_plus"
FIELDNAMES = ["name", "department_code", "type"]
_DESTINATION_TYPES = {"mer", "montagne", "nature", "ville"}


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _parse_destination_type(article) -> str | None:
    """Return the destination type from an article's CSS classes, or None."""
    return next((cls for cls in article.get("class", []) if cls in _DESTINATION_TYPES), None)


def _parse_department_code(article) -> str | None:
    """Extract the department code (e.g. '09', '2A') from an article element."""
    dept_elem = article.find("p", class_="col-4") or article.find("p", class_="align-self-center")
    if not dept_elem:
        return None
    match = re.search(r"\((\d+|2A|2B)\)", dept_elem.get_text(strip=True))
    return match.group(1) if match else None


def parse_destinations(html: str) -> list[dict]:
    """Parse all destination entries from a Famille Plus HTML page."""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for article in soup.find_all("article", class_="node--type-destination"):
        dest_type = _parse_destination_type(article)
        h5 = article.find("h5")
        name = h5.find("a").get_text(strip=True) if h5 and h5.find("a") else None
        dept = _parse_department_code(article)

        if name and dept and dest_type:
            results.append({"name": name, "department_code": dept, "type": dest_type})

    return results


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------


async def fetch_page(session: aiohttp.ClientSession, url: str) -> str | None:
    """Fetch a single HTML page, returning None on any failure."""
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning("Unexpected status %d for %s", resp.status, url)
                return None
            return await resp.text()
    except Exception as exc:
        logger.error("Failed to fetch %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def run(config: dict) -> str:
    """Scrape Famille Plus destinations and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        html = await fetch_page(session, scraper.url)
        if not html:
            return scraper.name

        destinations = parse_destinations(html)
        if not destinations:
            logger.warning("%s: no destinations found", scraper.name)
            return scraper.name

        key = write_csv_to_staging(
            data=destinations,
            fieldnames=FIELDNAMES,
            filename=f"{scraper.name}.csv",
            subfolder=scraper.target_folder,
            metadata={"source_url": scraper.url},
            pipeline_name="staging_current_labels",
        )

    logger.info("%s: scraped %d destinations → %s", scraper.name, len(destinations), key)
    return key
