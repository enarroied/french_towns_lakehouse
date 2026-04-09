import asyncio
import logging

import aiohttp
from bs4 import BeautifulSoup
from flows_staging.scrapers.models import FileMetadata
from flows_staging.scrapers.utils import get_scraper_config
from flows_staging.shared.download import upload_scraper_output


logger = logging.getLogger(__name__)

MODULE = "flows_staging.scrapers.scrape_petites_cites"
FIELDNAMES = ["city", "department"]


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def parse_sitemap(xml: str) -> list[str]:
    """Extract city detail page URLs from the sitemap XML."""
    soup = BeautifulSoup(xml, "xml")
    return [loc.text for loc in soup.find_all("loc") if "/cites/" in loc.text]


def parse_city_page(html: str) -> dict | None:
    """Parse city name and department from a Petites Cités de Caractère detail page."""
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1", class_="cover-title")
    city = h1.get_text(strip=True).lower() if h1 else None

    loc_div = soup.find("div", class_="location")
    if loc_div:
        loc_text = loc_div.get_text(strip=True)
        department = (
            loc_text.split(",")[1].strip().lower()
            if "," in loc_text
            else loc_text.strip().lower()
        )
    else:
        department = None

    return {"city": city, "department": department} if city and department else None


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------


async def fetch_sitemap(session: aiohttp.ClientSession, url: str) -> list[str]:
    """Fetch and parse the sitemap to collect all city detail URLs."""
    async with session.get(url) as resp:
        resp.raise_for_status()
        return parse_sitemap(await resp.text())


async def fetch_city(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    headers: dict,
) -> dict | None:
    """Fetch and parse a single city detail page, returning None on any failure."""
    async with semaphore:
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    return None
                return parse_city_page(await resp.text())
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def run(config: dict, known_hashes: dict | None = None) -> FileMetadata | None:
    """Scrape Petites Cités de Caractère and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    known_hashes = known_hashes or {}
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        urls = await fetch_sitemap(session, scraper.url)
        logger.info("%s: found %d city URLs", scraper.name, len(urls))

        if not urls:
            logger.warning("%s: no URLs found", scraper.name)
            return None

        semaphore = asyncio.Semaphore(scraper.concurrency)
        results = await asyncio.gather(
            *[fetch_city(session, semaphore, url, scraper.headers) for url in urls]
        )

        cities = [r for r in results if r is not None]

        if not cities:
            logger.warning("%s: no cities scraped", scraper.name)
            return None

        logger.info("%s: scraped %d cities", scraper.name, len(cities))

        return upload_scraper_output(
            data=cities,
            fieldnames=FIELDNAMES,
            scraper_name=scraper.name,
            target_folder=scraper.target_folder,
            known_hashes=known_hashes,
        )
