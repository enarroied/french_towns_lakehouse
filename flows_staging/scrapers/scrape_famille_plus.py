import logging
import re
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from flows_staging.shared.download import _write_csv_to_temp
from flows_staging.shared.download import calculate_md5
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.minio import STAGING_BUCKET
from flows_staging.scrapers.models import FileMetadata
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
    return next(
        (cls for cls in article.get("class", []) if cls in _DESTINATION_TYPES), None
    )


def _parse_department_code(article) -> str | None:
    """Extract the department code (e.g. '09', '2A') from an article element."""
    dept_elem = article.find("p", class_="col-4") or article.find(
        "p", class_="align-self-center"
    )
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


async def run(config: dict, known_hashes: dict | None = None) -> FileMetadata | None:
    """Scrape Famille Plus destinations and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    known_hashes = known_hashes or {}
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        html = await fetch_page(session, scraper.url)
        if not html:
            logger.error("%s: failed to fetch page", scraper.name)
            return None

        destinations = parse_destinations(html)
        if not destinations:
            logger.warning("%s: no destinations found", scraper.name)
            return None

        temp_dir = Path("/tmp/french_towns_downloads")
        temp_dir.mkdir(parents=True, exist_ok=True)
        csv_path = _write_csv_to_temp(
            data=destinations,
            fieldnames=FIELDNAMES,
            base_name=scraper.name,
            temp_dir=temp_dir,
        )

        md5 = calculate_md5(csv_path)
        size_mb = round(csv_path.stat().st_size / 1024**2, 2)
        base_name = f"{scraper.name}.csv"

        if base_name in known_hashes and known_hashes[base_name].get("md5") == md5:
            logger.info("⏭️ Skipping %s — hash unchanged", scraper.name)
            csv_path.unlink()
            return None

        minio_client = get_minio_client()
        key = f"{scraper.target_folder}/{csv_path.name}"

        minio_client.upload_file(
            Filename=str(csv_path),
            Bucket=STAGING_BUCKET,
            Key=key,
        )
        csv_path.unlink()

        logger.info(
            "%s: scraped %d destinations → %s", scraper.name, len(destinations), key
        )

        return FileMetadata(
            key=key,
            base_name=f"{scraper.name}.csv",
            filename_timestamp=csv_path.name,
            size_mb=size_mb,
            md5=md5,
        )
