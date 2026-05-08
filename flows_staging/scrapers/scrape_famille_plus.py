import logging
import re
import tempfile
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup
from flows_staging.scrapers.utils import get_scraper_config
from flows_staging.shared.config import get_config
from flows_staging.shared.download import write_csv_for_staging
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import StageConfig
from flows_staging.shared.staging_base import _process_single_file


logger = logging.getLogger(__name__)

MODULE = "flows_staging.scrapers.scrape_famille_plus"
FIELDNAMES = ["name", "department_code", "type"]
EXTENSION = ".csv"
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
        if not h5:
            continue
        a = h5.find("a")
        if not a:
            continue
        name = a.get_text(strip=True)
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


async def run(config: dict, run_id: str) -> bool:
    """Scrape Famille Plus destinations and stage via the shared pipeline.

    Scrapes the page, writes output to a temporary CSV, then hands off to
    `_process_single_file` which handles MD5 comparison, archiving the old
    version, uploading, and writing metadata — exactly like a downloader.

    Args:
        config: Full config dict (from config.yaml scrapers section).
        run_id: Unique flow run identifier.

    Returns:
        True if a file was staged, False if skipped or failed.
    """
    scraper = get_scraper_config(config, MODULE)
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        html = await fetch_page(session, scraper.url)
        if not html:
            logger.error("%s: failed to fetch page", scraper.name)
            return False

        destinations = parse_destinations(html)
        if not destinations:
            logger.warning("%s: no destinations found", scraper.name)
            return False

        logger.info("%s: scraped %d destinations", scraper.name, len(destinations))

    all_config = get_config()
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]
    minio_client = get_minio_client()

    stage_config = StageConfig(
        name=scraper.name,
        url=scraper.url,
        target_folder=scraper.target_folder,
        run_id=run_id,
        staging_bucket=staging_bucket,
        evidence_bucket=evidence_bucket,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        write_csv_for_staging(destinations, FIELDNAMES, scraper.name, temp_path)
        return _process_single_file(
            stage_config, minio_client, scraper.name, EXTENSION, temp_path
        )
