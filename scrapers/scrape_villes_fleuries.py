import asyncio
import logging
import re
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from flows.shared.download import _write_csv_to_temp
from flows.shared.download import calculate_md5
from flows.shared.minio import get_minio_client
from flows.shared.minio import STAGING_BUCKET
from scrapers.models import FileMetadata
from scrapers.utils import get_scraper_config


logger = logging.getLogger(__name__)

MODULE = "scrapers.scrape_villes_fleuries"
FIELDNAMES = ["commune", "region", "departement", "nb_fleurs"]


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _extract_text(html: str) -> str:
    """Strip HTML tags and return normalised lowercase text."""
    return BeautifulSoup(html, "html.parser").get_text().strip().lower()


def _extract_flower_count(html: str) -> int:
    """Extract the flower count from an image src path (e.g. ``.../3.png`` → 3)."""
    img = BeautifulSoup(html, "html.parser").find("img")
    if img and img.get("src"):
        match = re.search(r"/(\d+)\.png$", img["src"])
        if match:
            return int(match.group(1))
    return 0


def parse_row(row: list) -> dict:
    """Map a single raw API data row to a commune record."""
    return {
        "commune": _extract_text(row[0]),
        "region": _extract_text(row[1]),
        "departement": _extract_text(row[2]),
        "nb_fleurs": _extract_flower_count(row[4]),
    }


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------


def build_xhr_headers(user_agent: str, referrer: str) -> dict:
    """Build the XHR headers required by the Villes Fleuries POST endpoint."""
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://villes-et-villages-fleuris.com",
        "Referer": referrer,
        "User-Agent": user_agent,
        "X-Requested-With": "XMLHttpRequest",
    }


def build_search_payload(start: int, length: int) -> dict:
    """Build the POST body for a paginated search request."""
    columns = {
        f"columns[{i}][{key}]": value
        for i in range(5)
        for key, value in [
            ("data", str(i)),
            ("name", ""),
            ("searchable", "true"),
            ("orderable", "false"),
            ("search][value", ""),
            ("search][regex", "false"),
        ]
    }
    return {
        "draw": "1",
        **columns,
        "search[value]": "",
        "search[regex]": "false",
        "action": "search",
        "filters": "id=&distinction=&region=&departement=",
        "start": str(start),
        "length": str(length),
    }


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------


async def _refresh_session(session: aiohttp.ClientSession, referrer: str) -> None:
    """Re-fetch the listing page to refresh the session cookie."""
    async with session.get(referrer) as resp:
        resp.raise_for_status()


async def fetch_data_page(
    session: aiohttp.ClientSession,
    endpoint: str,
    payload: dict,
    referrer: str,
    max_retries: int = 3,
    backoff: tuple[float, ...] = (3.0, 10.0, 30.0),
) -> dict:
    """POST to the data endpoint, retrying with session refresh on HTTP 500."""
    for attempt in range(max_retries + 1):
        try:
            async with session.post(endpoint, data=payload) as resp:
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except aiohttp.ClientResponseError as exc:
            if exc.status == 500 and attempt < max_retries:
                await asyncio.sleep(backoff[attempt])
                try:
                    await _refresh_session(session, referrer)
                except Exception as err:
                    logger.warning("Session refresh failed: %s", err)
            else:
                raise
    return {}


async def fetch_all_rows(
    session: aiohttp.ClientSession,
    endpoint: str,
    referrer: str,
    total: int,
    page_size: int,
    crawl_delay: float,
) -> list:
    """Paginate through all records, respecting the crawl delay between requests."""
    all_rows = []
    for start in range(0, total, page_size):
        payload = build_search_payload(start, min(page_size, total - start))
        page = await fetch_data_page(session, endpoint, payload, referrer)
        all_rows.extend(page.get("data", []))
        await asyncio.sleep(crawl_delay)
    return all_rows


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def run(config: dict, known_hashes: dict | None = None) -> FileMetadata | None:
    """Scrape Villes et Villages Fleuris and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    endpoint: str = scraper.extra["endpoint"]
    page_size: int = scraper.extra.get("page_size", 1000)
    crawl_delay: float = scraper.extra.get("crawl_delay", 1)
    known_hashes = known_hashes or {}

    headers = build_xhr_headers(scraper.user_agent, scraper.url)
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=headers) as session:
        await _refresh_session(session, scraper.url)

        first_page = await fetch_data_page(
            session, endpoint, build_search_payload(0, page_size), scraper.url
        )
        total = int(first_page["recordsTotal"])
        logger.info("%s: %d total records", scraper.name, total)

        all_rows = await fetch_all_rows(
            session, endpoint, scraper.url, total, page_size, crawl_delay
        )
        communes = [parse_row(row) for row in all_rows]

        if not communes:
            logger.warning("%s: no data scraped", scraper.name)
            return None

        temp_dir = Path("/tmp/french_towns_downloads")
        temp_dir.mkdir(parents=True, exist_ok=True)
        csv_path = _write_csv_to_temp(
            data=communes,
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

        logger.info("%s: scraped %d communes → %s", scraper.name, len(communes), key)

        return FileMetadata(
            key=key,
            base_name=base_name,
            filename_timestamp=csv_path.name,
            size_mb=size_mb,
            md5=md5,
        )
