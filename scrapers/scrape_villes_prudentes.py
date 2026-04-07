import logging
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
            base_name=f"{scraper.name}.csv",
            filename_timestamp=csv_path.name,
            size_mb=size_mb,
            md5=md5,
        )
