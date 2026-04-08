import logging
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup
from flows_staging.scrapers.models import FileMetadata
from flows_staging.scrapers.utils import get_scraper_config
from flows_staging.shared.download import _write_csv_to_temp
from flows_staging.shared.download import calculate_md5
from flows_staging.shared.minio import STAGING_BUCKET
from flows_staging.shared.minio import get_minio_client


logger = logging.getLogger(__name__)

MODULE = "flows_staging.scrapers.scrape_plus_beaux_villages"
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


async def run(config: dict, known_hashes: dict | None = None) -> FileMetadata | None:
    """Scrape Les Plus Beaux Villages de France and upload to staging."""
    scraper = get_scraper_config(config, MODULE)
    known_hashes = known_hashes or {}
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        async with session.get(scraper.url) as resp:
            resp.raise_for_status()
            villages = parse_villages(await resp.text())

        if not villages:
            logger.warning("%s: no villages scraped", scraper.name)
            return None

        temp_dir = Path("/tmp/french_towns_downloads")
        temp_dir.mkdir(parents=True, exist_ok=True)
        csv_path = _write_csv_to_temp(
            data=villages,
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

        logger.info("%s: scraped %d villages → %s", scraper.name, len(villages), key)

        return FileMetadata(
            key=key,
            base_name=f"{scraper.name}.csv",
            filename_timestamp=csv_path.name,
            size_mb=size_mb,
            md5=md5,
        )
