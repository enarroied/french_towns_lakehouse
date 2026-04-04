import asyncio
import logging
import re
from pathlib import Path

import aiohttp
import yaml
from bs4 import BeautifulSoup
from flows.shared.minio import write_csv_to_staging
from scrapers.logging import get_scraper_logger


def load_config() -> dict:
    return yaml.safe_load(Path("config.yaml").open())


def extract_text(html: str) -> str:
    return BeautifulSoup(html, "html.parser").get_text().strip().lower()


def extract_flowers(html: str) -> int:
    img = BeautifulSoup(html, "html.parser").find("img")
    if img and img.get("src"):
        match = re.search(r"/(\d+)\.png$", img["src"])
        if match:
            return int(match.group(1))
    return 0


def parse_row(row: list) -> dict:
    return {
        "commune": extract_text(row[0]),
        "region": extract_text(row[1]),
        "departement": extract_text(row[2]),
        "nb_fleurs": extract_flowers(row[4]),
    }


async def init_session(session: aiohttp.ClientSession, referer: str) -> None:
    async with session.get(referer) as resp:
        resp.raise_for_status()


async def fetch_page(
    logger: logging.Logger,
    session: aiohttp.ClientSession,
    endpoint: str,
    payload: dict,
    referer: str,
    max_retries: int = 3,
    backoff: tuple[float, ...] = (3.0, 10.0, 30.0),
) -> dict:
    for attempt in range(max_retries + 1):
        try:
            async with session.post(endpoint, data=payload) as resp:
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except aiohttp.ClientResponseError as exc:
            if exc.status == 500 and attempt < max_retries:
                wait = backoff[attempt]
                await asyncio.sleep(wait)
                try:
                    await init_session(session, referer)
                except Exception as refresh_err:
                    logger.warning("Session refresh failed: %s", refresh_err)
            else:
                raise

    return {}


async def fetch_all_rows(
    logger: logging.Logger,
    session: aiohttp.ClientSession,
    endpoint: str,
    payload_base: dict,
    total: int,
    page_size: int,
    crawl_delay: float,
    referer: str,
) -> list:
    all_rows = []
    for start in range(0, total, page_size):
        end = min(start + page_size, total)
        logger.info("  Fetching rows %d-%d...", start, end)
        payload = {**payload_base, "start": str(start), "length": str(page_size)}
        page = await fetch_page(logger, session, endpoint, payload, referer)
        all_rows.extend(page["data"])
        await asyncio.sleep(crawl_delay)
    return all_rows


async def run(config: dict) -> str:
    logger = get_scraper_logger("scrape_villes_fleuries")
    scraper_config = next(
        s
        for s in config["scrapers"]
        if s["module"] == "scrapers.scrape_villes_fleuries"
    )

    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://villes-et-villages-fleuris.com",
        "referer": scraper_config["referer"],
        "user-agent": scraper_config.get(
            "user_agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        ),
        "x-requested-with": "XMLHttpRequest",
    }

    payload_base = {
        "draw": "1",
        **{
            f"columns[{i}][{k}]": v
            for i in range(5)
            for k, v in [
                ("data", str(i)),
                ("name", ""),
                ("searchable", "true"),
                ("orderable", "false"),
                ("search][value", ""),
                ("search][regex", "false"),
            ]
        },
        "search[value]": "",
        "search[regex]": "false",
        "action": "search",
        "filters": "id=&distinction=&region=&departement=",
    }

    page_size = scraper_config.get("page_size", 1000)
    crawl_delay = scraper_config.get("crawl_delay", 1)
    referer = scraper_config["referer"]

    async with aiohttp.ClientSession(headers=headers) as session:
        await init_session(session, referer)

        first_page = await fetch_page(
            logger,
            session,
            scraper_config["endpoint"],
            {**payload_base, "start": "0", "length": str(page_size)},
            referer,
        )
        total = int(first_page["recordsTotal"])
        logger.info("Total records: %d", total)

        all_raw = await fetch_all_rows(
            logger,
            session,
            scraper_config["endpoint"],
            payload_base,
            total,
            page_size,
            crawl_delay,
            referer,
        )

        parsed = [parse_row(row) for row in all_raw]

        key = write_csv_to_staging(
            data=parsed,
            fieldnames=["commune", "region", "departement", "nb_fleurs"],
            filename=f"{scraper_config['name']}.csv",
            subfolder=scraper_config.get("target_folder", "labels"),
            metadata={"source_url": referer},
            pipeline_name="staging_current_labels",
        )

        logger.info("Saved %d communes → %s", len(parsed), key)
    return key


async def main():
    logging.basicConfig(level=logging.INFO)
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
