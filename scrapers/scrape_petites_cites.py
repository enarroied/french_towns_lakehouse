import asyncio
from pathlib import Path

import aiohttp
import yaml
from bs4 import BeautifulSoup
from flows.shared.minio import write_csv_to_staging
from scrapers.logging import get_scraper_logger


def load_config() -> dict:
    return yaml.safe_load(Path("config.yaml").open())


async def fetch_sitemap(session: aiohttp.ClientSession, url: str) -> list[str]:
    async with session.get(url) as resp:
        resp.raise_for_status()
        xml = await resp.text()
    soup = BeautifulSoup(xml, "xml")
    return [loc.text for loc in soup.find_all("loc") if "/cites/" in loc.text]


async def scrape_city(
    session: aiohttp.ClientSession, url: str, headers: dict
) -> dict | None:
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")

        h1 = soup.find("h1", class_="cover-title")
        city = h1.get_text(strip=True).lower() if h1 else None

        loc_div = soup.find("div", class_="location")
        department = None
        if loc_div:
            loc_text = loc_div.get_text(strip=True)
            department = (
                loc_text.split(",")[1].strip().lower()
                if "," in loc_text
                else loc_text.strip().lower()
            )

        if city and department:
            return {"city": city, "department": department}
        return None
    except Exception:
        return None


async def worker(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    headers: dict,
):
    async with semaphore:
        return await scrape_city(session, url, headers)


async def run(config: dict) -> str:
    logger = get_scraper_logger("scrape_petites_cites")
    scraper_config = next(
        s for s in config["scrapers"] if s["module"] == "scrapers.scrape_petites_cites"
    )

    headers = {"User-Agent": scraper_config.get("user_agent", "FrenchTownsBot/1.0")}
    concurrency = scraper_config.get("concurrency", 5)

    async with aiohttp.ClientSession(headers=headers) as session:
        urls = await fetch_sitemap(session, scraper_config["sitemap_url"])
        logger.info("Found %d city URLs.", len(urls))

        if not urls:
            return scraper_config["name"]

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [worker(session, semaphore, url, headers) for url in urls]
        results = await asyncio.gather(*tasks)

        valid_results = [r for r in results if r is not None]

        key = write_csv_to_staging(
            data=valid_results,
            fieldnames=["city", "department"],
            filename=f"{scraper_config['name']}.csv",
            subfolder=scraper_config.get("target_folder", "labels"),
            metadata={"source_url": scraper_config["sitemap_url"]},
            pipeline_name="staging_current_labels",
        )

        logger.info("Scraped %d cities. Uploaded to %s", len(valid_results), key)
    return key


async def main():
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
