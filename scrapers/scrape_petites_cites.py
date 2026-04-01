import asyncio
import sys
from pathlib import Path

import aiohttp
import yaml
from bs4 import BeautifulSoup


sys.path.insert(0, str(Path(__file__).parent.parent))

from flows.shared.minio import write_csv_to_staging


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
                print(f"Error {resp.status} for {url}")
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
        print(f"Missing data on {url}")
        return None
    except Exception as e:
        print(f"Exception scraping {url}: {e}")
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
    scraper_config = next(
        s for s in config["scrapers"] if s["module"] == "scrapers.scrape_petites_cites"
    )

    headers = {"User-Agent": scraper_config.get("user_agent", "FrenchTownsBot/1.0")}
    concurrency = scraper_config.get("concurrency", 5)

    async with aiohttp.ClientSession(headers=headers) as session:
        urls = await fetch_sitemap(session, scraper_config["sitemap_url"])
        print(f"Found {len(urls)} city URLs.")

        if not urls:
            print("No city URLs found.")
            return scraper_config["name"]

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [worker(session, semaphore, url, headers) for url in urls]
        results = await asyncio.gather(*tasks)

        valid_results = [r for r in results if r is not None]

        key = write_csv_to_staging(
            data=valid_results,
            fieldnames=["city", "department"],
            filename=f"{scraper_config['name']}.csv",
            subfolder="scrapers",
            metadata={"source_url": scraper_config["sitemap_url"]},
            pipeline_name="staging_current_labels",
        )

        print(f"Scraped {len(valid_results)} cities. Uploaded to {key}")
    return key


async def main():
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
