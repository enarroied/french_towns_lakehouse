"""
Async scraper for Famille Plus website
Extracts destination information from https://www.familleplus.fr/fr/le-label/carte
"""

import asyncio
import re
import sys
from pathlib import Path

import aiohttp
import yaml
from bs4 import BeautifulSoup


sys.path.insert(0, str(Path(__file__).parent.parent))

from flows.shared.minio import write_csv_to_staging


def load_config() -> dict:
    """Load configuration from YAML file."""
    return yaml.safe_load(Path("config.yaml").open())


async def fetch_destinations(session: aiohttp.ClientSession, url: str) -> list[dict]:
    """Fetch all destination data from the map page."""
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                print(f"Error fetching page: {resp.status}")
                return []
            html = await resp.text()
    except Exception as e:
        print(f"Exception fetching page: {e}")
        return []

    soup = BeautifulSoup(html, "html.parser")

    destinations = []
    articles = soup.find_all("article", class_="node--type-destination")

    for article in articles:
        classes = article.get("class", [])
        dest_type = None
        for cls in classes:
            if cls in ["mer", "montagne", "nature", "ville"]:
                dest_type = cls
                break

        h5 = article.find("h5")
        if h5:
            a = h5.find("a")
            town_name = a.get_text(strip=True) if a else None
        else:
            town_name = None

        dept = None
        dept_elem = article.find("p", class_="col-4")
        if not dept_elem:
            dept_elem = article.find("p", class_="align-self-center")

        if dept_elem:
            dept_text = dept_elem.get_text(strip=True)
            match = re.search(r"\((\d+|[2A|2B])\)", dept_text)
            if match:
                dept = match.group(1)

        if town_name and dept and dest_type:
            destinations.append(
                {
                    "name": town_name,
                    "department_code": dept,
                    "type": dest_type,
                }
            )

    return destinations


async def run(config: dict) -> str:
    """Main execution function."""
    scraper_config = next(
        s for s in config["scrapers"] if s["module"] == "scrapers.scrape_famille_plus"
    )

    headers = {
        "User-Agent": scraper_config.get("user_agent", "FrenchTownsBot/1.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }
    url = scraper_config.get("url", "https://www.familleplus.fr/fr/le-label/carte")

    async with aiohttp.ClientSession(headers=headers) as session:
        destinations = await fetch_destinations(session, url)
        print(f"Found {len(destinations)} destinations.")

        if not destinations:
            print("No destinations found.")
            return scraper_config["name"]

        key = write_csv_to_staging(
            data=destinations,
            fieldnames=["name", "department_code", "type"],
            filename=f"{scraper_config['name']}.csv",
            subfolder="scrapers",
            metadata={"source_url": url},
            pipeline_name="staging_current_labels",
        )

        print(f"Scraped {len(destinations)} destinations. Uploaded to {key}")

        print("\nSample data:")
        for d in destinations[:5]:
            print(f"  - {d['name']} | {d['department_code']} | {d['type']}")

    return key


async def main():
    """Entry point."""
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
