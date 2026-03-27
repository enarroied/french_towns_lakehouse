"""
Async scraper for Famille Plus website
Extracts destination information from https://www.familleplus.fr/fr/le-label/carte
"""

import asyncio
import csv
import re
from pathlib import Path

import aiohttp
import yaml
from bs4 import BeautifulSoup


def load_config() -> dict:
    """Load configuration from YAML file."""
    with open("config.yaml") as f:
        return yaml.safe_load(f)


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

    # Find all destination articles
    destinations = []
    articles = soup.find_all("article", class_="node--type-destination")

    for article in articles:
        # Extract type from class (mer, montagne, nature, ville)
        classes = article.get("class", [])
        dest_type = None
        for cls in classes:
            if cls in ["mer", "montagne", "nature", "ville"]:
                dest_type = cls
                break

        # Extract town name from h5 > a
        h5 = article.find("h5")
        if h5:
            a = h5.find("a")
            town_name = a.get_text(strip=True) if a else None
        else:
            town_name = None

        # Extract department from the paragraph
        # Department is in format "Charente-Maritime (17)"
        dept = None
        # Check desktop version first
        dept_elem = article.find("p", class_="col-4")
        if not dept_elem:
            # Check mobile version
            dept_elem = article.find("p", class_="align-self-center")

        if dept_elem:
            dept_text = dept_elem.get_text(strip=True)
            # Extract department code from parentheses
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


async def run(config: dict) -> Path:
    """Main execution function."""
    # Find scraper config
    scraper_config = next(
        s for s in config["scrapers"] if s["module"] == "scrapers.scrape_famille_plus"
    )

    output_dir = Path(config["paths"]["scraper_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{scraper_config['name']}.csv"

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
            return output_path

        # Write to CSV
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["name", "department_code", "type"],
            )
            writer.writeheader()
            writer.writerows(destinations)

        print(f"Scraped {len(destinations)} destinations. Saved to {output_path}")

        # Show sample
        print("\nSample data:")
        for d in destinations[:5]:
            print(f"  - {d['name']} | {d['department_code']} | {d['type']}")

    return output_path


async def main():
    """Entry point."""
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
