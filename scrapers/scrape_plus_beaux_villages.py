import asyncio
import csv
from pathlib import Path

import aiohttp
import yaml
from bs4 import BeautifulSoup


def load_config() -> dict:
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def parse_villages(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    result_divs = soup.find_all("div", class_="result")

    for div in result_divs:
        name_div = div.find("div", class_="name")
        if not name_div:
            continue
        city = name_div.get_text(strip=True).lower()

        locality_div = div.find("div", class_="locality")
        if not locality_div:
            continue
        department = locality_div.get_text(strip=True).lower().split()[-1]

        if city and department:
            results.append({"city": city, "department": department})

    return results


async def run(config: dict) -> Path:
    scraper_config = next(
        s
        for s in config["scrapers"]
        if s["module"] == "scrapers.scrape_plus_beaux_villages"
    )
    output_dir = Path(config["paths"]["scraper_dir"])
    output_path = output_dir / f"{scraper_config['name']}.csv"

    headers = {
        "User-Agent": scraper_config.get("user_agent", "FrenchTownsBot/1.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3",
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(scraper_config["url"]) as resp:
            resp.raise_for_status()
            html = await resp.text()

        villages = parse_villages(html)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["city", "department"])
            writer.writeheader()
            writer.writerows(villages)

        print(f"Scraped {len(villages)} villages. Saved to {output_path}")
    return output_path


async def main():
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
