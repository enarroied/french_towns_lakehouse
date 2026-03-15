import asyncio
import csv
from pathlib import Path

import aiohttp
import yaml
from bs4 import BeautifulSoup


def load_config() -> dict:
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def parse_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="ea-advanced-data-table")
    if not table:
        print("Could not find the data table!")
        return []

    results = []
    rows = table.find("tbody").find_all("tr")
    print(f"Found {len(rows)} rows in the table")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 5:
            city = cells[1].get_text(strip=True).lower()
            department = cells[4].get_text(strip=True)
            department = (
                department.strip().zfill(2) if department.isdigit() else department
            )

            if city and department:
                results.append({"city": city, "department": department})

    return results


async def run(config: dict) -> Path:
    scraper_config = next(
        s
        for s in config["scrapers"]
        if s["module"] == "scrapers.scrape_villes_prudentes"
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

        villes = parse_table(html)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["city", "department"])
            writer.writeheader()
            writer.writerows(villes)

        print(f"Scraped {len(villes)} communes. Saved to {output_path}")
    return output_path


async def main():
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
