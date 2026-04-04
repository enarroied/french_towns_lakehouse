import asyncio
from pathlib import Path

import aiohttp
import yaml
from bs4 import BeautifulSoup
from flows.shared.minio import write_csv_to_staging
from scrapers.logging import get_scraper_logger


def load_config() -> dict:
    return yaml.safe_load(Path("config.yaml").open())


def parse_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="ea-advanced-data-table")
    if not table:
        return []

    results = []
    rows = table.find("tbody").find_all("tr")
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


async def run(config: dict) -> str:
    logger = get_scraper_logger("scrape_villes_prudentes")
    scraper_config = next(
        s
        for s in config["scrapers"]
        if s["module"] == "scrapers.scrape_villes_prudentes"
    )

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
        logger.info("Found %d communes.", len(villes))

        key = write_csv_to_staging(
            data=villes,
            fieldnames=["city", "department"],
            filename=f"{scraper_config['name']}.csv",
            subfolder=scraper_config.get("target_folder", "labels"),
            metadata={"source_url": scraper_config["url"]},
            pipeline_name="staging_current_labels",
        )

        logger.info("Scraped %d communes. Uploaded to %s", len(villes), key)
    return key


async def main():
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
