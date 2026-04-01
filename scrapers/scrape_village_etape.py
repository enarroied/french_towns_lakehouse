import asyncio
import json
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


async def fetch_village_urls(  # noqa: PLR0912, PLR0915
    session: aiohttp.ClientSession, base_url: str
) -> list[str]:
    """Fetch all village detail page URLs from the main listing."""
    all_urls = []
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}{page}/"

        print(f"Fetching page {page}...")

        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"Page {page} returned {resp.status}, stopping.")
                    break
                html = await resp.text()
        except Exception as e:
            print(f"Error fetching page {page}: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")

        links = []
        for element in soup.find_all(attrs={"data-ha-element-link": True}):
            link_data = element.get("data-ha-element-link")
            if link_data:
                try:
                    link_info = json.loads(link_data)
                    if "url" in link_info:
                        links.append(link_info["url"])
                except (json.JSONDecodeError, KeyError):
                    continue

        if not links:
            loop_items = soup.find_all("div", class_="e-loop-item")
            for item in loop_items:
                link = item.find("a", href=True)
                if link and "/village-etape/" in link["href"]:
                    links.append(link["href"])

        if not links:
            if page > 1:
                break
            headings = soup.find_all("h2", class_="elementor-heading-title")
            for heading in headings:
                parent = heading.find_parent("a")
                if parent and parent.get("href"):
                    links.append(parent["href"])

        if not links:
            print(f"No villages found on page {page}, stopping.")
            break

        all_urls.extend(links)
        print(f"Found {len(links)} villages on page {page}")

        pagination = soup.find("nav", class_="elementor-pagination")
        if pagination:
            next_link = pagination.find("a", class_="next")
            if not next_link:
                break
        elif page >= 4:
            break

        page += 1
        await asyncio.sleep(1)

    seen = set()
    unique_urls = []
    for url in all_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    return unique_urls


async def scrape_village(  # noqa: PLR0912
    session: aiohttp.ClientSession, url: str, headers: dict
) -> dict | None:
    """Scrape detailed information from a village page."""
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                print(f"Error {resp.status} for {url}")
                return None
            html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")

        name = None
        h1 = soup.find("h1", class_="elementor-heading-title")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            name_elem = soup.find("h1")
            if name_elem:
                name = name_elem.get_text(strip=True)

        region = None
        road = None

        icon_list = soup.find("ul", class_="elementor-icon-list-items")
        if icon_list and "elementor-inline-items" in icon_list.get("class", []):
            items = icon_list.find_all("li", class_="elementor-icon-list-item")
            texts = []
            for item in items:
                text_span = item.find("span", class_="elementor-icon-list-text")
                if text_span:
                    texts.append(text_span.get_text(strip=True))

            if len(texts) >= 2:
                region = texts[0]
                road = texts[1]

        description = None
        desc_elem = soup.find("div", class_="elementor-widget-text-editor")
        if desc_elem:
            p = desc_elem.find("p")
            if p:
                description = p.get_text(strip=True)

        practical = {}
        info_sections = soup.find_all("div", class_="elementor-icon-list-items")
        for section in info_sections:
            items = section.find_all("li", class_="elementor-icon-list-item")
            for item in items:
                text = item.find("span", class_="elementor-icon-list-text")
                if text:
                    text_content = text.get_text(strip=True)
                    if "mairie" in text_content.lower():
                        practical["mairie"] = text_content
                    elif "tourisme" in text_content.lower():
                        practical["tourist_office"] = text_content
                    elif "@" in text_content:
                        practical["email"] = text_content

        if not name:
            print(f"Missing name on {url}")
            return None

        return {
            "name": name,
            "url": url,
            "region": region,
            "road": road,
            "description": description,
            "practical": json.dumps(practical, ensure_ascii=False)
            if practical
            else None,
        }

    except Exception as e:
        print(f"Exception scraping {url}: {e}")
        return None


async def worker(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    headers: dict,
) -> dict | None:
    """Worker with concurrency control."""
    async with semaphore:
        await asyncio.sleep(0.5)
        return await scrape_village(session, url, headers)


async def run(config: dict) -> str:
    """Main execution function."""
    scraper_config = next(
        s for s in config["scrapers"] if s["module"] == "scrapers.scrape_village_etape"
    )

    headers = {
        "User-Agent": scraper_config.get("user_agent", "VillageEtapeBot/1.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }
    concurrency = scraper_config.get("concurrency", 5)
    base_url = scraper_config.get(
        "base_url", "https://village-etape.fr/nos-village-etape/"
    )

    async with aiohttp.ClientSession(headers=headers) as session:
        urls = await fetch_village_urls(session, base_url)
        print(f"Found {len(urls)} village URLs.")

        if not urls:
            print("No village URLs found.")
            return scraper_config["name"]

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [worker(session, semaphore, url, headers) for url in urls]
        results = await asyncio.gather(*tasks)

        valid_results = [r for r in results if r is not None]

        if valid_results:
            key = write_csv_to_staging(
                data=valid_results,
                fieldnames=[
                    "name",
                    "url",
                    "region",
                    "road",
                    "description",
                    "practical",
                ],
                filename=f"{scraper_config['name']}.csv",
                subfolder="scrapers",
                metadata={"source_url": base_url},
                pipeline_name="staging_current_labels",
            )
            print(f"Scraped {len(valid_results)} villages. Uploaded to {key}")
        else:
            print("No valid villages scraped.")

    return key if valid_results else scraper_config["name"]


async def main():
    """Entry point."""
    config = load_config()
    await run(config)


if __name__ == "__main__":
    asyncio.run(main())
