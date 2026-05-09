import asyncio
import json
import logging

import aiohttp
from bs4 import BeautifulSoup
from flows_staging.scrapers.utils import get_scraper_config
from flows_staging.scrapers.utils import stage_scraper_output


logger = logging.getLogger(__name__)

MODULE = "flows_staging.scrapers.scrape_village_etape"
FIELDNAMES = ["name", "url", "region", "road", "description", "practical"]
EXTENSION = ".csv"


# ---------------------------------------------------------------------------
# Parsers — listing pages
# ---------------------------------------------------------------------------


def _urls_from_json_attributes(soup: BeautifulSoup) -> list[str]:
    """Strategy 1: extract URLs from ``data-ha-element-link`` JSON attributes."""
    links = []
    for element in soup.find_all(attrs={"data-ha-element-link": True}):
        try:
            raw = element.get("data-ha-element-link")
            if not isinstance(raw, str):
                continue
            info = json.loads(raw)
            if "url" in info:
                links.append(info["url"])
        except (json.JSONDecodeError, KeyError):
            continue
    return links


def _urls_from_loop_items(soup: BeautifulSoup) -> list[str]:
    """Strategy 2: extract URLs from ``e-loop-item`` anchor tags."""
    links = []
    for item in soup.find_all("div", class_="e-loop-item"):
        link = item.find("a", href=True)
        if link and "/village-etape/" in link["href"]:
            links.append(link["href"])
    return links


def _urls_from_headings(soup: BeautifulSoup) -> list[str]:
    """Strategy 3: extract URLs from h2 headings wrapped in anchor tags."""
    links = []
    for heading in soup.find_all("h2", class_="elementor-heading-title"):
        parent = heading.find_parent("a")
        if parent and parent.get("href"):
            links.append(parent["href"])
    return links


def parse_listing_urls(soup: BeautifulSoup) -> list[str]:
    """Extract village detail URLs from a listing page, trying three strategies in order."""
    for strategy in (
        _urls_from_json_attributes,
        _urls_from_loop_items,
        _urls_from_headings,
    ):
        links = strategy(soup)
        if links:
            return links
    return []


_MAX_PAGES = 4


def has_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    """Return True if a subsequent listing page exists."""
    pagination = soup.find("nav", class_="elementor-pagination")
    if pagination:
        return pagination.find("a", class_="next") is not None
    return current_page < _MAX_PAGES


# ---------------------------------------------------------------------------
# Parsers — detail pages
# ---------------------------------------------------------------------------


def _parse_region_and_road(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    """Extract region and road from the inline icon list."""
    icon_list = soup.find("ul", class_="elementor-icon-list-items")
    if not icon_list:
        return None, None
    classes = icon_list.get("class")
    if not isinstance(classes, list) or "elementor-inline-items" not in classes:
        return None, None

    texts = [
        span.get_text(strip=True)
        for item in icon_list.find_all("li", class_="elementor-icon-list-item")
        if (span := item.find("span", class_="elementor-icon-list-text"))
    ]

    return (texts[0] if texts else None), (texts[1] if len(texts) >= 2 else None)


def _parse_practical_info(soup: BeautifulSoup) -> dict:
    """Extract practical contact info (mairie, tourist office, email) from icon lists."""
    practical = {}
    for section in soup.find_all("div", class_="elementor-icon-list-items"):
        for item in section.find_all("li", class_="elementor-icon-list-item"):
            span = item.find("span", class_="elementor-icon-list-text")
            if not span:
                continue
            text = span.get_text(strip=True)
            if "mairie" in text.lower():
                practical["mairie"] = text
            elif "tourisme" in text.lower():
                practical["tourist_office"] = text
            elif "@" in text:
                practical["email"] = text
    return practical


def parse_village_page(html: str, url: str) -> dict | None:
    """Parse a village detail page into a structured record."""
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1", class_="elementor-heading-title") or soup.find("h1")
    name = h1.get_text(strip=True) if h1 else None
    if not name:
        return None

    region, road = _parse_region_and_road(soup)

    desc_widget = soup.find("div", class_="elementor-widget-text-editor")
    description = None
    if desc_widget and (p := desc_widget.find("p")):
        description = p.get_text(strip=True)

    practical = _parse_practical_info(soup)

    return {
        "name": name,
        "url": url,
        "region": region,
        "road": road,
        "description": description,
        "practical": json.dumps(practical, ensure_ascii=False) if practical else None,
    }


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------


async def fetch_village_urls(
    session: aiohttp.ClientSession, base_url: str, headers: dict
) -> list[str]:
    """Crawl paginated listing pages and collect all village detail URLs."""
    all_urls: list[str] = []
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}{page}/"

        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    break
                html = await resp.text()
        except Exception:
            break

        soup = BeautifulSoup(html, "html.parser")
        links = parse_listing_urls(soup)

        if not links:
            break

        all_urls.extend(links)
        logger.info("%s: page %d — %d villages", "village_etape", page, len(links))

        if not has_next_page(soup, page):
            break

        page += 1
        await asyncio.sleep(1)

    seen: set[str] = set()
    return [u for u in all_urls if not (u in seen or seen.add(u))]


async def fetch_village(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    headers: dict,
) -> dict | None:
    """Fetch and parse a single village detail page."""
    async with semaphore:
        await asyncio.sleep(0.5)
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    return None
                return parse_village_page(await resp.text(), url)
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def run(config: dict, run_id: str) -> bool:
    """Scrape Village Étape listings and stage via the shared pipeline.

    Scrapes paginated listings and village detail pages, writes output to a
    temporary CSV, then hands off to `_process_single_file` which handles
    MD5 comparison, archiving the old version, uploading, and writing metadata.

    Args:
        config: Full config dict (from config.yaml scrapers section).
        run_id: Unique flow run identifier.

    Returns:
        True if a file was staged, False if skipped or failed.
    """
    scraper = get_scraper_config(config, MODULE)
    logger.info("Starting %s", scraper.name)

    async with aiohttp.ClientSession(headers=scraper.headers) as session:
        urls = await fetch_village_urls(session, scraper.url, scraper.headers)
        logger.info("%s: found %d village URLs", scraper.name, len(urls))

        if not urls:
            logger.warning("%s: no URLs found", scraper.name)
            return False

        semaphore = asyncio.Semaphore(scraper.concurrency)
        results = await asyncio.gather(
            *[fetch_village(session, semaphore, url, scraper.headers) for url in urls]
        )

        villages = [r for r in results if r is not None]

        if not villages:
            logger.warning("%s: no valid villages scraped", scraper.name)
            return False

        logger.info("%s: scraped %d villages", scraper.name, len(villages))

    return stage_scraper_output(scraper, run_id, villages, FIELDNAMES, EXTENSION)
