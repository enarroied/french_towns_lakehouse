import asyncio
import csv

import aiohttp
from bs4 import BeautifulSoup

# --- Configuration ---
SITEMAP_URL = "https://www.petitescitesdecaractere.com/cites-sitemap.xml"
OUTPUT_CSV = "petites_cites_departments.csv"
CONCURRENT_REQUESTS = 5  # number of pages to fetch at once
USER_AGENT = "MyFrenchCitiesBot/1.0 (eric.narro.ied@gmail.com.com) - Weekly data update"


# --- Helper to parse sitemap ---
async def fetch_sitemap_urls(session):
    """Fetch sitemap XML and return list of city page URLs."""
    print(f"Fetching sitemap from {SITEMAP_URL}")
    async with session.get(SITEMAP_URL) as resp:
        resp.raise_for_status()
        xml = await resp.text()
    soup = BeautifulSoup(xml, "xml")
    # Find all <loc> tags and filter those containing '/cites/'
    urls = [loc.text for loc in soup.find_all("loc") if "/cites/" in loc.text]
    print(f"Found {len(urls)} city URLs.")
    return urls


# --- Scrape a single city page ---
# --- Scrape a single city page ---
async def scrape_city(session, url):
    """Fetch a city page and extract city name and department (lowercase)."""
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                print(f"Error {resp.status} for {url}")
                return None
            html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")

        # Extract city name from <h1 class="cover-title">
        h1 = soup.find("h1", class_="cover-title")
        city = h1.get_text(strip=True).lower() if h1 else None

        # Extract department from <div class="location"> (format "Region, Department" or just "Department")
        loc_div = soup.find("div", class_="location")
        department = None
        if loc_div:
            loc_text = loc_div.get_text(strip=True)
            if "," in loc_text:
                # Split and take the part after the comma (the department)
                department = loc_text.split(",")[1].strip().lower()
            else:
                # No comma, so the whole string is the department (e.g., "Martinique")
                department = loc_text.strip().lower()

        if city and department:
            return {"city": city, "department": department}
        else:
            print(f"Missing data on {url}")
            return None

    except Exception as e:
        print(f"Exception scraping {url}: {e}")
        return None


# --- Worker that limits concurrency ---
async def worker(session, semaphore, url):
    async with semaphore:
        return await scrape_city(session, url)


# --- Main async function ---
async def main():
    # Headers to identify ourselves
    headers = {"User-Agent": USER_AGENT}

    async with aiohttp.ClientSession(headers=headers) as session:
        # Step 1: get all city URLs from sitemap
        city_urls = await fetch_sitemap_urls(session)

        if not city_urls:
            print("No city URLs found. Exiting.")
            return

        # Step 2: scrape all pages with limited concurrency
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [worker(session, semaphore, url) for url in city_urls]
        results = await asyncio.gather(*tasks)

        # Filter out None (failed scrapes)
        valid_results = [r for r in results if r is not None]

        # Step 3: write to CSV
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["city", "department"])
            writer.writeheader()
            writer.writerows(valid_results)

        print(f"\nDone! Scraped {len(valid_results)} cities. Saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
