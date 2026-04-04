import sys

from flows.scrapers import run_scraper


if __name__ == "__main__":
    if len(sys.argv) != 2:
        msg = "Usage: python -m flows.scrapers.scraper <scraper_name>"
        raise SystemExit(msg)
    run_scraper(sys.argv[1])
