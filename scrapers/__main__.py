"""Entry point for the scrapers package.

Run all scrapers:
    python -m scrapers

Run a single scraper by name:
    python -m scrapers famille_plus

Use a non-default config file:
    python -m scrapers --config path/to/config.yaml
"""

import argparse
import asyncio
import logging

from scrapers import run_all_scrapers
from scrapers.utils import load_config


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run French towns scrapers.")
    parser.add_argument(
        "scraper",
        nargs="?",
        help="Name of a single scraper to run (e.g. famille_plus). Runs all if omitted.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to the YAML config file (default: config.yaml).",
    )
    return parser.parse_args()


async def main() -> None:
    """Load config, optionally filter to one scraper, then run."""
    args = _parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    config = load_config(args.config)

    if args.scraper:
        config["scrapers"] = [
            s for s in config["scrapers"] if s["name"] == args.scraper
        ]
        if not config["scrapers"]:
            raise SystemExit(f"No scraper named '{args.scraper}' found in config.")

    await run_all_scrapers(config)


if __name__ == "__main__":
    asyncio.run(main())
