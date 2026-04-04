from __future__ import annotations

from pathlib import Path

import yaml

from scrapers.models import ScraperConfig


def load_config(path: str | Path = "config.yaml") -> dict:
    """Load the project YAML configuration file."""
    return yaml.safe_load(Path(path).open())


def get_scraper_config(config: dict, module: str) -> ScraperConfig:
    """Look up a scraper entry by module path and return a typed ``ScraperConfig``."""
    raw = next(s for s in config["scrapers"] if s["module"] == module)
    return ScraperConfig.from_dict(raw)
