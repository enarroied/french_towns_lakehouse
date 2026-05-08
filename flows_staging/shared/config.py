import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())

_PROJECT_ROOT = Path(__file__).parent.parent.parent

# Load once at import time
with (_PROJECT_ROOT / "config.yaml").open() as f:
    _config: dict[str, Any] = yaml.safe_load(f)


# Config accessors
def get_config() -> dict[str, Any]:
    return _config


def get_paths() -> dict[str, str]:
    return _config["paths"]


def get_downloads(domain_downloads: list[str] | None = None) -> list[dict[str, Any]]:
    """Get downloads from config, optionally filtered by domain.

    Args:
        domain_downloads: Optional list of download names to filter by.
            If None, returns all downloads.

    Returns:
        List of download configuration dictionaries.
    """
    downloads = _config.get("downloads", [])
    if domain_downloads is not None:
        downloads = [d for d in downloads if d["name"] in domain_downloads]
    return downloads


def get_scrapers() -> list[dict[str, Any]]:
    return _config.get("scrapers", [])


# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# dbt
DBT_PROJECT_DIR = _PROJECT_ROOT / "french_towns_dbt"
DBT_PROFILES_ARGS = ["--profiles-dir", str(DBT_PROJECT_DIR)]
