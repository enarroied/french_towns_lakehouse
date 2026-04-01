import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())

_config: dict[str, Any] | None = None


def get_config() -> dict[str, Any]:
    global _config
    if _config is None:
        config_path = Path("config.yaml")
        with config_path.open() as f:
            _config = yaml.safe_load(f)
    return _config


def get_paths() -> dict[str, str]:
    return get_config()["paths"]


def get_directories() -> list[str]:
    return get_config().get("directories", [])


def get_downloads() -> list[dict[str, Any]]:
    return get_config().get("downloads", [])


def get_scrapers() -> list[dict[str, Any]]:
    return get_config().get("scrapers", [])


def get_custom_parsers() -> list[dict[str, Any]]:
    return get_config().get("custom_parsers", [])


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

DBT_PROJECT_DIR = Path("french_towns_dbt")
DBT_PROFILES_ARGS = ["--profiles-dir", "."]
