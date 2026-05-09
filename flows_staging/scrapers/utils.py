from __future__ import annotations

import tempfile
from pathlib import Path

import yaml
from flows_staging.scrapers.models import ScraperConfig
from flows_staging.shared.config import get_config
from flows_staging.shared.download import write_csv_for_staging
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import StageConfig
from flows_staging.shared.staging_base import _process_single_file


def load_config(path: str | Path = "config.yaml") -> dict:
    """Load the project YAML configuration file."""
    return yaml.safe_load(Path(path).open())


def get_scraper_config(config: dict, module: str) -> ScraperConfig:
    """Look up a scraper entry by module path and return a typed ``ScraperConfig``."""
    raw = next(s for s in config["scrapers"] if s["module"] == module)
    return ScraperConfig.from_dict(raw)


def stage_scraper_output(
    scraper: ScraperConfig,
    run_id: str,
    data: list[dict],
    fieldnames: list[str],
    extension: str = ".csv",
) -> bool:
    """Write scraper data to CSV and stage via the shared pipeline.

    Args:
        scraper: ScraperConfig with name, url, target_folder.
        run_id: Unique flow run identifier.
        data: List of dicts to write as CSV rows.
        fieldnames: CSV column names.
        extension: File extension including the dot (default ``.csv``).

    Returns:
        True if file was staged, False if skipped.
    """
    all_config = get_config()
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]
    minio_client = get_minio_client()

    stage_config = StageConfig(
        name=scraper.name,
        url=scraper.url,
        target_folder=scraper.target_folder,
        run_id=run_id,
        staging_bucket=staging_bucket,
        evidence_bucket=evidence_bucket,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        write_csv_for_staging(data, fieldnames, scraper.name, temp_path)
        return _process_single_file(
            stage_config, minio_client, scraper.name, extension, temp_path
        )
