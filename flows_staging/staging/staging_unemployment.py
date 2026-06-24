"""Custom staging flow for unemployment data from INSEE base-cc-emploi-pop-active.

This flow reads a seed CSV with 34 download URLs (17 years x metro/COM),
downloads each ZIP, extracts the data, filters columns by year prefix,
and produces a unified CSV staged as ``s3://staging-current/demographics/unemployment_*.csv``.
"""

import asyncio
import logging
import re
import tempfile
from pathlib import Path

import pandas as pd
from flows.shared import log
from flows_staging.shared.audit import RUN_STATUS_FAILED
from flows_staging.shared.audit import RUN_STATUS_SUCCESS
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from flows_staging.shared.config import get_config
from flows_staging.shared.download import _download_file
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import StageConfig
from flows_staging.shared.staging_base import _process_single_file
from prefect import flow


logger = logging.getLogger(__name__)

SEED_PATH = Path("seeds/seed_insee_emploi_sources.csv")
TARGET_FOLDER = "demographics"
BASE_NAME = "unemployment"


def _find_data_file(directory: Path) -> Path:
    """Find the main data file in an extracted directory.

    Looks for ``.csv``, ``.xls`` or ``.xlsx`` files, excluding documentation.
    Returns the largest matching file.
    """
    candidates = (
        list(directory.glob("*.csv"))
        + list(directory.glob("*.CSV"))
        + list(directory.glob("*.xls"))
        + list(directory.glob("*.XLS"))
        + list(directory.glob("*.xlsx"))
        + list(directory.glob("*.XLSX"))
    )
    candidates = [
        c
        for c in candidates
        if not c.name.lower().startswith(("doc", "note", "dict", "nomen", "meta"))
    ]
    if not candidates:
        raise FileNotFoundError(f"No CSV or XLS file found in {directory}")
    return max(candidates, key=lambda p: p.stat().st_size)


def _extract_columns(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Keep only columns matching the P{YY}_ prefix for *year*, strip prefix, add year."""
    prefix = f"P{year % 100:02d}_"
    pattern = re.compile(re.escape(prefix))

    keep = [c for c in df.columns if c == "CODGEO" or pattern.match(c)]
    if not keep:
        pcols = [c for c in df.columns if c.startswith("P")]
        raise ValueError(
            f"No columns matching prefix {prefix!r} for year {year}. "
            f"Available P-prefixed columns ({len(pcols)}): {pcols[:10]}"
        )

    df = df[keep].copy()
    df.columns = [pattern.sub("", c) if c.startswith(prefix) else c for c in df.columns]
    df["year"] = year
    return df


async def _process_seed_row(
    row: dict,
    temp_path: Path,
    year_dfs: dict[int, list[pd.DataFrame]],
) -> None:
    """Download, extract, and transform a single seed row, appending to *year_dfs*."""
    year = row["year"]
    url = row["url"]
    fmt = row["format"]
    scope = row["scope"]

    download_name = f"unemp_{year}_{scope}.zip"
    log(f"Downloading {url}")
    for attempt in range(3):
        try:
            await _download_file(url, temp_path, download_name)
            break
        except Exception as exc:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, exc)
            if attempt == 2:
                raise
            await asyncio.sleep(5)

    data_path = _find_data_file(temp_path)

    if fmt == "csv":
        df = pd.read_csv(data_path, sep=";", dtype_backend="pyarrow")
    elif fmt == "xls":
        df = pd.read_excel(data_path, header=5, dtype_backend="pyarrow")
    else:
        raise ValueError(f"Unknown format: {fmt}")

    # Remove extracted file(s) to free space (keep the directory for subsequent downloads)
    for f in temp_path.iterdir():
        if f.name != download_name and not f.name.startswith("."):
            f.unlink()

    df = _extract_columns(df, year)
    year_dfs.setdefault(year, []).append(df)
    log(f"  -> {len(df)} rows for {year}/{scope}")


async def _staging_unemployment_impl() -> None:
    preflight()
    run_id = init_run(domain="demographics", technical_type="DOWNLOAD")

    try:
        config = get_config()
        staging_bucket = config["buckets"]["staging_current"]
        evidence_bucket = config["buckets"]["evidence_archive"]
        minio_client = get_minio_client()

        seed = pd.read_csv(SEED_PATH)
        seed = seed[seed["status"] == "active"]

        year_dfs: dict[int, list[pd.DataFrame]] = {}

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)

            for _, row in seed.iterrows():
                await _process_seed_row(row.to_dict(), temp_path, year_dfs)

            # Combine per year (union metro + COM) then concatenate all years
            combined = pd.concat(
                [pd.concat(frames, ignore_index=True) for frames in year_dfs.values()],
                ignore_index=True,
            )
            combined = combined.sort_values(["CODGEO", "year"]).reset_index(drop=True)

            # Write to temp dir
            output_path = temp_path / BASE_NAME
            combined.to_csv(output_path, index=False)
            log(f"Written {len(combined)} total rows to {output_path}")

            # Process through shared staging pipeline
            stage_config = StageConfig(
                name=BASE_NAME,
                url=SEED_PATH.as_posix(),
                target_folder=TARGET_FOLDER,
                run_id=run_id,
                staging_bucket=staging_bucket,
                evidence_bucket=evidence_bucket,
            )

            _process_single_file(
                stage_config, minio_client, BASE_NAME, ".csv", temp_path
            )

        finalize_run(run_id=run_id, status=RUN_STATUS_SUCCESS, number_files=1)
    except Exception:
        logger.exception("Flow failed")
        finalize_run(run_id=run_id, status=RUN_STATUS_FAILED, number_files=0)
        raise


@flow(name="staging_unemployment")
def staging_unemployment() -> None:
    """Download all INSEE unemployment ZIPs and produce a unified CSV."""
    asyncio.run(_staging_unemployment_impl())


if __name__ == "__main__":
    staging_unemployment()
