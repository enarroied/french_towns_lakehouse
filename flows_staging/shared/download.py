"""Download and file utilities for staging flows.

This module handles:
- Async HTTP file downloads with streaming
- ZIP extraction
- File renaming via regex patterns
- CSV writing for scraper output
- MD5 hashing and file size helpers
- Timestamp helpers for filenames
"""

import csv
import hashlib
import pathlib
import re
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
from flows.shared import log


def calculate_md5(file_path: Path) -> str:
    """Calculate MD5 hash of a file using chunked reading for memory efficiency.

    Args:
        file_path: Path to the file to hash.

    Returns:
        Hex-encoded MD5 digest string.
    """
    md5_hash = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def _get_file_size_mb(file_path: Path) -> float:
    """Return the file size in megabytes, rounded to two decimal places.

    Args:
        file_path: Path to the file.

    Returns:
        File size in MB.
    """
    return round(file_path.stat().st_size / 1024**2, 2)


def _add_timestamp_to_filename(base_name: str, extension: str) -> str:
    """Return a filename with a UTC timestamp inserted before the extension.

    Args:
        base_name: Name without extension (e.g., 'populations_historiques').
        extension: File extension including the dot (e.g., '.csv').

    Returns:
        Timestamped filename (e.g., 'populations_historiques_20250101T120000.csv').
    """
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    return f"{base_name}_{timestamp}{extension}"


async def _download_file(url: str, dest: pathlib.Path, filename: str) -> None:
    """Download a file from a URL, streaming to disk, then unzip if applicable.

    Args:
        url: HTTP URL to download from.
        dest: Directory to save the file into.
        filename: Name for the downloaded file.
    """
    filepath = dest / filename
    async with httpx.AsyncClient() as client, client.stream("GET", url) as response:
        response.raise_for_status()
        with filepath.open("wb") as f:
            async for chunk in response.aiter_bytes(chunk_size=8192):
                f.write(chunk)
    _unzip_file_if_needed(filepath)


def _unzip_file_if_needed(path: pathlib.Path) -> None:
    """Extract a ZIP file in-place and remove the archive, if the file is a ZIP.

    Uses magic bytes rather than file extension to detect ZIP files reliably,
    since some sources serve ZIPs without a .zip extension in the URL.

    Args:
        path: Path to a file that may be a ZIP archive.
    """
    if not zipfile.is_zipfile(path):
        return
    with zipfile.ZipFile(path) as zf:
        zf.extractall(path.parent)
    path.unlink()


def _rename_files(
    folder: pathlib.Path,
    patterns: list[str],
    targets: list[str],
) -> set[str]:
    """Rename files in a folder matching regex patterns to target names.

    Args:
        folder: Directory containing the files.
        patterns: List of regex patterns matching existing filenames.
        targets: List of new filenames corresponding to each pattern.

    Returns:
        Set of original filenames that were renamed.

    Raises:
        FileNotFoundError: If no file matches a given pattern.
    """
    matched = set()
    for pattern, target in zip(patterns, targets, strict=True):
        match = next(
            (f for f in folder.iterdir() if re.fullmatch(pattern, f.name)), None
        )
        if match is None:
            raise FileNotFoundError(f"No file matching pattern '{pattern}' in {folder}")
        match.rename(folder / target)
        matched.add(match.name)
    return matched


def _delete_unmatched_files(
    folder: pathlib.Path,
    targets: list[str],
    matched: set[str],
) -> None:
    """Remove files from a folder that are not in the target or matched sets.

    Args:
        folder: Directory to clean up.
        targets: Filenames to keep after renaming.
        matched: Original filenames that were renamed (already gone, kept for safety).
    """
    keep = set(targets) | matched
    for f in folder.iterdir():
        if f.name not in keep:
            f.unlink()


def write_csv_for_staging(
    data: list[dict],
    fieldnames: list[str],
    base_name: str,
    temp_dir: Path,
) -> Path:
    """Write scraper output to a CSV file ready for the shared staging pipeline.

    The file is named exactly `base_name` (no extension) so that
    `_process_single_file` in staging_base can handle it uniformly.

    Args:
        data: List of row dicts to write.
        fieldnames: CSV column names.
        base_name: File base name without extension (e.g., 'famille_plus').
        temp_dir: Directory to write into.

    Returns:
        Path to the written file.
    """
    file_path = temp_dir / base_name
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    log(f"📝 Written {len(data)} rows to {file_path}")
    return file_path
