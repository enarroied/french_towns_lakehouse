"""Generate 200px-wide thumbnails for DCIM photos referenced by visited communes.

Sources from the QFieldCloud-managed project folder by default.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from PIL import Image


THUMB_WIDTH = 200
JPEG_QUALITY = 85
EXTENSIONS = {".jpg", ".jpeg", ".png"}
DEFAULT_PROJECT_DIR = Path.home() / "QField" / "cloud" / "communes_qfield"
DEFAULT_DCIM_DIR = DEFAULT_PROJECT_DIR / "DCIM"
DEFAULT_GPKG = DEFAULT_PROJECT_DIR / "communes.gpkg"
DEFAULT_THUMB_DIR = Path(__file__).resolve().parents[2] / "blog" / "data" / "img"


def get_referenced_photos(gpkg_path: Path) -> set[str]:
    """Return filenames (without 'DCIM/' prefix) referenced in the GeoPackage."""
    if not gpkg_path.exists():
        return set()
    conn = sqlite3.connect(str(gpkg_path))
    try:
        rows = conn.execute(
            "SELECT DISTINCT photo FROM communes WHERE visited IS TRUE AND photo IS NOT NULL"
        ).fetchall()
    finally:
        conn.close()
    return {row[0].replace("DCIM/", "", 1) for row in rows if row[0]}


def _is_candidate(img_path: Path, referenced: set[str]) -> bool:
    """A source photo is a candidate once it's a real image and it's referenced."""
    return (
        img_path.suffix.lower() in EXTENSIONS
        and not img_path.name.startswith(".")
        and img_path.name in referenced
    )


def list_candidate_photos(dcim_dir: Path, referenced: set[str]) -> list[Path]:
    return [p for p in sorted(dcim_dir.iterdir()) if _is_candidate(p, referenced)]


def _is_up_to_date(source: Path, thumb_path: Path) -> bool:
    """True if an existing thumbnail is at least as new as its source photo."""
    return thumb_path.exists() and thumb_path.stat().st_mtime >= source.stat().st_mtime


def generate_thumbnail(
    source: Path, thumb_path: Path, quality: int = JPEG_QUALITY
) -> None:
    """Resize `source` to THUMB_WIDTH wide (preserving aspect ratio) and save it."""
    with Image.open(source) as img:
        img.load()
        ratio = THUMB_WIDTH / img.width
        new_height = int(img.height * ratio)
        thumb = img.resize((THUMB_WIDTH, new_height), Image.Resampling.LANCZOS)
        if thumb.mode not in ("RGB", "L"):
            thumb = thumb.convert("RGB")
        thumb.save(thumb_path, quality=quality)


def generate_thumbnails(
    dcim_dir: Path, thumb_dir: Path, referenced: set[str]
) -> tuple[int, int]:
    """Generate any missing/stale thumbnails. Returns (generated, skipped) counts."""
    generated = 0
    skipped = 0
    for source in list_candidate_photos(dcim_dir, referenced):
        thumb_path = thumb_dir / source.name
        if _is_up_to_date(source, thumb_path):
            skipped += 1
            continue
        generate_thumbnail(source, thumb_path)
        generated += 1
    return generated, skipped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate thumbnail images for visited-commune photos"
    )
    parser.add_argument(
        "--dcim-dir",
        type=Path,
        default=DEFAULT_DCIM_DIR,
        help=f"QField DCIM photo directory (default: {DEFAULT_DCIM_DIR})",
    )
    parser.add_argument(
        "--gpkg",
        type=Path,
        default=DEFAULT_GPKG,
        help=f"QField GeoPackage (default: {DEFAULT_GPKG})",
    )
    parser.add_argument(
        "--thumb-dir",
        type=Path,
        default=DEFAULT_THUMB_DIR,
        help=f"Thumbnail output directory (default: {DEFAULT_THUMB_DIR})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.thumb_dir.mkdir(parents=True, exist_ok=True)

    referenced = get_referenced_photos(args.gpkg)
    if not referenced:
        print("⚠️  No referenced photos found — skipping thumbnail generation")
        return

    generated, skipped = generate_thumbnails(args.dcim_dir, args.thumb_dir, referenced)
    print(f"✅ Thumbnails: {generated} generated, {skipped} skipped")


if __name__ == "__main__":
    main()
