"""Generate 200px-wide thumbnails for DCIM photos referenced by visited communes.

Sources from the QField project in ~/qgis_projects/communes by default.
"""

import argparse
import sqlite3
from pathlib import Path

from PIL import Image


THUMB_WIDTH = 200
EXTENSIONS = {".jpg", ".jpeg", ".png"}
DEFAULT_PROJECT_DIR = Path.home() / "qgis_projects" / "communes"
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


def main() -> None:
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
    args = parser.parse_args()

    dcim_dir: Path = args.dcim_dir
    thumb_dir: Path = args.thumb_dir
    thumb_dir.mkdir(parents=True, exist_ok=True)
    referenced = get_referenced_photos(args.gpkg)
    if not referenced:
        print("⚠️  No referenced photos found — skipping thumbnail generation")
        return

    generated = 0
    skipped = 0

    for img_path in sorted(dcim_dir.iterdir()):
        if img_path.suffix.lower() not in EXTENSIONS:
            continue
        if img_path.name.startswith("."):
            continue
        if img_path.name not in referenced:
            continue

        thumb_path = thumb_dir / img_path.name

        if (
            thumb_path.exists()
            and thumb_path.stat().st_mtime >= img_path.stat().st_mtime
        ):
            skipped += 1
            continue

        img = Image.open(img_path)
        img.load()
        ratio = THUMB_WIDTH / img.width
        new_height = int(img.height * ratio)
        thumb = img.resize((THUMB_WIDTH, new_height), Image.Resampling.LANCZOS)
        thumb.save(thumb_path, quality=85)
        generated += 1

    print(f"✅ Thumbnails: {generated} generated, {skipped} skipped")


if __name__ == "__main__":
    main()
