"""Generate 200px-wide thumbnails for DCIM photos referenced by visited communes."""

import sqlite3
from pathlib import Path

from PIL import Image


DCIM_DIR = Path("generate_qfield/communes/out/DCIM")
GPKG_PATH = Path("generate_qfield/communes/out/communes.gpkg")
THUMB_DIR = Path("blog/data/img")
THUMB_WIDTH = 200
EXTENSIONS = {".jpg", ".jpeg", ".png"}


def get_referenced_photos() -> set[str]:
    """Return filenames (without 'DCIM/' prefix) referenced in the GeoPackage."""
    if not GPKG_PATH.exists():
        return set()
    conn = sqlite3.connect(str(GPKG_PATH))
    try:
        rows = conn.execute(
            "SELECT DISTINCT photo FROM communes WHERE visited IS TRUE AND photo IS NOT NULL"
        ).fetchall()
    finally:
        conn.close()
    return {row[0].replace("DCIM/", "", 1) for row in rows if row[0]}


def main() -> None:
    THUMB_DIR.mkdir(parents=True, exist_ok=True)
    referenced = get_referenced_photos()
    if not referenced:
        print("⚠️  No referenced photos found — skipping thumbnail generation")
        return

    generated = 0
    skipped = 0

    for img_path in sorted(DCIM_DIR.iterdir()):
        if img_path.suffix.lower() not in EXTENSIONS:
            continue
        if img_path.name.startswith("."):
            continue
        if img_path.name not in referenced:
            continue

        thumb_path = THUMB_DIR / img_path.name

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
