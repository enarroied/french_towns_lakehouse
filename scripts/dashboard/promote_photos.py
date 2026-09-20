"""Promote field photos to GitHub-hosted URLs and purge them from the QField project.

The gpkg `photo` column can hold a mix of values:
  - "DCIM/<file>"        → still local (not yet shipped); works offline in QField
  - "<https://...>"      → shipped to <repo>/blog/data/img/; purged locally

A photo is only promoted once its copy exists in <repo>/blog/data/img/ (i.e. after
the dashboard refresh and a git push). Promoted photos are MOVED (not deleted) from
the project's DCIM folder into an archive, keeping the QField project light.

Usage:
  uv run python scripts/dashboard/promote_photos.py            # dry-run report
  uv run python scripts/dashboard/promote_photos.py --apply    # promote confirmed
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path


GITHUB_OWNER = "enarroied"
GITHUB_REPO = "french_towns_lakehouse"
GITHUB_BRANCH = "master"
THUMB_BASE = (
    f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}"
    "/blog/data/img"
)

DEFAULT_PROJECT_DIR = Path.home() / "qgis_projects" / "communes"
DEFAULT_GPKG = DEFAULT_PROJECT_DIR / "communes.gpkg"
DEFAULT_DCIM_DIR = DEFAULT_PROJECT_DIR / "DCIM"
DEFAULT_ARCHIVE_DIR = DEFAULT_PROJECT_DIR / "DCIM_archive"
DEFAULT_THUMB_DIR = Path(__file__).resolve().parents[2] / "blog" / "data" / "img"


@dataclass
class Summary:
    promoted: list[str]
    pending: list[str]
    missing_local: list[str]
    freed_bytes: int = 0


def _photo_filename(photo: str) -> str:
    return photo.removeprefix("DCIM/")


def _connect_gpkg(path: Path) -> sqlite3.Connection:
    """Open a GeoPackage, stubbing spatialite funcs used by its rtree triggers.

    Standard sqlite3 cannot evaluate ST_IsEmpty / ST_MinX / ... These are only
    referenced inside the spatial-index triggers' WHEN clauses. Our updates never
    touch `geom` or `fid`, so the trigger bodies never run; inert stubs are enough.
    """
    conn = sqlite3.connect(str(path))
    for name, arity in [
        ("ST_IsEmpty", 1),
        ("ST_MinX", 1),
        ("ST_MaxX", 1),
        ("ST_MinY", 1),
        ("ST_MaxY", 1),
    ]:
        conn.create_function(name, arity, lambda *v: 0)
    return conn


def promote(
    gpkg_path: Path,
    dcim_dir: Path,
    thumb_dir: Path,
    archive_dir: Path,
    dry_run: bool,
) -> Summary:
    """Rewrite ship-ready local photos to GitHub URLs and archive the originals."""
    summary = Summary(promoted=[], pending=[], missing_local=[])

    conn = _connect_gpkg(gpkg_path)
    try:
        rows = conn.execute(
            "SELECT fid, id, name, photo FROM communes WHERE photo LIKE 'DCIM/%'"
        ).fetchall()
    finally:
        conn.close()

    updates: list[tuple[str, int]] = []
    for fid, _, name, photo in rows:
        filename = _photo_filename(photo)
        server_copy = thumb_dir / filename

        if not server_copy.exists():
            summary.pending.append(name)
            continue

        summary.promoted.append(name)
        updates.append((f"{THUMB_BASE}/{filename}", fid))

        source = dcim_dir / filename
        if source.exists():
            freed = source.stat().st_size
            if not dry_run:
                archive_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source), str(archive_dir / filename))
            summary.freed_bytes += freed
        else:
            summary.missing_local.append(name)

    if updates and not dry_run:
        up_conn = _connect_gpkg(gpkg_path)
        try:
            up_conn.executemany("UPDATE communes SET photo = ? WHERE fid = ?", updates)
            up_conn.commit()
        finally:
            up_conn.close()

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Promote shipped photos to GitHub URLs and archive local originals"
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (default is a dry-run report)",
    )
    parser.add_argument(
        "--gpkg",
        type=Path,
        default=DEFAULT_GPKG,
        help=f"QField GeoPackage (default: {DEFAULT_GPKG})",
    )
    parser.add_argument(
        "--dcim-dir",
        type=Path,
        default=DEFAULT_DCIM_DIR,
        help=f"QField DCIM directory (default: {DEFAULT_DCIM_DIR})",
    )
    parser.add_argument(
        "--thumb-dir",
        type=Path,
        default=DEFAULT_THUMB_DIR,
        help=f"blog/data/img (default: {DEFAULT_THUMB_DIR})",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=DEFAULT_ARCHIVE_DIR,
        help=f"Archive for purged originals (default: {DEFAULT_ARCHIVE_DIR})",
    )
    args = parser.parse_args()

    summary = promote(
        args.gpkg,
        args.dcim_dir,
        args.thumb_dir,
        args.archive_dir,
        dry_run=not args.apply,
    )

    mode = "APPLIED" if args.apply else "DRY RUN (no changes made)"
    print(f"=== {mode} ===")
    print(f"  promoted     : {len(summary.promoted)}")
    print(f"  pending      : {len(summary.pending)} (not yet in blog/data/img)")
    print(
        f"  missing local: {len(summary.missing_local)} (URL set, original already gone)"
    )
    print(
        f"  freed bytes  : {summary.freed_bytes:,} ({summary.freed_bytes / 1024:.0f} KiB)"
    )

    if args.apply:
        print(
            "\n💡 Next: open the project in QGIS, re-package for QField/QFieldCloud,"
            "\n   and let the next sync drop the purged files from the app."
        )
    else:
        print(
            "\n   Re-run with --apply once you have committed & pushed the new"
            "\n   blog/data/img thumbnails."
        )


if __name__ == "__main__":
    main()
