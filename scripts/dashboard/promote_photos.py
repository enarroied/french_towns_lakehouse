"""Promote field photos to GitHub-hosted URLs and purge them from the QField project.

The gpkg `photo` column can hold a mix of values:
  - "DCIM/<file>"        → still local (not yet shipped); works offline in QField
  - "<https://...>"      → shipped to <repo>/blog/data/img/; purged locally

A photo is only promoted once its copy exists in <repo>/blog/data/img/ (i.e. after
the dashboard refresh and a git push). Promoted photos are MOVED (not deleted) from
the project's DCIM folder into an archive OUTSIDE the packaged project
(~/QField_photo_archive/communes/DCIM_archive), keeping the QField project light.

As a second pass, any local original that corresponds to an already-promoted (URL)
photo — or to no commune at all — is stale residue of a previous sync and gets
moved to the same archive, so re-runs keep the packaged folder empty of dead weight.
QField's regenerable preview cache (DCIM/thumbs) is deleted rather than archived.

Usage:
  uv run python scripts/dashboard/promote_photos.py            # dry-run report
  uv run python scripts/dashboard/promote_photos.py --apply    # promote confirmed
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path


GITHUB_OWNER = "enarroied"
GITHUB_REPO = "french_towns_lakehouse"
GITHUB_BRANCH = "master"
THUMB_BASE = (
    f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}"
    "/blog/data/img"
)

DEFAULT_PROJECT_DIR = Path.home() / "QField" / "cloud" / "communes_qfield"
DEFAULT_GPKG = DEFAULT_PROJECT_DIR / "communes.gpkg"
DEFAULT_DCIM_DIR = DEFAULT_PROJECT_DIR / "DCIM"
DEFAULT_ARCHIVE_DIR = Path.home() / "QField_photo_archive" / "communes" / "DCIM_archive"
DEFAULT_THUMB_DIR = Path(__file__).resolve().parents[2] / "blog" / "data" / "img"
EXTENSIONS = {".jpg", ".jpeg", ".png"}


@dataclass
class Summary:
    promoted: list[str]
    pending: list[str]
    missing_local: list[str]
    stale: list[str] = field(default_factory=list)
    cache_removed: list[str] = field(default_factory=list)
    freed_bytes: int = 0


def _photo_filename(photo: str) -> str:
    return photo.removeprefix("DCIM/").rsplit("/", 1)[-1]


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


def _archive_residue(
    dcim_dir: Path,
    archive_dir: Path,
    referenced: set[str],
    already_url: set[str],
    dry_run: bool,
) -> tuple[list[str], list[str], int]:
    """Move unused originals into the archive and drop regenerable thumb cache."""
    stale: list[str] = []
    cache_removed: list[str] = []
    freed = 0
    if not dcim_dir.is_dir():
        return stale, cache_removed, freed

    for img_path in sorted(dcim_dir.iterdir()):
        if img_path.is_dir():
            if img_path.name == "thumbs":
                for thumb in sorted(img_path.iterdir()):
                    if thumb.suffix.lower() not in EXTENSIONS:
                        continue
                    freed += thumb.stat().st_size
                    if not dry_run:
                        thumb.unlink()
                    cache_removed.append(thumb.name)
            continue
        if img_path.suffix.lower() not in EXTENSIONS:
            continue
        if img_path.name in referenced and img_path.name not in already_url:
            continue
        freed += img_path.stat().st_size
        if not dry_run:
            archive_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(img_path), str(archive_dir / img_path.name))
        stale.append(img_path.name)
    return stale, cache_removed, freed


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
        all_photos = [
            r[0]
            for r in conn.execute("SELECT photo FROM communes WHERE photo IS NOT NULL")
        ]
        rows = conn.execute(
            "SELECT fid, id, name, photo FROM communes WHERE photo LIKE 'DCIM/%'"
        ).fetchall()
    finally:
        conn.close()

    referenced = {_photo_filename(p) for p in all_photos}
    already_url = {_photo_filename(p) for p in all_photos if p.startswith("https://")}

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

    if dcim_dir.is_dir():
        stale, cache_removed, freed = _archive_residue(
            dcim_dir, archive_dir, referenced, already_url, dry_run
        )
        summary.stale = stale
        summary.cache_removed = cache_removed
        summary.freed_bytes += freed

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
    print(f"  stale purged  : {len(summary.stale)} (residue moved out of the project)")
    print(
        f"  cache purged  : {len(summary.cache_removed)} (regenerable DCIM/thumbs removed)"
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
