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
from contextlib import closing
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
DCIM_PREFIX = "DCIM/"
THUMBS_SUBDIR = "thumbs"


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #


@dataclass
class PlannedPromotion:
    """A local DCIM/ photo whose ship-ready copy already exists in the repo."""

    fid: int
    name: str
    url: str
    source_path: Path
    source_exists: bool


@dataclass
class Summary:
    promoted: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    missing_local: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    cache_removed: list[str] = field(default_factory=list)
    freed_bytes: int = 0


def _photo_filename(photo: str) -> str:
    return photo.removeprefix(DCIM_PREFIX).rsplit("/", 1)[-1]


# --------------------------------------------------------------------------- #
# GeoPackage access
# --------------------------------------------------------------------------- #


def _connect_gpkg(path: Path) -> sqlite3.Connection:
    """Open a GeoPackage, stubbing spatialite funcs used by its rtree triggers.

    Standard sqlite3 cannot evaluate ST_IsEmpty / ST_MinX / ... These are only
    referenced inside the spatial-index triggers' WHEN clauses. Our updates never
    touch `geom` or `fid`, so the trigger bodies never run; inert stubs are enough.
    """
    conn = sqlite3.connect(str(path))
    for name in ("ST_IsEmpty", "ST_MinX", "ST_MaxX", "ST_MinY", "ST_MaxY"):
        conn.create_function(name, 1, lambda *_: 0)
    return conn


# --------------------------------------------------------------------------- #
# Planning (read-only: no files moved, no rows written)
# --------------------------------------------------------------------------- #


def plan_promotions(
    conn: sqlite3.Connection, dcim_dir: Path, thumb_dir: Path
) -> tuple[list[PlannedPromotion], list[str], set[str], set[str]]:
    """Decide what would happen, without touching disk or the database.

    Returns (promotions, pending_names, referenced_filenames, already_url_filenames).
    """
    all_photos = [
        row[0]
        for row in conn.execute("SELECT photo FROM communes WHERE photo IS NOT NULL")
    ]
    referenced = {_photo_filename(p) for p in all_photos}
    already_url = {_photo_filename(p) for p in all_photos if p.startswith("https://")}

    rows = conn.execute(
        f"SELECT fid, name, photo FROM communes WHERE photo LIKE '{DCIM_PREFIX}%'"
    ).fetchall()

    promotions: list[PlannedPromotion] = []
    pending: list[str] = []
    for fid, name, photo in rows:
        filename = _photo_filename(photo)
        if not (thumb_dir / filename).exists():
            pending.append(name)
            continue
        source = dcim_dir / filename
        promotions.append(
            PlannedPromotion(
                fid=fid,
                name=name,
                url=f"{THUMB_BASE}/{filename}",
                source_path=source,
                source_exists=source.exists(),
            )
        )
    return promotions, pending, referenced, already_url


def _is_stale(filename: str, referenced: set[str], already_url: set[str]) -> bool:
    """An original is stale once nothing references it, or its commune already
    moved on to a URL (i.e. it's leftover from a previous sync)."""
    return filename not in referenced or filename in already_url


def _list_originals(dcim_dir: Path) -> list[Path]:
    """Photo files directly inside DCIM/, excluding subdirectories like thumbs/."""
    return [
        p
        for p in sorted(dcim_dir.iterdir())
        if p.is_file() and p.suffix.lower() in EXTENSIONS
    ]


def _list_thumb_cache(dcim_dir: Path) -> list[Path]:
    """QField's regenerable preview cache, if present."""
    thumbs_dir = dcim_dir / THUMBS_SUBDIR
    if not thumbs_dir.is_dir():
        return []
    return [p for p in sorted(thumbs_dir.iterdir()) if p.suffix.lower() in EXTENSIONS]


def plan_residue(
    dcim_dir: Path,
    referenced: set[str],
    already_url: set[str],
    excluded: set[str] | None = None,
) -> tuple[list[Path], list[Path]]:
    """Find stale originals and cached thumbs that are safe to remove.

    ``excluded`` names originals that will be archived by ``apply_promotions``
    (e.g. a basename that is simultaneously an already-URL reference); those
    must not also be offered as stale residue, or they'd be double-moved.

    Returns (stale_originals, cache_files) — both are files, not yet touched.
    """
    if not dcim_dir.is_dir():
        return [], []

    excluded = excluded or set()
    stale = [
        p
        for p in _list_originals(dcim_dir)
        if _is_stale(p.name, referenced, already_url) and p.name not in excluded
    ]
    cache_files = _list_thumb_cache(dcim_dir)
    return stale, cache_files


# --------------------------------------------------------------------------- #
# Execution (the only functions allowed to touch disk or the database)
# --------------------------------------------------------------------------- #


def _archive(path: Path, archive_dir: Path, dry_run: bool) -> int:
    """Move `path` into `archive_dir` (unless dry_run) and return its size in bytes."""
    freed = path.stat().st_size
    if not dry_run:
        archive_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(archive_dir / path.name))
    return freed


def _delete(path: Path, dry_run: bool) -> int:
    freed = path.stat().st_size
    if not dry_run:
        path.unlink()
    return freed


def apply_promotions(
    conn: sqlite3.Connection,
    promotions: list[PlannedPromotion],
    archive_dir: Path,
    dry_run: bool,
) -> tuple[list[str], int]:
    """Archive each promoted photo's local original and update its gpkg row."""
    missing_local: list[str] = []
    freed_bytes = 0

    for promo in promotions:
        if promo.source_exists:
            freed_bytes += _archive(promo.source_path, archive_dir, dry_run)
        else:
            missing_local.append(promo.name)

    if promotions and not dry_run:
        conn.executemany(
            "UPDATE communes SET photo = ? WHERE fid = ?",
            [(p.url, p.fid) for p in promotions],
        )
        conn.commit()

    return missing_local, freed_bytes


def apply_residue(
    stale: list[Path], cache_files: list[Path], archive_dir: Path, dry_run: bool
) -> tuple[list[str], list[str], int]:
    freed_bytes = 0
    for path in stale:
        freed_bytes += _archive(path, archive_dir, dry_run)
    for path in cache_files:
        freed_bytes += _delete(path, dry_run)
    return [p.name for p in stale], [p.name for p in cache_files], freed_bytes


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def promote(
    gpkg_path: Path, dcim_dir: Path, thumb_dir: Path, archive_dir: Path, dry_run: bool
) -> Summary:
    with closing(_connect_gpkg(gpkg_path)) as conn:
        promotions, pending, referenced, already_url = plan_promotions(
            conn, dcim_dir, thumb_dir
        )
        moved_by_first_pass = {
            p.source_path.name for p in promotions if p.source_exists and not dry_run
        }
        stale, cache_files = plan_residue(
            dcim_dir, referenced, already_url, excluded=moved_by_first_pass
        )

        missing_local, promo_freed = apply_promotions(
            conn, promotions, archive_dir, dry_run
        )
        stale_names, cache_names, residue_freed = apply_residue(
            stale, cache_files, archive_dir, dry_run
        )

    return Summary(
        promoted=[p.name for p in promotions],
        pending=pending,
        missing_local=missing_local,
        stale=stale_names,
        cache_removed=cache_names,
        freed_bytes=promo_freed + residue_freed,
    )


def print_report(summary: Summary, applied: bool) -> None:
    mode = "APPLIED" if applied else "DRY RUN (no changes made)"
    print(f"=== {mode} ===")
    print(f"  promoted     : {len(summary.promoted)}")
    print(f"  pending      : {len(summary.pending)} (not yet in blog/data/img)")
    print(
        f"  missing local: {len(summary.missing_local)} (URL set, original already gone)"
    )
    print(f"  stale purged : {len(summary.stale)} (residue moved out of the project)")
    print(
        f"  cache purged : {len(summary.cache_removed)} (regenerable DCIM/thumbs removed)"
    )
    print(
        f"  freed bytes  : {summary.freed_bytes:,} ({summary.freed_bytes / 1024:.0f} KiB)"
    )

    if applied:
        print(
            "\n💡 Next: open the project in QGIS, re-package for QField/QFieldCloud,"
            "\n   and let the next sync drop the purged files from the app."
        )
    else:
        print(
            "\n   Re-run with --apply once you have committed & pushed the new"
            "\n   blog/data/img thumbnails."
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Promote shipped photos to GitHub URLs and archive local originals"
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = promote(
        args.gpkg,
        args.dcim_dir,
        args.thumb_dir,
        args.archive_dir,
        dry_run=not args.apply,
    )
    print_report(summary, applied=args.apply)


if __name__ == "__main__":
    main()
