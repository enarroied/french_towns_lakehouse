"""Tests for the QField photo promotion script (mixed local/URL coexistence)."""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def promote_mod():
    path = REPO_ROOT / "scripts" / "dashboard" / "promote_photos.py"
    spec = importlib.util.spec_from_file_location("promote_photos", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _make_gpkg(path: Path) -> None:
    """Build a minimal sqlite db mimicking the communes layer."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE communes (fid INTEGER, id TEXT, name TEXT, photo, geom BLOB)"
    )
    conn.executemany(
        "INSERT INTO communes VALUES (?, ?, ?, ?, NULL)",
        [
            (1, "85083", "Épine", "DCIM/a.jpg"),
            (2, "85011", "Barbâtre", "DCIM/b.jpg"),
            (
                3,
                "85099",
                "Girouard",
                "https://raw.githubusercontent.com/enarroied/french_towns_lakehouse"
                "/master/blog/data/img/a.jpg",
            ),
        ],
    )
    conn.commit()
    conn.close()


def _photo_values(path: Path) -> list[str]:
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute("SELECT photo FROM communes ORDER BY fid").fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


@pytest.fixture
def project(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    proj = tmp_path / "project"
    dcim = proj / "DCIM"
    blog_img = proj / "blog_img"
    blog_img.mkdir(parents=True)
    dcim.mkdir(parents=True)

    _make_gpkg(proj / "communes.gpkg")
    (dcim / "a.jpg").write_bytes(b"aaa-fullsize")
    (dcim / "b.jpg").write_bytes(b"bbb-unshipped")
    (dcim / "other.jpg").write_bytes(b"sss-extra")
    thumbs = dcim / "thumbs"
    thumbs.mkdir(parents=True)
    (thumbs / "a.jpg").write_bytes(b"aaa-thumbcache")
    (blog_img / "a.jpg").write_bytes(b"aaa-thumb")

    return proj, dcim, blog_img, tmp_path / "DCIM_archive"


def test_dry_run_changes_nothing(promote_mod, project) -> None:
    proj, dcim, blog_img, archive = project
    gpkg = proj / "communes.gpkg"

    result = promote_mod.promote(gpkg, dcim, blog_img, archive, dry_run=True)

    assert result.promoted == ["Épine"]
    assert result.pending == ["Barbâtre"]
    assert result.missing_local == []
    assert result.stale == ["a.jpg", "other.jpg"]
    assert result.cache_removed == ["a.jpg"]
    assert result.freed_bytes > 0
    assert _photo_values(gpkg)[0] == "DCIM/a.jpg"
    assert (dcim / "a.jpg").exists()
    assert (dcim / "other.jpg").exists()
    assert (dcim / "thumbs" / "a.jpg").exists()
    assert not archive.exists()


def test_apply_promotes_only_shipped_photos(promote_mod, project) -> None:
    proj, dcim, blog_img, archive = project
    gpkg = proj / "communes.gpkg"

    result = promote_mod.promote(gpkg, dcim, blog_img, archive, dry_run=False)

    assert result.promoted == ["Épine"]
    assert result.pending == ["Barbâtre"]

    photos = _photo_values(gpkg)
    assert photos[0].startswith("https://raw.githubusercontent.com/enarroied")
    assert photos[0].endswith("/blog/data/img/a.jpg")
    assert photos[1] == "DCIM/b.jpg"
    assert photos[2].startswith("https://")  # already-URL row untouched

    assert not (dcim / "a.jpg").exists()
    assert (archive / "a.jpg").exists()
    assert (dcim / "b.jpg").exists()
    assert not (dcim / "other.jpg").exists()
    assert (archive / "other.jpg").exists()
    assert result.stale == ["other.jpg"]
    assert result.cache_removed == ["a.jpg"]
    assert not (dcim / "thumbs" / "a.jpg").exists()


def test_apply_with_missing_local_source(promote_mod, tmp_path: Path) -> None:
    proj = tmp_path / "project"
    dcim = proj / "DCIM"
    blog_img = proj / "blog_img"
    blog_img.mkdir(parents=True)
    dcim.mkdir(parents=True)

    _make_gpkg(proj / "communes.gpkg")
    (blog_img / "a.jpg").write_bytes(b"aaa-thumb")
    (dcim / "b.jpg").write_bytes(b"bbb-unshipped")

    archive = tmp_path / "DCIM_archive"
    result = promote_mod.promote(
        proj / "communes.gpkg", dcim, blog_img, archive, dry_run=False
    )

    assert "Épine" in result.promoted
    assert result.missing_local == ["Épine"]
    assert result.freed_bytes == 0
    photos = _photo_values(proj / "communes.gpkg")
    assert photos[0].startswith("https://raw.githubusercontent.com")
    assert not (archive / "a.jpg").exists()


def test_apply_archives_stale_residue_from_previous_sync(promote_mod, tmp_path) -> None:
    """Originals of already-URL photos (and strays) are archived, not deleted."""
    proj = tmp_path / "project"
    dcim = proj / "DCIM"
    blog_img = proj / "blog_img"
    blog_img.mkdir(parents=True)
    dcim.mkdir(parents=True)

    _make_gpkg(proj / "communes.gpkg")
    (dcim / "a.jpg").write_bytes(b"aaa-stale-url-duplicate")
    (dcim / "b.jpg").write_bytes(b"bbb-unshipped")
    (dcim / "zzz.jpg").write_bytes(b"zzz-stray")

    archive = tmp_path / "DCIM_archive"
    result = promote_mod.promote(
        proj / "communes.gpkg", dcim, blog_img, archive, dry_run=False
    )

    assert result.stale == ["a.jpg", "zzz.jpg"]
    assert not (dcim / "a.jpg").exists()
    assert not (dcim / "zzz.jpg").exists()
    assert (archive / "a.jpg").exists()
    assert (archive / "zzz.jpg").exists()
    assert (dcim / "b.jpg").exists()
    assert result.freed_bytes > 0


def test_apply_survives_spatial_rtree_triggers(promote_mod, tmp_path: Path) -> None:
    """Real GeoPackages fire AFTER UPDATE triggers using ST_* functions."""
    proj = tmp_path / "project"
    dcim = proj / "DCIM"
    blog_img = proj / "blog_img"
    blog_img.mkdir(parents=True)
    dcim.mkdir(parents=True)

    _make_gpkg(proj / "communes.gpkg")
    conn = sqlite3.connect(str(proj / "communes.gpkg"))
    conn.execute(
        """
        CREATE TRIGGER rtree_communes_geom_update AFTER UPDATE ON communes
        WHEN NEW.geom NOT NULL AND NOT ST_IsEmpty(NEW.geom)
        BEGIN
            SELECT 1;
        END
        """
    )
    conn.commit()
    conn.close()

    (blog_img / "a.jpg").write_bytes(b"aaa-thumb")
    (dcim / "a.jpg").write_bytes(b"aaa-fullsize")

    result = promote_mod.promote(
        proj / "communes.gpkg", dcim, blog_img, tmp_path / "DCIM_archive", dry_run=False
    )

    assert "Épine" in result.promoted
    assert _photo_values(proj / "communes.gpkg")[0].startswith(
        "https://raw.githubusercontent.com"
    )


def _open_conn(mod, path: Path) -> sqlite3.Connection:
    return mod._connect_gpkg(path)


def test_plan_promotions_classifies_pending_vs_ready(promote_mod, project) -> None:
    proj, dcim, blog_img, archive = project
    conn = _open_conn(promote_mod, proj / "communes.gpkg")
    try:
        promotions, pending, referenced, already_url = promote_mod.plan_promotions(
            conn, dcim, blog_img
        )
    finally:
        conn.close()

    assert [p.name for p in promotions] == ["Épine"]
    assert pending == ["Barbâtre"]
    assert referenced == {"a.jpg", "b.jpg"}
    assert already_url == {"a.jpg"}

    promo = promotions[0]
    assert promo.fid == 1
    assert promo.url == f"{promote_mod.THUMB_BASE}/a.jpg"
    assert promo.source_path == dcim / "a.jpg"
    assert promo.source_exists is True


def test_plan_promotions_marks_source_missing(promote_mod, tmp_path: Path) -> None:
    proj = tmp_path / "project"
    dcim = proj / "DCIM"
    dcim.mkdir(parents=True)
    blog_img = proj / "blog_img"
    blog_img.mkdir(parents=True)
    (blog_img / "a.jpg").write_bytes(b"aaa-thumb")

    _make_gpkg(proj / "communes.gpkg")
    conn = _open_conn(promote_mod, proj / "communes.gpkg")
    try:
        promotions, pending, _, _ = promote_mod.plan_promotions(conn, dcim, blog_img)
    finally:
        conn.close()

    assert len(promotions) == 1
    assert promotions[0].name == "Épine"
    assert promotions[0].source_exists is False
    assert pending == ["Barbâtre"]


def test_apply_promotions_dry_run_is_inert(promote_mod, project) -> None:
    proj, dcim, blog_img, archive = project
    conn = _open_conn(promote_mod, proj / "communes.gpkg")
    try:
        promotions, *_ = promote_mod.plan_promotions(conn, dcim, blog_img)
        missing_local, freed = promote_mod.apply_promotions(
            conn, promotions, archive, dry_run=True
        )
    finally:
        conn.close()

    assert missing_local == []
    assert freed == (dcim / "a.jpg").stat().st_size
    assert (dcim / "a.jpg").exists()
    assert not archive.exists()
    assert _photo_values(proj / "communes.gpkg")[0] == "DCIM/a.jpg"


def test_apply_promotions_archives_and_updates(promote_mod, project) -> None:
    proj, dcim, blog_img, archive = project
    conn = _open_conn(promote_mod, proj / "communes.gpkg")
    expected_size = (dcim / "a.jpg").stat().st_size
    try:
        promotions, *_ = promote_mod.plan_promotions(conn, dcim, blog_img)
        missing_local, freed = promote_mod.apply_promotions(
            conn, promotions, archive, dry_run=False
        )
    finally:
        conn.close()

    assert missing_local == []
    assert freed == expected_size
    assert not (dcim / "a.jpg").exists()
    assert (archive / "a.jpg").exists()
    assert _photo_values(proj / "communes.gpkg")[0] == f"{promote_mod.THUMB_BASE}/a.jpg"


def test_apply_residue_dry_run_is_inert(promote_mod, project) -> None:
    proj, dcim, blog_img, archive = project
    stale, cache_files = promote_mod.plan_residue(
        dcim, {"a.jpg", "b.jpg"}, {"a.jpg"}, excluded=set()
    )

    stale_names, cache_names, freed = promote_mod.apply_residue(
        stale, cache_files, archive, dry_run=True
    )

    assert stale_names == ["a.jpg", "other.jpg"]
    assert cache_names == ["a.jpg"]
    assert freed > 0
    assert (dcim / "a.jpg").exists()
    assert (dcim / "other.jpg").exists()
    assert (dcim / "thumbs" / "a.jpg").exists()
    assert not archive.exists()


@pytest.mark.parametrize(
    ("filename", "referenced", "already_url", "expected"),
    [
        ("b.jpg", {"a.jpg", "b.jpg"}, {"a.jpg"}, False),
        ("zzz.jpg", {"a.jpg", "b.jpg"}, set(), True),
        ("a.jpg", {"a.jpg"}, {"a.jpg"}, True),
        ("a.jpg", {"a.jpg", "b.jpg"}, {"a.jpg"}, True),
    ],
)
def test_is_stale_logic(
    promote_mod, filename, referenced, already_url, expected
) -> None:
    assert promote_mod._is_stale(filename, referenced, already_url) is expected


def test_plan_residue_excludes_files_moved_by_first_pass(promote_mod, tmp_path) -> None:
    """Regression: a file that's both referenced and already-URL must not be
    offered as stale residue when apply_promotions will archive it, or it would
    be double-moved."""
    dcim = tmp_path / "DCIM"
    thumbs = dcim / "thumbs"
    thumbs.mkdir(parents=True)
    (dcim / "a.jpg").write_bytes(b"aaa")
    (dcim / "other.jpg").write_bytes(b"other")
    (thumbs / "a.jpg").write_bytes(b"thumb")

    in_both_bands = {"a.jpg"}
    referenced = {"a.jpg", "b.jpg"}
    already_url = {"a.jpg"}

    stale_excluded, _ = promote_mod.plan_residue(
        dcim, referenced, already_url, excluded=in_both_bands
    )
    stale_naive, _ = promote_mod.plan_residue(dcim, referenced, already_url)

    assert [p.name for p in stale_excluded] == ["other.jpg"]
    assert [p.name for p in stale_naive] == ["a.jpg", "other.jpg"]
