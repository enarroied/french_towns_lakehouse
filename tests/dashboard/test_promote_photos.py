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
    (blog_img / "a.jpg").write_bytes(b"aaa-thumb")

    return proj, dcim, blog_img, tmp_path / "DCIM_archive"


def test_dry_run_changes_nothing(promote_mod, project) -> None:
    proj, dcim, blog_img, archive = project
    gpkg = proj / "communes.gpkg"

    result = promote_mod.promote(gpkg, dcim, blog_img, archive, dry_run=True)

    assert result.promoted == ["Épine"]
    assert result.pending == ["Barbâtre"]
    assert result.missing_local == []
    assert result.freed_bytes > 0
    assert _photo_values(gpkg)[0] == "DCIM/a.jpg"
    assert (dcim / "a.jpg").exists()
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
    assert (dcim / "other.jpg").exists()


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
