"""Tests for the DCIM thumbnail generation script."""

from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
from pathlib import Path

import pytest
from PIL import Image


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def thumb_mod():
    path = REPO_ROOT / "scripts" / "dashboard" / "generate_thumbnails.py"
    spec = importlib.util.spec_from_file_location("generate_thumbnails", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _make_gpkg(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE communes (fid INTEGER, visited INTEGER, photo TEXT)")
    conn.executemany(
        "INSERT INTO communes (fid, visited, photo) VALUES (?, ?, ?)",
        [
            (1, 1, "DCIM/a.jpg"),
            (2, 0, "DCIM/unvisited.jpg"),
            (3, 1, "DCIM/nested/deep.png"),
            (4, 1, None),
        ],
    )
    conn.commit()
    conn.close()


def test_get_referenced_photos_visited_only(thumb_mod, tmp_path: Path) -> None:
    gpkg = tmp_path / "communes.gpkg"
    _make_gpkg(gpkg)

    referenced = thumb_mod.get_referenced_photos(gpkg)

    assert referenced == {"a.jpg", "nested/deep.png"}


def test_get_referenced_photos_missing_gpkg(thumb_mod, tmp_path: Path) -> None:
    assert thumb_mod.get_referenced_photos(tmp_path / "nope.gpkg") == set()


def test_list_candidate_photos_filters(thumb_mod, tmp_path: Path) -> None:
    dcim = tmp_path / "DCIM"
    dcim.mkdir(parents=True)
    (dcim / "a.jpg").write_bytes(b"x")
    (dcim / "b.jpg").write_bytes(b"x")
    (dcim / "c.png").write_bytes(b"x")
    (dcim / ".hidden.jpg").write_bytes(b"x")
    (dcim / "notes.doc").write_bytes(b"x")
    (dcim / "sub").mkdir()
    (dcim / "sub" / "d.jpg").write_bytes(b"x")

    candidates = thumb_mod.list_candidate_photos(dcim, {"a.jpg", "c.png"})

    assert [p.name for p in candidates] == ["a.jpg", "c.png"]


@pytest.mark.parametrize(
    ("thumb_mtime", "source_mtime", "expected"),
    [
        (None, 100, False),
        (50, 100, False),
        (100, 50, True),
    ],
)
def test_is_up_to_date(thumb_mod, tmp_path, thumb_mtime, source_mtime, expected):
    source = tmp_path / "a.jpg"
    source.write_bytes(b"x")
    os.utime(source, (source_mtime, source_mtime))
    thumb = tmp_path / "thumb.jpg"
    if thumb_mtime is not None:
        thumb.write_bytes(b"x")
        os.utime(thumb, (thumb_mtime, thumb_mtime))
    assert thumb_mod._is_up_to_date(source, thumb) is expected


def _make_image(path: Path, mode: str, size: tuple[int, int]) -> None:
    if mode == "RGBA":
        img = Image.new("RGBA", size, (255, 0, 0, 128))
    elif mode == "RGB":
        img = Image.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3))
    else:  # pragma: no cover
        raise AssertionError(f"unhandled mode {mode}")
    img.save(path)


def test_generate_thumbnail_produces_jpeg(thumb_mod, tmp_path: Path) -> None:
    source = tmp_path / "a.jpg"
    thumb = tmp_path / "thumb.jpg"
    _make_image(source, "RGB", (400, 300))

    thumb_mod.generate_thumbnail(source, thumb)

    assert thumb.exists()
    with Image.open(thumb) as img:
        assert img.format == "JPEG"
        assert img.size == (200, 150)


def test_generate_thumbnail_handles_rgba_source(thumb_mod, tmp_path: Path) -> None:
    source = tmp_path / "a.png"
    thumb = tmp_path / "thumb.jpg"
    _make_image(source, "RGBA", (400, 300))

    thumb_mod.generate_thumbnail(source, thumb)

    with Image.open(thumb) as img:
        assert img.format == "JPEG"
        assert img.mode == "RGB"
        assert img.size == (200, 150)


def test_generate_thumbnail_respects_quality(thumb_mod, tmp_path: Path) -> None:
    source = tmp_path / "a.jpg"
    lo = tmp_path / "lo.jpg"
    hi = tmp_path / "hi.jpg"
    _make_image(source, "RGB", (400, 300))

    thumb_mod.generate_thumbnail(source, lo, quality=10)
    thumb_mod.generate_thumbnail(source, hi, quality=95)

    assert lo.stat().st_size < hi.stat().st_size


def test_generate_thumbnails_counts_and_reports_skips(
    thumb_mod, tmp_path: Path
) -> None:
    dcim = tmp_path / "DCIM"
    thumbs = tmp_path / "thumbs"
    dcim.mkdir(parents=True)
    thumbs.mkdir(parents=True)
    source = dcim / "a.jpg"
    _make_image(source, "RGB", (400, 300))
    referenced = {"a.jpg"}

    generated, skipped = thumb_mod.generate_thumbnails(dcim, thumbs, referenced)
    assert (generated, skipped) == (1, 0)
    assert (thumbs / "a.jpg").exists()

    generated, skipped = thumb_mod.generate_thumbnails(dcim, thumbs, referenced)
    assert (generated, skipped) == (0, 1)
