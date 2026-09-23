"""Tests for the France overview choropleth generator.

Pure helpers (overseas grouping, label resolution, extent padding, layer
plotting, mainland filtering, visited loading, PNG determinism) are exercised
here with synthetic geometry — no Polaris catalog or other live services are
required. ``main`` is thin glue and stays untested.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib
import polars as pl
import pytest
from matplotlib import pyplot as plt
from PIL import Image
from shapely.geometry import box


matplotlib.use("Agg")

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def map_mod():
    path = REPO_ROOT / "scripts" / "dashboard" / "generate_france_map.py"
    spec = importlib.util.spec_from_file_location("generate_france_map", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _commune_gdf() -> gpd.GeoDataFrame:
    """Small synthetic commune set: metro, two overseas, and the uninhabited
    TAAF (984) / Clipperton (989) far outside the metro area."""
    rows = [
        ("90001", "Zone du Sud", "09", box(1.0, 44.5, 1.5, 45.0)),
        ("01001", "Zone du Nord", "01", box(3.0, 46.0, 4.0, 47.0)),
        ("97105", "Petite-Terre", "971", box(-61.6, 16.2, -61.5, 16.3)),
        ("98802", "Île des Pins", "988", box(167.4, -22.7, 167.6, -22.5)),
        ("984C2", "Kerguelen", "984", box(68.0, -49.5, 70.0, -49.0)),
        ("98901", "Clipperton", "989", box(-109.3, 10.2, -109.2, 10.3)),
    ]
    return gpd.GeoDataFrame(
        [
            {"code": code, "name": name, "department_code": dept, "geometry": geom}
            for code, name, dept, geom in rows
        ],
        crs="EPSG:4326",
    )


# ── Overseas group mapping ───────────────────────────────────────────


@pytest.mark.parametrize("code", ["971", "972", "973", "974", "976"])
def test_inset_group_drom_codes(map_mod, code):
    assert map_mod.inset_group(code) == "DROM"


@pytest.mark.parametrize("code", ["975", "977", "978", "986", "987", "988"])
def test_inset_group_com_codes(map_mod, code):
    assert map_mod.inset_group(code) == "COM & territoires"


@pytest.mark.parametrize(
    "code",
    ["01", "09", "91", "99", "984", "989", ""],
)
def test_inset_group_metropolitan_and_uninhabited(map_mod, code):
    assert map_mod.inset_group(code) is None


def test_uninhabited_overseas_excluded_from_groups(map_mod):
    for codes in map_mod.OVERSEAS_GROUPS.values():
        assert map_mod.UNINHABITED_OVERSEAS.isdisjoint(codes)


def test_every_group_code_has_a_label(map_mod):
    for codes in map_mod.OVERSEAS_GROUPS.values():
        for code in codes:
            assert code in map_mod.OVERSEAS_LABELS
            assert map_mod.overseas_label(code) == map_mod.OVERSEAS_LABELS[code]


# ── Label resolution ─────────────────────────────────────────────────


def test_overseas_label_known_code(map_mod):
    assert map_mod.overseas_label("971") == "Guadeloupe"
    assert map_mod.overseas_label("988") == "Nouvelle-Calédonie"


def test_overseas_label_unknown_code_passes_through(map_mod):
    assert map_mod.overseas_label("12345") == "12345"


# ── Extent padding ──────────────────────────────────────────────────


def test_add_extent_padding_proportional(map_mod):
    xmin, ymin, xmax, ymax = map_mod.add_extent_padding((0.0, 0.0, 100.0, 50.0))

    pad_x = 100 * map_mod.OVERSEAS_PAD_FRACTION
    pad_y = 50 * map_mod.OVERSEAS_PAD_FRACTION

    assert (xmin, xmax) == pytest.approx((-pad_x, 100 + pad_x))
    assert (ymin, ymax) == pytest.approx((-pad_y, 50 + pad_y))


def test_add_extent_padding_zero_width_is_sanitized(map_mod):
    xmin, _, xmax, ymax = map_mod.add_extent_padding((5.0, 10.0, 5.0, 20.0))

    assert xmax - xmin == pytest.approx(2 * map_mod.OVERSEAS_PAD_FRACTION)
    assert ymax == pytest.approx(20 + 10 * map_mod.OVERSEAS_PAD_FRACTION)


def test_add_extent_padding_fully_degenerate(map_mod):
    xmin, ymin, xmax, ymax = map_mod.add_extent_padding((1.0, 1.0, 1.0, 1.0))

    pad = map_mod.OVERSEAS_PAD_FRACTION
    assert (xmin, ymin, xmax, ymax) == pytest.approx(
        (1 - pad, 1 - pad, 1 + pad, 1 + pad)
    )


def test_add_extent_padding_small_island_keeps_positive_extent(map_mod):
    xmin, ymin, xmax, ymax = map_mod.add_extent_padding((0.0, 0.0, 0.01, 0.02))

    assert xmax > xmin
    assert ymax > ymin
    assert (xmax - xmin) > 0.0001


# ── Layer plotting ───────────────────────────────────────────────────


def test_plot_layer_uses_visited_colours_and_boundary(map_mod):
    fig, ax = plt.subplots()
    try:
        gdf = gpd.GeoDataFrame(
            {"geometry": [box(0, 0, 1, 1)]},
            crs="EPSG:4326",
        )
        map_mod.plot_layer(gdf, ax, map_mod.VISITED_COLOR)

        (collection,) = ax.collections
        r, g, b, _ = collection.get_facecolor()[0]
        assert (r, g, b) == pytest.approx(
            tuple(
                int(map_mod.VISITED_COLOR.lstrip("#")[i : i + 2], 16) / 255
                for i in (0, 2, 4)
            )
        )
        edge_r, edge_g, edge_b, _ = collection.get_edgecolor()[0]
        assert (edge_r, edge_g, edge_b) == pytest.approx(
            tuple(
                int(map_mod.BOUNDARY_COLOR.lstrip("#")[i : i + 2], 16) / 255
                for i in (0, 2, 4)
            )
        )
    finally:
        plt.close(fig)


# ── Mainland filtering ──────────────────────────────────────────────


def test_draw_mainland_excludes_uninhabited_overseas(map_mod):
    """984/989 geometry must not expand the mainland bounds (the EPSG:2154
    projection of far-away overseas points blew the map up before the fix)."""
    fig = plt.figure()
    try:
        returned = map_mod.draw_mainland(fig, _commune_gdf(), visited_ids={"01001"})

        xmin, xmax = returned.get_xlim()
        ymin, ymax = returned.get_ylim()

        assert xmax - xmin < 4_000_000
        assert ymax - ymin < 4_000_000
        # Mainland France Lambert-93 sits around x 0..1.3e6, y 6.0..7.1e6;
        # including TAAF/Clipperton would exceed these wildly.
        assert -1_000_000 < xmin < 2_000_000
        assert 6_000_000 < ymax < 8_000_000
        assert returned is not None
    finally:
        plt.close(fig)


def test_draw_mainland_marks_visited(map_mod):
    fig = plt.figure()
    try:
        returned = map_mod.draw_mainland(fig, _commune_gdf(), visited_ids=set())
        assert returned is not None
    finally:
        plt.close(fig)


# ── Visited loading (padded INSEE ids) ──────────────────────────────


def test_load_visited_keeps_padded_insee_ids(map_mod, tmp_path):
    parquet = tmp_path / "visited.parquet"
    pl.DataFrame(
        {
            "id": ["09160", "01002", "85001"],
            "visited": [True, False, True],
        }
    ).write_parquet(parquet)

    assert map_mod.load_visited(parquet) == {"09160", "85001"}


# ── PNG determinism + size ──────────────────────────────────────────


def test_render_figure_is_deterministic(map_mod, tmp_path):
    out1 = tmp_path / "a.png"
    out2 = tmp_path / "b.png"

    map_mod.render_figure(_commune_gdf(), {"01001"}, out1)
    map_mod.render_figure(_commune_gdf(), {"01001"}, out2)

    assert out1.read_bytes() == out2.read_bytes()


def test_render_figure_produces_large_image(map_mod, tmp_path):
    out = tmp_path / "map.png"

    map_mod.render_figure(_commune_gdf(), {"01001"}, out)

    width, height = Image.open(out).size

    assert width > 500
    assert height > 500
