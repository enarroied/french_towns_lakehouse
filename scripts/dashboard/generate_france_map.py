"""Render a lightweight sign-coloured map of visited vs unvisited communes.

Produces a static PNG (matplotlib) with all commune polygons from the
lakehouse (Polaris ``dim_geography``): visited communes are green, the rest
red.

Metropolitan France is displayed in Lambert-93. Overseas departments and
territories are displayed in small inset maps in a dedicated left-hand
sidebar. Each overseas territory gets its own automatically sized geographic
extent based on its actual geometry.

Pure helpers are DB- and IO-free so they can be unit-tested without a
Polaris catalog; ``main`` is a thin glue layer that fetches geometry and the
visited set, runs sanity checks, and saves the image.

Usage:

    uv run python scripts/dashboard/generate_france_map.py
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import geopandas as gpd
import matplotlib
import matplotlib.axes
import matplotlib.figure
import matplotlib.pyplot as plt
import polars as pl
import shapely.wkb
from dotenv import find_dotenv
from dotenv import load_dotenv
from matplotlib.lines import Line2D


# ── Global tweakables ────────────────────────────────────────────────

VISITED_COLOR = "#2ecc71"
UNVISITED_COLOR = "#e74c3c"
BOUNDARY_COLOR = "#ffffff"
BOUNDARY_WIDTH = 0.2

METRO_CRS = "EPSG:2154"

# Simplification is performed in degrees because the source geometry is
# EPSG:4326. This is only for the PNG, not the source data.
SIMPLIFY_TOLERANCE = 0.0005

PAD_FRACTION = 0.02
OVERSEAS_PAD_FRACTION = 0.12

DPI = 160
FIG_SIZE = (12, 10)


# ── Overseas layout ─────────────────────────────────────────────────

# These are deliberately separated from the graphical layout.
#
# DROM:
#   971 Guadeloupe
#   972 Martinique
#   973 French Guiana
#   974 Réunion
#   976 Mayotte
#
# COM / other overseas territories:
#   975 Saint-Pierre-et-Miquelon
#   977 Saint-Barthélemy
#   978 Saint-Martin
#   986 Wallis-et-Futuna
#   987 French Polynesia
#   988 New Caledonia

OVERSEAS_GROUPS = {
    "DROM": [
        "971",
        "972",
        "973",
        "974",
        "976",
    ],
    "COM & territoires": [
        "975",
        "977",
        "978",
        "986",
        "987",
        "988",
    ],
}

# TAAF (984) and Clipperton (989) are uninhabited, so they get neither a
# sidebar panel nor a place in the mainland transform.
UNINHABITED_OVERSEAS = {"984", "989"}


OVERSEAS_LABELS = {
    "971": "Guadeloupe",
    "972": "Martinique",
    "973": "Guyane",
    "974": "La Réunion",
    "975": "St-Pierre-et-Miquelon",
    "976": "Mayotte",
    "977": "St-Barthélemy",
    "978": "St-Martin",
    "986": "Wallis-et-Futuna",
    "987": "Polynésie française",
    "988": "Nouvelle-Calédonie",
}


# Position of the two large panels in figure coordinates:
#
# left, bottom, width, height
#
# The mainland map starts at x ~= 0.23, leaving the entire left side
# available for these panels.
OVERSEAS_PANELS = {
    "DROM": (0.015, 0.52, 0.19, 0.40),
    "COM & territoires": (0.015, 0.06, 0.19, 0.40),
}


CATALOG_TABLE = "polaris.lakehouse.dim_geography"


DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[2]
    / "blog"
    / "dashboards"
    / "visited-towns"
    / "images"
    / "france_visited.png"
)


DEFAULT_PARQUET = (
    Path(__file__).resolve().parents[2] / "data" / "dashboard" / "visited_towns.parquet"
)


# ── Pure helpers ─────────────────────────────────────────────────────


def overseas_label(department_code: str) -> str:
    """Return a human-readable overseas territory label."""
    return OVERSEAS_LABELS.get(department_code, department_code)


def inset_group(department_code: str) -> str | None:
    """Return the overseas group for a department code."""
    for group, codes in OVERSEAS_GROUPS.items():
        if department_code in codes:
            return group

    return None


def add_extent_padding(
    bounds: tuple[float, float, float, float],
    fraction: float = OVERSEAS_PAD_FRACTION,
) -> tuple[float, float, float, float]:
    """Add proportional padding to xmin/ymin/xmax/ymax bounds.

    Handles very small or degenerate extents so tiny islands do not collapse
    to an unusable map.
    """
    xmin, ymin, xmax, ymax = bounds

    width = xmax - xmin
    height = ymax - ymin

    if width == 0:
        width = 1.0

    if height == 0:
        height = 1.0

    pad_x = width * fraction
    pad_y = height * fraction

    return (
        xmin - pad_x,
        ymin - pad_y,
        xmax + pad_x,
        ymax + pad_y,
    )


def plot_layer(
    gdf: gpd.GeoDataFrame,
    ax: matplotlib.axes.Axes,
    color: str,
) -> None:
    """Plot a layer using the map's standard colours and boundaries."""
    gdf.plot(
        ax=ax,
        color=color,
        edgecolor=BOUNDARY_COLOR,
        linewidth=BOUNDARY_WIDTH,
    )


# ── Data loading ─────────────────────────────────────────────────────


def load_geometry(conn: duckdb.DuckDBPyConnection) -> gpd.GeoDataFrame:
    """Fetch current commune polygons from the catalog as a GeoDataFrame."""
    df = conn.execute(
        f"""
        SELECT
            commune_id AS code,
            name,
            department_code,
            geometry_wkb AS wkb
        FROM {CATALOG_TABLE}
        WHERE is_current
        ORDER BY commune_id
        """
    ).fetch_df()

    df["geometry"] = df["wkb"].apply(lambda wkb: shapely.wkb.loads(bytes(wkb)))

    gdf = gpd.GeoDataFrame(
        df.drop(columns=["wkb"]),
        geometry="geometry",
        crs="EPSG:4326",
    )

    # Simplification is only applied to the plotting copy.
    gdf.geometry = gdf.geometry.simplify(
        SIMPLIFY_TOLERANCE,
        preserve_topology=True,
    )

    return gdf


def load_visited(parquet_path: Path) -> set[str]:
    """Load commune IDs marked as visited from the dashboard parquet."""
    df = pl.read_parquet(str(parquet_path))

    return set(
        df.filter(pl.col("visited") == True)["id"]  # noqa: E712
    )


# ── Mainland map ────────────────────────────────────────────────────


def draw_mainland(
    fig: matplotlib.figure.Figure,
    gdf: gpd.GeoDataFrame,
    visited_ids: set[str],
) -> matplotlib.axes.Axes:
    """Draw metropolitan France in Lambert-93."""

    mainland = gdf[
        gdf["department_code"].map(inset_group).isna()
        & ~gdf["department_code"].isin(UNINHABITED_OVERSEAS)
    ].copy()

    metro = mainland.to_crs(METRO_CRS)

    metro["visited"] = metro["code"].isin(visited_ids)

    # Deliberately reserve the left side of the figure for overseas maps.
    ax = fig.add_axes((0.225, 0.065, 0.755, 0.86))

    bounds = metro.total_bounds

    pad_x = (bounds[2] - bounds[0]) * PAD_FRACTION
    pad_y = (bounds[3] - bounds[1]) * PAD_FRACTION

    ax.set_xlim(
        bounds[0] - pad_x,
        bounds[2] + pad_x,
    )

    ax.set_ylim(
        bounds[1] - pad_y,
        bounds[3] + pad_y,
    )

    ax.set_aspect("equal")
    ax.set_axis_off()

    # Draw the large unvisited layer first.
    unvisited = metro[~metro["visited"]]

    if not unvisited.empty:
        plot_layer(
            unvisited,
            ax,
            UNVISITED_COLOR,
        )

    # Draw visited communes second so they remain clearly visible.
    visited = metro[metro["visited"]]

    if not visited.empty:
        plot_layer(
            visited,
            ax,
            VISITED_COLOR,
        )

    return ax


# ── Overseas maps ───────────────────────────────────────────────────


def draw_overseas_territory(
    fig: matplotlib.figure.Figure,
    subset: gpd.GeoDataFrame,
    visited_ids: set[str],
    position: tuple[float, float, float, float],
) -> None:
    """Draw one overseas territory in its own small map."""

    if subset.empty:
        return

    department_code = str(subset.iloc[0]["department_code"])

    ax = fig.add_axes(position)

    # The overseas geometry is already in EPSG:4326.
    bounds = subset.total_bounds

    xmin, ymin, xmax, ymax = add_extent_padding(bounds)

    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    ax.set_aspect("equal")
    ax.set_axis_off()

    # Draw all territory communes first.
    plot_layer(
        subset,
        ax,
        UNVISITED_COLOR,
    )

    # Then overlay visited communes.
    visited = subset[subset["code"].isin(visited_ids)]

    if not visited.empty:
        plot_layer(
            visited,
            ax,
            VISITED_COLOR,
        )

    # Department code + territory name.
    ax.set_title(
        f"{department_code}  {overseas_label(department_code)}",
        fontsize=6.5,
        pad=1,
    )


def draw_overseas_panel(
    fig: matplotlib.figure.Figure,
    gdf: gpd.GeoDataFrame,
    visited_ids: set[str],
    group: str,
) -> None:
    """Draw one framed overseas panel containing its territory maps."""

    panel_x, panel_y, panel_width, panel_height = OVERSEAS_PANELS[group]

    codes = OVERSEAS_GROUPS[group]

    # ── Panel frame ────────────────────────────────────────────────

    panel = fig.add_axes(
        (
            panel_x,
            panel_y,
            panel_width,
            panel_height,
        )
    )

    panel.set_xlim(0, 1)
    panel.set_ylim(0, 1)
    panel.set_xticks([])
    panel.set_yticks([])

    for spine in panel.spines.values():
        spine.set_visible(True)
        spine.set_linewidth(0.8)

    panel.patch.set_facecolor("white")
    panel.set_zorder(0)

    panel.set_title(
        group,
        fontsize=9,
        fontweight="bold",
        pad=4,
    )

    # ── Territory grid ────────────────────────────────────────────

    n = len(codes)

    # Two columns works well for both groups:
    #
    # DROM:
    #   971 | 972
    #   973 | 974
    #   976 |
    #
    # COM:
    #   975 | 977
    #   978 | 986
    #   987 | 988
    ncols = 2
    nrows = (n + ncols - 1) // ncols

    # Space inside the panel reserved for the individual maps.
    inner_left = panel_x + panel_width * 0.035
    inner_right = panel_x + panel_width * 0.965

    inner_bottom = panel_y + panel_height * 0.055
    inner_top = panel_y + panel_height * 0.88

    total_width = inner_right - inner_left
    total_height = inner_top - inner_bottom

    horizontal_gap = panel_width * 0.025
    vertical_gap = panel_height * 0.025

    cell_width = (total_width - horizontal_gap * (ncols - 1)) / ncols

    cell_height = (total_height - vertical_gap * (nrows - 1)) / nrows

    for index, code in enumerate(codes):
        subset = gdf[gdf["department_code"].astype(str) == code]

        if subset.empty:
            continue

        row = index // ncols
        col = index % ncols

        x = inner_left + col * (cell_width + horizontal_gap)

        y = inner_top - (row + 1) * cell_height - row * vertical_gap

        draw_overseas_territory(
            fig=fig,
            subset=subset,
            visited_ids=visited_ids,
            position=(
                x,
                y,
                cell_width,
                cell_height,
            ),
        )


# ── Legend ──────────────────────────────────────────────────────────


def draw_legend(fig: matplotlib.figure.Figure) -> None:
    """Add the visited/remaining legend."""

    handles = [
        Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=VISITED_COLOR,
            markersize=10,
            label="Visited",
        ),
        Line2D(
            [0],
            [0],
            marker="s",
            color="w",
            markerfacecolor=UNVISITED_COLOR,
            markersize=10,
            label="Remaining",
        ),
    ]

    fig.legend(
        handles=handles,
        loc="lower right",
        bbox_to_anchor=(0.975, 0.025),
        frameon=False,
    )


# ── Figure rendering ────────────────────────────────────────────────


def render_figure(
    gdf: gpd.GeoDataFrame,
    visited_ids: set[str],
    out_path: Path,
) -> Path:
    """Draw the complete map and save it to ``out_path``."""

    fig = plt.figure(
        figsize=FIG_SIZE,
        facecolor="white",
    )

    # ── Main map ───────────────────────────────────────────────────

    draw_mainland(
        fig,
        gdf,
        visited_ids,
    )

    # ── Overseas sidebar ──────────────────────────────────────────

    draw_overseas_panel(
        fig,
        gdf,
        visited_ids,
        "DROM",
    )

    draw_overseas_panel(
        fig,
        gdf,
        visited_ids,
        "COM & territoires",
    )

    # ── Title ──────────────────────────────────────────────────────

    fig.suptitle(
        f"Visited communes ({len(visited_ids)} / {len(gdf)})",
        y=0.965,
        fontsize=14,
    )

    # ── Legend ─────────────────────────────────────────────────────

    draw_legend(fig)

    # ── Output ─────────────────────────────────────────────────────

    out_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fig.savefig(
        out_path,
        dpi=DPI,
        bbox_inches="tight",
        pad_inches=0.1,
        facecolor="white",
    )

    plt.close(fig)

    return out_path


# ── Polaris connection ──────────────────────────────────────────────


def ensure_extension(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    """Load a DuckDB extension, installing it on first use if necessary."""
    try:
        conn.execute(f"LOAD {name};")
    except duckdb.IOException:
        conn.execute(f"INSTALL {name};")
        conn.execute(f"LOAD {name};")


def connect_polaris() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection to the local Polaris catalog."""

    conn = duckdb.connect()

    ensure_extension(conn, "spatial")

    conn.execute(
        "CREATE SECRET polaris_secret (TYPE iceberg, "
        f"CLIENT_ID '{os.environ['POLARIS_CLIENT_ID']}', "
        f"CLIENT_SECRET '{os.environ['POLARIS_CLIENT_SECRET']}', "
        "ENDPOINT "
        "'http://localhost:8181/api/catalog')"
    )

    conn.execute(
        "ATTACH 'french_towns' AS polaris "
        "(TYPE iceberg, "
        "ENDPOINT "
        "'http://localhost:8181/api/catalog', "
        "SECRET 'polaris_secret')"
    )

    return conn


# ── Main ────────────────────────────────────────────────────────────


def main() -> None:
    """Load data, validate it, render the map, and report the output."""

    parser = argparse.ArgumentParser(
        description="Render the visited-France choropleth image"
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"PNG output path (default: {DEFAULT_OUTPUT})",
    )

    parser.add_argument(
        "--parquet",
        type=Path,
        default=DEFAULT_PARQUET,
        help=f"Dashboard master parquet (default: {DEFAULT_PARQUET})",
    )

    args = parser.parse_args()

    load_dotenv(find_dotenv())

    matplotlib.use("Agg")

    conn = connect_polaris()

    try:
        gdf = load_geometry(conn)
    finally:
        conn.close()

    visited = load_visited(args.parquet)

    # ── Sanity checks ──────────────────────────────────────────────

    if len(gdf) < 30_000:
        raise SystemExit(
            f"Sanity check failed: only {len(gdf)} communes in {CATALOG_TABLE}"
        )

    missing = visited - set(gdf["code"])

    if missing:
        raise SystemExit(
            "Sanity check failed: visited communes missing geometry: "
            + ", ".join(sorted(missing))
        )

    # Check that all expected overseas departments/territories exist.
    expected_overseas = {code for codes in OVERSEAS_GROUPS.values() for code in codes}

    available_overseas = set(gdf["department_code"].astype(str))

    missing_overseas = expected_overseas - available_overseas

    if missing_overseas:
        raise SystemExit(
            "Sanity check failed: expected overseas departments/"
            "territories missing from geometry: " + ", ".join(sorted(missing_overseas))
        )

    # ── Render ─────────────────────────────────────────────────────

    render_figure(
        gdf,
        visited,
        args.output,
    )

    size_kb = args.output.stat().st_size / 1024

    print(
        f"OK {args.output} — "
        f"{len(visited):,} visited / "
        f"{len(gdf):,} communes "
        f"({size_kb:.0f} KiB)"
    )


if __name__ == "__main__":
    main()
