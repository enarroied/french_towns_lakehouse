"""Render a lightweight sign-coloured map of visited vs unvisited communes.

Produces a static PNG (matplotlib) with all commune polygons from the
lakehouse (Polaris ``dim_geography``): visited communes are green, the rest
red. The metropolitan mainland covers the centre; overseas territories are
grouped into two thin-framed panels (DROM and COM) down the left, each
territory in its own enlarged cell.

Pure helpers (``inset_group``, ``render_figure``) are DB- and IO-free so they
can be unit-tested without a Polaris catalog; ``main`` is a thin glue layer
that fetches geometry and the visited set, runs sanity checks, and saves the
image.

Usage:
  uv run python scripts/dashboard/generate_france_map.py
"""

from __future__ import annotations

import argparse
import math
import os
from pathlib import Path

import duckdb
import geopandas as gpd
import matplotlib
import matplotlib.axes
import matplotlib.patches
import matplotlib.pyplot as plt
import polars as pl
import shapely.wkb
from dotenv import find_dotenv
from dotenv import load_dotenv


# ── Global tweakables (adjust colours/layout in one place) ─────────
VISITED_COLOR = "#2ecc71"  # green
UNVISITED_COLOR = "#e74c3c"  # red
BOUNDARY_COLOR = "#ffffff"
BOUNDARY_WIDTH = 0.2
METRO_CRS = "EPSG:2154"  # native Lambert-93 for metropolitan France + Corsica
SIMPLIFY_TOLERANCE = 0.0005  # degrees (plot-only, keeps PNG crisp + fast)
PAD_FRACTION = 0.02
DPI = 160
FIG_SIZE = (12, 10)

# ── DOM-TOM panels ─────────────────────────────────────────────
# Two thin-framed panels along the left of the figure, classic-map style:
# one for the five DROM (full departments), one for the other territories.
# Each territory gets its own cell with a tight window around its measured
# lon/lat extent so the islands render large and legible.
# 984 (TAAF) and 989 (Clipperton) are uninhabited and omitted.
DROM = "DROM"
COM = "COM & territoires"

# code -> (xmin, xmax, ymin, ymax) window around the territory's extent.
OVERSEAS_WINDOWS = {
    # DROM — départements et régions d'outre-mer
    "971": (-61.9, -60.9, 15.7, 16.6),  # Guadeloupe
    "972": (-61.3, -60.7, 14.3, 15.0),  # Martinique
    "973": (-54.7, -51.5, 2.0, 5.8),  # Guyane
    "974": (55.1, 55.9, -21.5, -20.8),  # La Réunion
    "976": (44.9, 45.4, -13.1, -12.5),  # Mayotte
    # COM & autres territoires
    "975": (-56.5, -56.0, 46.6, 47.2),  # Saint-Pierre-et-Miquelon
    "977": (-63.1, -62.7, 17.8, 18.1),  # Saint-Martin
    "978": (-63.3, -62.9, 17.9, 18.2),  # Saint-Barthélemy
    "986": (-178.3, -176.1, -14.5, -13.1),  # Wallis-et-Futuna
    "987": (-154.8, -134.4, -28.0, -7.8),  # Polynésie française
    "988": (163.5, 168.2, -23.0, -19.4),  # Nouvelle-Calédonie
}

TERRITORY_GROUP = {
    "971": DROM,
    "972": DROM,
    "973": DROM,
    "974": DROM,
    "976": DROM,
    "975": COM,
    "977": COM,
    "978": COM,
    "986": COM,
    "987": COM,
    "988": COM,
}
UNINHABITED_OVERSEAS = frozenset({"984", "989"})  # TAAF, Clipperton: dropped

# Panel placement in figure coords (x, y, width, height). The left strip stays
# clear of the mainland (France starts around x≈0.24 of the figure width).
DROM_PANEL = (0.02, 0.52, 0.20, 0.30)
COM_PANEL = (0.02, 0.13, 0.20, 0.37)
CELL_COLS = 2
CELL_GAP = 0.008
CELL_PAD = 0.01
PANEL_HEADER = 0.024


def inset_group(department_code: str) -> str | None:
    """Map a department code to its DOM-TOM panel (``DROM``/``COM``), else None."""
    return TERRITORY_GROUP.get(department_code)


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


def load_geometry(conn: duckdb.DuckDBPyConnection) -> gpd.GeoDataFrame:
    """Fetch current commune polygons from the catalog as a GeoDataFrame."""
    df = conn.execute(
        f"""
        SELECT
            commune_id              AS code,
            name,
            department_code,
            geometry_wkb            AS wkb
        FROM {CATALOG_TABLE}
        WHERE is_current
        ORDER BY commune_id
        """
    ).fetch_df()
    df["geometry"] = df["wkb"].apply(lambda wkb: shapely.wkb.loads(bytes(wkb)))
    gdf = gpd.GeoDataFrame(
        df.drop(columns=["wkb"]), geometry="geometry", crs="EPSG:4326"
    )
    gdf.geometry = gdf.geometry.simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
    return gdf


def load_visited(parquet_path: Path) -> set[str]:
    """Commune ids marked as visited, from the dashboard master parquet."""
    df = pl.read_parquet(str(parquet_path))
    return set(df.filter(pl.col("visited") == True)["id"])  # noqa: E712


def plot_layer(gdf: gpd.GeoDataFrame, ax: matplotlib.axes.Axes, color: str) -> None:
    gdf.plot(ax=ax, color=color, edgecolor=BOUNDARY_COLOR, linewidth=BOUNDARY_WIDTH)


def draw_territory_cell(
    fig: plt.Figure,
    gdf: gpd.GeoDataFrame,
    visited_ids: set[str],
    code: str,
    rect: tuple[float, float, float, float],
) -> None:
    """Draw one overseas territory enlarged into its own small map cell."""
    ax = fig.add_axes(rect)
    subset = gdf[gdf["department_code"] == code]
    if subset.empty:
        return
    xmin, xmax, ymin, ymax = OVERSEAS_WINDOWS[code]
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_aspect("equal")
    ax.set_axis_off()
    plot_layer(subset, ax, UNVISITED_COLOR)
    cell_visited = subset[subset["code"].isin(visited_ids)]
    if not cell_visited.empty:
        plot_layer(cell_visited, ax, VISITED_COLOR)
    ax.set_frame_on(True)
    for spine in ax.spines.values():
        spine.set_color("0.6")
        spine.set_linewidth(0.4)
    fig.text(
        rect[0] + 0.004,
        rect[1] + rect[3] - 0.005,
        code,
        ha="left",
        va="top",
        fontsize=6,
        color="0.3",
    )


def draw_panel(
    fig: plt.Figure,
    gdf: gpd.GeoDataFrame,
    visited_ids: set[str],
    title: str,
    codes: list[str],
    rect: tuple[float, float, float, float],
) -> None:
    """Draw a thin-framed panel of territory cells (``code`` per cell)."""
    x, y, w, h = rect
    rows = math.ceil(len(codes) / CELL_COLS)
    cell_w = (w - 2 * CELL_PAD - (CELL_COLS - 1) * CELL_GAP) / CELL_COLS
    cell_h = (h - PANEL_HEADER - 2 * CELL_PAD - (rows - 1) * CELL_GAP) / rows
    for index, code in enumerate(codes):
        col, row = divmod(index, CELL_COLS)
        cx = x + CELL_PAD + col * (cell_w + CELL_GAP)
        cy = y + CELL_PAD + (rows - 1 - row) * (cell_h + CELL_GAP)
        draw_territory_cell(fig, gdf, visited_ids, code, (cx, cy, cell_w, cell_h))
    fig.patches.append(
        matplotlib.patches.Rectangle(
            (x, y),
            w,
            h,
            fill=False,
            edgecolor="0.2",
            linewidth=0.8,
            transform=fig.transFigure,
        )
    )
    fig.text(
        x + w / 2,
        y + h + 0.005,
        title,
        ha="center",
        va="bottom",
        fontsize=8,
        fontweight="bold",
    )


def render_figure(
    gdf: gpd.GeoDataFrame,
    visited_ids: set[str],
    out_path: Path,
) -> Path:
    """Draw the choropleth and save it to ``out_path``."""
    mainland = gdf[
        gdf["department_code"].map(inset_group).isna()
        & ~gdf["department_code"].isin(UNINHABITED_OVERSEAS)
    ]
    metro = mainland.to_crs(METRO_CRS)
    metro = metro.assign(visited=metro["code"].isin(visited_ids))

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    bounds = metro.total_bounds
    pad_x = (bounds[2] - bounds[0]) * PAD_FRACTION
    pad_y = (bounds[3] - bounds[1]) * PAD_FRACTION
    ax.set_xlim(bounds[0] - pad_x, bounds[2] + pad_x)
    ax.set_ylim(bounds[1] - pad_y, bounds[3] + pad_y)
    ax.set_aspect("equal")
    ax.set_axis_off()

    plot_layer(metro[~metro["visited"]], ax, UNVISITED_COLOR)
    plot_layer(metro[metro["visited"]], ax, VISITED_COLOR)

    draw_panel(
        fig,
        gdf,
        visited_ids,
        DROM,
        ["971", "972", "973", "974", "976"],
        DROM_PANEL,
    )
    draw_panel(
        fig,
        gdf,
        visited_ids,
        COM,
        ["975", "977", "978", "986", "987", "988"],
        COM_PANEL,
    )

    fig.suptitle(
        f"Visited communes ({len(visited_ids)} / {len(gdf)})", y=0.97, fontsize=14
    )
    fig.legend(
        handles=[
            plt.Line2D(
                [0],
                [0],
                marker="s",
                color="w",
                markerfacecolor=VISITED_COLOR,
                markersize=10,
                label="Visited",
            ),
            plt.Line2D(
                [0],
                [0],
                marker="s",
                color="w",
                markerfacecolor=UNVISITED_COLOR,
                markersize=10,
                label="Remaining",
            ),
        ],
        loc="lower right",
        frameon=False,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        out_path,
        dpi=DPI,
        bbox_inches="tight",
        pad_inches=0.2,
        facecolor="white",
    )
    plt.close(fig)
    return out_path


def connect_polaris() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("LOAD spatial;")
    conn.execute(
        "CREATE SECRET polaris_secret (TYPE iceberg, "
        f"CLIENT_ID '{os.environ['POLARIS_CLIENT_ID']}', "
        f"CLIENT_SECRET '{os.environ['POLARIS_CLIENT_SECRET']}', "
        "ENDPOINT 'http://localhost:8181/api/catalog')"
    )
    conn.execute(
        "ATTACH 'french_towns' AS polaris (TYPE iceberg, "
        "ENDPOINT 'http://localhost:8181/api/catalog', SECRET 'polaris_secret')"
    )
    return conn


def main() -> None:
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

    render_figure(gdf, visited, args.output)
    size_kb = args.output.stat().st_size / 1024
    print(
        f"✅ {args.output} — {len(visited):,} visited / {len(gdf):,} "
        f"communes ({size_kb:.0f} KiB)"
    )


if __name__ == "__main__":
    main()
