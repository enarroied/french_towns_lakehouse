"""Render a lightweight sign-coloured map of visited vs unvisited communes.

Produces a static PNG (matplotlib) with all commune polygons from the
lakehouse (Polaris ``dim_geography``): visited communes are green, the rest
red. DOM-TOM territories are drawn as small inset boxes, classic-map style.

Pure helpers (``inset_group``, ``render_figure``) are DB- and IO-free so they
can be unit-tested without a Polaris catalog; ``main`` is a thin glue layer
that fetches geometry and the visited set, runs sanity checks, and saves the
image.

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

# DOM-TOM groupings with fixed (xmin, xmax, ymin, ymax) lon/lat windows so the
# island communes remain visible instead of shrinking inside whole-ocean bounds.
# 984 (TAAF) and 989 (Clipperton) are uninhabited and dropped to avoid distortion.
# Position = inset box placement in figure coordinates.
INSETS = {
    "americas": (
        {"971", "972", "973", "977", "978"},
        (0.015, 0.06, 0.15, 0.16),
        (-63.7, -50.9, 1.6, 18.9),
    ),
    "indian": (
        {"974", "976"},
        (0.015, 0.235, 0.15, 0.125),
        (44.7, 56.1, -21.9, -12.1),
    ),
    "polynesia": (
        {"987"},
        (0.015, 0.375, 0.15, 0.14),
        (-155.4, -133.6, -28.6, -7.0),
    ),
    "caledonia": (
        {"988"},
        (0.015, 0.53, 0.15, 0.12),
        (163.1, 168.6, -23.4, -19.0),
    ),
    "wallis": (
        {"986"},
        (0.015, 0.66, 0.13, 0.10),
        (-178.7, -175.8, -14.9, -12.8),
    ),
    "atl": (
        {"975"},
        (0.015, 0.78, 0.12, 0.085),
        (-56.7, -55.8, 46.4, 47.4),
    ),
}

UNINHABITED_OVERSEAS = {"984", "989"}  # TAAF and Clipperton: dropped, not inset-able
GROUP_BY_CODE = {
    code: group for group, (codes, _, _) in INSETS.items() for code in codes
} | dict.fromkeys(UNINHABITED_OVERSEAS, "uninhabited")
WINDOWS = {group: window for group, (_, _, window) in INSETS.items()}


def inset_group(department_code: str) -> str | None:
    """Map a department code to the DOM-TOM inset it belongs to (or None)."""
    return GROUP_BY_CODE.get(department_code)


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


def render_figure(
    gdf: gpd.GeoDataFrame,
    visited_ids: set[str],
    out_path: Path,
) -> Path:
    """Draw the choropleth and save it to ``out_path``."""
    mainland = gdf[gdf["department_code"].map(inset_group).isna()]
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

    for group, (codes, position, _window) in INSETS.items():
        subset = gdf[gdf["department_code"].isin(codes)]
        if subset.empty:
            continue
        inset = fig.add_axes(position)
        xmin, xmax, ymin, ymax = WINDOWS[group]
        inset.set_xlim(xmin, xmax)
        inset.set_ylim(ymin, ymax)
        inset.set_aspect("equal")
        inset.set_axis_off()
        inset_visited = subset[subset["code"].isin(visited_ids)]
        plot_layer(subset, inset, UNVISITED_COLOR)
        if not inset_visited.empty:
            plot_layer(inset_visited, inset, VISITED_COLOR)
        inset.set_title(group, fontsize=7, pad=2)

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
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight", facecolor="white")
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
