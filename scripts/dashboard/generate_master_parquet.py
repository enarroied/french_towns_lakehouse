"""Merge QField GeoPackage visited data with lakehouse gold → master parquet.

Produces:
  data/dashboard/visited_towns.parquet — commune-level data
  data/dashboard/departments.geojson  — department boundaries for choropleth
    (generated separately, see scripts/dashboard/generate_departments_geojson.py)
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import duckdb
from dotenv import find_dotenv
from dotenv import load_dotenv


logger = logging.getLogger(__name__)

GITHUB_OWNER = "enarroied"
GITHUB_REPO = "french_towns_lakehouse"
GITHUB_BRANCH = "master"
THUMB_BASE_URL = (
    f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}"
    "/blog/data/img"
)
DEFAULT_GPKG = Path.home() / "QField" / "cloud" / "communes_qfield" / "communes.gpkg"
POLARIS_CATALOG_URL = "http://localhost:8181/api/catalog"
POLARIS_CATALOG_NAME = "french_towns"
POPULATION_YEAR = 2023

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "dashboard"
OUTPUT_PARQUET = OUTPUT_DIR / "visited_towns.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge QField GeoPackage visited data with lakehouse gold"
    )
    parser.add_argument(
        "--gpkg",
        type=Path,
        default=DEFAULT_GPKG,
        help=f"Path to the QField GeoPackage (default: {DEFAULT_GPKG})",
    )
    return parser.parse_args()


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def ensure_extension(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    """Load a DuckDB extension, installing it on first use if necessary."""
    try:
        conn.execute(f"LOAD {name};")
    except duckdb.IOException:
        conn.execute(f"INSTALL {name};")
        conn.execute(f"LOAD {name};")


def connect_to_polaris() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the iceberg/spatial extensions and Polaris attached."""
    client_id = require_env("POLARIS_CLIENT_ID")
    client_secret = require_env("POLARIS_CLIENT_SECRET")

    conn = duckdb.connect()
    ensure_extension(conn, "iceberg")
    ensure_extension(conn, "spatial")
    conn.execute(
        """
        CREATE SECRET polaris_secret (
            TYPE iceberg,
            CLIENT_ID $client_id,
            CLIENT_SECRET $client_secret,
            ENDPOINT $endpoint
        )
        """,
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "endpoint": POLARIS_CATALOG_URL,
        },
    )
    conn.execute(
        f"""
        ATTACH '{POLARIS_CATALOG_NAME}' AS polaris (
            TYPE iceberg,
            ENDPOINT '{POLARIS_CATALOG_URL}',
            SECRET 'polaris_secret'
        )
        """
    )
    return conn


def load_visited_from_gpkg(conn: duckdb.DuckDBPyConnection, gpkg_path: Path) -> None:
    """Load the 'visited' rows from the QField GeoPackage into a temp table."""
    if not gpkg_path.exists():
        raise SystemExit(f"GeoPackage not found: {gpkg_path}")

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE visited AS
        SELECT
            id,
            visited,
            visit_date,
            photo,
            "YouTube URL" AS youtube_url,
            "Medium URL" AS medium_url
        FROM ST_READ($gpkg_path)
        WHERE visited IS TRUE
        """,
        {"gpkg_path": str(gpkg_path)},
    )


def build_commune_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Join lakehouse gold tables with the visited temp table into commune_data."""
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE commune_data AS
        WITH communes AS (
            SELECT
                c.id,
                c.name,
                c.department_code,
                c.department_name,
                c.region_name,
                c.territory_type,
                g.area_km2,
                ST_Y(ST_GeomFromWKB(g.centroid_wkb)) AS latitude,
                ST_X(ST_GeomFromWKB(g.centroid_wkb)) AS longitude,
                g.is_mountain,
                g.is_island_commune,
                p.population
            FROM polaris.lakehouse.dim_communes c
            JOIN polaris.lakehouse.dim_geography g ON c.id = g.commune_id
            LEFT JOIN polaris.lakehouse.fact_population p
                ON c.id = p.id AND p.year = $population_year
            WHERE c.is_current
        )
        SELECT
            c.*,
            v.visited,
            v.visit_date,
            v.photo,
            v.youtube_url,
            v.medium_url,
            CASE
                WHEN v.photo IS NOT NULL AND starts_with(v.photo, 'DCIM/') THEN
                    $thumb_base || '/' || regexp_replace(v.photo, '^DCIM/', '')
                WHEN v.photo IS NOT NULL THEN v.photo
                ELSE NULL
            END AS photo_thumbnail_url
        FROM communes c
        LEFT JOIN visited v ON c.id = v.id
        ORDER BY c.department_code, c.name
        """,
        {"population_year": POPULATION_YEAR, "thumb_base": THUMB_BASE_URL},
    )


def export_parquet(
    conn: duckdb.DuckDBPyConnection, output_path: Path
) -> tuple[int, int]:
    """Write commune_data to parquet and return (total_rows, visited_rows)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(
        "COPY commune_data TO $path (FORMAT PARQUET)",
        {"path": str(output_path)},
    )
    total, visited = conn.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE visited IS TRUE) FROM commune_data"
    ).fetchone()
    return total, visited


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_dotenv(find_dotenv())
    args = parse_args()

    conn = connect_to_polaris()
    try:
        load_visited_from_gpkg(conn, args.gpkg)
        build_commune_data(conn)
        total, visited = export_parquet(conn, OUTPUT_PARQUET)
    finally:
        conn.close()

    logger.info(
        "✅ %s — %s communes (%s visited)", OUTPUT_PARQUET, f"{total:,}", f"{visited:,}"
    )
    logger.info(
        "ℹ️  Department GeoJSON skipped — run "
        "scripts/dashboard/generate_departments_geojson.py separately"
    )


if __name__ == "__main__":
    main()
