"""Merge QField GeoPackage visited data with lakehouse gold → master parquet.

Produces:
  data/dashboard/visited_towns.parquet — commune-level data
  data/dashboard/departments.geojson  — department boundaries for choropleth
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
from dotenv import find_dotenv
from dotenv import load_dotenv


DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "dashboard"
PARQUET_PATH = DATA_DIR / "visited_towns.parquet"
GEOJSON_PATH = DATA_DIR / "departments.geojson"
GITHUB_OWNER = "enarroied"
GITHUB_REPO = "french_towns_lakehouse"
GITHUB_BRANCH = "main"
THUMB_BASE = (
    f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}"
    "/blog/data/img"
)
GPKG_PATH = "generate_qfield/communes/out/communes.gpkg"


def main() -> None:
    load_dotenv(find_dotenv())
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()
    conn.execute("LOAD iceberg;")
    conn.execute("LOAD spatial;")
    conn.execute(f"""
        CREATE SECRET polaris_secret (
            TYPE iceberg,
            CLIENT_ID '{os.environ["POLARIS_CLIENT_ID"]}',
            CLIENT_SECRET '{os.environ["POLARIS_CLIENT_SECRET"]}',
            ENDPOINT 'http://localhost:8181/api/catalog'
        )
    """)
    conn.execute("""
        ATTACH 'french_towns' AS polaris (
            TYPE iceberg,
            ENDPOINT 'http://localhost:8181/api/catalog',
            SECRET 'polaris_secret'
        )
    """)

    conn.execute(f"""
        CREATE TABLE visited AS
        SELECT
            id,
            visited,
            visit_date,
            photo,
            "YouTube URL" AS youtube_url,
            "Medium URL" AS medium_url
        FROM ST_READ('{GPKG_PATH}')
        WHERE visited IS TRUE
    """)

    conn.execute(f"""
        CREATE TEMP TABLE commune_data AS
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
                ON c.id = p.id AND p.year = 2023
            WHERE c.is_current
        )
        SELECT
            c.*,
            v.visited,
            v.visit_date,
            v.photo,
            v.youtube_url,
            v.medium_url,
            CASE WHEN v.photo IS NOT NULL THEN
                '{THUMB_BASE}/' || regexp_replace(v.photo, '^DCIM/', '')
            ELSE NULL END AS photo_thumbnail_url
        FROM communes c
        LEFT JOIN visited v ON c.id = v.id
        ORDER BY c.department_code, c.name
    """)

    conn.execute(f"COPY commune_data TO '{PARQUET_PATH}' (FORMAT PARQUET)")

    total = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{PARQUET_PATH}')"
    ).fetchone()[0]
    visited = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{PARQUET_PATH}') WHERE visited IS TRUE"
    ).fetchone()[0]

    print(f"✅ {PARQUET_PATH} — {total:,} communes ({visited:,} visited)")
    print(
        "ℹ️  Department GeoJSON skipped — run scripts/dashboard/generate_departments_geojson.py separately"
    )
    conn.close()


if __name__ == "__main__":
    main()
