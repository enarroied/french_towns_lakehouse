"""Generate a GeoPackage of monument points for QField data collection.

Produces:
  out/monuments.gpkg — Point layer with reference, name, domain,
                       protection_level, commune_name, department,
                       visited (bool), photo (varchar)

Usage:
  uv run python generate_qfield/monuments/generate_monuments_gpkg.py
  uv run python generate_qfield/monuments/generate_monuments_gpkg.py --department 75
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb


OUT_DIR = Path(__file__).parent / "out"
GPKG_PATH = OUT_DIR / "monuments.gpkg"


def create_gpkg(department: str | None) -> int:
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute(
        "CREATE SECRET minio_secret (TYPE s3, PROVIDER config, "
        f"KEY_ID '{os.environ.get('AWS_ACCESS_KEY_ID', 'eric')}', "
        f"SECRET '{os.environ.get('AWS_SECRET_ACCESS_KEY', 'eric1234')}', "
        f"ENDPOINT '{os.environ.get('AWS_ENDPOINT', 'localhost:19000')}', "
        "REGION 'us-east-1', USE_SSL false, URL_STYLE 'path')"
    )

    where = f"WHERE c.department_code = '{department}'" if department else ""

    conn.execute(f"""
        COPY (
            SELECT DISTINCT ON (m.reference)
                m.reference::VARCHAR(10)          AS reference,
                m.name::VARCHAR(255)              AS name,
                m.domain::VARCHAR(100)            AS domain,
                m.denomination::VARCHAR(255)      AS denomination,
                m.protection_level::VARCHAR(10)   AS protection_level,
                m.nature::VARCHAR(50)             AS nature,
                m.century::VARCHAR(255)           AS century,
                m.longitude::DOUBLE               AS longitude,
                m.latitude::DOUBLE                AS latitude,
                c.name::VARCHAR(255)              AS commune_name,
                c.department_code::VARCHAR(3)     AS department_code,
                c.department_name::VARCHAR(255)   AS department_name,
                ST_Point(m.longitude, m.latitude) AS geom,
                FALSE                             AS visited,
                NULL::VARCHAR                     AS photo
            FROM read_parquet('s3://validated/dim_monument.parquet') m
            JOIN read_parquet('s3://validated/bridge_monument_communes.parquet') b
                ON m.reference = b.monument_reference
            JOIN read_parquet('s3://validated/dim_communes.parquet') c
                ON b.commune_code = c.id
            {where}
            ORDER BY m.reference, b.is_primary DESC
        ) TO '{GPKG_PATH}' (FORMAT GDAL, DRIVER 'GPKG')
    """)

    count = conn.execute(
        "SELECT count(*) FROM read_parquet('s3://validated/dim_monument.parquet')"
    ).fetchone()[0]
    conn.close()
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a GeoPackage of monument points for QField"
    )
    parser.add_argument(
        "--department",
        "-d",
        help="Filter by department code (e.g. 75). Omit for all monuments.",
    )
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    num = create_gpkg(args.department)
    suffix = f" in department {args.department}" if args.department else ""
    print(f"{num} monuments{suffix} → {GPKG_PATH}" if num else "No data found.")


if __name__ == "__main__":
    main()
