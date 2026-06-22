"""Generate a GeoPackage of commune centroids for QField data collection.

Produces:
  out/communes.gpkg — Point layer with id, name, department_code,
                      department_name, visited (bool), visit_date, photo

Usage:
  uv run python generate_qfield/communes/generate_communes_gpkg.py
  uv run python generate_qfield/communes/generate_communes_gpkg.py --department 75
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb


OUT_DIR = Path(__file__).parent / "out"
GPKG_PATH = OUT_DIR / "communes.gpkg"


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

    where = f"WHERE department_code = '{department}'" if department else ""

    conn.execute(f"""
        COPY (
            SELECT c.id::VARCHAR(5) AS id,
                   c.name::VARCHAR(255) AS name,
                   c.department_code::VARCHAR(3) AS department_code,
                   c.department_name::VARCHAR(255) AS department_name,
                   g.centroid AS geom,
                   FALSE AS visited,
                   NULL::DATE AS visit_date,
                   NULL::VARCHAR AS photo
            FROM read_parquet('s3://validated/dim_communes.parquet') c
            JOIN read_parquet('s3://validated/dim_geography.parquet') g
                ON c.id = g.commune_id
            {where}
        ) TO '{GPKG_PATH}' (FORMAT GDAL, DRIVER 'GPKG')
    """)

    count = conn.execute(
        f"SELECT count(*) FROM read_parquet('s3://validated/dim_communes.parquet') {where}"
    ).fetchone()[0]
    conn.close()
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a GeoPackage of commune centroids for QField"
    )
    parser.add_argument(
        "--department",
        "-d",
        help="Filter by department code (e.g. 75). Omit for all communes.",
    )
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    num = create_gpkg(args.department)
    print(f"{num} communes → {GPKG_PATH}" if num else "No data found.")


if __name__ == "__main__":
    main()
