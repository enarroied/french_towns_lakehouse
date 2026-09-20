"""Generate a GeoPackage of commune centroids for QField data collection.

Produces:
  <project-dir>/communes.gpkg — Point layer with id, name, department_code,
                       department_name, visited (bool), visit_date, photo

Usage:
  uv run python generate_qfield/communes/generate_communes_gpkg.py
  uv run python generate_qfield/communes/generate_communes_gpkg.py --department 75
  uv run python generate_qfield/communes/generate_communes_gpkg.py --project-dir ~/qgis_projects/communes --force
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path

import duckdb


DEFAULT_PROJECT_DIR = Path.home() / "qgis_projects" / "communes"


def has_field_data(gpkg_path: Path) -> bool:
    """Return True if the GeoPackage already contains collected field data."""
    if not gpkg_path.exists():
        return False
    conn = sqlite3.connect(str(gpkg_path))
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM communes WHERE visited = 1 OR photo IS NOT NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    return count > 0


def create_gpkg(department: str | None, project_dir: Path) -> int:
    out_dir = project_dir / "out"
    gpkg_path = out_dir / "communes.gpkg"

    if gpkg_path.exists() and has_field_data(gpkg_path):
        raise SystemExit(
            f"Refusing to overwrite {gpkg_path}: it contains collected field data "
            "(visited/photo rows). Use --force only if you really want to reset "
            "the project to a fresh baseline."
        )

    out_dir.mkdir(parents=True, exist_ok=True)

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
        ) TO '{gpkg_path}' (FORMAT GDAL, DRIVER 'GPKG')
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
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=DEFAULT_PROJECT_DIR,
        help=f"Output project directory (default: {DEFAULT_PROJECT_DIR})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing GeoPackage even if it contains field data",
    )
    args = parser.parse_args()

    num = create_gpkg(args.department, args.project_dir)
    print(
        f"{num} communes → {args.project_dir / 'out' / 'communes.gpkg'}"
        if num
        else "No data found."
    )


if __name__ == "__main__":
    main()
