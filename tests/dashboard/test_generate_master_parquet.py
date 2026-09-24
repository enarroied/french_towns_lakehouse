"""Tests for the master-parquet generator (QField GeoPackage + lakehouse).

All heavy lifting (SQL joins, transformations, exports) runs against an
in-memory duckdb connection with a fake ``polaris.lakehouse`` catalog and a
minimal GeoPackage, so no live Polaris or network is required. ``main`` and
``connect_to_polaris`` stay untested (thin glue).

Note: ``build_commune_data`` LEFT JOINs the ``visited`` temp table created by
``load_visited_from_gpkg`` — tests that build commune data must seed that
table first (via the loader or ``_seed_visited``).
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import duckdb
import polars as pl
import pytest
from shapely import wkb
from shapely.geometry import Point


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def mp_mod():
    path = REPO_ROOT / "scripts" / "dashboard" / "generate_master_parquet.py"
    spec = importlib.util.spec_from_file_location("generate_master_parquet", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── Fake lakehouse fixtures ──────────────────────────────────────────


COMMUNES = [
    ("01001", "Bourg", "01", "Ain", "Auvergne-Rhône-Alpes", "commune", True),
    ("85011", "Barbâtre", "85", "Vendée", "Pays de la Loire", "commune", True),
    (
        "97701",
        "St-Barth",
        "977",
        "Saint-Barthélemy",
        "Outre-mer",
        "collectivité d'outre-mer",
        True,
    ),
    ("99999", "Départie", "01", "Ain", "Auvergne-Rhône-Alpes", "commune", False),
]

GEOGRAPHY = [
    ("01001", 100.5, wkb.dumps(Point(5.0, 46.0)), True, False),
    ("85011", 50.0, wkb.dumps(Point(-2.0, 46.5)), False, True),
    ("97701", 25.0, wkb.dumps(Point(-62.8, 17.9)), False, False),
    ("99999", 10.0, wkb.dumps(Point(2.0, 45.0)), False, False),
]

POPULATION = [
    ("01001", 2023, 5000),
    ("01001", 1999, 4000),
    ("85011", 2023, 1000),
]


@pytest.fixture
def conn(mp_mod) -> duckdb.DuckDBPyConnection:
    """In-memory duckdb connection with a fake ``polaris.lakehouse`` catalog."""
    connection = duckdb.connect()
    mp_mod.ensure_extension(connection, "spatial")
    connection.execute("ATTACH ':memory:' AS polaris (TYPE duckdb)")
    connection.execute("CREATE SCHEMA polaris.lakehouse")
    connection.execute(
        """
        CREATE TABLE polaris.lakehouse.dim_communes (
            id VARCHAR,
            name VARCHAR,
            department_code VARCHAR,
            department_name VARCHAR,
            region_name VARCHAR,
            territory_type VARCHAR,
            is_current BOOLEAN
        )
        """
    )
    connection.executemany(
        "INSERT INTO polaris.lakehouse.dim_communes VALUES (?,?,?,?,?,?,?)",
        COMMUNES,
    )
    connection.execute(
        """
        CREATE TABLE polaris.lakehouse.dim_geography (
            commune_id VARCHAR,
            area_km2 DOUBLE,
            centroid_wkb BLOB,
            is_mountain BOOLEAN,
            is_island_commune BOOLEAN
        )
        """
    )
    connection.executemany(
        "INSERT INTO polaris.lakehouse.dim_geography VALUES (?,?,?,?,?)",
        GEOGRAPHY,
    )
    connection.execute(
        """
        CREATE TABLE polaris.lakehouse.fact_population (
            id VARCHAR,
            year INTEGER,
            population BIGINT
        )
        """
    )
    connection.executemany(
        "INSERT INTO polaris.lakehouse.fact_population VALUES (?,?,?)",
        POPULATION,
    )
    yield connection
    connection.close()


def _make_gpkg(
    path: Path,
    rows: list[tuple],
) -> None:
    """Build a minimal valid GeoPackage readable by ST_READ."""
    db = sqlite3.connect(str(path))
    db.executescript(
        """
        CREATE TABLE gpkg_spatial_ref_sys (
            srs_name TEXT, srs_id INTEGER, organization TEXT,
            organization_coordsys_id INTEGER, definition TEXT, description TEXT
        );
        INSERT INTO gpkg_spatial_ref_sys VALUES ('WGS 84', 4326, 'EPSG', 4326, '', '');
        CREATE TABLE gpkg_contents (
            table_name TEXT, data_type TEXT, identifier TEXT, description TEXT,
            last_change DATETIME, min_x DOUBLE, min_y DOUBLE, max_x DOUBLE,
            max_y DOUBLE, srs_id INTEGER
        );
        INSERT INTO gpkg_contents VALUES (
            'communes', 'features', 'communes', '', '2023-01-01',
            0, 0, 0, 0, 4326
        );
        CREATE TABLE gpkg_geometry_columns (
            table_name TEXT, column_name TEXT, geometry_type_name TEXT,
            srs_id INTEGER, z INTEGER, m INTEGER
        );
        INSERT INTO gpkg_geometry_columns VALUES ('communes', 'geom', 'POINT', 4326, 0, 0);
        CREATE TABLE gpkg_ogr_contents (table_name TEXT, feature_count INTEGER);
        CREATE TABLE communes (
            id TEXT, visited INTEGER, visit_date TEXT, photo TEXT,
            "YouTube URL" TEXT, "Medium URL" TEXT, geom BLOB
        );
        """
    )
    db.executemany(
        "INSERT INTO communes VALUES (?,?,?,?,?,?,NULL)",
        rows,
    )
    db.commit()
    db.close()


def _seed_visited(conn: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    """Create the ``visited`` temp table directly (bypassing the gpkg loader)."""
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE visited ("
        "id VARCHAR, visited BOOLEAN, visit_date VARCHAR, photo VARCHAR, "
        "youtube_url VARCHAR, medium_url VARCHAR)"
    )
    conn.executemany(
        "INSERT INTO visited VALUES (?,?,?,?,?,?)",
        rows,
    )


# ── GeoPackage loader ────────────────────────────────────────────────


def test_load_visited_from_gpkg_filters_and_renames(mp_mod, conn, tmp_path):
    gpkg = tmp_path / "project.gpkg"
    _make_gpkg(
        gpkg,
        [
            (
                "01001",
                1,
                "2023-06-01",
                "DCIM/2023/a.jpg",
                "https://youtu.be/a",
                "https://medium.com/a",
            ),
            ("85011", 0, None, None, None, None),
            ("77777", 1, "2023-08-01", "DCIM/b.jpg", None, None),
        ],
    )

    mp_mod.load_visited_from_gpkg(conn, gpkg)

    rows = conn.execute(
        "SELECT id, visited, visit_date, photo, youtube_url, medium_url "
        "FROM visited ORDER BY id"
    ).fetchall()

    assert rows == [
        (
            "01001",
            True,
            "2023-06-01",
            "DCIM/2023/a.jpg",
            "https://youtu.be/a",
            "https://medium.com/a",
        ),
        ("77777", True, "2023-08-01", "DCIM/b.jpg", None, None),
    ]


def test_load_visited_from_gpkg_missing_file_raises(mp_mod, conn, tmp_path):
    with pytest.raises(SystemExit, match="GeoPackage not found"):
        mp_mod.load_visited_from_gpkg(conn, tmp_path / "missing.gpkg")


# ── Commune data build ───────────────────────────────────────────────


def _build_with_gpkg(mp_mod, conn, tmp_path):
    gpkg = tmp_path / "project.gpkg"
    _make_gpkg(
        gpkg,
        [
            (
                "01001",
                1,
                "2023-06-01",
                "DCIM/2023/a.jpg",
                "https://youtu.be/a",
                "https://medium.com/a",
            ),
            (
                "85011",
                1,
                "2023-07-01",
                "https://already.tld/p.png",
                "https://youtu.be/b",
                "",
            ),
            ("97701", 0, None, None, None, None),
            ("77777", 1, "2023-08-01", "DCIM/b.jpg", None, None),
        ],
    )
    mp_mod.load_visited_from_gpkg(conn, gpkg)
    mp_mod.build_commune_data(conn)
    return gpkg


def test_build_commune_data_joins_and_transforms(mp_mod, conn, tmp_path):
    _build_with_gpkg(mp_mod, conn, tmp_path)

    rows = conn.execute(
        "SELECT id, name, department_code, department_name, region_name, "
        "territory_type, visited, population, latitude, longitude, "
        "is_mountain, is_island_commune "
        "FROM commune_data ORDER BY department_code, name"
    ).fetchall()

    assert [r[0] for r in rows] == ["01001", "85011", "97701"]

    bourg = rows[0]
    assert bourg[1:6] == (
        "Bourg",
        "01",
        "Ain",
        "Auvergne-Rhône-Alpes",
        "commune",
    )
    assert bourg[6]  # visited (gpkg stores it as int 1)
    assert bourg[7:12] == (5000, 46.0, 5.0, True, False)  # 2023 pop, not 1999

    barbatre = rows[1]
    assert barbatre[1:6] == ("Barbâtre", "85", "Vendée", "Pays de la Loire", "commune")
    assert barbatre[7:12] == (1000, 46.5, -2.0, False, True)

    stbarth = rows[2]
    assert stbarth[5] == "collectivité d'outre-mer"
    assert stbarth[6] is None  # unvisited commune retained by the LEFT JOIN
    assert stbarth[7] is None  # no population row
    assert stbarth[8:10] == (17.9, -62.8)


def test_build_commune_data_photo_thumbnail_url_case(mp_mod, conn, tmp_path):
    _seed_visited(
        conn,
        [
            ("01001", True, "2023-06-01", "DCIM/2023/a.jpg", None, None),
            ("85011", True, "2023-07-01", "https://already.tld/p.png", None, None),
        ],
    )
    mp_mod.build_commune_data(conn)

    rows = conn.execute(
        "SELECT id, photo, photo_thumbnail_url FROM commune_data ORDER BY id"
    ).fetchall()

    by_id = {r[0]: r for r in rows}
    assert by_id["01001"][2] == f"{mp_mod.THUMB_BASE_URL}/2023/a.jpg"
    assert by_id["85011"][2] == "https://already.tld/p.png"
    assert by_id["97701"][2] is None


# ── Parquet export ───────────────────────────────────────────────────


def test_export_parquet_writes_file_and_counts(mp_mod, conn, tmp_path):
    _build_with_gpkg(mp_mod, conn, tmp_path)
    out = tmp_path / "out" / "visited_towns.parquet"

    total, visited = mp_mod.export_parquet(conn, out)

    assert (total, visited) == (3, 2)
    assert out.exists()

    df = pl.read_parquet(out)
    assert df.height == 3
    # duckdb writes the gpkg int flag: 1 = visited, NULL otherwise
    assert df.filter(pl.col("visited") == 1).height == 2
    assert df.filter(pl.col("id") == "01001")["population"][0] == 5000
    assert df.filter(pl.col("id") == "97701")["visited"][0] is None


# ── Env validation ───────────────────────────────────────────────────


def test_require_env_missing_raises(mp_mod, monkeypatch):
    monkeypatch.delenv("POLARIS_MISSING_ENV_VAR", raising=False)

    with pytest.raises(SystemExit, match="Missing required environment variable"):
        mp_mod.require_env("POLARIS_MISSING_ENV_VAR")


def test_require_env_returns_value(mp_mod, monkeypatch):
    monkeypatch.setenv("POLARIS_PRESENT_ENV_VAR", "sekret")

    assert mp_mod.require_env("POLARIS_PRESENT_ENV_VAR") == "sekret"
