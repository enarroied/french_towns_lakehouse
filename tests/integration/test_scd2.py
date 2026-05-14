"""Tests for flows_integration.shared.scd2 — column mapping and SCD2 merge logic."""

import duckdb
import pytest
from flows_integration.shared.scd2 import GEOMETRY_TYPES
from flows_integration.shared.scd2 import SCD_METADATA_COLS
from flows_integration.shared.scd2 import SCHEMA
from flows_integration.shared.scd2 import _map_columns
from flows_integration.shared.scd2 import _parquet_columns


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    c = duckdb.connect()
    yield c
    c.close()


def _describe_columns(
    conn: duckdb.DuckDBPyConnection, source: str
) -> list[tuple[str, str]]:
    """Helper: extract column names + base types via DESCRIBE (mirrors _parquet_columns logic)."""
    result = conn.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    return [(row[0], row[1].split("(")[0].strip()) for row in result]


# ---------------------------------------------------------------------------
# Column extraction (mirrors _parquet_columns logic without S3 dependency)
# ---------------------------------------------------------------------------


class TestColumnExtraction:
    def test_returns_column_names_and_types(self, conn):
        conn.execute("CREATE TABLE raw_test (id INTEGER, label VARCHAR)")
        conn.execute("INSERT INTO raw_test VALUES (1, 'x')")
        result = _describe_columns(conn, "raw_test")
        assert ("id", "INTEGER") in result
        assert ("label", "VARCHAR") in result

    def test_strips_type_parameters(self, conn):
        conn.execute("CREATE TABLE typed_test (val DECIMAL(10,2))")
        conn.execute("INSERT INTO typed_test VALUES (1.5)")
        result = _describe_columns(conn, "typed_test")
        assert result == [("val", "DECIMAL")]

    def test_empty_table_returns_columns(self, conn):
        conn.execute("CREATE TABLE empty_test (x INTEGER)")
        result = _describe_columns(conn, "empty_test")
        assert result == [("x", "INTEGER")]

    def test_parquet_columns_uses_s3_by_default(self):
        conn = duckdb.connect()
        with pytest.raises(duckdb.HTTPException, match="HTTP GET error"):
            _parquet_columns(conn, "nonexistent_table")
        conn.close()


# ---------------------------------------------------------------------------
# _map_columns
# ---------------------------------------------------------------------------


class TestMapColumns:
    def test_plain_columns(self):
        raw = [("name", "VARCHAR"), ("dept", "VARCHAR"), ("pop", "INTEGER")]
        plain, select_no_pref, select_pref, all_business, inc_hash, existing_hash = (
            _map_columns(raw)
        )
        assert plain == raw
        assert select_no_pref == ["name", "dept", "pop"]
        assert select_pref == ["inc.name", "inc.dept", "inc.pop"]
        assert all_business == ["name", "dept", "pop"]
        assert inc_hash == [
            "inc.name::VARCHAR",
            "inc.dept::VARCHAR",
            "inc.pop::VARCHAR",
        ]
        assert existing_hash == [
            "CAST(existing.name AS VARCHAR)",
            "CAST(existing.dept AS VARCHAR)",
            "CAST(existing.pop AS VARCHAR)",
        ]

    def test_geometry_columns_split_into_wkb_and_srid(self):
        raw = [("geom", "GEOMETRY"), ("name", "VARCHAR")]
        plain, select_no_pref, select_pref, all_business, inc_hash, existing_hash = (
            _map_columns(raw)
        )
        assert ("geom_wkb", "BINARY") in plain
        assert ("geom_srid", "INTEGER") in plain
        assert "ST_AsWKB(geom) AS geom_wkb" in select_no_pref
        assert "4326 AS geom_srid" in select_no_pref
        assert "ST_AsWKB(inc.geom) AS geom_wkb" in select_pref
        assert "4326 AS geom_srid" in select_pref
        assert "geom_wkb" in all_business
        assert "geom_srid" in all_business
        # Geometry hash uses ST_AsWKB, not geom_wkb directly
        assert any("ST_AsWKB" in h for h in inc_hash)
        assert any("geom_wkb" in h for h in existing_hash)

    def test_only_geometry(self):
        raw = [("geom", "GEOMETRY")]
        plain, *_ = _map_columns(raw)
        assert ("geom_wkb", "BINARY") in plain
        assert ("geom_srid", "INTEGER") in plain

    def test_empty_input(self):
        raw: list[tuple[str, str]] = []
        plain, select_no_pref, select_pref, all_business, inc_hash, existing_hash = (
            _map_columns(raw)
        )
        assert plain == []
        assert select_no_pref == []
        assert select_pref == []
        assert all_business == []
        assert inc_hash == []
        assert existing_hash == []

    def test_mixed_geometry_and_regular(self):
        raw = [("id", "INTEGER"), ("geom", "GEOMETRY"), ("label", "VARCHAR")]
        plain, select_no_pref, select_pref, all_business, inc_hash, existing_hash = (
            _map_columns(raw)
        )
        assert len(plain) == 4
        assert ("id", "INTEGER") in plain
        assert ("geom_wkb", "BINARY") in plain
        assert ("geom_srid", "INTEGER") in plain
        assert select_no_pref[0] == "id"
        assert select_pref[1] == "ST_AsWKB(inc.geom) AS geom_wkb"
        assert "geom_wkb" in all_business
        assert len(inc_hash) == 3
        assert len(existing_hash) == 3


# ---------------------------------------------------------------------------
# SCD2 merge logic (tested with real SQL operations using local tables)
# ---------------------------------------------------------------------------


class TestSCD2MergeLogic:
    """Test SCD2 SQL pattern — same logic as run_scd2() but without S3 dependency."""

    SCHEMA = "lakehouse"
    META = [
        "surrogate_key",
        "effective_date",
        "expiry_date",
        "is_current",
        "inserted_at",
    ]

    def _create_dim_table(self, conn, table_name, cols: list[str]):
        meta_typed = [
            "surrogate_key VARCHAR",
            "effective_date DATE",
            "expiry_date DATE",
            "is_current BOOLEAN",
            "inserted_at TIMESTAMP",
        ]
        business_cols = ", ".join(f"{c} VARCHAR" for c in cols)
        all_cols = ", ".join(meta_typed) + (
            ", " + business_cols if business_cols else ""
        )
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.SCHEMA}")
        conn.execute(f"CREATE TABLE {self.SCHEMA}.{table_name} ({all_cols})")

    def _load_incoming(self, conn, table_name, cols: list[str], rows: list[dict]):
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        if not rows:
            conn.execute(
                f"CREATE TABLE {table_name} AS "
                f"SELECT * FROM (VALUES (NULL::VARCHAR)) AS tmp({cols[0]}) WHERE false"
            )
            return
        placeholders = ", ".join("?" for _ in rows[0])
        col_list = ", ".join(rows[0].keys())
        conn.execute(
            f"CREATE TABLE {table_name} ({', '.join(f'{c} VARCHAR' for c in rows[0])})"
        )
        for row in rows:
            conn.execute(
                f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
                list(row.values()),
            )

    def test_new_row_inserted_when_empty(self, conn):
        self._create_dim_table(conn, "dim_test", ["name", "dept"])
        self._load_incoming(
            conn,
            "inc",
            ["name", "dept"],
            [
                {"name": "ville_x", "dept": "99"},
            ],
        )
        nk_join = "inc.name = cur.name"

        conn.execute(
            """
            INSERT INTO lakehouse.dim_test
            SELECT
                gen_random_uuid()::VARCHAR AS surrogate_key,
                CURRENT_DATE               AS effective_date,
                DATE '9999-12-31'          AS expiry_date,
                true                       AS is_current,
                now()                      AS inserted_at,
                inc.name, inc.dept
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.dim_test cur
                WHERE cur.is_current = true AND """
            + nk_join
            + """
            )
        """
        )

        rows = conn.execute(
            "SELECT name, dept, is_current FROM lakehouse.dim_test"
        ).fetchall()
        assert rows == [("ville_x", "99", True)]

    def test_duplicate_not_inserted(self, conn):
        self._create_dim_table(conn, "dim_test", ["name", "dept"])
        self._load_incoming(
            conn,
            "inc",
            ["name", "dept"],
            [
                {"name": "ville_y", "dept": "10"},
            ],
        )
        nk_join = "inc.name = cur.name"

        conn.execute(
            """
            INSERT INTO lakehouse.dim_test
            SELECT
                gen_random_uuid()::VARCHAR,
                CURRENT_DATE,
                DATE '9999-12-31',
                true,
                now(),
                inc.name, inc.dept
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.dim_test cur
                WHERE cur.is_current = true AND """
            + nk_join
            + """
            )
        """
        )

        inserted = conn.execute(
            """
            SELECT COUNT(*) FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.dim_test cur
                WHERE cur.is_current = true AND """
            + nk_join
            + """
            )
        """
        ).fetchone()[0]
        assert inserted == 0

    def test_changed_row_expires_old_and_inserts_new(self, conn):
        self._create_dim_table(conn, "dim_test", ["name", "dept", "population"])
        self._load_incoming(
            conn,
            "inc",
            ["name", "dept", "population"],
            [
                {"name": "ville_z", "dept": "20", "population": "700"},
            ],
        )

        conn.execute("""
            INSERT INTO lakehouse.dim_test
            SELECT gen_random_uuid()::VARCHAR, CURRENT_DATE,
                   DATE '9999-12-31', true, now(),
                   inc.name, inc.dept, inc.population
            FROM inc
        """)

        # Second run with changed data
        conn.execute("DROP TABLE IF EXISTS inc")
        self._load_incoming(
            conn,
            "inc",
            ["name", "dept", "population"],
            [
                {"name": "ville_z", "dept": "20", "population": "999"},
            ],
        )

        conn.execute("""
            UPDATE lakehouse.dim_test AS existing
            SET is_current = false, expiry_date = CURRENT_DATE
            WHERE existing.is_current = true
              AND EXISTS (
                  SELECT 1 FROM inc
                  WHERE inc.name = existing.name
                    AND md5(CONCAT_WS('|', inc.name::VARCHAR, inc.dept::VARCHAR, inc.population::VARCHAR))
                        != md5(CONCAT_WS('|', existing.name::VARCHAR, existing.dept::VARCHAR, CAST(existing.population AS VARCHAR)))
              )
        """)

        conn.execute("""
            INSERT INTO lakehouse.dim_test
            SELECT gen_random_uuid()::VARCHAR, CURRENT_DATE,
                   DATE '9999-12-31', true, now(),
                   inc.name, inc.dept, inc.population
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.dim_test cur
                WHERE cur.is_current = true AND inc.name = cur.name
            )
        """)

        current = conn.execute(
            "SELECT population, is_current FROM lakehouse.dim_test WHERE is_current = true"
        ).fetchone()
        assert current[0] == "999"

        expired = conn.execute(
            "SELECT population, is_current FROM lakehouse.dim_test WHERE is_current = false"
        ).fetchone()
        assert expired[0] == "700"

    def test_removed_nk_expires(self, conn):
        self._create_dim_table(conn, "dim_test", ["name", "dept"])
        self._load_incoming(
            conn,
            "inc",
            ["name", "dept"],
            [
                {"name": "ville_a", "dept": "01"},
                {"name": "ville_b", "dept": "02"},
            ],
        )

        conn.execute("""
            INSERT INTO lakehouse.dim_test
            SELECT gen_random_uuid()::VARCHAR, CURRENT_DATE,
                   DATE '9999-12-31', true, now(),
                   inc.name, inc.dept
            FROM inc
        """)

        # Second run: only ville_a
        conn.execute("DROP TABLE IF EXISTS inc")
        self._load_incoming(
            conn,
            "inc",
            ["name", "dept"],
            [
                {"name": "ville_a", "dept": "01"},
            ],
        )

        conn.execute("""
            UPDATE lakehouse.dim_test AS existing
            SET is_current = false, expiry_date = CURRENT_DATE
            WHERE existing.is_current = true
              AND existing.name NOT IN (SELECT name FROM inc)
        """)

        ville_b = conn.execute(
            "SELECT is_current FROM lakehouse.dim_test WHERE name = 'ville_b'"
        ).fetchone()
        assert ville_b[0] is False

    def test_multi_column_natural_key(self, conn):
        self._create_dim_table(conn, "dim_multi", ["col_a", "col_b", "val"])
        self._load_incoming(
            conn,
            "inc",
            ["col_a", "col_b", "val"],
            [
                {"col_a": "a1", "col_b": "b1", "val": "10"},
            ],
        )
        nk_join = "inc.col_a = cur.col_a AND inc.col_b = cur.col_b"

        conn.execute(
            """
            INSERT INTO lakehouse.dim_multi
            SELECT gen_random_uuid()::VARCHAR, CURRENT_DATE,
                   DATE '9999-12-31', true, now(),
                   inc.col_a, inc.col_b, inc.val
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.dim_multi cur
                WHERE cur.is_current = true AND """
            + nk_join
            + """
            )
        """
        )

        rows = conn.execute(
            "SELECT col_a, col_b, val, is_current FROM lakehouse.dim_multi"
        ).fetchall()
        assert rows == [("a1", "b1", "10", True)]


class TestConstants:
    def test_schema(self):
        assert SCHEMA == "lakehouse"

    def test_metadata_cols(self):
        assert "surrogate_key" in SCD_METADATA_COLS
        assert "effective_date" in SCD_METADATA_COLS
        assert "expiry_date" in SCD_METADATA_COLS
        assert "is_current" in SCD_METADATA_COLS
        assert "inserted_at" in SCD_METADATA_COLS
        assert len(SCD_METADATA_COLS) == 5

    def test_geometry_types(self):
        assert "GEOMETRY" in GEOMETRY_TYPES
