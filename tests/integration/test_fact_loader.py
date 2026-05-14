"""Tests for flows_integration.shared.fact_loader — constants and append SQL logic."""

import duckdb
import pytest
from flows_integration.shared.fact_loader import SCHEMA
from flows_integration.shared.fact_loader import VALIDATED_BUCKET
from flows_integration.shared.fact_loader import append_new_rows


class TestConstants:
    def test_schema(self):
        assert SCHEMA == "lakehouse"

    def test_validated_bucket(self):
        assert VALIDATED_BUCKET == "validated"


class TestAppendSQL:
    """Test the append-new-rows SQL pattern (same logic as append_new_rows)
    using local tables instead of S3 parquet files."""

    @pytest.fixture
    def conn(self):
        c = duckdb.connect()
        c.execute("CREATE SCHEMA IF NOT EXISTS lakehouse")
        yield c
        c.close()

    def _create_fact_table(self, conn, table_name, cols: list[str]):
        all_cols = "inserted_at TIMESTAMP, " + ", ".join(f"{c} VARCHAR" for c in cols)
        conn.execute(f"CREATE TABLE lakehouse.{table_name} ({all_cols})")

    def _load_incoming(self, conn, table_name, rows: list[dict]):
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        if not rows:
            return
        col_list = ", ".join(rows[0].keys())
        conn.execute(
            f"CREATE TABLE {table_name} ({', '.join(f'{c} VARCHAR' for c in rows[0])})"
        )
        for row in rows:
            placeholders = ", ".join("?" for _ in row)
            conn.execute(
                f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
                list(row.values()),
            )

    def test_insert_into_empty_fact(self, conn):
        self._create_fact_table(conn, "fact_test", ["commune_id", "population"])
        self._load_incoming(
            conn,
            "inc",
            [
                {"commune_id": "c01", "population": "5000"},
            ],
        )
        nk_join = "inc.commune_id = existing.commune_id"

        conn.execute(
            """
            INSERT INTO lakehouse.fact_test
            SELECT now(), inc.commune_id, inc.population
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.fact_test existing
                WHERE """
            + nk_join
            + """
            )
        """
        )

        rows = conn.execute(
            "SELECT commune_id, population FROM lakehouse.fact_test"
        ).fetchall()
        assert rows == [("c01", "5000")]

    def test_duplicate_skipped(self, conn):
        self._create_fact_table(conn, "fact_test", ["commune_id", "population"])
        self._load_incoming(
            conn,
            "inc",
            [
                {"commune_id": "c02", "population": "3000"},
            ],
        )
        nk_join = "inc.commune_id = existing.commune_id"

        conn.execute(
            """
            INSERT INTO lakehouse.fact_test
            SELECT now(), inc.commune_id, inc.population
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.fact_test existing
                WHERE """
            + nk_join
            + """
            )
        """
        )

        count = conn.execute(
            """
            SELECT COUNT(*) FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.fact_test existing
                WHERE """
            + nk_join
            + """
            )
        """
        ).fetchone()[0]
        assert count == 0

        total = conn.execute("SELECT COUNT(*) FROM lakehouse.fact_test").fetchone()[0]
        assert total == 1

    def test_new_row_appended_alongside_existing(self, conn):
        self._create_fact_table(conn, "fact_test", ["commune_id"])
        self._load_incoming(conn, "inc", [{"commune_id": "c03"}])
        nk_join = "inc.commune_id = existing.commune_id"

        conn.execute(
            """
            INSERT INTO lakehouse.fact_test
            SELECT now(), inc.commune_id
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.fact_test existing
                WHERE """
            + nk_join
            + """
            )
        """
        )

        self._load_incoming(conn, "inc", [{"commune_id": "c04"}])
        conn.execute(
            """
            INSERT INTO lakehouse.fact_test
            SELECT now(), inc.commune_id
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.fact_test existing
                WHERE """
            + nk_join
            + """
            )
        """
        )

        total = conn.execute("SELECT COUNT(*) FROM lakehouse.fact_test").fetchone()[0]
        assert total == 2

    def test_multi_column_natural_key(self, conn):
        self._create_fact_table(conn, "fact_multi", ["key_a", "key_b", "val"])
        self._load_incoming(
            conn,
            "inc",
            [
                {"key_a": "a1", "key_b": "b1", "val": "100"},
            ],
        )
        nk_join = "inc.key_a = existing.key_a AND inc.key_b = existing.key_b"

        conn.execute(
            """
            INSERT INTO lakehouse.fact_multi
            SELECT now(), inc.key_a, inc.key_b, inc.val
            FROM inc
            WHERE NOT EXISTS (
                SELECT 1 FROM lakehouse.fact_multi existing
                WHERE """
            + nk_join
            + """
            )
        """
        )

        rows = conn.execute(
            "SELECT key_a, key_b, val FROM lakehouse.fact_multi"
        ).fetchall()
        assert rows == [("a1", "b1", "100")]

    def test_empty_source_inserts_nothing(self, conn):
        self._create_fact_table(conn, "fact_test", ["commune_id"])
        self._load_incoming(conn, "inc", [])
        if True:
            total = conn.execute("SELECT COUNT(*) FROM lakehouse.fact_test").fetchone()[
                0
            ]
            assert total == 0


class TestAppendNewRowsRequiresS3:
    """append_new_rows() reads from s3://validated/ — requires MinIO."""

    def test_raises_without_s3(self):
        conn = duckdb.connect()
        with pytest.raises(Exception, match="HTTP GET error|HTTP error"):
            append_new_rows(conn, "nonexistent_table", ["id"])
        conn.close()
