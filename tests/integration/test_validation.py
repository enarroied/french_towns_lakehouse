"""Tests for flows_integration.shared.validation — validated parquet checks."""

from unittest.mock import MagicMock
from unittest.mock import patch

import duckdb
import pytest
from flows_integration.shared.validation import _table_path
from flows_integration.shared.validation import assert_validated_exists


class TestAssertValidatedExists:
    def test_passes_with_real_parquet(self, tmp_path):
        conn = duckdb.connect()
        parquet_path = tmp_path / "real_test.parquet"
        conn.execute("COPY (SELECT 1 AS x) TO '" + str(parquet_path) + "'")
        with patch(
            "flows_integration.shared.validation._table_path",
            return_value=str(parquet_path),
        ):
            assert_validated_exists(conn, "real_test")
        conn.close()

    def test_raises_with_empty_real_parquet(self, tmp_path):
        conn = duckdb.connect()
        parquet_path = tmp_path / "empty_test.parquet"
        conn.execute(
            "COPY (SELECT * FROM (VALUES (NULL)) WHERE false) TO '"
            + str(parquet_path)
            + "'"
        )
        with (
            patch(
                "flows_integration.shared.validation._table_path",
                return_value=str(parquet_path),
            ),
            pytest.raises(RuntimeError, match="empty"),
        ):
            assert_validated_exists(conn, "empty_test")
        conn.close()

    def test_passes_with_rows_using_mock_conn(self):
        """Use a mocked connection to verify execute/fetchone flow."""
        mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_conn.execute.return_value.fetchone.return_value = [5]
        with patch(
            "flows_integration.shared.validation._table_path",
            return_value="s3://validated/dummy.parquet",
        ):
            assert_validated_exists(mock_conn, "dummy")
            mock_conn.execute.assert_called_once()

    def test_raises_when_parquet_is_empty_mock(self):
        mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_conn.execute.return_value.fetchone.return_value = [0]
        with (
            patch(
                "flows_integration.shared.validation._table_path",
                return_value="s3://validated/empty.parquet",
            ),
            pytest.raises(RuntimeError, match="empty"),
        ):
            assert_validated_exists(mock_conn, "empty")

    def test_raises_when_read_fails(self):
        conn = duckdb.connect()
        with (
            patch(
                "flows_integration.shared.validation._table_path",
                return_value="nonexistent",
            ),
            pytest.raises(RuntimeError, match="does not exist"),
        ):
            assert_validated_exists(conn, "dummy")
        conn.close()

    def test_table_path_format(self):
        result = _table_path("my_table")
        assert result == "s3://validated/my_table.parquet"
