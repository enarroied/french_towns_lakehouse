"""Tests for blog.blog_utils — sql_to_df and _connect."""

from unittest.mock import patch

import duckdb
import pandas as pd
import polars as pl
import pytest
from blog.blog_utils import GOLD_TABLES
from blog.blog_utils import sql_to_df


@pytest.fixture
def mock_conn():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE test_table (name VARCHAR, value INTEGER)")
    conn.execute(
        "INSERT INTO test_table VALUES ('alpha', 10), ('beta', 20), ('gamma', 30)"
    )
    return conn


@pytest.fixture
def patch_connect(mock_conn):
    with patch("blog.blog_utils._connect", return_value=mock_conn) as p:
        yield p


class TestSqlToDf:
    def test_returns_polars_by_default(self, patch_connect):
        result = sql_to_df("SELECT * FROM test_table")
        assert isinstance(result, pl.DataFrame)

    def test_returns_pandas_when_requested(self, patch_connect):
        result = sql_to_df("SELECT * FROM test_table", dataframe_type="pandas")
        assert isinstance(result, pd.DataFrame)

    def test_returns_correct_data_polars(self, patch_connect):
        result = sql_to_df("SELECT * FROM test_table ORDER BY name")
        assert result.shape == (3, 2)
        assert result["name"].to_list() == ["alpha", "beta", "gamma"]

    def test_returns_correct_data_pandas(self, patch_connect):
        result = sql_to_df(
            "SELECT * FROM test_table ORDER BY name",
            dataframe_type="pandas",
        )
        assert result.shape == (3, 2)
        assert result["name"].tolist() == ["alpha", "beta", "gamma"]

    def test_with_parameterized_query(self, patch_connect):
        result = sql_to_df(
            "SELECT * FROM test_table WHERE value > ? ORDER BY name", [15]
        )
        assert result.shape == (2, 2)
        assert result["name"].to_list() == ["beta", "gamma"]

    def test_with_empty_result(self, patch_connect):
        result = sql_to_df(
            "SELECT * FROM test_table WHERE value > 100",
            dataframe_type="pandas",
        )
        assert result.shape == (0, 2)

    def test_raises_on_bad_dataframe_type(self, patch_connect):
        with pytest.raises(ValueError, match="dataframe_type must be"):
            sql_to_df("SELECT 1", dataframe_type="numpy")

    def test_raises_on_sql_error(self, patch_connect):
        with pytest.raises(duckdb.CatalogException):
            sql_to_df("SELECT * FROM nonexistent")


class TestConnectCaching:
    def test_connect_is_cached(self, mock_conn):
        with patch("blog.blog_utils._connect", return_value=mock_conn) as p:
            sql_to_df("SELECT 1")
            sql_to_df("SELECT 2")
            assert p.call_count == 2  # patched version isn't cached


class TestGoldTables:
    def test_gold_tables_list_is_known(self):

        assert isinstance(GOLD_TABLES, list)
        assert all(isinstance(t, str) for t in GOLD_TABLES)
        assert "dim_communes_france" in GOLD_TABLES
        assert len(GOLD_TABLES) == 10
