"""Tests for flows_integration.shared.connection — DuckDB connection factory."""

import os
from unittest.mock import MagicMock
from unittest.mock import patch

import duckdb
from flows_integration.shared.connection import get_duckdb_connection


def _required_env() -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": "test_key",
        "AWS_SECRET_ACCESS_KEY": "test_secret",
        "AWS_ENDPOINT": "localhost:19000",
        "POLARIS_CLIENT_ID": "test_user",
        "POLARIS_CLIENT_SECRET": "test_pass",
    }


class TestGetDuckDBConnection:
    def test_returns_duckdb_connection_with_mocked_execute(self):
        with (
            patch.dict(os.environ, _required_env(), clear=False),
            patch("flows_integration.shared.connection.duckdb.connect") as mock_connect,
        ):
            mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
            mock_connect.return_value = mock_conn
            conn = get_duckdb_connection()
            assert conn is mock_conn

    def test_installs_iceberg_and_httpfs(self):
        with (
            patch.dict(os.environ, _required_env(), clear=False),
            patch("flows_integration.shared.connection.duckdb.connect") as mock_connect,
        ):
            mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
            mock_connect.return_value = mock_conn
            get_duckdb_connection()
            calls = [args[0][0] for args in mock_conn.execute.call_args_list]
            assert "INSTALL iceberg; LOAD iceberg;" in calls
            assert "INSTALL httpfs; LOAD httpfs;" in calls

    def test_creates_minio_secret_from_env(self):
        with (
            patch.dict(os.environ, _required_env(), clear=False),
            patch("flows_integration.shared.connection.duckdb.connect") as mock_connect,
        ):
            mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
            mock_connect.return_value = mock_conn
            get_duckdb_connection()
            sql_calls = [args[0][0] for args in mock_conn.execute.call_args_list]
            secret_sql = [s for s in sql_calls if "CREATE SECRET" in s and "minio" in s]
            assert len(secret_sql) == 1
            assert "test_key" in secret_sql[0]
            assert "test_secret" in secret_sql[0]
            assert "localhost:19000" in secret_sql[0]

    def test_creates_polaris_secret_from_env(self):
        with (
            patch.dict(os.environ, _required_env(), clear=False),
            patch("flows_integration.shared.connection.duckdb.connect") as mock_connect,
        ):
            mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
            mock_connect.return_value = mock_conn
            get_duckdb_connection()
            sql_calls = [args[0][0] for args in mock_conn.execute.call_args_list]
            secret_sql = [
                s for s in sql_calls if "CREATE SECRET" in s and "polaris" in s
            ]
            assert len(secret_sql) == 1
            assert "test_user" in secret_sql[0]
            assert "test_pass" in secret_sql[0]

    def test_attaches_polaris_catalog(self):
        with (
            patch.dict(os.environ, _required_env(), clear=False),
            patch("flows_integration.shared.connection.duckdb.connect") as mock_connect,
        ):
            mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
            mock_connect.return_value = mock_conn
            get_duckdb_connection()
            sql_calls = [args[0][0] for args in mock_conn.execute.call_args_list]
            attach_sql = [s for s in sql_calls if "ATTACH" in s]
            assert len(attach_sql) == 1
            assert "french_towns" in attach_sql[0]
