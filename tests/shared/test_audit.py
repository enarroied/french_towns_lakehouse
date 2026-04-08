"""Tests for flows_staging.shared.audit module."""

from datetime import datetime
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from flows_staging.shared.audit import _check_db  # noqa: PLC0415
from flows_staging.shared.audit import _check_internet_connection  # noqa: PLC0415
from flows_staging.shared.audit import _check_minio  # noqa: PLC0415
from flows_staging.shared.audit import _update_latest_run  # noqa: PLC0415
from flows_staging.shared.audit import _update_run_status  # noqa: PLC0415
from flows_staging.shared.audit import _write_file_metadata  # noqa: PLC0415
from flows_staging.shared.audit import get_latest_hashes  # noqa: PLC0415


class TestGetLatestHashes:
    """Tests for get_latest_hashes function."""

    def test_returns_empty_dict_when_no_hashes(self, mock_duckdb_conn):
        """Should return empty dict when no hashes exist."""
        mock_duckdb_conn.execute.return_value.fetchall.return_value = []

        with patch("flows_staging.shared.audit._conn", return_value=mock_duckdb_conn):
            result = get_latest_hashes()

        assert result == {}


class TestWriteFileMetadata:
    """Tests for _write_file_metadata function."""

    def test_marks_old_files_as_not_latest(self, mock_duckdb_conn):
        """Should set is_latest=0 for existing files with same name."""
        _write_file_metadata(
            conn=mock_duckdb_conn,
            run_id="test-run-id",
            name="test.csv",
            filename_timestamp="test_20240101.csv",
            source_url=None,
            size_mb=1.5,
            md5_hash="abc123",
            bucket="test-bucket",
            file_location="path/test.csv",
            now=datetime.now(),
        )

        calls = mock_duckdb_conn.execute.call_args_list
        assert len(calls) >= 1
        assert "UPDATE file_metadata SET is_latest = 0" in calls[0][0][0]

    def test_inserts_new_file_record(self, mock_duckdb_conn):
        """Should insert new file record."""
        _write_file_metadata(
            conn=mock_duckdb_conn,
            run_id="test-run-id",
            name="test.csv",
            filename_timestamp="test_20240101.csv",
            source_url="https://example.com/test.csv",
            size_mb=1.5,
            md5_hash="abc123",
            bucket="test-bucket",
            file_location="path/test.csv",
            now=datetime.now(),
        )

        calls = mock_duckdb_conn.execute.call_args_list
        assert len(calls) >= 2
        assert "INSERT INTO file_metadata" in calls[1][0][0]


class TestUpdateLatestRun:
    """Tests for _update_latest_run function."""

    def test_marks_previous_runs_as_not_latest(self, mock_duckdb_conn):
        """Should set is_latest=0 for previous runs of same domain."""
        _update_latest_run(mock_duckdb_conn, "new-run-id", "demographics")

        mock_duckdb_conn.execute.assert_called_once()
        call_args = mock_duckdb_conn.execute.call_args[0]
        assert "UPDATE flow_run_metadata SET is_latest = 0" in call_args[0]
        assert "demographics" in call_args[1]
        assert "new-run-id" in call_args[1]


class TestUpdateRunStatus:
    """Tests for _update_run_status function."""

    def test_updates_status_and_end_time(self, mock_duckdb_conn):
        """Should update status, end_time, and number_files."""
        now = datetime.now()
        _update_run_status(
            mock_duckdb_conn,
            run_id="test-run-id",
            status="SUCCESS",
            number_files=5,
            now=now,
        )

        mock_duckdb_conn.execute.assert_called_once()
        call_args = mock_duckdb_conn.execute.call_args[0]
        assert "UPDATE flow_run_metadata" in call_args[0]
        assert "status" in call_args[0]
        assert "end_time" in call_args[0]
        assert "number_files" in call_args[0]


class TestCheckDB:
    """Tests for _check_db function."""

    def test_calls_migrate(self):
        """Should call _migrate function."""
        with (
            patch("flows_staging.shared.audit._migrate") as mock_migrate,
            patch("flows_staging.shared.audit.get_run_logger") as mock_logger,
        ):
            mock_logger.return_value = MagicMock()
            _check_db()
            mock_migrate.assert_called_once()

    def test_raises_on_migrate_failure(self):
        """Should raise RuntimeError when migration fails."""
        with (
            patch(
                "flows_staging.shared.audit._migrate", side_effect=Exception("DB Error")
            ),
            patch("flows_staging.shared.audit.get_run_logger") as mock_logger,
        ):
            mock_logger.return_value = MagicMock()
            with pytest.raises(RuntimeError, match="Metadata DB not writable"):
                _check_db()


class TestCheckMinIO:
    """Tests for _check_minio function."""

    def test_checks_minio_connection(self):
        """Should call MinIO list_buckets."""
        mock_client = MagicMock()
        with (
            patch(
                "flows_staging.shared.minio.get_minio_client",
                return_value=mock_client,
            ),
            patch("flows_staging.shared.audit.get_run_logger") as mock_logger,
        ):
            mock_logger.return_value = MagicMock()
            _check_minio()
            mock_client.list_buckets.assert_called_once()

    def test_raises_on_minio_failure(self):
        """Should raise RuntimeError when MinIO fails."""
        with (
            patch(
                "flows_staging.shared.minio.get_minio_client",
                side_effect=Exception("Connection refused"),
            ),
            patch("flows_staging.shared.audit.get_run_logger") as mock_logger,
        ):
            mock_logger.return_value = MagicMock()
            with pytest.raises(RuntimeError, match="MinIO not reachable"):
                _check_minio()


class TestCheckInternetConnection:
    """Tests for _check_internet_connection function."""

    def test_checks_internet_with_httpx(self):
        """Should use httpx to check internet connection."""
        with (
            patch("flows_staging.shared.audit.httpx.head") as mock_head,
            patch("flows_staging.shared.audit.get_run_logger") as mock_logger,
        ):
            mock_logger.return_value = MagicMock()
            _check_internet_connection()
            mock_head.assert_called_once_with("https://www.cloudflare.com", timeout=5)

    def test_raises_on_no_internet(self):
        """Should raise RuntimeError when no internet."""
        with (
            patch(
                "flows_staging.shared.audit.httpx.head",
                side_effect=Exception("Network unreachable"),
            ),
            patch("flows_staging.shared.audit.get_run_logger") as mock_logger,
        ):
            mock_logger.return_value = MagicMock()
            with pytest.raises(RuntimeError, match="No internet connection"):
                _check_internet_connection()
