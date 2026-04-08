"""Shared pytest fixtures for mocking external dependencies."""

import csv
import zipfile
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


# -----------------------------------------------------------------------------
# MinIO / S3 Mock
# -----------------------------------------------------------------------------


@pytest.fixture
def mock_minio_client():
    """Mock MinIO client that tracks uploads without actually connecting."""
    mock_client = MagicMock()
    mock_client.head_bucket.return_value = {}
    mock_client.create_bucket.return_value = {}
    mock_client.upload_file.return_value = None
    mock_client.put_object.return_value = None
    mock_client.copy_object.return_value = None
    mock_client.delete_object.return_value = None
    mock_client.list_objects_v2.return_value = {"Contents": []}

    with patch("flows_staging.shared.minio.get_minio_client", return_value=mock_client):
        yield mock_client


@pytest.fixture
def mock_ensure_bucket_exists():
    """Mock bucket existence check."""
    with patch("flows_staging.shared.minio.ensure_bucket_exists"):
        yield


# -----------------------------------------------------------------------------
# DuckDB Mock
# -----------------------------------------------------------------------------


@pytest.fixture
def mock_duckdb_conn():
    """Mock DuckDB connection for audit tests."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock()
    mock_conn.close.return_value = None

    with patch("flows_staging.shared.audit._conn", return_value=mock_conn):
        yield mock_conn


# -----------------------------------------------------------------------------
# Config Mock
# -----------------------------------------------------------------------------


@pytest.fixture
def sample_config():
    """Sample configuration matching config.yaml structure."""
    return {
        "paths": {
            "temp_dir": "/tmp/test_downloads",
            "input_dir": "input",
            "output_dir": "data/processed",
            "scraper_dir": "data/scraper",
            "custom_dir": "data/custom",
        },
        "buckets": {
            "staging_current": "staging-current",
            "staging_historical": "staging-historical",
            "validated": "validated",
            "validated_historical": "validated-historical",
            "rejected": "rejected",
            "evidence_archive": "evidence-archive",
            "lakehouse": "lakehouse",
        },
        "download": {
            "concurrency": 3,
            "timeout_seconds": 120,
        },
        "downloads": [
            {
                "name": "populations_historiques",
                "url": "https://api.insee.fr/melodi/file/DS_POPULATIONS_HISTORIQUES/DS_POPULATIONS_HISTORIQUES_CSV_FR",
                "filename": "populations_historiques.zip",
                "domain": "demographics",
                "target_folder": "demographics",
            },
            {
                "name": "salaries",
                "url": "https://api.insee.fr/melodi/file/DS_BTS_SAL_EQTP_SEX_PCS/DS_BTS_SAL_EQTP_SEX_PCS_2023_CSV_FR",
                "filename": "salaries.csv",
                "domain": "demographics",
                "target_folder": "demographics",
            },
        ],
        "scrapers": [
            {
                "name": "villes_fleuries",
                "module": "flows_staging.scrapers.scrape_villes_fleuries",
                "enabled": True,
                "domain": "labels",
                "target_folder": "labels",
                "url": "https://villes-et-villages-fleuris.com/les-communes-labelisees",
                "endpoint": "https://villes-et-villages-fleuris.com/pages/post/commune",
                "user_agent": "TestAgent/1.0",
                "page_size": 1000,
                "crawl_delay": 1,
            },
            {
                "name": "petites_cites",
                "module": "flows_staging.scrapers.scrape_petites_cites",
                "enabled": True,
                "domain": "labels",
                "target_folder": "labels",
                "url": "https://www.petitescitesdecaractere.com/cites-sitemap.xml",
                "user_agent": "TestAgent/1.0",
                "concurrency": 5,
            },
        ],
        "custom_parsers": [
            {
                "name": "ville_sportive",
                "module": "flows_staging.custom_parsers.parse_ville_sportive",
                "enabled": True,
                "domain": "labels",
                "target_folder": "labels",
                "input_dir": "tests/fixtures",
                "pdf_file": "sample.pdf",
            },
        ],
    }


# -----------------------------------------------------------------------------
# HTTP Mock
# -----------------------------------------------------------------------------


@pytest.fixture
def mock_httpx_response():
    """Factory for creating mock httpx responses."""

    def _make_response(
        status: int = 200,
        json_data: dict | None = None,
        content: bytes = b"",
        headers: dict | None = None,
    ):
        mock_response = MagicMock()
        mock_response.status_code = status
        mock_response.json.return_value = json_data or {}
        mock_response.content = content
        mock_response.headers = headers or {}
        mock_response.raise_for_status = MagicMock()
        if status >= 400:
            mock_response.raise_for_status.side_effect = Exception(f"HTTP {status}")
        return mock_response

    return _make_response


# -----------------------------------------------------------------------------
# File System Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def temp_csv_file(tmp_path):
    """Create a temporary CSV file for testing."""
    csv_path = tmp_path / "test.csv"
    data = [
        {"commune": "paris", "region": "ile-de-france", "departement": "75"},
        {"commune": "lyon", "region": "auvergne-rhone-alpes", "departement": "69"},
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["commune", "region", "departement"])
        writer.writeheader()
        writer.writerows(data)

    return csv_path


@pytest.fixture
def temp_zip_file(tmp_path):
    """Create a temporary ZIP file with a CSV inside."""
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data.csv", "id,value\n1,foo\n2,bar\n")

    return zip_path


# -----------------------------------------------------------------------------
# Known Hashes Fixture
# -----------------------------------------------------------------------------


@pytest.fixture
def sample_known_hashes():
    """Sample known hashes for testing hash comparison."""
    return {
        "populations_historiques.csv": {
            "md5": "abc123def456",
            "file_location": "demographics/DS_POPULATIONS_HISTORIQUES_data.csv",
            "last_modified": "2024-01-01T00:00:00Z",
        },
        "villes_fleuries.csv": {
            "md5": "xyz789ghi012",
            "file_location": "labels/villes_fleuries_20240101_120000.csv",
            "last_modified": "2024-01-01T00:00:00Z",
        },
    }
