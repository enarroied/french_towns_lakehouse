"""Tests for flows_staging.shared.download module."""

from pathlib import Path

from flows_staging.shared.download import _get_file_location
from flows_staging.shared.download import _process_extracted_files
from flows_staging.shared.download import _should_skip_file
from flows_staging.shared.download import _timestamped_csv_name
from flows_staging.shared.download import calculate_md5
from flows_staging.shared.download import write_csv_to_temp
from flows_staging.shared.models import KnownFileHash


class TestTimestampedCsvName:
    """Tests for _timestamped_csv_name function."""

    def test_returns_csv_file(self):
        """Result should always end with .csv."""
        result = _timestamped_csv_name("my_data")
        assert result.endswith(".csv")

    def test_includes_base_name(self):
        """Result should include the base name."""
        result = _timestamped_csv_name("populations")
        assert "populations" in result

    def test_includes_timestamp(self):
        """Result should include a timestamp."""
        result = _timestamped_csv_name("test")
        assert "_" in result
        assert result.startswith("test_")


class TestCalculateMD5:
    """Tests for calculate_md5 function."""

    def test_same_content_same_hash(self, temp_csv_file):
        """Same file content should produce the same hash."""
        hash1 = calculate_md5(temp_csv_file)
        hash2 = calculate_md5(temp_csv_file)
        assert hash1 == hash2

    def test_different_content_different_hash(self, tmp_path):
        """Different file content should produce different hashes."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("content a")
        file2.write_text("content b")

        hash1 = calculate_md5(file1)
        hash2 = calculate_md5(file2)
        assert hash1 != hash2

    def test_returns_32_character_hex(self, temp_csv_file):
        """MD5 hash should be 32 characters of hex."""
        result = calculate_md5(temp_csv_file)
        assert len(result) == 32
        assert all(c in "0123456789abcdef" for c in result)


class TestWriteCsvToTemp:
    """Tests for write_csv_to_temp function."""

    def test_creates_csv_file(self, tmp_path):
        """Should create a CSV file in the temp directory."""
        data = [{"name": "Alice", "age": "30"}]
        fieldnames = ["name", "age"]

        result = write_csv_to_temp(data, fieldnames, "test", tmp_path)

        assert result.exists()
        assert result.suffix == ".csv"

    def test_includes_header(self, tmp_path):
        """CSV should include header row."""
        data = [{"name": "Alice"}]
        fieldnames = ["name", "age"]

        result = write_csv_to_temp(data, fieldnames, "test", tmp_path)

        content = result.read_text()
        assert "name" in content

    def test_includes_data(self, tmp_path):
        """CSV should include data rows."""
        data = [{"name": "Alice", "age": "30"}]
        fieldnames = ["name", "age"]

        result = write_csv_to_temp(data, fieldnames, "test", tmp_path)

        content = result.read_text()
        assert "Alice" in content
        assert "30" in content

    def test_returns_path_to_file(self, tmp_path):
        """Should return Path to the created file."""
        data = [{"name": "Bob"}]
        fieldnames = ["name"]

        result = write_csv_to_temp(data, fieldnames, "myfile", tmp_path)

        assert isinstance(result, Path)
        assert result.name.startswith("myfile_")


class TestShouldSkipFile:
    """Tests for _should_skip_file function."""

    def test_returns_true_when_hash_matches(self, sample_known_hashes):
        """Should return True when file hash matches known hash."""
        result = _should_skip_file(
            base_name="populations_historiques",
            md5="abc123def456",
            known_hashes=sample_known_hashes,
        )
        assert result is True

    def test_returns_false_when_hash_differs(self, sample_known_hashes):
        """Should return False when file hash differs from known hash."""
        result = _should_skip_file(
            base_name="populations_historiques",
            md5="different_hash",
            known_hashes=sample_known_hashes,
        )
        assert result is False

    def test_returns_false_when_file_unknown(self, sample_known_hashes):
        """Should return False when file is not in known_hashes."""
        result = _should_skip_file(
            base_name="unknown_file",
            md5="any_hash",
            known_hashes=sample_known_hashes,
        )
        assert result is False


class TestGetFileLocation:
    """Tests for _get_file_location function."""

    def test_returns_location_when_exists(self, sample_known_hashes):
        """Should return file_location when file is in known_hashes."""
        result = _get_file_location(
            base_name="populations_historiques",
            known_hashes=sample_known_hashes,
        )
        assert result == "demographics/DS_POPULATIONS_HISTORIQUES_data.csv"

    def test_returns_none_when_file_unknown(self, sample_known_hashes):
        """Should return None when file is not in known_hashes."""
        result = _get_file_location(
            base_name="unknown.csv",
            known_hashes=sample_known_hashes,
        )
        assert result is None


class TestProcessExtractedFiles:
    """Tests for _process_extracted_files function."""

    def test_returns_empty_for_nonexistent_files(
        self, tmp_path, mock_minio_client, sample_known_hashes
    ):
        """Should return empty list when no files exist."""
        nonexistent = [tmp_path / "does_not_exist.csv"]
        result = _process_extracted_files(
            extracted_files=nonexistent,
            known_hashes=sample_known_hashes,
            minio_client=mock_minio_client,
            staging_bucket="test-bucket",
            base_name="any_file",
        )
        assert result == []

    def test_skips_unchanged_files(
        self, tmp_path, mock_minio_client, sample_known_hashes
    ):
        """Should skip files whose hash matches known hash."""
        # Create a file and calculate its actual MD5
        file_path = tmp_path / "populations_historiques.csv"
        file_path.write_text("dummy content")
        actual_hash = calculate_md5(file_path)

        # Update known_hashes to match the file content (use base_name as key)
        sample_known_hashes["populations_historiques"] = KnownFileHash(
            md5=actual_hash,
            filename_timestamp="20240101_120000",
            file_location="demographics/DS_POPULATIONS_HISTORIQUES_data.csv",
        )

        result = _process_extracted_files(
            extracted_files=[file_path],
            known_hashes=sample_known_hashes,
            minio_client=mock_minio_client,
            staging_bucket="test-bucket",
            base_name="populations_historiques",
        )
        # No upload should happen for unchanged file
        assert mock_minio_client.upload_file.call_count == 0
        assert result == []

    def test_uploads_new_files(self, tmp_path, mock_minio_client, sample_known_hashes):
        """Should upload files with new/changed content."""
        file_path = tmp_path / "new_file.csv"
        file_path.write_text("new content here")

        result = _process_extracted_files(
            extracted_files=[file_path],
            known_hashes=sample_known_hashes,
            minio_client=mock_minio_client,
            staging_bucket="test-bucket",
            base_name="new_file",
        )

        assert mock_minio_client.upload_file.called
        assert len(result) == 1
        assert result[0].base_name == "new_file"

    def test_uploads_to_target_folder(
        self, tmp_path, mock_minio_client, sample_known_hashes
    ):
        """Should upload files to target_folder prefix when specified."""
        file_path = tmp_path / "data.csv"
        file_path.write_text("data content")

        result = _process_extracted_files(
            extracted_files=[file_path],
            known_hashes={},
            minio_client=mock_minio_client,
            staging_bucket="test-bucket",
            base_name="data",
            target_folder="demographics",
        )

        assert len(result) == 1
        call_args = mock_minio_client.upload_file.call_args
        assert call_args[1]["Key"].startswith("demographics/")
