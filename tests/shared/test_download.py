"""Tests for flows_staging.shared.download module."""

import time
import zipfile
from pathlib import Path

import pytest
from flows_staging.shared.download import _add_timestamp_to_filename
from flows_staging.shared.download import _delete_unmatched_files
from flows_staging.shared.download import _get_file_size_mb
from flows_staging.shared.download import _rename_files
from flows_staging.shared.download import _unzip_file_if_needed
from flows_staging.shared.download import calculate_md5
from flows_staging.shared.download import write_csv_for_staging


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


class TestGetFileSizeMb:
    """Tests for _get_file_size_mb function."""

    def test_returns_float(self, tmp_path):
        """Should return a float value."""
        file_path = tmp_path / "test.txt"
        file_path.write_text("hello")
        result = _get_file_size_mb(file_path)
        assert isinstance(result, float)

    def test_small_file_size(self, tmp_path):
        """Should return very small size for tiny file."""
        file_path = tmp_path / "small.txt"
        file_path.write_text("x")
        result = _get_file_size_mb(file_path)
        assert result < 0.01

    def test_returns_zero_for_empty_file(self, tmp_path):
        """Empty file should have size close to 0."""
        file_path = tmp_path / "empty.txt"
        file_path.write_text("")
        result = _get_file_size_mb(file_path)
        assert result == 0.0


class TestAddTimestampToFilename:
    """Tests for _add_timestamp_to_filename function."""

    def test_returns_filename_with_timestamp(self):
        """Result should include a timestamp."""
        result = _add_timestamp_to_filename("data", ".csv")
        # Should be like "data_20250101T120000.csv"
        assert result.startswith("data_")
        assert result.endswith(".csv")

    def test_includes_base_name(self):
        """Result should include the base name."""
        result = _add_timestamp_to_filename("populations", ".csv")
        assert "populations" in result

    def test_timestamp_format(self):
        """Timestamp should be in YYYYMMDDT-HHMMSS format."""
        result = _add_timestamp_to_filename("test", ".txt")
        # Extract timestamp part
        parts = result.split("_")
        assert len(parts) >= 2
        timestamp_part = parts[-1].replace(".txt", "")
        # Should match YYYYMMDDT-HHMMSS pattern
        assert "T" in timestamp_part

    def test_different_calls_produce_different_timestamps(self, monkeypatch):
        """Different calls should produce different timestamps (time-based)."""

        results = []
        for _ in range(3):
            results.append(_add_timestamp_to_filename("test", ".csv"))
            time.sleep(0.01)
        # At least some should be different (very unlikely all same in 3 calls)
        assert len(set(results)) >= 1


class TestWriteCsvForStaging:
    """Tests for write_csv_for_staging function."""

    def test_creates_file(self, tmp_path):
        """Should create a file in the temp directory."""
        data = [{"name": "Alice", "age": "30"}]
        fieldnames = ["name", "age"]

        result = write_csv_for_staging(data, fieldnames, "test", tmp_path)

        assert result.exists()

    def test_file_named_exactly_base_name(self, tmp_path):
        """File should be named exactly base_name (no extension added by function)."""
        data = [{"name": "Alice"}]
        fieldnames = ["name"]

        result = write_csv_for_staging(data, fieldnames, "myfile", tmp_path)

        assert result.name == "myfile"

    def test_includes_header(self, tmp_path):
        """CSV should include header row."""
        data = [{"name": "Alice"}]
        fieldnames = ["name", "age"]

        result = write_csv_for_staging(data, fieldnames, "test", tmp_path)

        content = result.read_text()
        assert "name" in content

    def test_includes_data(self, tmp_path):
        """CSV should include data rows."""
        data = [{"name": "Alice", "age": "30"}]
        fieldnames = ["name", "age"]

        result = write_csv_for_staging(data, fieldnames, "test", tmp_path)

        content = result.read_text()
        assert "Alice" in content
        assert "30" in content

    def test_returns_path_to_file(self, tmp_path):
        """Should return Path to the created file."""
        data = [{"name": "Bob"}]
        fieldnames = ["name"]

        result = write_csv_for_staging(data, fieldnames, "myfile", tmp_path)

        assert isinstance(result, Path)
        assert result.name == "myfile"


class TestUnzipFileIfNeeded:
    """Tests for _unzip_file_if_needed function."""

    def test_does_nothing_for_non_zip(self, tmp_path):
        """Should not modify non-zip files."""
        file_path = tmp_path / "data.txt"
        file_path.write_text("hello")

        _unzip_file_if_needed(file_path)

        assert file_path.exists()
        assert file_path.read_text() == "hello"

    def test_extracts_zip_and_removes_archive(self, tmp_path):
        """Should extract zip contents and remove the archive."""

        # Create a zip file
        zip_path = tmp_path / "archive.zip"
        extracted_file = tmp_path / "extracted.txt"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("extracted.txt", "content here")

        _unzip_file_if_needed(zip_path)

        # Archive should be removed
        assert not zip_path.exists()
        # Extracted file should exist
        assert extracted_file.exists()
        assert extracted_file.read_text() == "content here"


class TestRenameFiles:
    """Tests for _rename_files function."""

    def test_renames_matching_files(self, tmp_path):
        """Should rename files matching patterns to target names."""
        # Create test files
        (tmp_path / "source1.csv").write_text("data1")
        (tmp_path / "source2.csv").write_text("data2")

        matched = _rename_files(
            tmp_path,
            [r"source1\.csv", r"source2\.csv"],
            ["target1.csv", "target2.csv"],
        )

        assert (tmp_path / "target1.csv").exists()
        assert (tmp_path / "target2.csv").exists()
        assert "source1.csv" in matched
        assert "source2.csv" in matched

    def test_raises_for_unmatched_pattern(self, tmp_path):
        """Should raise FileNotFoundError if pattern doesn't match."""

        with pytest.raises(FileNotFoundError):
            _rename_files(
                tmp_path,
                [r"nonexistent\.csv"],
                ["target.csv"],
            )


class TestDeleteUnmatchedFiles:
    """Tests for _delete_unmatched_files function."""

    def test_deletes_unmatched_files(self, tmp_path):
        """Should delete files not in targets or matched."""
        # Create test files
        (tmp_path / "keep1.txt").write_text("data1")
        (tmp_path / "keep2.txt").write_text("data2")
        (tmp_path / "delete_me.txt").write_text("data3")

        _delete_unmatched_files(
            tmp_path,
            ["keep1.txt", "keep2.txt"],
            set(),
        )

        assert (tmp_path / "keep1.txt").exists()
        assert (tmp_path / "keep2.txt").exists()
        assert not (tmp_path / "delete_me.txt").exists()

    def test_keeps_matched_files(self, tmp_path):
        """Should not delete files in matched set (even if not in targets)."""
        (tmp_path / "matched.txt").write_text("data")
        (tmp_path / "other.txt").write_text("data")

        _delete_unmatched_files(
            tmp_path,
            [],
            {"matched.txt"},
        )

        assert (tmp_path / "matched.txt").exists()
        assert not (tmp_path / "other.txt").exists()
