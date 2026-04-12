"""Tests for flows_staging.shared.config module."""

from flows_staging.shared.config import get_config
from flows_staging.shared.config import get_downloads
from flows_staging.shared.config import get_paths
from flows_staging.shared.config import get_scrapers


class TestGetConfig:
    """Tests for get_config function."""

    def test_returns_dict(self):
        """Should return a dictionary."""
        result = get_config()
        assert isinstance(result, dict)

    def test_has_required_keys(self):
        """Config should have required top-level keys."""
        result = get_config()
        assert "paths" in result
        assert "buckets" in result
        assert "download" in result

    def test_returns_same_instance(self):
        """Should return the same config instance on multiple calls."""
        result1 = get_config()
        result2 = get_config()
        assert result1 is result2


class TestGetPaths:
    """Tests for get_paths function."""

    def test_returns_paths_dict(self):
        """Should return paths dictionary."""
        result = get_paths()
        assert isinstance(result, dict)

    def test_has_temp_dir(self):
        """Paths should include temp_dir."""
        result = get_paths()
        assert "temp_dir" in result

    def test_temp_dir_is_string(self):
        """temp_dir should be a string path."""
        result = get_paths()
        assert isinstance(result["temp_dir"], str)


class TestGetDownloads:
    """Tests for get_get_downloads function."""

    def test_returns_list(self):
        """Should return a list of downloads."""
        result = get_downloads()
        assert isinstance(result, list)

    def test_downloads_have_required_fields(self):
        """Each download should have required fields."""
        downloads = get_downloads()
        if downloads:
            download = downloads[0]
            assert "name" in download
            assert "url" in download
            assert "domain" in download
            assert "target_folder" in download


class TestGetScrapers:
    """Tests for get_scrapers function."""

    def test_returns_list(self):
        """Should return a list of scrapers."""
        result = get_scrapers()
        assert isinstance(result, list)

    def test_scrapers_have_required_fields(self):
        """Each scraper should have required fields."""
        scrapers = get_scrapers()
        if scrapers:
            scraper = scrapers[0]
            assert "name" in scraper
            assert "module" in scraper
            assert "enabled" in scraper
            assert "domain" in scraper
            assert "target_folder" in scraper

    def test_scrapers_module_path_starts_with_flows_staging(self):
        """Scraper module paths should start with flows_staging."""
        scrapers = get_scrapers()
        for scraper in scrapers:
            assert scraper["module"].startswith("flows_staging")
