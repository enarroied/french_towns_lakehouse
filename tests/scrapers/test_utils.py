"""Tests for flows_staging.scrapers.utils module."""

import pytest
from flows_staging.scrapers.utils import get_scraper_config


class TestGetScraperConfig:
    """Tests for get_scraper_config function."""

    def test_returns_scraper_by_module_path(self, sample_config):
        """Should return the scraper config matching the module path."""
        result = get_scraper_config(
            config=sample_config,
            module="flows_staging.scrapers.scrape_villes_fleuries",
        )

        assert result.name == "villes_fleuries"
        assert result.module == "flows_staging.scrapers.scrape_villes_fleuries"

    def test_raises_stop_iteration_when_not_found(self, sample_config):
        """Should raise StopIteration when no matching scraper found."""
        with pytest.raises(StopIteration):
            get_scraper_config(
                config=sample_config,
                module="nonexistent.module",
            )

    def test_returns_all_scraper_fields(self, sample_config):
        """Should return a ScraperConfig with all fields."""
        result = get_scraper_config(
            config=sample_config,
            module="flows_staging.scrapers.scrape_petites_cites",
        )

        assert result.name == "petites_cites"
        assert result.enabled is True
        assert result.domain == "labels"
        assert result.target_folder == "labels"
        assert result.url == "https://www.petitescitesdecaractere.com/cites-sitemap.xml"

    def test_scraper_has_extra_fields(self, sample_config):
        """Scrapers with extra fields should have them in extra dict."""
        result = get_scraper_config(
            config=sample_config,
            module="flows_staging.scrapers.scrape_villes_fleuries",
        )

        assert "endpoint" in result.extra
        assert result.extra["page_size"] == 1000
        assert result.extra["crawl_delay"] == 1
