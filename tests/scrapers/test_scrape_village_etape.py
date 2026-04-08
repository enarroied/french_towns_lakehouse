"""Tests for flows_staging.scrapers.scrape_village_etape module."""


import pytest
from bs4 import BeautifulSoup
from flows_staging.scrapers.scrape_village_etape import _parse_practical_info
from flows_staging.scrapers.scrape_village_etape import _parse_region_and_road
from flows_staging.scrapers.scrape_village_etape import _urls_from_headings
from flows_staging.scrapers.scrape_village_etape import _urls_from_json_attributes
from flows_staging.scrapers.scrape_village_etape import _urls_from_loop_items
from flows_staging.scrapers.scrape_village_etape import has_next_page
from flows_staging.scrapers.scrape_village_etape import parse_listing_urls
from flows_staging.scrapers.scrape_village_etape import parse_village_page


@pytest.fixture
def soup():
    """Create BeautifulSoup parser."""
    return BeautifulSoup


class TestUrlsFromJsonAttributes:
    """Tests for _urls_from_json_attributes function."""

    def test_extracts_url_from_valid_json(self):
        """Should extract URL from valid JSON attribute."""
        html = '<div data-ha-element-link=\'{"url": "/village-etape/test"}\'></div>'
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_json_attributes(soup)

        assert "/village-etape/test" in result

    def test_extracts_multiple_urls(self):
        """Should extract multiple URLs from JSON attributes."""
        html = """
        <div data-ha-element-link='{"url": "/village-etape/a"}'></div>
        <div data-ha-element-link='{"url": "/village-etape/b"}'></div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_json_attributes(soup)

        assert len(result) == 2
        assert "/village-etape/a" in result
        assert "/village-etape/b" in result

    def test_ignores_invalid_json(self):
        """Should ignore elements with invalid JSON."""
        html = """
        <div data-ha-element-link="not json"></div>
        <div data-ha-element-link=\'{"other": "value"}\'></div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_json_attributes(soup)

        assert result == []

    def test_handles_missing_url_key(self):
        """Should ignore JSON without url key."""
        html = '<div data-ha-element-link=\'{"other": "value"}\'></div>'
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_json_attributes(soup)

        assert result == []


class TestUrlsFromLoopItems:
    """Tests for _urls_from_loop_items function."""

    def test_extracts_url_from_e_loop_item(self):
        """Should extract URL from e-loop-item div."""
        html = """
        <div class="e-loop-item">
            <a href="/village-etape/test">Test Village</a>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_loop_items(soup)

        assert "/village-etape/test" in result

    def test_filters_non_village_urls(self):
        """Should filter out URLs that don't contain village-etape."""
        html = """
        <div class="e-loop-item">
            <a href="/other/page">Other</a>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_loop_items(soup)

        assert result == []

    def test_extracts_multiple_items(self):
        """Should extract URLs from multiple loop items."""
        html = """
        <div class="e-loop-item">
            <a href="/village-etape/village-1">Village 1</a>
        </div>
        <div class="e-loop-item">
            <a href="/village-etape/village-2">Village 2</a>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_loop_items(soup)

        assert len(result) == 2


class TestUrlsFromHeadings:
    """Tests for _urls_from_headings function."""

    def test_extracts_url_from_heading_parent(self):
        """Should extract URL from heading's parent anchor (anchor wraps heading)."""
        html = """
        <a href="/village-etape/test">
            <h2 class="elementor-heading-title">Test Village</h2>
        </a>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_headings(soup)

        assert "/village-etape/test" in result

    def test_ignores_headings_without_anchor_parent(self):
        """Should ignore headings without anchor parent."""
        html = '<h2 class="elementor-heading-title">Just Text</h2>'
        soup = BeautifulSoup(html, "html.parser")

        result = _urls_from_headings(soup)

        assert result == []


class TestParseListingUrls:
    """Tests for parse_listing_urls function."""

    def test_uses_first_successful_strategy(self):
        """Should return results from first strategy that finds URLs."""
        html = """
        <div data-ha-element-link='{"url": "/village-etape/from-json"}'></div>
        <div class="e-loop-item">
            <a href="/village-etape/from-loop">From Loop</a>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = parse_listing_urls(soup)

        # Should use JSON strategy first
        assert "/village-etape/from-json" in result
        assert "/village-etape/from-loop" not in result

    def test_returns_empty_list_when_no_urls_found(self):
        """Should return empty list when no strategies find URLs."""
        html = "<div>No URLs here</div>"
        soup = BeautifulSoup(html, "html.parser")

        result = parse_listing_urls(soup)

        assert result == []


class TestHasNextPage:
    """Tests for has_next_page function."""

    def test_returns_true_when_next_link_exists(self):
        """Should return True when next page link exists."""
        html = """
        <nav class="elementor-pagination">
            <a class="next" href="/page/2">Next</a>
        </nav>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = has_next_page(soup, current_page=1)

        assert result is True

    def test_returns_false_when_no_next_link(self):
        """Should return False when no next page link."""
        html = '<nav class="elementor-pagination"><span>1</span></nav>'
        soup = BeautifulSoup(html, "html.parser")

        result = has_next_page(soup, current_page=1)

        assert result is False

    def test_returns_true_up_to_page_3_when_no_pagination(self):
        """Should return True for pages 1-3 when no pagination found."""
        html = "<div>No pagination</div>"
        soup = BeautifulSoup(html, "html.parser")

        assert has_next_page(soup, current_page=1) is True
        assert has_next_page(soup, current_page=3) is True
        assert has_next_page(soup, current_page=4) is False


class TestParseRegionAndRoad:
    """Tests for _parse_region_and_road function."""

    def test_extracts_region_and_road(self):
        """Should extract region and road from icon list."""
        html = """
        <ul class="elementor-icon-list-items elementor-inline-items">
            <li class="elementor-icon-list-item">
                <span class="elementor-icon-list-text">Nouvelle-Aquitaine</span>
            </li>
            <li class="elementor-icon-list-item">
                <span class="elementor-icon-list-text">Route du Poisson</span>
            </li>
        </ul>
        """
        soup = BeautifulSoup(html, "html.parser")

        region, road = _parse_region_and_road(soup)

        assert region == "Nouvelle-Aquitaine"
        assert road == "Route du Poisson"

    def test_returns_none_when_no_icon_list(self):
        """Should return None when no icon list found."""
        html = "<div>No icon list</div>"
        soup = BeautifulSoup(html, "html.parser")

        region, road = _parse_region_and_road(soup)

        assert region is None
        assert road is None

    def test_handles_partial_data(self):
        """Should handle cases with only region or only road."""
        html = """
        <ul class="elementor-icon-list-items elementor-inline-items">
            <li class="elementor-icon-list-item">
                <span class="elementor-icon-list-text">Region Only</span>
            </li>
        </ul>
        """
        soup = BeautifulSoup(html, "html.parser")

        region, road = _parse_region_and_road(soup)

        assert region == "Region Only"
        assert road is None


class TestParsePracticalInfo:
    """Tests for _parse_practical_info function."""

    def test_extracts_mairie(self):
        """Should extract mairie info."""
        html = """
        <div class="elementor-icon-list-items">
            <li class="elementor-icon-list-item">
                <span class="elementor-icon-list-text">Mairie: 1 Rue Principale</span>
            </li>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _parse_practical_info(soup)

        assert "mairie" in result

    def test_extracts_tourist_office(self):
        """Should extract tourist office info."""
        html = """
        <div class="elementor-icon-list-items">
            <li class="elementor-icon-list-item">
                <span class="elementor-icon-list-text">Office de Tourisme: 5 Avenue des Arts</span>
            </li>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _parse_practical_info(soup)

        assert "tourist_office" in result

    def test_extracts_email(self):
        """Should extract email."""
        html = """
        <div class="elementor-icon-list-items">
            <li class="elementor-icon-list-item">
                <span class="elementor-icon-list-text">Contact: info@example.com</span>
            </li>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        result = _parse_practical_info(soup)

        assert "email" in result
        assert result["email"] == "Contact: info@example.com"

    def test_returns_empty_dict_when_no_info(self):
        """Should return empty dict when no practical info found."""
        html = "<div>No info</div>"
        soup = BeautifulSoup(html, "html.parser")

        result = _parse_practical_info(soup)

        assert result == {}


class TestParseVillagePage:
    """Tests for parse_village_page function."""

    def test_parses_complete_page(self):
        """Should parse a complete village page."""
        html = """
        <html>
        <body>
            <h1 class="elementor-heading-title">Test Village</h1>
            <ul class="elementor-icon-list-items elementor-inline-items">
                <li class="elementor-icon-list-item">
                    <span class="elementor-icon-list-text">Occitanie</span>
                </li>
                <li class="elementor-icon-list-item">
                    <span class="elementor-icon-list-text">Rue de la Paix</span>
                </li>
            </ul>
            <div class="elementor-widget-text-editor">
                <p>A beautiful village description.</p>
            </div>
        </body>
        </html>
        """
        url = "https://example.com/village-etape/test"

        result = parse_village_page(html, url)

        assert result is not None
        assert result["name"] == "Test Village"
        assert result["url"] == url
        assert result["region"] == "Occitanie"
        assert result["road"] == "Rue de la Paix"

    def test_returns_none_when_no_name(self):
        """Should return None when no village name found."""
        html = "<html><body><div>No h1</div></body></html>"
        url = "https://example.com/test"

        result = parse_village_page(html, url)

        assert result is None

    def test_handles_missing_optional_fields(self):
        """Should handle pages with missing optional fields."""
        html = """
        <html>
        <body>
            <h1>Village Without Details</h1>
        </body>
        </html>
        """
        url = "https://example.com/test"

        result = parse_village_page(html, url)

        assert result is not None
        assert result["name"] == "Village Without Details"
        assert result["region"] is None
        assert result["road"] is None
