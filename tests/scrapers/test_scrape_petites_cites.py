"""Tests for flows_staging.scrapers.scrape_petites_cites module."""

from flows_staging.scrapers.scrape_petites_cites import parse_city_page
from flows_staging.scrapers.scrape_petites_cites import parse_sitemap


class TestParseSitemap:
    """Tests for parse_sitemap function."""

    def test_extracts_city_urls(self):
        """Should extract URLs containing '/cites/'."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset>
            <url>
                <loc>https://example.com/cities/list</loc>
            </url>
            <url>
                <loc>https://example.com/cites/rochefort</loc>
            </url>
            <url>
                <loc>https://example.com/cites/saint-malo</loc>
            </url>
            <url>
                <loc>https://example.com/about</loc>
            </url>
        </urlset>
        """
        result = parse_sitemap(xml)

        assert len(result) == 2
        assert "https://example.com/cites/rochefort" in result
        assert "https://example.com/cites/saint-malo" in result
        assert "https://example.com/cities/list" not in result
        assert "https://example.com/about" not in result

    def test_returns_empty_list_when_no_cities(self):
        """Should return empty list when no /cites/ URLs found."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset>
            <url>
                <loc>https://example.com/about</loc>
            </url>
            <url>
                <loc>https://example.com/contact</loc>
            </url>
        </urlset>
        """
        result = parse_sitemap(xml)

        assert result == []

    def test_handles_empty_xml(self):
        """Should return empty list for empty XML."""
        result = parse_sitemap("")
        assert result == []

    def test_extracts_all_cities(self):
        """Should extract all URLs containing /cites/."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset>
            <url><loc>https://site.com/cites/a</loc></url>
            <url><loc>https://site.com/cites/b</loc></url>
            <url><loc>https://site.com/cites/c</loc></url>
        </urlset>
        """
        result = parse_sitemap(xml)

        assert len(result) == 3


class TestParseCityPage:
    """Tests for parse_city_page function."""

    def test_parses_valid_page(self):
        """Should parse city name and department from valid page."""
        html = """
        <html>
            <body>
                <h1 class="cover-title">Rochefort</h1>
                <div class="location">Charente-Maritime, France (17)</div>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result is not None
        assert result["city"] == "rochefort"
        assert result["department"] == "france (17)"

    def test_returns_none_when_no_h1(self):
        """Should return None when no h1 with cover-title found."""
        html = """
        <html>
            <body>
                <div class="location">Some location</div>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result is None

    def test_returns_none_when_no_location(self):
        """Should return None when no location div found."""
        html = """
        <html>
            <body>
                <h1 class="cover-title">Some City</h1>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result is None

    def test_handles_location_without_comma(self):
        """Should handle location text without comma separator."""
        html = """
        <html>
            <body>
                <h1 class="cover-title">Toulouse</h1>
                <div class="location">Haute-Garonne (31)</div>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result is not None
        assert result["city"] == "toulouse"
        assert result["department"] == "haute-garonne (31)"

    def test_city_name_is_lowercase(self):
        """City name should be converted to lowercase."""
        html = """
        <html>
            <body>
                <h1 class="cover-title">PARIS</h1>
                <div class="location">Ile-de-France, France (75)</div>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result["city"] == "paris"

    def test_department_is_lowercase(self):
        """Department should be converted to lowercase."""
        html = """
        <html>
            <body>
                <h1 class="cover-title">Lyon</h1>
                <div class="location">RHONE, FRANCE (69)</div>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result["department"] == "france (69)"

    def test_extracts_only_department_from_comma_separated(self):
        """Should extract only part after comma for department."""
        html = """
        <html>
            <body>
                <h1 class="cover-title">Brest</h1>
                <div class="location">Finistere, FINISTERE (29)</div>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result is not None
        assert result["department"] == "finistere (29)"

    def test_returns_none_when_city_is_empty(self):
        """Should return None when h1 exists but is empty."""
        html = """
        <html>
            <body>
                <h1 class="cover-title"></h1>
                <div class="location">Some location</div>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result is None

    def test_handles_accented_characters(self):
        """Should correctly handle accented characters."""
        html = """
        <html>
            <body>
                <h1 class="cover-title">Guérande</h1>
                <div class="location">Loire-Atlantique, France (44)</div>
            </body>
        </html>
        """
        result = parse_city_page(html)

        assert result is not None
        assert result["city"] == "guérande"
