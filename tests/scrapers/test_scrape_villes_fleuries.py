"""Tests for flows_staging.scrapers.scrape_villes_fleuries module."""

from flows_staging.scrapers.scrape_villes_fleuries import _extract_flower_count
from flows_staging.scrapers.scrape_villes_fleuries import _extract_text
from flows_staging.scrapers.scrape_villes_fleuries import build_search_payload
from flows_staging.scrapers.scrape_villes_fleuries import build_xhr_headers
from flows_staging.scrapers.scrape_villes_fleuries import parse_row


class TestExtractText:
    """Tests for _extract_text function."""

    def test_strips_html_tags(self):
        """Should strip HTML tags from input."""
        result = _extract_text("<b>Paris</b>")
        assert result == "paris"

    def test_normalizes_to_lowercase(self):
        """Should convert text to lowercase."""
        result = _extract_text("LYON MARSEILLE")
        assert result == "lyon marseille"

    def test_strips_whitespace(self):
        """Should strip leading and trailing whitespace."""
        result = _extract_text("  Nice  ")
        assert result == "nice"

    def test_handles_nested_tags(self):
        """Should handle nested HTML tags."""
        result = _extract_text("<div><span><b>Toulouse</b></span></div>")
        assert result == "toulouse"

    def test_handles_empty_html(self):
        """Should handle empty HTML."""
        result = _extract_text("")
        assert result == ""


class TestExtractFlowerCount:
    """Tests for _extract_flower_count function."""

    def test_extracts_count_from_img_src(self):
        """Should extract flower count from image src path."""
        html = '<img src="/images/4.png">'
        assert _extract_flower_count(html) == 4

    def test_extracts_one_flower(self):
        """Should handle 1 flower case."""
        html = '<img src="/images/1.png" alt="1 fleur">'
        assert _extract_flower_count(html) == 1

    def test_extracts_two_flowers(self):
        """Should handle 2 flowers case."""
        html = '<img src="/assets/2.png" />'
        assert _extract_flower_count(html) == 2

    def test_extracts_three_flowers(self):
        """Should handle 3 flowers case."""
        html = '<img src="https://example.com/3.png">'
        assert _extract_flower_count(html) == 3

    def test_returns_zero_when_no_match(self):
        """Should return 0 when no flower count is found."""
        html = '<div class="content">no image here</div>'
        assert _extract_flower_count(html) == 0

    def test_returns_zero_for_empty_html(self):
        """Should return 0 for empty HTML."""
        assert _extract_flower_count("") == 0


class TestParseRow:
    """Tests for parse_row function."""

    def test_parses_valid_row(self):
        """Should correctly parse a valid row."""
        row = [
            "<b>Paris</b>",
            "<i>Ile-de-France</i>",
            "<span>75</span>",
            "",  # Column 3 is typically empty
            '<img src="/images/4.png">',
        ]
        result = parse_row(row)

        assert result["commune"] == "paris"
        assert result["region"] == "ile-de-france"
        assert result["departement"] == "75"
        assert result["nb_fleurs"] == 4

    def test_handles_accented_characters(self):
        """Should correctly handle accented characters."""
        row = [
            "<b>Guérande</b>",
            "<i>Bretagne</i>",
            "<span>44</span>",
            "",
            '<img src="/images/3.png">',
        ]
        result = parse_row(row)

        assert result["commune"] == "guérande"
        assert result["region"] == "bretagne"

    def test_handles_special_characters_in_department(self):
        """Should handle special characters like A/B for Corsica."""
        row = [
            "<b>Ajaccio</b>",
            "<i>Corse</i>",
            "<span>2A</span>",
            "",
            '<img src="/images/2.png">',
        ]
        result = parse_row(row)

        assert result["departement"] == "2a"


class TestBuildXRHHeaders:
    """Tests for build_xhr_headers function."""

    def test_includes_required_headers(self):
        """Should include all required headers for XHR request."""
        headers = build_xhr_headers("TestAgent/1.0", "https://example.com/page")

        assert "Accept" in headers
        assert "Content-Type" in headers
        assert "User-Agent" in headers
        assert "X-Requested-With" in headers
        assert headers["X-Requested-With"] == "XMLHttpRequest"

    def test_uses_correct_referrer(self):
        """Should use provided referrer URL."""
        referrer = "https://villes-et-villages-fleuris.com/les-communes-labelisees"
        headers = build_xhr_headers("TestAgent/1.0", referrer)

        assert headers["Referer"] == referrer

    def test_uses_correct_origin(self):
        """Should set Origin header correctly."""
        headers = build_xhr_headers(
            "TestAgent/1.0", "https://villes-et-villages-fleuris.com/page"
        )

        assert headers["Origin"] == "https://villes-et-villages-fleuris.com"


class TestBuildSearchPayload:
    """Tests for build_search_payload function."""

    def test_includes_action(self):
        """Payload should include action field."""
        payload = build_search_payload(0, 100)
        assert payload["action"] == "search"

    def test_includes_start_parameter(self):
        """Payload should include start parameter."""
        payload = build_search_payload(50, 100)
        assert payload["start"] == "50"

    def test_includes_length_parameter(self):
        """Payload should include length parameter."""
        payload = build_search_payload(0, 500)
        assert payload["length"] == "500"

    def test_includes_search_settings(self):
        """Payload should include search settings."""
        payload = build_search_payload(0, 100)
        assert "search[value]" in payload
        assert "search[regex]" in payload

    def test_includes_columns(self):
        """Payload should include column definitions."""
        payload = build_search_payload(0, 100)
        assert "columns[0][data]" in payload
        assert "columns[0][searchable]" in payload
