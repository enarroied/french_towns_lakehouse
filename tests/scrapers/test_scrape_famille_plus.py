"""Tests for flows_staging.scrapers.scrape_famille_plus module."""

from bs4 import BeautifulSoup
from flows_staging.scrapers.scrape_famille_plus import _DESTINATION_TYPES
from flows_staging.scrapers.scrape_famille_plus import _parse_department_code
from flows_staging.scrapers.scrape_famille_plus import _parse_destination_type
from flows_staging.scrapers.scrape_famille_plus import parse_destinations


class TestParseDestinationType:
    """Tests for _parse_destination_type function."""

    def test_finds_mer_type(self):
        """Should find 'mer' destination type."""
        html = '<div class="other-class mer">content</div>'
        soup = BeautifulSoup(html, "html.parser")
        article = soup.find("div")
        result = _parse_destination_type(article)
        assert result == "mer"

    def test_finds_montagne_type(self):
        """Should find 'montagne' destination type."""
        html = '<div class="montagne">content</div>'
        soup = BeautifulSoup(html, "html.parser")
        article = soup.find("div")
        result = _parse_destination_type(article)
        assert result == "montagne"

    def test_finds_nature_type(self):
        """Should find 'nature' destination type."""
        html = '<div class="nature large">content</div>'
        soup = BeautifulSoup(html, "html.parser")
        article = soup.find("div")
        result = _parse_destination_type(article)
        assert result == "nature"

    def test_finds_ville_type(self):
        """Should find 'ville' destination type."""
        html = '<div class="ville">content</div>'
        soup = BeautifulSoup(html, "html.parser")
        article = soup.find("div")
        result = _parse_destination_type(article)
        assert result == "ville"

    def test_returns_none_when_no_match(self):
        """Should return None when no destination type found."""
        html = '<div class="other-class unknown">content</div>'
        soup = BeautifulSoup(html, "html.parser")
        article = soup.find("div")
        result = _parse_destination_type(article)
        assert result is None

    def test_returns_none_when_no_classes(self):
        """Should return None when article has no classes."""
        html = "<div>No classes here</div>"
        soup = BeautifulSoup(html, "html.parser")
        article = soup.find("div")
        result = _parse_destination_type(article)
        assert result is None

    def test_destination_types_defined(self):
        """Destination types should be properly defined."""
        assert "mer" in _DESTINATION_TYPES
        assert "montagne" in _DESTINATION_TYPES
        assert "nature" in _DESTINATION_TYPES
        assert "ville" in _DESTINATION_TYPES


class TestParseDepartmentCode:
    """Tests for _parse_department_code function."""

    def test_extracts_numeric_dept_code(self):
        """Should extract numeric department code."""
        html = '<p class="col-4">Some text (75)</p>'
        soup = BeautifulSoup(html, "html.parser")
        result = _parse_department_code(soup)
        assert result == "75"

    def test_extracts_corsican_2a(self):
        """Should extract Corsican '2A' department code."""
        html = '<p class="col-4">Corse (2A)</p>'
        soup = BeautifulSoup(html, "html.parser")
        result = _parse_department_code(soup)
        assert result == "2A"

    def test_extracts_corsican_2b(self):
        """Should extract Corsican '2B' department code."""
        html = '<p class="col-4">Haute-Corse (2B)</p>'
        soup = BeautifulSoup(html, "html.parser")
        result = _parse_department_code(soup)
        assert result == "2B"

    def test_extracts_from_align_self_center(self):
        """Should extract from 'align-self-center' class."""
        html = '<p class="align-self-center">Paris (75)</p>'
        soup = BeautifulSoup(html, "html.parser")
        result = _parse_department_code(soup)
        assert result == "75"

    def test_returns_none_when_no_paragraph(self):
        """Should return None when no suitable paragraph found."""
        html = "<div>No paragraph here</div>"
        soup = BeautifulSoup(html, "html.parser")
        result = _parse_department_code(soup)
        assert result is None

    def test_returns_none_when_no_dept_code(self):
        """Should return None when no department code in parentheses."""
        html = '<p class="col-4">No department code here</p>'
        soup = BeautifulSoup(html, "html.parser")
        result = _parse_department_code(soup)
        assert result is None


class TestParseDestinations:
    """Tests for parse_destinations function."""

    def test_parses_single_destination(self):
        """Should parse a single destination entry."""
        html = """
        <html>
        <body>
            <article class="node--type-destination mer">
                <h5><a href="#">Biarritz</a></h5>
                <p class="col-4">Pyrenees-Atlantiques (64)</p>
            </article>
        </body>
        </html>
        """
        results = parse_destinations(html)

        assert len(results) == 1
        assert results[0]["name"] == "Biarritz"
        assert results[0]["department_code"] == "64"
        assert results[0]["type"] == "mer"

    def test_parses_multiple_destinations(self):
        """Should parse multiple destination entries."""
        html = """
        <html>
        <body>
            <article class="node--type-destination ville">
                <h5><a href="#">Lyon</a></h5>
                <p class="col-4">Rhone (69)</p>
            </article>
            <article class="node--type-destination montagne">
                <h5><a href="#">Chamonix</a></h5>
                <p class="col-4">Haute-Savoie (74)</p>
            </article>
        </body>
        </html>
        """
        results = parse_destinations(html)

        assert len(results) == 2
        assert results[0]["name"] == "Lyon"
        assert results[1]["name"] == "Chamonix"

    def test_skips_articles_without_name(self):
        """Should skip articles without a name (h5/a)."""
        html = """
        <html>
        <body>
            <article class="node--type-destination mer">
                <p class="col-4">No h5 here (64)</p>
            </article>
        </body>
        </html>
        """
        results = parse_destinations(html)

        assert len(results) == 0

    def test_skips_articles_without_department(self):
        """Should skip articles without department code."""
        html = """
        <html>
        <body>
            <article class="node--type-destination mer">
                <h5><a href="#">Biarritz</a></h5>
                <p class="col-4">No code here</p>
            </article>
        </body>
        </html>
        """
        results = parse_destinations(html)

        assert len(results) == 0

    def test_skips_articles_without_type(self):
        """Should skip articles without destination type."""
        html = """
        <html>
        <body>
            <article class="node--type-destination">
                <h5><a href="#">Biarritz</a></h5>
                <p class="col-4">Pyrenees-Atlantiques (64)</p>
            </article>
        </body>
        </html>
        """
        results = parse_destinations(html)

        assert len(results) == 0

    def test_returns_empty_list_for_empty_page(self):
        """Should return empty list for page with no destinations."""
        html = "<html><body><p>No destinations here</p></body></html>"
        results = parse_destinations(html)

        assert results == []

    def test_handles_all_destination_types(self):
        """Should handle all four destination types."""
        html = """
        <html>
        <body>
            <article class="node--type-destination mer">
                <h5><a href="#">Sea Place</a></h5>
                <p class="col-4">Coast (00)</p>
            </article>
            <article class="node--type-destination montagne">
                <h5><a href="#">Mountain Place</a></h5>
                <p class="col-4">Alps (00)</p>
            </article>
            <article class="node--type-destination nature">
                <h5><a href="#">Nature Place</a></h5>
                <p class="col-4">Forest (00)</p>
            </article>
            <article class="node--type-destination ville">
                <h5><a href="#">City Place</a></h5>
                <p class="col-4">Metro (00)</p>
            </article>
        </body>
        </html>
        """
        results = parse_destinations(html)

        assert len(results) == 4
        types = {r["type"] for r in results}
        assert types == {"mer", "montagne", "nature", "ville"}
