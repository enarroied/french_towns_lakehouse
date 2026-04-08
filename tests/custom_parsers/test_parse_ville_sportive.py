"""Tests for flows_staging.custom_parsers.parse_ville_sportive module."""

from collections import defaultdict

import pytest
from flows_staging.custom_parsers.parse_ville_sportive import COL_BREAKS
from flows_staging.custom_parsers.parse_ville_sportive import ENTRY_RE
from flows_staging.custom_parsers.parse_ville_sportive import JUNK_RE
from flows_staging.custom_parsers.parse_ville_sportive import _flush
from flows_staging.custom_parsers.parse_ville_sportive import col_index
from flows_staging.custom_parsers.parse_ville_sportive import parse_palmares


class TestColIndex:
    """Tests for col_index function."""

    def test_returns_0_for_left_column(self):
        """Should return 0 for x < first break."""
        assert col_index(50) == 0
        assert col_index(100) == 0
        assert col_index(189) == 0

    def test_returns_1_for_middle_column(self):
        """Should return 1 for first_break <= x < second_break."""
        assert col_index(190) == 1
        assert col_index(250) == 1
        assert col_index(369) == 1

    def test_returns_2_for_right_column(self):
        """Should return 2 for x >= second_break."""
        assert col_index(370) == 2
        assert col_index(500) == 2
        assert col_index(1000) == 2

    def test_boundary_at_first_break(self):
        """Boundary at COL_BREAKS[0] (190) should return 1."""
        assert col_index(COL_BREAKS[0]) == 1

    def test_boundary_at_second_break(self):
        """Boundary at COL_BREAKS[1] (370) should return 2."""
        assert col_index(COL_BREAKS[1]) == 2


class TestFlush:
    """Tests for _flush function."""

    def test_skips_empty_words(self):
        """Should skip when words list is empty."""
        col_tokens = defaultdict(list)
        _flush(0, [], col_tokens, 0)
        assert (0, 0) not in col_tokens

    def test_skips_junk_text(self):
        """Should skip text matching JUNK_RE pattern."""
        col_tokens = defaultdict(list)
        words = [{"text": "Palmar", "top": 100, "x0": 50}]
        _flush(0, words, col_tokens, 0)
        assert (0, 0) not in col_tokens

    def test_handles_corsican_dept_codes(self):
        """Should handle Corsican dept codes like 2A, 2B in _flush."""
        col_tokens = defaultdict(list)
        # The ENTRY_RE pattern expects full text like "Commune (2A)"
        words = [{"text": "Bastia (2A)", "top": 100, "x0": 50}]
        _flush(0, words, col_tokens, 0)
        # The entry should be added because ENTRY_RE matches "(2A)"
        assert len(col_tokens[(0, 0)]) > 0


class TestRegexPatterns:
    """Tests for regex patterns used in parsing."""

    def test_entry_re_matches_commune_format(self):
        """ENTRY_RE should match 'Commune (XX)' format."""
        assert ENTRY_RE.match("Paris (75)") is not None
        assert ENTRY_RE.match("Lyon (69)") is not None
        assert ENTRY_RE.match("Bastia (2A)") is not None

    def test_entry_re_extracts_name_and_dept(self):
        """ENTRY_RE should capture commune name and dept code."""
        match = ENTRY_RE.match("Marseille (13)")
        assert match is not None
        assert match.group(1).strip() == "Marseille"
        assert match.group(2) == "13"

    def test_entry_re_handles_leading_spaces(self):
        """ENTRY_RE should handle leading whitespace."""
        match = ENTRY_RE.match("  Nice (06)")
        assert match is not None
        assert match.group(1).strip() == "Nice"

    def test_entry_re_handles_2a_2b(self):
        """ENTRY_RE should handle Corsican codes."""
        assert ENTRY_RE.match("Ajaccio (2A)") is not None
        assert ENTRY_RE.match("Bastia (2B)") is not None

    def test_entry_re_no_match_without_parentheses(self):
        """ENTRY_RE should not match without parentheses."""
        assert ENTRY_RE.match("Paris") is None
        assert ENTRY_RE.match("75") is None

    def test_junk_re_matches_palmar(self):
        """JUNK_RE should match 'Palmar' (palmares header)."""
        assert JUNK_RE.search("Palmares") is not None
        assert JUNK_RE.search("PALMAR") is not None

    def test_junk_re_matches_year(self):
        """JUNK_RE should match 4-digit years."""
        assert JUNK_RE.search("2024") is not None
        assert JUNK_RE.search("2025") is not None

    def test_junk_re_matches_internet(self):
        """JUNK_RE should match 'internet'."""
        assert JUNK_RE.search("internet") is not None

    def test_junk_re_matches_label(self):
        """JUNK_RE should match 'labelli'."""
        assert JUNK_RE.search("labellisee") is not None


class TestParsePalmares:
    """Tests for parse_palmares function using real PDF fixture."""

    def test_parses_sample_pdf(self):
        """Should parse the sample PDF fixture."""
        pdf_path = pytest.importorskip("pathlib").Path("tests/fixtures/sample.pdf")

        if not pdf_path.exists():
            pytest.skip("Sample PDF fixture not found")

        results = parse_palmares(pdf_path)

        # Should find some entries
        assert len(results) > 0, "Should find entries in PDF"

        # Each result should have required fields
        for row in results:
            assert "commune" in row
            assert "dept_code" in row
            assert "nb_lauriers" in row

    def test_dept_code_format(self):
        """Dept codes should be 2 digits or Corsican letters (2A/2B) or overseas (971-976)."""
        pdf_path = pytest.importorskip("pathlib").Path("tests/fixtures/sample.pdf")

        if not pdf_path.exists():
            pytest.skip("Sample PDF fixture not found")

        results = parse_palmares(pdf_path)

        for row in results:
            dept = row["dept_code"]
            # French dept codes: 01-95 (2 digits), 2A/2B (Corsica), 971-976 (overseas)
            is_valid = (dept.isdigit() and len(dept) in (2, 3)) or dept in ("2A", "2B")
            assert is_valid, f"Invalid dept code: {dept}"

    def test_nb_lauriers_valid_range(self):
        """nb_lauriers should be 1-4."""
        pdf_path = pytest.importorskip("pathlib").Path("tests/fixtures/sample.pdf")

        if not pdf_path.exists():
            pytest.skip("Sample PDF fixture not found")

        results = parse_palmares(pdf_path)

        for row in results:
            assert 1 <= row["nb_lauriers"] <= 4, (
                f"nb_lauriers {row['nb_lauriers']} should be 1-4"
            )

    def test_commune_is_lowercase(self):
        """Commune names should be lowercase."""
        pdf_path = pytest.importorskip("pathlib").Path("tests/fixtures/sample.pdf")

        if not pdf_path.exists():
            pytest.skip("Sample PDF fixture not found")

        results = parse_palmares(pdf_path)

        for row in results:
            assert row["commune"] == row["commune"].lower(), (
                f"Commune {row['commune']} should be lowercase"
            )
