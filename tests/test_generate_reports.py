from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from generate_reports.renderer import _build_table
from generate_reports.renderer import create_slide_comparison_combined
from generate_reports.renderer import create_slide_hero_combined
from generate_reports.renderer import create_slide_table_png
from generate_reports.renderer import create_slide_trend
from generate_reports.renderer import generate_dept_pdf
from PIL import Image


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pop_timeseries() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2015, 2016, 2017, 2018, 2019],
            "population": [5000, 5100, 5200, 5300, 5400],
            "year_evolution": [None, 100, 100, 100, 100],
            "year_evolution_percent": [None, 2.0, 1.96, 1.92, 1.89],
        }
    )


@pytest.fixture
def sal_timeseries() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2019, 2020, 2021, 2022, 2023],
            "mean_salary": [25000, 26000, 27000, 28000, 29000],
        }
    )


@pytest.fixture
def pop_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2019, 2018, 2017, 2016, 2015],
            "population": [5400, 5300, 5200, 5100, 5000],
            "year_evolution": [100, 100, 100, 100, None],
            "year_evolution_percent": [1.89, 1.92, 1.96, 2.0, None],
        }
    )


@pytest.fixture
def sal_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2023, 2022, 2021, 2020, 2019],
            "mean_salary": [29000, 28000, 27000, 26000, 25000],
        }
    )


# ---------------------------------------------------------------------------
# Test create_slide_hero_combined
# ---------------------------------------------------------------------------


class TestCreateSlideHeroCombined:
    def test_valid_data(self, tmp_path: Path) -> None:

        output = tmp_path / "hero.png"
        create_slide_hero_combined(
            "Foxton",
            "Test Dept",
            50000,
            35000,
            output,
            pop_year=2023,
            sal_year=2023,
        )
        assert output.exists()
        img = Image.open(output)
        assert img.size == (1920, 1080)

    def test_pop_none(self, tmp_path: Path) -> None:

        output = tmp_path / "hero.png"
        create_slide_hero_combined("Foxton", "Test Dept", None, 35000, output)
        assert output.exists()
        Image.open(output).verify()

    def test_sal_none(self, tmp_path: Path) -> None:

        output = tmp_path / "hero.png"
        create_slide_hero_combined("Foxton", "Test Dept", 50000, None, output)
        assert output.exists()
        Image.open(output).verify()

    def test_both_none(self, tmp_path: Path) -> None:

        output = tmp_path / "hero.png"
        create_slide_hero_combined("Foxton", "Test Dept", None, None, output)
        assert output.exists()
        Image.open(output).verify()

    def test_pop_nan(self, tmp_path: Path) -> None:

        output = tmp_path / "hero.png"
        create_slide_hero_combined("Foxton", "Test Dept", float("nan"), 35000, output)
        assert output.exists()
        Image.open(output).verify()

    def test_pop_none_year(self, tmp_path: Path) -> None:

        output = tmp_path / "hero.png"
        create_slide_hero_combined(
            "Foxton",
            "Test Dept",
            50000,
            35000,
            output,
            pop_year=None,
            sal_year=2023,
        )
        assert output.exists()


# ---------------------------------------------------------------------------
# Test create_slide_trend
# ---------------------------------------------------------------------------


class TestCreateSlideTrend:
    def test_population_metric(
        self, tmp_path: Path, pop_timeseries: pd.DataFrame
    ) -> None:

        output = tmp_path / "trend.png"
        result = create_slide_trend("Foxton", pop_timeseries, "population", output)
        assert result is None
        assert output.exists()
        img = Image.open(output)
        assert img.size[0] > 0 and img.size[1] > 0

    def test_salary_metric(self, tmp_path: Path, sal_timeseries: pd.DataFrame) -> None:

        output = tmp_path / "trend.png"
        result = create_slide_trend("Foxton", sal_timeseries, "salary", output)
        assert result is None
        assert output.exists()

    def test_empty_df(self, tmp_path: Path) -> None:

        output = tmp_path / "trend.png"
        result = create_slide_trend("Foxton", pd.DataFrame(), "population", output)
        assert result is None
        assert output.exists()

    def test_all_nan_df(self, tmp_path: Path) -> None:

        df = pd.DataFrame({"year": [2020], "population": [float("nan")]})
        output = tmp_path / "trend.png"
        result = create_slide_trend("Foxton", df, "population", output)
        assert result is None
        assert output.exists()


# ---------------------------------------------------------------------------
# Test create_slide_table_png
# ---------------------------------------------------------------------------


class TestCreateSlideTablePng:
    def test_population_table(self, tmp_path: Path, pop_history: pd.DataFrame) -> None:

        output = tmp_path / "table.png"
        create_slide_table_png(
            pop_history, "Foxton", "Test Dept", "99", "population", output
        )
        assert output.exists()
        Image.open(output).verify()

    def test_salary_table(self, tmp_path: Path, sal_history: pd.DataFrame) -> None:

        output = tmp_path / "table.png"
        create_slide_table_png(
            sal_history, "Foxton", "Test Dept", "99", "salary", output
        )
        assert output.exists()
        Image.open(output).verify()

    def test_empty_df(self, tmp_path: Path) -> None:

        output = tmp_path / "table.png"
        create_slide_table_png(
            pd.DataFrame(), "Foxton", "Test Dept", "99", "population", output
        )
        assert output.exists()
        Image.open(output).verify()


# ---------------------------------------------------------------------------
# Test create_slide_comparison_combined
# ---------------------------------------------------------------------------


class TestCreateSlideComparisonCombined:
    def test_both_metrics(self, tmp_path: Path) -> None:

        output = tmp_path / "comparison.png"
        create_slide_comparison_combined(
            "Foxton",
            50000,
            45000,
            1.11,
            35000,
            32000,
            1.09,
            output,
        )
        assert output.exists()
        Image.open(output).verify()

    def test_pop_only(self, tmp_path: Path) -> None:

        output = tmp_path / "comparison.png"
        create_slide_comparison_combined(
            "Foxton",
            50000,
            45000,
            1.11,
            None,
            None,
            None,
            output,
        )
        assert output.exists()
        Image.open(output).verify()

    def test_sal_only(self, tmp_path: Path) -> None:

        output = tmp_path / "comparison.png"
        create_slide_comparison_combined(
            "Foxton",
            None,
            None,
            None,
            35000,
            32000,
            1.09,
            output,
        )
        assert output.exists()
        Image.open(output).verify()

    def test_neither(self, tmp_path: Path) -> None:

        output = tmp_path / "comparison.png"
        create_slide_comparison_combined(
            "Foxton",
            None,
            None,
            None,
            None,
            None,
            None,
            output,
        )
        assert output.exists()
        Image.open(output).verify()

    def test_below_avg_ratio(self, tmp_path: Path) -> None:

        output = tmp_path / "comparison.png"
        create_slide_comparison_combined(
            "Foxton",
            4000,
            5000,
            0.8,
            None,
            None,
            None,
            output,
        )
        assert output.exists()
        Image.open(output).verify()

    def test_nan_values(self, tmp_path: Path) -> None:

        output = tmp_path / "comparison.png"
        create_slide_comparison_combined(
            "Foxton",
            float("nan"),
            float("nan"),
            None,
            float("nan"),
            float("nan"),
            None,
            output,
        )
        assert output.exists()
        Image.open(output).verify()


# ---------------------------------------------------------------------------
# Test _build_table
# ---------------------------------------------------------------------------


class TestBuildTable:
    def test_population_type(self) -> None:

        df = pd.DataFrame(
            {
                "Year": [2020, 2019],
                "Population": [5000, 4800],
                "Population Evolution": [200, 150],
                "Population Evolution (%)": [4.17, 3.23],
            }
        )
        gt = _build_table(df, "Title", "Subtitle", "population")
        assert gt is not None
        html = gt.as_raw_html()
        assert "2020" in html
        assert "5,000" in html

    def test_salary_type(self) -> None:

        df = pd.DataFrame(
            {
                "Year": [2023, 2022],
                "Mean Salary (€)": [35000, 34000],
            }
        )
        gt = _build_table(df, "Title", "Subtitle", "salary")
        assert gt is not None
        html = gt.as_raw_html()
        assert "35,000" in html

    def test_empty_df(self) -> None:

        gt = _build_table(pd.DataFrame(), "Title", "Subtitle", "population")
        assert gt is None


# ---------------------------------------------------------------------------
# Test generate_dept_pdf  (minimal smoke)
# ---------------------------------------------------------------------------


class TestGenerateDeptPdf:
    def test_pdf_generated(self, tmp_path: Path) -> None:

        slide_dir = tmp_path / "slides"
        slide_dir.mkdir()
        hero = slide_dir / "001_slide1.png"
        img = Image.new("RGB", (100, 100))
        img.save(hero)

        df = pd.DataFrame(
            {
                "name": ["Foxton"],
                "population": [5000],
                "population_growth_pct": [2.0],
                "population_ratio": [1.2],
                "mean_salary": [35000],
                "salary_ratio": [1.1],
            }
        )
        pdf_path = tmp_path / "dept.pdf"
        generate_dept_pdf([hero], df, "Test Dept", pdf_path)
        assert pdf_path.exists()
        assert pdf_path.stat().st_size > 0

    def test_pdf_no_slides(self, tmp_path: Path) -> None:

        df = pd.DataFrame(
            {
                "name": ["Foxton"],
                "population": [5000],
                "population_growth_pct": [2.0],
                "population_ratio": [1.2],
                "mean_salary": [35000],
                "salary_ratio": [1.1],
            }
        )
        pdf_path = tmp_path / "dept.pdf"
        # empty slide list → should still generate a PDF with just the summary table
        generate_dept_pdf([], df, "Test Dept", pdf_path)
        assert pdf_path.exists()

    def test_pdf_empty_df(self, tmp_path: Path) -> None:

        pdf_path = tmp_path / "dept.pdf"
        generate_dept_pdf([], pd.DataFrame(), "Test Dept", pdf_path)
        # no content → should log a warning and skip
        # We just verify it doesn't crash
