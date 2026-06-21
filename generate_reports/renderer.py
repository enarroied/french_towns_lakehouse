import logging
import os
from pathlib import Path

import altair as alt
import pandas as pd
from generate_reports.config import ACCENT_BLUE
from generate_reports.config import ACCENT_GREEN
from generate_reports.config import ACCENT_RED
from generate_reports.config import DARK_BG
from generate_reports.config import DPI
from generate_reports.config import HEIGHT
from generate_reports.config import LIGHT_GRAY
from generate_reports.config import MARGIN_X
from generate_reports.config import MARGIN_Y
from generate_reports.config import WHITE
from generate_reports.config import WIDTH
from generate_reports.config import get_font_path
from generate_reports.queries import get_population_history
from generate_reports.queries import get_population_timeseries
from generate_reports.queries import get_salary_history
from generate_reports.queries import get_salary_timeseries
from generate_reports.tables import create_department_summary_table
from great_tables import GT
from pdf2image import convert_from_bytes
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont
from weasyprint import HTML


logger = logging.getLogger(__name__)

alt.data_transformers.enable("default")
alt.renderers.enable("default")


def _text_bbox(text: str, font: ImageFont.FreeTypeFont) -> tuple[int, int, int, int]:
    return font.getbbox(text)


def _draw_top_left_text(
    draw,
    text: str,
    font: ImageFont.FreeTypeFont,
    fill: tuple[int, int, int] = WHITE,
) -> None:
    draw.text((MARGIN_X, MARGIN_Y), text, font=font, fill=fill)


def create_slide_hero_combined(
    city_name: str,
    dept_name: str,
    pop_value: int | None,
    sal_value: int | None,
    output_path: str | Path,
    pop_year: int | None = None,
    sal_year: int | None = None,
) -> None:
    img = Image.new("RGB", (WIDTH, HEIGHT), DARK_BG)
    draw = ImageDraw.Draw(img)

    font_city = ImageFont.truetype(get_font_path(light=True), 48)
    font_dept = ImageFont.truetype(get_font_path(light=True), 28)
    font_value = ImageFont.truetype(get_font_path(bold=True), 160)
    font_label = ImageFont.truetype(get_font_path(), 36)

    draw.text((MARGIN_X, MARGIN_Y), city_name, font=font_city, fill=LIGHT_GRAY)
    draw.text((MARGIN_X, MARGIN_Y + 60), dept_name, font=font_dept, fill=LIGHT_GRAY)

    center_x = WIDTH // 2

    def _draw_value(draw, value, label, x_center, color):
        if value is None or pd.isna(value):
            return
        value_str = f"{int(value):,}"
        bbox = _text_bbox(value_str, font_value)
        vw = bbox[2] - bbox[0]
        vh = bbox[3] - bbox[1]
        vx = x_center - vw // 2
        vy = (HEIGHT - vh) // 2 - 60
        draw.text((vx, vy), value_str, font=font_value, fill=WHITE)

        lb = _text_bbox(label, font_label)
        lw = lb[2] - lb[0]
        lx = x_center - lw // 2
        draw.text((lx, vy + vh + 60), label, font=font_label, fill=color)

    pop_label = "Population" + (f" ({pop_year})" if pop_year else "")
    sal_label = "Mean Salary (€)" + (f" ({sal_year})" if sal_year else "")

    _draw_value(draw, pop_value, pop_label, center_x // 2, ACCENT_GREEN)
    _draw_value(draw, sal_value, sal_label, center_x + center_x // 2, ACCENT_BLUE)

    img.save(output_path, dpi=(DPI, DPI), optimize=True)


def create_slide_trend(
    city_name: str,
    timeseries_df: pd.DataFrame,
    metric: str,
    output_path: str | Path,
) -> None:
    if timeseries_df.empty:
        img = Image.new("RGB", (WIDTH, HEIGHT), DARK_BG)
        draw = ImageDraw.Draw(img)
        font = ImageFont.truetype(get_font_path(), 36)
        cx = WIDTH // 2
        cy = HEIGHT // 2
        bbox = _text_bbox(f"No trend data for {city_name}", font)
        draw.text(
            (cx - (bbox[2] - bbox[0]) // 2, cy),
            f"No trend data for {city_name}",
            font=font,
            fill=ACCENT_RED,
        )
        img.save(output_path, dpi=(DPI, DPI))
        return None

    if metric == "population":
        y_col = "population"
        title = f"{city_name} — Population Trend"
        y_title = "Population"
    else:
        y_col = "mean_salary"
        title = f"{city_name} — Mean Salary Trend"
        y_title = "Mean Salary (€)"

    df_clean = timeseries_df.dropna(subset=["year", y_col]).copy()
    if df_clean.empty:
        return create_slide_trend(city_name, pd.DataFrame(), metric, output_path)

    df_clean["year"] = df_clean["year"].astype(int)

    chart = (
        alt.Chart(df_clean)
        .mark_line(
            color="#4CAF50",
            strokeWidth=4,
            point=alt.OverlayMarkDef(color="#4CAF50", size=60),
        )
        .encode(
            x=alt.X("year:O", title="Year", axis=alt.Axis(labelAngle=0)),
            y=alt.Y(y_col + ":Q", title=y_title),
        )
        .properties(
            title=title,
            width=1600,
            height=700,
            background="#141923",
            padding=30,
        )
        .configure_title(
            fontSize=36,
            font="Montserrat",
            color="white",
            anchor="start",
        )
        .configure_axis(
            labelFontSize=24,
            titleFontSize=28,
            labelColor="white",
            titleColor="white",
            gridColor="#333333",
        )
        .configure_view(strokeWidth=0)
    )

    chart.save(str(output_path), scale_factor=1)
    return None


def create_slide_table_png(
    df: pd.DataFrame,
    commune_name: str,
    department_name: str,
    department_code: str,
    table_type: str,
    output_path: str | Path,
) -> None:
    if df.empty or "year" not in df.columns:
        img = Image.new("RGB", (WIDTH, HEIGHT), DARK_BG)
        draw = ImageDraw.Draw(img)
        font = ImageFont.truetype(get_font_path(), 36)
        cx = WIDTH // 2
        cy = HEIGHT // 2
        bbox = _text_bbox(f"No data for {commune_name}", font)
        draw.text(
            (cx - (bbox[2] - bbox[0]) // 2, cy),
            f"No data for {commune_name}",
            font=font,
            fill=ACCENT_RED,
        )
        img.save(output_path, dpi=(DPI, DPI))
        return

    if table_type == "population":
        table_df = df[
            ["year", "population", "year_evolution", "year_evolution_percent"]
        ].copy()
        table_df.columns = [
            "Year",
            "Population",
            "Population Evolution",
            "Population Evolution (%)",
        ]
        title = f"Population for {commune_name} ({department_name} - {department_code})"
    else:
        table_df = df[["year", "mean_salary"]].copy()
        table_df.columns = ["Year", "Mean Salary (€)"]
        title = (
            f"Salary History for {commune_name} ({department_name} - {department_code})"
        )

    max_year = int(table_df["Year"].iloc[0])
    min_year = int(table_df["Year"].iloc[-1])
    subtitle = f"Data for Years {min_year} - {max_year}"

    gt = _build_table(table_df, title, subtitle, table_type)
    if gt is None:
        img = Image.new("RGB", (WIDTH, HEIGHT), DARK_BG)
        draw = ImageDraw.Draw(img)
        font = ImageFont.truetype(get_font_path(), 36)
        cx = WIDTH // 2
        cy = HEIGHT // 2
        bbox = _text_bbox(f"No data for {commune_name}", font)
        draw.text(
            (cx - (bbox[2] - bbox[0]) // 2, cy),
            f"No data for {commune_name}",
            font=font,
            fill=ACCENT_RED,
        )
        img.save(output_path, dpi=(DPI, DPI))
        return

    html = gt.as_raw_html()
    slide_css = f"""
        @page {{
            size: {WIDTH}px {HEIGHT}px;
            margin: 60px;
        }}
        body {{
            background-color: white;
            color: #222;
            font-family: 'Montserrat', 'DejaVu Sans', sans-serif;
            padding: 40px;
        }}
        .gt_table {{
            background-color: white !important;
            color: #222 !important;
            width: 100%;
        }}
        .gt_title {{
            color: #111 !important;
            font-size: 32px !important;
        }}
        .gt_subtitle {{
            color: #555 !important;
            font-size: 20px !important;
        }}
        .gt_heading {{
            background-color: #f5f5f5 !important;
        }}
        th {{
            background-color: #e0e0e0 !important;
            color: #111 !important;
            border-bottom: 2px solid #4CAF50 !important;
            font-size: 18px !important;
        }}
        td {{
            color: #222 !important;
            border-bottom: 1px solid #ddd !important;
            font-size: 16px !important;
        }}
        .gt_col_heading, .gt_row {{
            background-color: white !important;
        }}
    """

    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <style>{slide_css}</style>
</head>
<body>
    {html}
</body>
</html>"""

    pdf_bytes = HTML(string=full_html).write_pdf()
    images = convert_from_bytes(pdf_bytes, dpi=72, first_page=1, last_page=1)
    if images:
        images[0].save(str(output_path))


def _build_table(
    table_df: pd.DataFrame,
    title: str,
    subtitle: str,
    table_type: str,
) -> GT | None:
    if table_df.empty:
        return None

    def _fmt_arrow(v, fmt, suffix=""):
        if pd.isna(v):
            return "—"
        if v > 0:
            return f'<span style="color:green">&#9650; {v:{fmt}}{suffix}</span>'
        if v < 0:
            return f'<span style="color:red">&#9660; {v:{fmt}}{suffix}</span>'
        return f'<span style="color:gray">&#9644; {v:{fmt}}{suffix}</span>'

    if table_type == "population":
        tbl = table_df.copy()
        tbl["Population Evolution"] = tbl["Population Evolution"].apply(
            lambda v: _fmt_arrow(v, "+,.0f")
        )
        tbl["Population Evolution (%)"] = tbl["Population Evolution (%)"].apply(
            lambda v: _fmt_arrow(v, "+.1f", suffix="%")
        )

        gt = (
            GT(tbl)
            .tab_header(title=title, subtitle=subtitle)
            .fmt_integer(columns="Population")
            .fmt_markdown(columns="Population Evolution")
            .fmt_markdown(columns="Population Evolution (%)")
            .cols_align(
                align="right",
                columns=["Population Evolution", "Population Evolution (%)"],
            )
        )
    else:
        tbl = table_df.copy()
        gt = (
            GT(tbl)
            .tab_header(title=title, subtitle=subtitle)
            .fmt_integer(columns="Mean Salary (€)")
            .cols_align(align="right", columns="Mean Salary (€)")
        )

    return gt


def create_slide_comparison_combined(
    city_name: str,
    pop_val: float | None,
    pop_dept_avg: float | None,
    pop_ratio: float | None,
    sal_val: float | None,
    sal_dept_avg: float | None,
    sal_ratio: float | None,
    output_path: str | Path,
) -> None:
    metrics = []

    if (
        pop_val is not None
        and not pd.isna(pop_val)
        and pop_dept_avg is not None
        and not pd.isna(pop_dept_avg)
    ):
        metrics.append(
            {
                "name": "Population",
                "city_val": float(pop_val),
                "dept_val": float(pop_dept_avg),
                "ratio": pop_ratio
                if pop_ratio is not None and not pd.isna(pop_ratio)
                else pop_val / pop_dept_avg,
            }
        )

    if (
        sal_val is not None
        and not pd.isna(sal_val)
        and sal_dept_avg is not None
        and not pd.isna(sal_dept_avg)
    ):
        metrics.append(
            {
                "name": "Mean Salary (€)",
                "city_val": float(sal_val),
                "dept_val": float(sal_dept_avg),
                "ratio": sal_ratio
                if sal_ratio is not None and not pd.isna(sal_ratio)
                else sal_val / sal_dept_avg,
            }
        )

    if not metrics:
        img = Image.new("RGB", (WIDTH, HEIGHT), DARK_BG)
        draw = ImageDraw.Draw(img)
        font = ImageFont.truetype(get_font_path(), 36)
        cx = WIDTH // 2
        cy = HEIGHT // 2
        bbox = _text_bbox(f"No comparison data for {city_name}", font)
        draw.text(
            (cx - (bbox[2] - bbox[0]) // 2, cy),
            f"No comparison data for {city_name}",
            font=font,
            fill=ACCENT_RED,
        )
        img.save(output_path, dpi=(DPI, DPI))
        return

    color_scale = alt.Scale(
        domain=["Dept Avg", city_name],
        range=["#42A5F5", "#4CAF50"],
    )

    sub_charts = []
    for i, m in enumerate(metrics):
        max_val = max(m["city_val"], m["dept_val"])
        dept_pct = -m["dept_val"] / max_val * 100
        city_pct = m["city_val"] / max_val * 100

        rows = [
            {
                "row": "",
                "type": "Dept Avg",
                "pct": dept_pct,
                "raw_value": f"{int(m['dept_val']):,}",
            },
            {
                "row": "",
                "type": city_name,
                "pct": city_pct,
                "raw_value": f"{int(m['city_val']):,}",
            },
        ]
        df = pd.DataFrame(rows)

        ratio_text = (
            f"{m['ratio']:.1f}x {'above' if m['ratio'] >= 1 else 'below'} dept avg"
        )

        show_legend = i == 0
        legend_param = (
            alt.Legend(
                title="",
                titleFontSize=24,
                labelFontSize=22,
                labelColor="white",
                orient="top",
            )
            if show_legend
            else None
        )

        bar = (
            alt.Chart(df)
            .mark_bar(size=40)
            .encode(
                x=alt.X(
                    "pct:Q",
                    title="",
                    axis=alt.Axis(grid=False, labels=False, ticks=False),
                    scale=alt.Scale(domain=[-110, 110]),
                ),
                y=alt.Y(
                    "row:N",
                    title=m["name"],
                    axis=alt.Axis(
                        titleFontSize=28,
                        titleColor="white",
                        labelFontSize=24,
                        labelColor="white",
                        domain=False,
                        ticks=False,
                        labels=False,
                    ),
                ),
                color=alt.Color(
                    "type:N",
                    scale=color_scale,
                    legend=legend_param,
                ),
            )
        )

        dept_text = (
            alt.Chart(df[df["type"] == "Dept Avg"])
            .mark_text(
                align="right",
                baseline="middle",
                dx=-5,
                fontSize=22,
                color="white",
            )
            .encode(
                x=alt.X("pct:Q"),
                y=alt.Y("row:N"),
                text=alt.Text("raw_value:N"),
            )
        )
        city_text = (
            alt.Chart(df[df["type"] == city_name])
            .mark_text(
                align="left",
                baseline="middle",
                dx=5,
                fontSize=22,
                color="white",
            )
            .encode(
                x=alt.X("pct:Q"),
                y=alt.Y("row:N"),
                text=alt.Text("raw_value:N"),
            )
        )

        ratio_df = pd.DataFrame([{"label": ratio_text}])
        ratio_text_layer = (
            alt.Chart(ratio_df)
            .mark_text(
                align="center",
                baseline="top",
                dy=25,
                fontSize=22,
                color="#FF9800",
                fontStyle="bold",
            )
            .encode(x=alt.value(600), y=alt.value(140), text=alt.Text("label:N"))
        )

        chart = alt.layer(bar, dept_text, city_text, ratio_text_layer).properties(
            width=1200,
            height=200,
        )
        sub_charts.append(chart)

    if len(sub_charts) == 1:
        final = sub_charts[0]
    else:
        final = alt.vconcat(*sub_charts).resolve_legend(color="shared")

    final = (
        final.properties(
            title=alt.TitleParams(
                text=f"{city_name} — Comparison vs Department",
                fontSize=36,
                font="Montserrat",
                color="white",
                anchor="start",
            ),
            background="#141923",
            padding=40,
        )
        .configure_view(strokeWidth=0)
        .configure_axis(grid=False)
    )

    final.save(str(output_path), scale_factor=1)


def generate_city_slides(
    conn,
    city_row: dict,
    output_dir: str | Path,
) -> None:
    commune_id = city_row["id"]
    name = city_row["name"]
    dept_code = city_row["department_code"]
    dept_name = city_row["department_name"]

    slide_dir = Path(output_dir) / dept_code
    slide_dir.mkdir(parents=True, exist_ok=True)

    pop_ts = get_population_timeseries(conn, commune_id)
    sal_ts = get_salary_timeseries(conn, commune_id)
    pop_hist = get_population_history(conn, commune_id)
    sal_hist = get_salary_history(conn, commune_id)

    pop_val = city_row.get("population")
    pop_year = city_row.get("latest_population_year")
    pop_ratio = city_row.get("population_ratio")
    sal_val = city_row.get("mean_salary")
    sal_year = city_row.get("latest_salary_year")
    sal_ratio = city_row.get("salary_ratio")
    pop_avg = city_row.get("dept_avg_population")
    sal_avg = city_row.get("dept_avg_salary")

    pop_hist_labeled = pop_hist.copy()
    pop_hist_labeled["name"] = name
    pop_hist_labeled["department_name"] = dept_name
    pop_hist_labeled["department_code"] = dept_code

    sal_hist_labeled = sal_hist.copy()
    sal_hist_labeled["name"] = name
    sal_hist_labeled["department_name"] = dept_name
    sal_hist_labeled["department_code"] = dept_code

    create_slide_hero_combined(
        name,
        dept_name,
        int(pop_val) if pop_val is not None and not pd.isna(pop_val) else None,
        int(sal_val) if sal_val is not None and not pd.isna(sal_val) else None,
        slide_dir / f"{commune_id}_slide1.png",
        pop_year=int(pop_year)
        if pop_year is not None and not pd.isna(pop_year)
        else None,
        sal_year=int(sal_year)
        if sal_year is not None and not pd.isna(sal_year)
        else None,
    )

    create_slide_trend(
        name,
        pop_ts,
        "population",
        slide_dir / f"{commune_id}_slide2.png",
    )

    create_slide_table_png(
        pop_hist_labeled,
        name,
        dept_name,
        dept_code,
        "population",
        slide_dir / f"{commune_id}_slide3.png",
    )

    create_slide_trend(
        name,
        sal_ts,
        "salary",
        slide_dir / f"{commune_id}_slide4.png",
    )

    create_slide_table_png(
        sal_hist_labeled,
        name,
        dept_name,
        dept_code,
        "salary",
        slide_dir / f"{commune_id}_slide5.png",
    )

    create_slide_comparison_combined(
        name,
        float(pop_val) if pop_val is not None and not pd.isna(pop_val) else None,
        float(pop_avg) if pop_avg is not None and not pd.isna(pop_avg) else None,
        pop_ratio,
        float(sal_val) if sal_val is not None and not pd.isna(sal_val) else None,
        float(sal_avg) if sal_avg is not None and not pd.isna(sal_avg) else None,
        sal_ratio,
        slide_dir / f"{commune_id}_slide6.png",
    )


def generate_dept_pdf(
    city_slides: list[Path],
    df: pd.DataFrame,
    department_name: str,
    output_path: str | Path,
) -> None:
    pages = []

    for slide_path in city_slides:
        if slide_path.exists():
            pages.append(
                f'<img src="{slide_path.resolve().as_uri()}" style="width:100%;page-break-after:always;">'
            )

    gt = create_department_summary_table(df, department_name)
    if gt is not None:
        pages.append(gt.as_raw_html())

    if not pages:
        logger.warning("No content for department %s, skipping PDF", department_name)
        return

    css = """
        @page {
            size: A4 landscape;
            margin: 10mm;
        }
        body {
            background-color: #141923;
            margin: 0;
            padding: 0;
        }
        img {
            display: block;
            max-width: 100%;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            color: white;
            font-family: 'Montserrat', 'DejaVu Sans', sans-serif;
        }
        th, td {
            padding: 8px 12px;
            text-align: right;
        }
        th {
            background-color: #2a2f3f;
            color: white;
            font-weight: bold;
            border-bottom: 2px solid #4CAF50;
        }
        td {
            color: white;
            border-bottom: 1px solid #333;
        }
    """

    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <style>{css}</style>
</head>
<body>
    {"".join(pages)}
</body>
</html>"""

    os.environ["WEASYPRINT_ALLOW_LOCAL"] = "true"
    HTML(string=full_html).write_pdf(str(output_path))
