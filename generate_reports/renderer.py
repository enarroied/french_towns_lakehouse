import logging
import os
from pathlib import Path

import altair as alt
import pandas as pd
from generate_reports.config import HEIGHT
from generate_reports.config import WIDTH
from generate_reports.queries import get_population_history
from generate_reports.queries import get_population_timeseries
from generate_reports.queries import get_salary_history
from generate_reports.queries import get_salary_timeseries
from great_tables import GT
from pdf2image import convert_from_bytes
from weasyprint import HTML


logger = logging.getLogger(__name__)

alt.data_transformers.enable("default")
alt.renderers.enable("default")


def create_slide_hero_combined(
    city_name: str,
    dept_name: str,
    pop_value: int | None,
    sal_value: int | None,
    output_path: str | Path,
    pop_year: int | None = None,
    sal_year: int | None = None,
) -> None:
    pop_is_null = pop_value is None or (
        isinstance(pop_value, float) and pd.isna(pop_value)
    )
    sal_is_null = sal_value is None or (
        isinstance(sal_value, float) and pd.isna(sal_value)
    )
    if pop_is_null and sal_is_null:
        logger.info(
            "Skipping hero slide for %s: no population or salary data", city_name
        )
        return

    pop_label = "Population" + (f" ({pop_year})" if pop_year else "")
    sal_label = "Mean Salary (€)" + (f" ({sal_year})" if sal_year else "")

    pop_html = ""
    if not pop_is_null:
        pop_html = f"""
    <div style="position:absolute;top:50%;left:25%;transform:translate(-50%,-60%);text-align:center;">
      <div style="font-size:100px;font-weight:700;color:white;line-height:1.1;">{int(pop_value):,}</div>
      <div style="font-size:28px;color:#4CAF50;margin-top:20px;">{pop_label}</div>
    </div>"""

    sal_html = ""
    if not sal_is_null:
        sal_html = f"""
    <div style="position:absolute;top:50%;left:75%;transform:translate(-50%,-60%);text-align:center;">
      <div style="font-size:100px;font-weight:700;color:white;line-height:1.1;">{int(sal_value):,}</div>
      <div style="font-size:28px;color:#42A5F5;margin-top:20px;">{sal_label}</div>
    </div>"""

    html_content = f"""<div style="width:100%;height:100vh;background-color:#141923;color:white;font-family:Montserrat,'DejaVu Sans',sans-serif;position:relative;overflow:hidden;">
  <div style="position:absolute;top:40px;left:40px;">
    <div style="font-size:32px;font-weight:300;color:#c8c8c8;">{city_name}</div>
    <div style="font-size:20px;font-weight:300;color:#c8c8c8;margin-top:6px;">{dept_name}</div>
  </div>
  {pop_html}
  {sal_html}
</div>"""

    Path(output_path).with_suffix(".html").write_text(html_content)


def create_slide_trend(
    city_name: str,
    timeseries_df: pd.DataFrame,
    metric: str,
    output_path: str | Path,
) -> None:
    if metric == "population":
        y_col = "population"
        title = f"{city_name} — Population Trend"
        y_title = "Population"
    else:
        y_col = "mean_salary"
        title = f"{city_name} — Mean Salary Trend"
        y_title = "Mean Salary (€)"

    if timeseries_df.empty or y_col not in timeseries_df.columns:
        return

    df_clean = timeseries_df.dropna(subset=["year", y_col]).copy()
    if len(df_clean) < 2:
        return

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

    chart.save(str(output_path), scale_factor=0.5)
    return


def create_slide_table_png(
    df: pd.DataFrame,
    commune_name: str,
    department_name: str,
    department_code: str,
    table_type: str,
    output_path: str | Path,
) -> None:
    if df.empty or "year" not in df.columns:
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
        return

    html = gt.as_raw_html()
    Path(output_path).with_suffix(".html").write_text(html)
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
        images[0].save(str(output_path), optimize=True)


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

    final.save(str(output_path), scale_factor=0.5)


def generate_city_slides(
    conn,
    city_row: dict,
    output_dir: str | Path,
) -> None:
    commune_id = city_row["id"]
    name = city_row["name"]
    dept_code = city_row["department_code"]
    dept_name = city_row["department_name"]
    name_slug = (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("'", "")
        .replace("/", "_")
    )

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
        slide_dir / f"{commune_id}_{name_slug}_slide1.png",
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
        slide_dir / f"{commune_id}_{name_slug}_slide2.png",
    )

    create_slide_table_png(
        pop_hist_labeled,
        name,
        dept_name,
        dept_code,
        "population",
        slide_dir / f"{commune_id}_{name_slug}_slide3.png",
    )

    create_slide_trend(
        name,
        sal_ts,
        "salary",
        slide_dir / f"{commune_id}_{name_slug}_slide4.png",
    )

    create_slide_table_png(
        sal_hist_labeled,
        name,
        dept_name,
        dept_code,
        "salary",
        slide_dir / f"{commune_id}_{name_slug}_slide5.png",
    )

    create_slide_comparison_combined(
        name,
        float(pop_val) if pop_val is not None and not pd.isna(pop_val) else None,
        float(pop_avg) if pop_avg is not None and not pd.isna(pop_avg) else None,
        pop_ratio,
        float(sal_val) if sal_val is not None and not pd.isna(sal_val) else None,
        float(sal_avg) if sal_avg is not None and not pd.isna(sal_avg) else None,
        sal_ratio,
        slide_dir / f"{commune_id}_{name_slug}_slide6.png",
    )


def _build_combined_page_html(slide_path: Path, next_path: Path) -> str:
    table_html = next_path.with_suffix(".html").read_text()
    return f"""<div style="display:flex;width:100%;height:100vh;page-break-after:always;">
  <div style="width:50%;background-color:#141923;display:flex;align-items:center;justify-content:center;overflow:hidden;">
    <img src="{slide_path.resolve().as_uri()}" style="max-width:100%;max-height:100%;object-fit:contain;">
  </div>
  <div style="width:50%;background-color:white;padding:20px;overflow:auto;display:flex;align-items:center;justify-content:center;">
    <div style="width:100%;">
      <style>
        .gt_table {{ font-size: 10px !important; }}
        .gt_title {{ font-size: 14px !important; }}
        .gt_subtitle {{ font-size: 10px !important; }}
        .gt_heading {{ font-size: 12px !important; }}
        th {{ font-size: 10px !important; padding: 4px 6px !important; }}
        td {{ font-size: 9px !important; padding: 3px 6px !important; }}
      </style>
      {table_html}
    </div>
  </div>
</div>"""


def _build_toc_html(toc_items: list[tuple[str, str]], department_name: str) -> str:
    toc_items_sorted = sorted(toc_items, key=lambda x: x[0].lower())
    toc_entries = "".join(
        f'<li><a href="#city-{cid}" style="color:#4CAF50;text-decoration:none;font-size:14px;">{name}</a></li>'
        for name, cid in toc_items_sorted
    )
    return f"""<div style="page-break-after:always;color:white;padding:40px;background-color:#141923;min-height:100%;">
    <h1 style="font-family:Montserrat,DejaVu Sans,sans-serif;font-size:36px;">{department_name}</h1>
    <h2 style="font-family:Montserrat,DejaVu Sans,sans-serif;font-size:28px;color:#aaa;">Table of Contents</h2>
    <ul style="columns:3;list-style:none;padding:0;font-family:Montserrat,DejaVu Sans,sans-serif;">{toc_entries}</ul>
</div>"""


def _process_slides(
    city_slides: list[Path],
    city_names: dict,
) -> tuple[list[str], list[tuple[str, str]]]:
    pages: list[str] = []
    toc_items: list[tuple[str, str]] = []
    seen_ids: set[str] = set()

    i = 0
    while i < len(city_slides):
        slide_path = city_slides[i]
        if not slide_path.exists():
            i += 1
            continue

        stem = slide_path.stem
        commune_id = stem.split("_")[0]
        slide_num = int(stem.split("_")[-1].replace("slide", ""))

        if commune_id not in seen_ids:
            seen_ids.add(commune_id)
            name = city_names.get(commune_id, commune_id)
            toc_items.append((name, commune_id))
            pages.append(f'<a id="city-{commune_id}"></a>')

        is_trend = slide_num in (2, 4)
        has_next = i + 1 < len(city_slides)
        if is_trend and has_next:
            next_path = city_slides[i + 1]
            next_stem = next_path.stem
            next_slide_num = int(next_stem.split("_")[-1].replace("slide", ""))
            next_is_table = next_slide_num == slide_num + 1
            next_same_commune = next_stem.split("_")[0] == commune_id
            next_has_html = next_path.with_suffix(".html").exists()

            if next_is_table and next_same_commune and next_has_html:
                pages.append(_build_combined_page_html(slide_path, next_path))
                i += 2
                continue

        if slide_path.suffix == ".html":
            html_content = slide_path.read_text()
            pages.append(f'<div style="page-break-after:always;">{html_content}</div>')
        else:
            html_path = slide_path.with_suffix(".html")
            if html_path.exists():
                table_html = html_path.read_text()
                pages.append(
                    f'<div style="page-break-after:always;background-color:white;padding:40px;">{table_html}</div>'
                )
            else:
                pages.append(
                    f'<img src="{slide_path.resolve().as_uri()}" style="width:100%;page-break-after:always;">'
                )
        i += 1

    return pages, toc_items


def generate_dept_pdf(
    city_slides: list[Path],
    df: pd.DataFrame,
    department_name: str,
    output_path: str | Path,
) -> None:
    if df.empty or "id" not in df.columns:
        city_names = {}
    else:
        city_names = dict(zip(df["id"], df["name"], strict=False))

    pages, toc_items = _process_slides(city_slides, city_names)
    toc_html = _build_toc_html(toc_items, department_name) if toc_items else ""

    if not pages and not toc_html:
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
    """

    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <style>{css}</style>
</head>
<body>
    {toc_html}
    {"".join(pages)}
</body>
</html>"""

    os.environ["WEASYPRINT_ALLOW_LOCAL"] = "true"
    HTML(string=full_html).write_pdf(str(output_path))
