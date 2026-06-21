import pandas as pd
from great_tables import GT


def _fmt_arrow(v, fmt: str, suffix: str = "") -> str:
    if pd.isna(v):
        return "—"
    if v > 0:
        return f'<span style="color:green">&#9650; {v:{fmt}}{suffix}</span>'
    if v < 0:
        return f'<span style="color:red">&#9660; {v:{fmt}}{suffix}</span>'
    return f'<span style="color:gray">&#9644; {v:{fmt}}{suffix}</span>'


def create_department_summary_table(
    df: pd.DataFrame, department_name: str, year_range: str = ""
) -> GT | None:
    if df.empty:
        return None

    table_df = df[
        [
            "name",
            "population",
            "population_growth_pct",
            "population_ratio",
            "mean_salary",
            "salary_ratio",
        ]
    ].copy()

    table_df["population_growth_pct"] = table_df["population_growth_pct"].apply(
        lambda v: _fmt_arrow(v, "+.1f", suffix="%")
    )
    table_df["population_ratio"] = table_df["population_ratio"].apply(
        lambda v: f"{v:.1f}" if not pd.isna(v) else "—"
    )
    table_df["salary_ratio"] = table_df["salary_ratio"].apply(
        lambda v: f"{v:.1f}" if not pd.isna(v) else "—"
    )

    subtitle = department_name
    if year_range:
        subtitle += f" ({year_range})"

    return (
        GT(table_df)
        .tab_header(
            title=f"Department Summary: {department_name}",
            subtitle=subtitle,
        )
        .cols_label(
            name="City",
            population="Population",
            population_growth_pct="Population Growth",
            population_ratio="Ratio to Dept Avg",
            mean_salary="Mean Salary (€)",
            salary_ratio="Salary Ratio to Dept Avg",
        )
        .fmt_integer(columns="population")
        .fmt_integer(columns="mean_salary")
        .fmt_markdown(columns="population_growth_pct")
        .cols_align(
            align="right",
            columns=[
                "population",
                "population_growth_pct",
                "population_ratio",
                "mean_salary",
                "salary_ratio",
            ],
        )
        .cols_align(align="left", columns="name")
    )


def create_population_table(df: pd.DataFrame) -> GT | None:
    if df.empty:
        return None

    commune_name = df["name"].iloc[0]
    department_name = df["department_name"].iloc[0]
    department_code = df["department_code"].iloc[0]
    max_year = int(df["year"].iloc[0])
    min_year = int(df["year"].iloc[-1])

    table_df = df[
        [
            "year",
            "population",
            "year_evolution",
            "year_evolution_percent",
        ]
    ].copy()

    table_df.columns = [
        "Year",
        "Population",
        "Population Evolution",
        "Population Evolution (%)",
    ]

    table_df["Population Evolution"] = table_df["Population Evolution"].apply(
        lambda v: _fmt_arrow(v, "+,.0f")
    )
    table_df["Population Evolution (%)"] = table_df["Population Evolution (%)"].apply(
        lambda v: _fmt_arrow(v, "+.1f", suffix="%")
    )

    return (
        GT(table_df)
        .tab_header(
            title=f"Population for {commune_name} ({department_name} - {department_code})",
            subtitle=f"Data for Years {min_year} - {max_year}",
        )
        .fmt_integer(columns="Population")
        .fmt_markdown(columns="Population Evolution")
        .fmt_markdown(columns="Population Evolution (%)")
        .cols_align(
            align="right",
            columns=["Population Evolution", "Population Evolution (%)"],
        )
    )


def create_salary_table(df: pd.DataFrame) -> GT | None:
    if df.empty:
        return None

    commune_name = df["name"].iloc[0]
    department_name = df["department_name"].iloc[0]
    department_code = df["department_code"].iloc[0]
    max_year = int(df["year"].iloc[0])
    min_year = int(df["year"].iloc[-1])

    table_df = df[["year", "mean_salary"]].copy()
    table_df.columns = ["Year", "Mean Salary (€)"]

    return (
        GT(table_df)
        .tab_header(
            title=f"Salary History for {commune_name} ({department_name} - {department_code})",
            subtitle=f"Data for Years {min_year} - {max_year}",
        )
        .fmt_integer(columns="Mean Salary (€)")
        .cols_align(align="right", columns="Mean Salary (€)")
    )
