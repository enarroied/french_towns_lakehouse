"""SQL utility for lakehouse queries from blog posts."""

import os
from functools import cache

import duckdb
import pandas as pd
import polars as pl
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())

GOLD_TABLES = [
    "dim_communes",
    "dim_geography",
    "dim_zip_codes",
    "bridge_communes_zip_codes",
    "fact_population",
    "fact_salaries",
    "dim_neighbour_communes",
    "fact_equipment",
    "dim_equipment",
    "dim_source",
    "dim_calendar",
    "bridge_source_links",
    "fact_unemployment",
]


@cache
def _connect() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("LOAD iceberg;")
    conn.execute(f"""
        CREATE SECRET polaris_secret (
            TYPE iceberg,
            CLIENT_ID '{os.environ["POLARIS_CLIENT_ID"]}',
            CLIENT_SECRET '{os.environ["POLARIS_CLIENT_SECRET"]}',
            ENDPOINT 'http://localhost:8181/api/catalog'
        )
    """)
    conn.execute("""
        ATTACH 'french_towns' AS polaris (
            TYPE iceberg,
            ENDPOINT 'http://localhost:8181/api/catalog',
            SECRET 'polaris_secret'
        )
    """)
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    for tbl in GOLD_TABLES:
        conn.execute(
            f"CREATE OR REPLACE VIEW gold.{tbl} AS SELECT * FROM polaris.lakehouse.{tbl}"
        )
    return conn


def sql_to_df(
    query: str,
    params: list | None = None,
    *,
    dataframe_type: str = "polars",
) -> pl.DataFrame | pd.DataFrame:
    """Execute a query against the lakehouse and return a DataFrame.

    Parameters
    ----------
    query : str
        SQL query string.
    params : list | None, optional
        Query parameters for parameterized queries.
    dataframe_type : str, default ``"polars"``
        One of ``"polars"`` or ``"pandas"``.

    Returns
    -------
    pl.DataFrame or pd.DataFrame
    """
    res = _connect().execute(query, params or [])
    if dataframe_type == "polars":
        return res.pl()
    if dataframe_type == "pandas":
        return res.fetchdf()
    msg = f"dataframe_type must be 'polars' or 'pandas', got '{dataframe_type}'"
    raise ValueError(msg)
