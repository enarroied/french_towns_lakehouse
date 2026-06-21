from pathlib import Path

import duckdb
import pandas as pd


_QUERY_DIR = Path(__file__).parent


def load_sql(name: str, subdir: str = "general_report") -> str:
    path = _QUERY_DIR / subdir / f"{name}.sql"
    return path.read_text()


def execute_sql(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    params: list | None = None,
    limit: int | None = None,
    subdir: str = "general_report",
) -> pd.DataFrame:
    query = load_sql(name, subdir)
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    return conn.execute(query, params or []).fetchdf()
