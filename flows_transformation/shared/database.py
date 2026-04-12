from pathlib import Path

import duckdb
from prefect import task


PROJECT_ROOT = Path(__file__).parent.parent.parent


@task
def ensure_work_database_exists() -> None:
    """Ensures DuckDB database exists with v1.5.0 storage format."""
    db_path = PROJECT_ROOT / "french_towns.duckdb"

    if db_path.exists():
        print(f"Database already exists at {db_path}")
        return

    conn = duckdb.connect()
    conn.execute(f"ATTACH '{db_path}' AS french_towns (STORAGE_VERSION 'v1.5.0')")
    conn.close()
    print(f"Database initialized at {db_path}")
