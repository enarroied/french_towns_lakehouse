import logging
from pathlib import Path

import duckdb
from prefect import task


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent


@task
def ensure_work_database_exists() -> None:
    """Ensures DuckDB database exists with v1.5.0 storage format."""
    db_path = PROJECT_ROOT / "french_towns.duckdb"

    if db_path.exists():
        logger.info("Database already exists at %s", db_path)
        return

    conn = duckdb.connect()
    conn.execute(f"ATTACH '{db_path}' AS french_towns (STORAGE_VERSION 'v1.5.0')")
    conn.close()
    logger.info("Database initialized at %s", db_path)
