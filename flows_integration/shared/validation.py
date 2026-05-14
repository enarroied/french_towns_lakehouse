import duckdb
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())

VALIDATED_BUCKET = "validated"


def _table_path(table_name: str) -> str:
    return f"s3://{VALIDATED_BUCKET}/{table_name}.parquet"


def assert_validated_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    path = _table_path(table_name)
    try:
        result = conn.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()
        if result[0] == 0:
            raise RuntimeError(f"Validated parquet is empty: {path}")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(
            f"Validated parquet does not exist or cannot be read: {path}"
        ) from e
