from datetime import datetime
from datetime import timedelta
from datetime import timezone

import duckdb


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


def assert_validated_fresh(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    max_age_hours: int = 48,
) -> None:
    path = _table_path(table_name)
    try:
        result = conn.execute(
            f"SELECT max(file_last_modified) FROM parquet_file_metadata('{path}')"
        ).fetchone()
        mtime = result[0]
        if mtime is None:
            return
        if isinstance(mtime, datetime):
            age = datetime.now(timezone.utc) - mtime
        else:
            age = datetime.now(timezone.utc) - datetime.fromtimestamp(
                mtime, tz=timezone.utc
            )
    except Exception as e:
        raise RuntimeError(f"Cannot determine freshness for {table_name}: {e}") from e

    if age > timedelta(hours=max_age_hours):
        raise RuntimeError(
            f"Validated parquet for {table_name} is {age.total_seconds() / 3600:.1f}h old "
            f"(max {max_age_hours}h)"
        )
