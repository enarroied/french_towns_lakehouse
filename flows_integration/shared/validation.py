import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import boto3
import duckdb
from botocore.client import Config
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


def assert_validated_fresh(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    max_age_hours: int = 48,
) -> None:
    _ = conn
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{os.environ['AWS_ENDPOINT']}",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            use_ssl=False,
            config=Config(signature_version="s3v4"),
        )
        resp = s3.head_object(Bucket=VALIDATED_BUCKET, Key=f"{table_name}.parquet")
        mtime = resp["LastModified"]
    except Exception as e:
        raise RuntimeError(f"Cannot determine freshness for {table_name}: {e}") from e

    age = datetime.now(timezone.utc) - mtime
    if age > timedelta(hours=max_age_hours):
        raise RuntimeError(
            f"Validated parquet for {table_name} is {age.total_seconds() / 3600:.1f}h old "
            f"(max {max_age_hours}h)"
        )
