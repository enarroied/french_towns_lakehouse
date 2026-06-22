import os

import duckdb
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())


def setup_duckdb_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("LOAD httpfs;")

    conn.execute(f"""
        CREATE SECRET minio_creds (
            TYPE s3,
            KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
            SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
            ENDPOINT '{os.environ["AWS_ENDPOINT"]}',
            REGION 'us-east-1',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)

    conn.execute("CREATE SCHEMA IF NOT EXISTS silver;")

    parquet_files = {
        "dim_communes": "s3://validated/dim_communes.parquet",
        "dim_geography": "s3://validated/dim_geography.parquet",
        "fact_population": "s3://validated/fact_population.parquet",
        "fact_salaries": "s3://validated/fact_salaries.parquet",
    }

    for name, path in parquet_files.items():
        conn.execute(
            f"CREATE OR REPLACE VIEW silver.{name} AS SELECT * FROM read_parquet('{path}')"
        )

    return conn
