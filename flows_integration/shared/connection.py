import os

import duckdb
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    conn.execute(f"""
        CREATE SECRET minio_secret (
            TYPE s3,
            KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
            SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
            ENDPOINT '{os.environ["AWS_ENDPOINT"]}',
            REGION 'us-east-1',
            USE_SSL false,
            URL_STYLE 'path'
        )
    """)

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

    return conn
