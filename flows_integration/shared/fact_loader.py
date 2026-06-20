import duckdb


SCHEMA = "lakehouse"
VALIDATED_BUCKET = "validated"


def drop_table_if_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Drop an Iceberg table in Polaris if it exists (for schema migration)."""
    full_name = f"polaris.{SCHEMA}.{table_name}"
    conn.execute(f"DROP TABLE IF EXISTS {full_name}")


def append_new_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    natural_key: list[str],
) -> int:
    source_path = f"s3://{VALIDATED_BUCKET}/{table_name}.parquet"
    full_name = f"polaris.{SCHEMA}.{table_name}"

    conn.execute(
        f"CREATE TEMP VIEW incoming AS SELECT * FROM read_parquet('{source_path}')"
    )

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_name} AS
        SELECT now() AS inserted_at, incoming.* FROM incoming WHERE false
    """)

    nk_join = " AND ".join(f"inc.{k} = existing.{k}" for k in natural_key)

    inserted = conn.execute(f"""
        SELECT COUNT(*) FROM incoming inc
        WHERE NOT EXISTS (
            SELECT 1 FROM {full_name} existing
            WHERE {nk_join}
        )
    """).fetchone()[0]

    conn.execute(f"""
        INSERT INTO {full_name}
        SELECT now() AS inserted_at, inc.*
        FROM incoming inc
        WHERE NOT EXISTS (
            SELECT 1 FROM {full_name} existing
            WHERE {nk_join}
        )
    """)

    conn.execute("DROP VIEW IF EXISTS incoming")
    return inserted
