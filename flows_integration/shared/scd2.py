import duckdb


SCHEMA = "lakehouse"
VALIDATED_BUCKET = "validated"
SCD_METADATA_COLS = {
    "surrogate_key",
    "effective_date",
    "expiry_date",
    "is_current",
    "inserted_at",
}


def _column_list(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    result = conn.execute(
        f"SELECT column_name, column_type FROM information_schema.columns "
        f"WHERE table_schema = '{SCHEMA}' AND table_name = '{table_name}'"
    ).fetchall()
    return [row[0] for row in result]


def _business_columns(all_cols: list[str]) -> list[str]:
    return [c for c in all_cols if c not in SCD_METADATA_COLS]


def run_scd2(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    natural_key: list[str],
    business_columns: list[str] | None = None,
) -> int:
    source_path = f"s3://{VALIDATED_BUCKET}/{table_name}.parquet"
    full_name = f"polaris.{SCHEMA}.{table_name}"

    conn.execute(
        f"CREATE TEMP VIEW incoming AS SELECT * FROM read_parquet('{source_path}')"
    )

    if business_columns is None:
        existing_cols = _column_list(conn, table_name)
        business_columns = _business_columns(existing_cols) if existing_cols else []

    nk_join = " AND ".join(f"inc.{k} = cur.{k}" for k in natural_key)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_name} AS
        SELECT
            gen_random_uuid()::VARCHAR AS surrogate_key,
            CURRENT_DATE               AS effective_date,
            DATE '9999-12-31'          AS expiry_date,
            true                       AS is_current,
            now()                      AS inserted_at,
            incoming.*
        FROM incoming
        WHERE false
    """)

    conn.execute(f"""
        UPDATE {full_name} AS existing
        SET
            is_current  = false,
            expiry_date = CURRENT_DATE
        WHERE existing.is_current = true
          AND ({" OR ".join(f"existing.{k} NOT IN (SELECT {k} FROM incoming)" for k in natural_key)})
    """)

    conn.execute(f"""
        UPDATE {full_name} AS existing
        SET
            is_current  = false,
            expiry_date = CURRENT_DATE
        WHERE existing.is_current = true
          AND EXISTS (
              SELECT 1 FROM incoming inc
              WHERE {" AND ".join(f"existing.{k} = inc.{k}" for k in natural_key)}
                AND md5(CONCAT_WS('|', {",".join(f"inc.{c}::VARCHAR" for c in business_columns)}))
                    != md5(CONCAT_WS('|', {",".join(f"CAST(existing.{c} AS VARCHAR)" for c in business_columns)}))
          )
    """)

    conn.execute(f"""
        INSERT INTO {full_name}
        SELECT
            gen_random_uuid()::VARCHAR AS surrogate_key,
            CURRENT_DATE               AS effective_date,
            DATE '9999-12-31'          AS expiry_date,
            true                       AS is_current,
            now()                      AS inserted_at,
            inc.*
        FROM incoming inc
        WHERE NOT EXISTS (
            SELECT 1 FROM {full_name} cur
            WHERE cur.is_current = true AND {nk_join}
        )
    """)

    inserted = conn.execute(f"""
        SELECT COUNT(*) FROM incoming inc
        WHERE NOT EXISTS (
            SELECT 1 FROM {full_name} cur
            WHERE cur.is_current = true AND {nk_join}
        )
    """).fetchone()[0]

    conn.execute("DROP VIEW IF EXISTS incoming")
    return inserted
