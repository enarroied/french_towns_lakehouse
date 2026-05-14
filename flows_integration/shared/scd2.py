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
GEOMETRY_TYPES = {"GEOMETRY"}


def _parquet_columns(
    conn: duckdb.DuckDBPyConnection, table_name: str
) -> list[tuple[str, str]]:
    source_path = f"s3://{VALIDATED_BUCKET}/{table_name}.parquet"
    result = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{source_path}')"
    ).fetchall()
    return [(row[0], row[1].split("(")[0].strip()) for row in result]


def _map_columns(
    raw: list[tuple[str, str]],
) -> tuple[
    list[tuple[str, str]], list[str], list[str], list[str], list[str], list[str]
]:
    plain: list[tuple[str, str]] = []
    select_no_pref: list[str] = []
    select_pref: list[str] = []
    all_business: list[str] = []
    inc_hash_exprs: list[str] = []
    existing_hash_exprs: list[str] = []
    for name, typ in raw:
        if typ in GEOMETRY_TYPES:
            plain.append((f"{name}_wkb", "BINARY"))
            plain.append((f"{name}_srid", "INTEGER"))
            select_no_pref.append(f"ST_AsWKB({name}) AS {name}_wkb")
            select_no_pref.append(f"4326 AS {name}_srid")
            select_pref.append(f"ST_AsWKB(inc.{name}) AS {name}_wkb")
            select_pref.append(f"4326 AS {name}_srid")
            all_business.append(f"{name}_wkb")
            all_business.append(f"{name}_srid")
            inc_hash_exprs.append(f"ST_AsWKB(inc.{name})::VARCHAR")
            existing_hash_exprs.append(f"CAST(existing.{name}_wkb AS VARCHAR)")
        else:
            plain.append((name, typ))
            select_no_pref.append(name)
            select_pref.append(f"inc.{name}")
            all_business.append(name)
            inc_hash_exprs.append(f"inc.{name}::VARCHAR")
            existing_hash_exprs.append(f"CAST(existing.{name} AS VARCHAR)")
    return (
        plain,
        select_no_pref,
        select_pref,
        all_business,
        inc_hash_exprs,
        existing_hash_exprs,
    )


def run_scd2(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    natural_key: list[str],
) -> int:
    source_path = f"s3://{VALIDATED_BUCKET}/{table_name}.parquet"
    full_name = f"polaris.{SCHEMA}.{table_name}"

    conn.execute(
        f"CREATE TEMP VIEW incoming AS SELECT * FROM read_parquet('{source_path}')"
    )

    raw_cols = _parquet_columns(conn, table_name)
    (
        _,
        select_no_pref,
        select_pref,
        all_business,
        inc_hash_exprs,
        existing_hash_exprs,
    ) = _map_columns(raw_cols)

    nk_join = " AND ".join(f"inc.{k} = cur.{k}" for k in natural_key)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_name} AS
        SELECT
            gen_random_uuid()::VARCHAR AS surrogate_key,
            CURRENT_DATE               AS effective_date,
            DATE '9999-12-31'          AS expiry_date,
            true                       AS is_current,
            now()                      AS inserted_at,
            {", ".join(select_no_pref)}
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
                AND md5(CONCAT_WS('|', {",".join(inc_hash_exprs)}))
                    != md5(CONCAT_WS('|', {",".join(existing_hash_exprs)}))
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
            {", ".join(select_pref)}
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
