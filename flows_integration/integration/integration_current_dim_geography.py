from flows_integration.shared.connection import get_duckdb_connection
from flows_integration.shared.fact_loader import append_new_rows
from flows_integration.shared.scd2 import run_scd2
from flows_integration.shared.validation import assert_validated_exists
from flows_integration.shared.validation import assert_validated_fresh
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from prefect import flow


DIM_TABLES = [
    ("dim_communes_france", ["id"], None),
    ("dim_zip_codes", ["id"], None),
]

BRIDGE_TABLES = [
    ("bridge_communes_zip_codes", ["commune_id", "zip_code_id"]),
]


@flow(name="integration_current_dim_geography")
def integration_current_dim_geography() -> None:
    preflight()
    run_id = init_run(domain="geography", layer="INTEGRATION", technical_type="ICEBERG")

    try:
        conn = get_duckdb_connection()

        for table_name, nk, business_cols in DIM_TABLES:
            assert_validated_exists(conn, table_name)
            assert_validated_fresh(conn, table_name)
            run_scd2(conn, table_name, nk, business_cols)

        for table_name, nk in BRIDGE_TABLES:
            assert_validated_exists(conn, table_name)
            assert_validated_fresh(conn, table_name)
            append_new_rows(conn, table_name, nk)

        finalize_run(run_id=run_id, status="SUCCESS", number_files=3)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED")
        raise


if __name__ == "__main__":
    integration_current_dim_geography()
