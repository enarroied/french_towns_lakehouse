from flows_integration.shared.connection import get_duckdb_connection
from flows_integration.shared.fact_loader import append_new_rows
from flows_integration.shared.scd2 import run_scd2
from flows_integration.shared.validation import assert_validated_exists
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from prefect import flow


DIM_TABLES = [
    ("dim_monument", ["reference"]),
]

BRIDGE_TABLES = [
    ("bridge_monument_communes", ["monument_reference", "commune_code"]),
]

FACT_TABLES = [
    ("fact_classification_history", ["monument_reference", "event_date"]),
]

AGGREGATE_TABLES = [
    ("dim_commune_monument_stats", ["commune_id"]),
]


@flow(name="integration_current_cultural_heritage")
def integration_current_cultural_heritage() -> None:
    preflight()
    run_id = init_run(
        domain="cultural_heritage", layer="INTEGRATION", technical_type="ICEBERG"
    )

    try:
        conn = get_duckdb_connection()

        for table_name, nk in DIM_TABLES:
            assert_validated_exists(conn, table_name)
            run_scd2(conn, table_name, nk)

        for table_name, nk in BRIDGE_TABLES:
            assert_validated_exists(conn, table_name)
            append_new_rows(conn, table_name, nk)

        for table_name, nk in FACT_TABLES:
            assert_validated_exists(conn, table_name)
            append_new_rows(conn, table_name, nk)

        for table_name, nk in AGGREGATE_TABLES:
            assert_validated_exists(conn, table_name)
            append_new_rows(conn, table_name, nk)

        finalize_run(run_id=run_id, status="SUCCESS", number_files=4)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED")
        raise


if __name__ == "__main__":
    integration_current_cultural_heritage()
