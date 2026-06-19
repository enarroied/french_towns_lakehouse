from flows_integration.shared.connection import get_duckdb_connection
from flows_integration.shared.fact_loader import append_new_rows
from flows_integration.shared.fact_loader import drop_table_if_exists
from flows_integration.shared.validation import assert_validated_exists
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from prefect import flow


FACT_TABLES = [
    ("fact_population", ["id", "year"]),
    ("fact_salaries", ["id", "year"]),
]


@flow(name="integration_current_fact_demographics")
def integration_current_fact_demographics() -> None:
    preflight()
    run_id = init_run(
        domain="demographics", layer="INTEGRATION", technical_type="ICEBERG"
    )

    try:
        conn = get_duckdb_connection()

        for table_name, nk in FACT_TABLES:
            assert_validated_exists(conn, table_name)
            drop_table_if_exists(conn, table_name)
            append_new_rows(conn, table_name, nk)

        finalize_run(run_id=run_id, status="SUCCESS", number_files=2)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED")
        raise


if __name__ == "__main__":
    integration_current_fact_demographics()
