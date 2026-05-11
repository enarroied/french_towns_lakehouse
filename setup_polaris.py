"""One-time Polaris setup: create the lakehouse namespace if it does not exist.

Idempotent — safe to re-run.
"""

from flows_integration.shared.connection import get_duckdb_connection


def main() -> None:
    conn = get_duckdb_connection()
    conn.execute("CREATE SCHEMA IF NOT EXISTS polaris.lakehouse;")
    conn.close()

    print("✓ Polaris catalog 'french_towns' attached")
    print("✓ Namespace 'lakehouse' ready")
    print()
    print("To query your lakehouse:")
    print()
    print("    ATTACH 'french_towns' AS polaris (")
    print("        TYPE iceberg,")
    print("        ENDPOINT 'http://localhost:8181/api/catalog',")
    print("        SECRET 'polaris_secret'")
    print("    );")
    print("    SELECT * FROM polaris.lakehouse.dim_communes_france;")


if __name__ == "__main__":
    main()
