import duckdb


# Creates the file with v1.5.0 storage format.
# Run once: uv run python scripts/init_db.py
conn = duckdb.connect()  # start in memory
conn.execute(
    "ATTACH './french_towns.duckdb' AS french_towns (STORAGE_VERSION 'v1.5.0')"
)
conn.close()
print("Database initialized with storage version v1.5.0")
