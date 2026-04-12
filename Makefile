init-db:
    uv run python scripts/init_db.py

run:
    dbt run

test:
    dbt test
