import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

import duckdb
from prefect import get_run_logger
from prefect import task


_DB_PATH = Path(__file__).parent.parent.parent / ".data/metadata.db"
_MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "migrations"

TechnicalType = Literal["STAGING", "TRANSFORMATION", "INTEGRATION"]


def _conn():
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(_DB_PATH))
    for sql_file in sorted(_MIGRATIONS_DIR.glob("*.sql")):
        conn.execute(sql_file.read_text())
    return conn


@task
def init_run(domain: str, layer: TechnicalType = "STAGING") -> str:
    run_id = str(uuid.uuid4())
    with _conn() as conn:
        conn.execute(
            """INSERT INTO flow_run_metadata (run_id, domain_name, layer, status, start_time)
               VALUES (?, ?, ?, 'STARTED', ?)""",
            [run_id, domain, layer, datetime.now()],
        )
    get_run_logger().info(f"▶ Run started: {domain}/{layer} [{run_id[:8]}]")
    return run_id


@task
def log_upload(
    run_id: str,
    name: str,
    keys: list[str],
    source_url: str | None = None,
    size_mb: float | None = None,
    bucket: str | None = None,
) -> None:
    with _conn() as conn:
        conn.execute(
            """INSERT INTO file_metadata
               (file_id, run_id, filename, source_url, size_mb, bucket, upload_timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                str(uuid.uuid4()),
                run_id,
                name,
                source_url,
                size_mb,
                bucket,
                datetime.now(),
            ],
        )
    get_run_logger().info(f"✅ {name} → {keys}")


@task
def finalize_run(run_id: str, status: str = "SUCCESS", number_files: int = 0) -> None:
    with _conn() as conn:
        conn.execute(
            """UPDATE flow_run_metadata
               SET status=?, end_time=?, number_files=?
               WHERE run_id=?""",
            [status, datetime.now(), number_files, run_id],
        )
    get_run_logger().info(
        f"{'✅' if status == 'SUCCESS' else '❌'} Run {status} [{run_id[:8]}] — {number_files} file(s)"
    )
