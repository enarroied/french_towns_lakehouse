import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

import duckdb
import httpx
from prefect import get_run_logger
from prefect import task


_DB_PATH = Path(__file__).parent.parent.parent / ".data/metadata.db"
_MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "migrations"

TechnicalType = Literal["STAGING", "TRANSFORMATION", "INTEGRATION"]
TechnicalSubtype = Literal["DOWNLOAD", "SCRAPER", "DBT", "API"]


def _conn():
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(_DB_PATH))
    for sql_file in sorted(_MIGRATIONS_DIR.glob("*.sql")):
        conn.execute(sql_file.read_text())
    return conn


@task
def preflight() -> None:
    logger = get_run_logger()

    try:
        with _conn() as conn:
            conn.execute("SELECT 1")
        logger.info("✅ Metadata DB reachable")
    except Exception as e:
        raise RuntimeError(f"Metadata DB not writable: {e}") from e

    try:
        from flows.shared.minio import get_minio_client  # noqa: PLC0415

        get_minio_client().list_buckets()
        logger.info("✅ MinIO reachable")
    except Exception as e:
        raise RuntimeError(f"MinIO not reachable: {e}") from e

    try:
        httpx.head("https://www.cloudflare.com", timeout=5)
        logger.info("✅ Internet reachable")
    except Exception as e:
        raise RuntimeError(f"No internet connection: {e}") from e


@task
def init_run(
    domain: str,
    layer: TechnicalType = "STAGING",
    technical_type: TechnicalSubtype = "DOWNLOAD",
) -> str:
    run_id = str(uuid.uuid4())
    with _conn() as conn:
        conn.execute(
            """INSERT INTO flow_run_metadata
               (run_id, domain_name, layer, status, start_time, technical_type)
               VALUES (?, ?, ?, 'STARTED', ?, ?)""",
            [run_id, domain, layer, datetime.now(), technical_type],
        )
    get_run_logger().info(
        f"▶ Run started: {domain}/{layer}/{technical_type} [{run_id[:8]}]"
    )
    return run_id


@task
def get_latest_hashes() -> dict[str, dict]:
    """Returns {filename: {md5, filename_timestamp}} for all is_latest=1 rows."""
    with _conn() as conn:
        rows = conn.execute(
            """SELECT filename, md5_hash, filename_timestamp
               FROM file_metadata WHERE is_latest = 1"""
        ).fetchall()
    return {row[0]: {"md5": row[1], "filename_timestamp": row[2]} for row in rows}


@task
def log_upload(
    run_id: str,
    name: str,
    filename_timestamp: str,
    keys: list[str],
    source_url: str | None = None,
    size_mb: float | None = None,
    md5_hash: str | None = None,
    bucket: str | None = None,
) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE file_metadata SET is_latest = 0 WHERE filename = ?",
            [name],
        )
        conn.execute(
            """INSERT INTO file_metadata
               (file_id, run_id, filename, filename_timestamp, source_url,
                size_mb, md5_hash, bucket, upload_timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                str(uuid.uuid4()),
                run_id,
                name,
                filename_timestamp,
                source_url,
                size_mb,
                md5_hash,
                bucket,
                datetime.now(),
            ],
        )
    get_run_logger().info(
        f"✅ {name} → {filename_timestamp} | {size_mb}MB | {md5_hash}"
    )


@task
def finalize_run(run_id: str, status: str = "SUCCESS", number_files: int = 0) -> None:
    with _conn() as conn:
        if status == "SUCCESS":
            domain = conn.execute(
                "SELECT domain_name FROM flow_run_metadata WHERE run_id = ?", [run_id]
            ).fetchone()[0]
            conn.execute(
                """UPDATE flow_run_metadata SET is_latest = 0
                   WHERE domain_name = ? AND run_id != ?""",
                [domain, run_id],
            )
        conn.execute(
            """UPDATE flow_run_metadata
               SET status = ?, end_time = ?, number_files = ?
               WHERE run_id = ?""",
            [status, datetime.now(), number_files, run_id],
        )
    get_run_logger().info(
        f"{'✅' if status == 'SUCCESS' else '❌'} Run {status} [{run_id[:8]}] — {number_files} file(s)"
    )
