"""PostgreSQL audit database layer.

All audit tables live in the `audit` schema of the `metadata` database.
This module owns all connections, migrations, and queries.
"""

import os
import uuid
from datetime import datetime
from typing import TYPE_CHECKING

import psycopg2
import psycopg2.extensions
from dotenv import find_dotenv
from dotenv import load_dotenv
from flows_staging.shared.models import FileMetadataRecord
from flows_staging.shared.models import KnownFileHash
from flows_staging.shared.models import StageConfig
from psycopg2 import pool


load_dotenv(find_dotenv())

if TYPE_CHECKING:
    from flows_staging.scrapers.models import FileMetadata


DB_URL: str | None = os.environ.get("AUDIT_DATABASE_URL")
SCHEMA = "audit"

_pool: pool.ThreadedConnectionPool | None = None


def _get_conn() -> psycopg2.extensions.connection:
    global _pool  # noqa: PLW0603
    if DB_URL is None:
        raise RuntimeError(
            "AUDIT_DATABASE_URL environment variable is not set. Run: source .env"
        )
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(1, 4, DB_URL)
    return _pool.getconn()


def _put_conn(conn: psycopg2.extensions.connection) -> None:
    if _pool is not None:
        _pool.putconn(conn)


## ── Migration ──────────────────────────────────────────────────────────

MIGRATIONS: list[str] = [
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.flow_run_metadata (
        run_id UUID PRIMARY KEY,
        domain_name TEXT NOT NULL,
        layer TEXT NOT NULL,
        status TEXT NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        technical_type TEXT,
        number_files INTEGER,
        is_latest SMALLINT DEFAULT 1
    )""",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.file_metadata (
        file_id UUID PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES {SCHEMA}.flow_run_metadata(run_id),
        filename TEXT NOT NULL,
        filename_timestamp TEXT NOT NULL,
        file_location TEXT NOT NULL,
        source_url TEXT,
        size_mb DOUBLE PRECISION NOT NULL,
        md5_hash TEXT NOT NULL,
        bucket TEXT,
        upload_timestamp TIMESTAMP,
        is_latest SMALLINT DEFAULT 1
    )""",
]


def migrate() -> None:
    """Create the audit schema and tables if they do not exist."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            for sql in MIGRATIONS:
                cur.execute(sql)
        conn.commit()
    finally:
        _put_conn(conn)


def check_reachable() -> None:
    """Verify the PostgreSQL server is reachable with a simple ``SELECT 1``."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    finally:
        _put_conn(conn)


## ── Query helper (for callers like validation.py) ──────────────────────


def query(sql: str, params: list | None = None) -> list[tuple]:
    """Execute a read query and return all rows."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            return cur.fetchall()
    finally:
        _put_conn(conn)


## ── Flow run metadata ──────────────────────────────────────────────────


def init_run(domain: str, layer: str, technical_type: str) -> str:
    """Insert a new flow run record and return its UUID."""
    run_id = str(uuid.uuid4())
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {SCHEMA}.flow_run_metadata "
                "(run_id, domain_name, layer, status, start_time, technical_type) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [run_id, domain, layer, "STARTED", datetime.now(), technical_type],
            )
        conn.commit()
    finally:
        _put_conn(conn)
    return run_id


def _update_latest_run(conn, run_id: str, domain: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.flow_run_metadata SET is_latest = 0 "
            "WHERE domain_name = %s AND run_id != %s",
            [domain, run_id],
        )
        cur.execute(
            f"UPDATE {SCHEMA}.flow_run_metadata SET is_latest = 1 WHERE run_id = %s",
            [run_id],
        )


def _update_run_status(
    conn, run_id: str, status: str, number_files: int, now: datetime
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.flow_run_metadata "
            "SET status = %s, end_time = %s, number_files = %s WHERE run_id = %s",
            [status, now, number_files, run_id],
        )


def finalize_run(run_id: str, status: str, number_files: int = 0) -> None:
    """Mark a flow run as finished — updates status, end time, and latest-run flag."""
    now = datetime.now()
    conn = _get_conn()
    try:
        if status == "SUCCESS":
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT domain_name FROM {SCHEMA}.flow_run_metadata WHERE run_id = %s",
                    [run_id],
                )
                row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"run_id {run_id} not found in flow_run_metadata")
            _update_latest_run(conn, run_id, row[0])
        _update_run_status(conn, run_id, status, number_files, now)
        conn.commit()
    finally:
        _put_conn(conn)


## ── File metadata ──────────────────────────────────────────────────────


def get_latest_hashes() -> dict[str, KnownFileHash]:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT filename, md5_hash, filename_timestamp, file_location "
                f"FROM {SCHEMA}.file_metadata WHERE is_latest = 1"
            )
            rows = cur.fetchall()
    finally:
        _put_conn(conn)
    return {
        row[0]: KnownFileHash(
            md5=row[1],
            filename_timestamp=row[2],
            file_location=row[3],
        )
        for row in rows
    }


def get_latest_hash(filename: str) -> str:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT md5_hash FROM {SCHEMA}.file_metadata "
                "WHERE is_latest = 1 AND filename = %s",
                [filename],
            )
            row = cur.fetchone()
    finally:
        _put_conn(conn)
    return row[0] if row else ""


def get_latest_filename_timestamp(filename: str) -> str:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT filename_timestamp FROM {SCHEMA}.file_metadata "
                "WHERE is_latest = 1 AND filename = %s",
                [filename],
            )
            row = cur.fetchone()
    finally:
        _put_conn(conn)
    return row[0] if row else ""


def write_file_metadata(
    config: StageConfig,
    record: FileMetadataRecord,
    now: datetime,
) -> None:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SCHEMA}.file_metadata SET is_latest = 0 WHERE filename = %s",
                [record.name],
            )
            cur.execute(
                f"INSERT INTO {SCHEMA}.file_metadata "
                "(file_id, run_id, filename, filename_timestamp, source_url, "
                "size_mb, md5_hash, bucket, upload_timestamp, file_location) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    str(uuid.uuid4()),
                    config.run_id,
                    record.name,
                    record.filename_timestamp,
                    record.source_url,
                    record.size_mb,
                    record.md5_hash,
                    record.bucket,
                    now,
                    record.file_location,
                ],
            )
        conn.commit()
    finally:
        _put_conn(conn)


def log_upload(
    run_id: str,
    file_metadata: "FileMetadata",
    bucket: str | None = None,
) -> None:
    now = datetime.now()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SCHEMA}.file_metadata SET is_latest = 0 WHERE filename = %s",
                [file_metadata.base_name],
            )
            cur.execute(
                f"INSERT INTO {SCHEMA}.file_metadata "
                "(file_id, run_id, filename, filename_timestamp, source_url, "
                "size_mb, md5_hash, bucket, upload_timestamp, file_location) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    str(uuid.uuid4()),
                    run_id,
                    file_metadata.base_name,
                    file_metadata.filename_timestamp,
                    file_metadata.source_url,
                    file_metadata.size_mb,
                    file_metadata.md5,
                    bucket,
                    now,
                    file_metadata.key,
                ],
            )
        conn.commit()
    finally:
        _put_conn(conn)
