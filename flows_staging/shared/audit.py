import uuid
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Literal

import duckdb
import httpx
from flows.shared import log
from flows_staging.shared.models import FileMetadataRecord
from flows_staging.shared.models import KnownFileHash
from flows_staging.shared.models import StageConfig
from prefect import task


if TYPE_CHECKING:
    from flows_staging.scrapers.models import FileMetadata


_DB_PATH = Path(__file__).parent.parent.parent / ".data/metadata.db"
_MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "migrations"

TechnicalType = Literal["STAGING", "TRANSFORMATION", "INTEGRATION"]
TechnicalSubtype = Literal["DOWNLOAD", "SCRAPER", "DBT", "API"]
RunStatus = Literal["STARTED", "SUCCESS", "FAILED"]

RUN_STATUS_SUCCESS: RunStatus = "SUCCESS"
RUN_STATUS_STARTED: RunStatus = "STARTED"
RUN_STATUS_FAILED: RunStatus = "FAILED"


def _conn() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection to the metadata database."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(_DB_PATH))


def _migrate() -> None:
    with _conn() as conn:
        for sql_file in sorted(_MIGRATIONS_DIR.glob("*.sql")):
            conn.execute(sql_file.read_text())


def _check_db() -> None:
    try:
        _migrate()
        log("✅ Metadata DB reachable")
    except Exception as e:
        raise RuntimeError(f"Metadata DB not writable: {e}") from e


def _check_minio() -> None:
    try:
        from flows_staging.shared.minio import get_minio_client  # noqa: PLC0415

        get_minio_client().list_buckets()
        log("✅ MinIO reachable")
    except Exception as e:
        raise RuntimeError(f"MinIO not reachable: {e}") from e


def _check_internet_connection() -> None:
    try:
        httpx.head("https://www.cloudflare.com", timeout=5)
        log("✅ Internet reachable")
    except Exception as e:
        raise RuntimeError(f"No internet connection: {e}") from e


@task(retries=3, retry_delay_seconds=30)
def preflight() -> None:
    _check_db()
    _check_minio()
    _check_internet_connection()


@task
def init_run(
    domain: str,
    layer: TechnicalType = "STAGING",
    technical_type: TechnicalSubtype = "DOWNLOAD",
) -> str:
    run_id = str(uuid.uuid4())
    now = datetime.now()
    with _conn() as conn:
        conn.execute(
            """INSERT INTO flow_run_metadata
               (run_id, domain_name, layer, status, start_time, technical_type)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [run_id, domain, layer, RUN_STATUS_STARTED, now, technical_type],
        )
    log(f"▶ Run started: {domain}/{layer}/{technical_type} [{run_id[:8]}]")
    return run_id


@task
def get_latest_hashes() -> dict[str, KnownFileHash]:
    """Returns {filename: KnownFileHash} for all is_latest=1 rows."""
    with _conn() as conn:
        rows = conn.execute(
            """SELECT filename, md5_hash, filename_timestamp, file_location
               FROM file_metadata WHERE is_latest = 1"""
        ).fetchall()
    return {
        row[0]: KnownFileHash(
            md5=row[1],
            filename_timestamp=row[2],
            file_location=row[3],
        )
        for row in rows
    }


def get_latest_hash(filename: str) -> str:
    """Return MD5 of the latest known file, or empty string if none exists.

    Args:
        filename: Base filename to look up (e.g., 'populations_historiques.csv').

    Returns:
        MD5 hash string from the latest record, or "" if not found.
    """
    with _conn() as conn:
        rows = conn.execute(
            """SELECT md5_hash
               FROM file_metadata
               WHERE is_latest = 1 AND filename = ?""",
            [filename],
        ).fetchall()
    return rows[0][0] if rows else ""


def get_latest_filename_timestamp(filename: str) -> str:
    """Return the timestamped filename currently in staging, or empty string.

    Args:
        filename: Base filename to look up.

    Returns:
        Timestamped filename (e.g., 'populations_historiques_20250101T120000.csv'),
        or "" if no record exists.
    """
    with _conn() as conn:
        rows = conn.execute(
            """SELECT filename_timestamp
               FROM file_metadata
               WHERE is_latest = 1 AND filename = ?""",
            [filename],
        ).fetchall()
    return rows[0][0] if rows else ""


def _write_file_metadata(
    config: StageConfig,
    record: FileMetadataRecord,
    now: datetime,
) -> None:
    """Write file metadata to DuckDB, marking the previous version as not latest.

    Opens its own database connection, updates is_latest=0 for the old record,
    and inserts a new file_metadata row.

    Args:
        config: Staging configuration containing the current run_id.
        record: Per-file metadata to insert.
        now: UTC timestamp for the upload record.
    """
    with _conn() as conn:
        conn.execute(
            "UPDATE file_metadata SET is_latest = 0 WHERE filename = ?",
            [record.name],
        )
        conn.execute(
            """INSERT INTO file_metadata
               (file_id, run_id, filename, filename_timestamp, source_url,
                size_mb, md5_hash, bucket, upload_timestamp, file_location)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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


@task
def log_upload(
    run_id: str,
    file_metadata: "FileMetadata",
    bucket: str | None = None,
) -> None:
    now = datetime.now()
    with _conn() as conn:
        conn.execute(
            "UPDATE file_metadata SET is_latest = 0 WHERE filename = ?",
            [file_metadata.base_name],
        )
        conn.execute(
            """INSERT INTO file_metadata
               (file_id, run_id, filename, filename_timestamp, source_url,
                size_mb, md5_hash, bucket, upload_timestamp, file_location)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
    log(
        f"✅ {file_metadata.base_name} → {file_metadata.filename_timestamp} | {file_metadata.size_mb}MB | {file_metadata.md5}"
    )


def _update_latest_run(
    conn: duckdb.DuckDBPyConnection, run_id: str, domain: str
) -> None:
    """Mark the current run as latest and demote previous runs for the domain.

    Args:
        conn: Active DuckDB connection.
        run_id: New run to mark as latest.
        domain: Domain used to find previous latest runs.
    """
    conn.execute(
        """UPDATE flow_run_metadata SET is_latest = 0
           WHERE domain_name = ? AND run_id != ?""",
        [domain, run_id],
    )
    conn.execute(
        "UPDATE flow_run_metadata SET is_latest = 1 WHERE run_id = ?",
        [run_id],
    )


def _update_run_status(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    status: RunStatus,
    number_files: int,
    now: datetime,
) -> None:
    """Update the flow run metadata with completion status.

    Args:
        conn: Active DuckDB connection.
        run_id: Unique flow run identifier.
        status: Final status (STARTED, SUCCESS, or FAILED).
        number_files: Count of files successfully staged.
        now: Timestamp when the status was recorded.
    """
    conn.execute(
        """UPDATE flow_run_metadata
           SET status = ?, end_time = ?, number_files = ?
           WHERE run_id = ?""",
        [status, now, number_files, run_id],
    )


@task
def finalize_run(
    run_id: str,
    status: RunStatus = RUN_STATUS_SUCCESS,
    number_files: int = 0,
) -> None:
    """Mark a flow run as complete and update its status in the metadata database.

    On SUCCESS, also demotes previous runs for the same domain so only this
    run is marked as latest.

    Args:
        run_id: Unique flow run identifier.
        status: Final status (SUCCESS or FAILED).
        number_files: Count of files successfully staged.
    """
    now = datetime.now()
    with _conn() as conn:
        if status == RUN_STATUS_SUCCESS:
            row = conn.execute(
                "SELECT domain_name FROM flow_run_metadata WHERE run_id = ?",
                [run_id],
            ).fetchone()
            if row is None:
                raise RuntimeError(f"run_id {run_id} not found in flow_run_metadata")
            _update_latest_run(conn, run_id, row[0])
        _update_run_status(conn, run_id, status, number_files, now)

    icon = "✅" if status == RUN_STATUS_SUCCESS else "❌"
    log(f"{icon} Run {status} [{run_id[:8]}] — {number_files} file(s)")
