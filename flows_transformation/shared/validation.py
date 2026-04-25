from pathlib import Path

import duckdb
from flows.shared import log
from flows_staging.shared.minio import get_minio_client
from prefect import task


_DB_PATH = Path(__file__).parent.parent.parent / ".data/metadata.db"

SOURCE_FOLDERS: dict[str, str] = {
    "french_communes": "geography",
    "arrondissements": "geography",
    "departements": "geography",
    "zip_codes": "geography",
    "populations_historiques": "demographics",
    "salaries": "demographics",
}

# Map flow source names to actual MinIO file prefixes
SOURCE_PREFIXES: dict[str, str] = {
    "french_communes": "french_communes",
    "arrondissements": "arrondissements",
    "departements": "departements",
    "zip_codes": "zip_codes",
    "populations_historiques": "DS_POPULATIONS_HISTORIQUES_data",
    "salaries": "DS_BTS_SAL_EQTP_SEX_PCS_2023_data",
}


def _conn():
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(_DB_PATH))


@task
def validate_inputs(source_names: list[str]) -> None:
    """Validate that exactly 1 file exists in MinIO and audit DB for each source."""
    minio_client = get_minio_client()
    staging_bucket = "staging-current"

    for source_name in source_names:
        folder = SOURCE_FOLDERS.get(source_name, "")
        minio_prefix = SOURCE_PREFIXES.get(source_name, source_name)
        prefix = f"{folder}/{minio_prefix}"

        try:
            response = minio_client.list_objects(Bucket=staging_bucket, Prefix=prefix)
            files_in_minio = [obj["Key"] for obj in response.get("Contents", [])]
        except Exception as e:
            raise RuntimeError(
                f"Failed to list MinIO objects for {source_name}: {e}"
            ) from e

        with _conn() as conn:
            rows = conn.execute(
                """SELECT filename, md5_hash, filename_timestamp
                   FROM file_metadata
                   WHERE filename LIKE ? || '%' AND is_latest = 1
                   ORDER BY upload_timestamp DESC
                   LIMIT 1""",
                [minio_prefix],
            ).fetchall()

        num_minio = len(files_in_minio)
        num_audit = len(rows)

        if num_minio != 1:
            raise RuntimeError(
                f"Input validation failed for {source_name}: "
                f"expected 1 file in MinIO, found {num_minio}: {files_in_minio}"
            )

        if num_audit != 1:
            raise RuntimeError(
                f"Input validation failed for {source_name}: "
                f"expected 1 audit record, found {num_audit}"
            )

        md5 = rows[0][1]
        timestamp = rows[0][2]
        log("info", f"✅ {source_name} validated: {timestamp} | md5: {md5}")
