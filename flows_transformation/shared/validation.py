from flows.shared import log
from flows_staging.shared import audit_db as db
from flows_staging.shared.minio import get_minio_client
from prefect import task


SOURCE_FOLDERS: dict[str, str] = {
    "french_communes": "geography",
    "cog_ensemble": "geography",
    "arrondissements": "geography",
    "departements": "geography",
    "zip_codes": "geography",
    "populations_historiques": "demographics",
    "salaries": "demographics",
    "births": "demographics",
    "deaths": "demographics",
    "family": "demographics",
    "migration": "demographics",
    "bpe": "equipment",
    "dim_equipment": "dim_equipment",
    "french_holidays": "dim_calendar",
    "market_holidays": "dim_calendar",
    "religious_holidays": "dim_calendar",
    "french_presidents": "dim_calendar",
    "french_prime_ministers": "dim_calendar",
    "french_legislatures": "dim_calendar",
    "lunar_phases": "dim_calendar",
    "sources": "dim_source",
    "bridge_model_sources": "dim_source",
    "unemployment": "demographics",
    "monuments_historiques": "cultural_heritage",
}

SOURCE_PREFIXES: dict[str, str] = {
    "french_communes": "french_towns",
    "cog_ensemble": "v",
    "arrondissements": "arrondissements",
    "departements": "departements",
    "zip_codes": "zip_codes",
    "populations_historiques": "historical_population",
    "salaries": "historical_salaries",
    "births": "births",
    "deaths": "deaths",
    "family": "family",
    "migration": "migration",
    "bpe": "bpe",
    "dim_equipment": "dim_equipment",
    "french_holidays": "french_holidays",
    "market_holidays": "market_holidays",
    "religious_holidays": "religious_holidays",
    "french_presidents": "french_presidents",
    "french_prime_ministers": "french_prime_ministers",
    "french_legislatures": "french_legislatures",
    "lunar_phases": "lunar_phases",
    "sources": "sources",
    "bridge_model_sources": "bridge_model_sources",
    "unemployment": "unemployment",
    "monuments_historiques": "monuments_historiques",
}

SOURCE_EXPECTED_COUNTS: dict[str, int] = {
    "cog_ensemble": 5,
}


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

        expected = SOURCE_EXPECTED_COUNTS.get(source_name, 1)

        rows = db.query(
            "SELECT filename, md5_hash, filename_timestamp "
            "FROM audit.file_metadata "
            "WHERE filename LIKE %s || '%%' AND is_latest = 1 "
            "ORDER BY upload_timestamp DESC",
            [minio_prefix],
        )
        # For multi-file sources, deduplicate by filename
        seen = set()
        deduped = []
        for row in rows:
            if row[0] not in seen:
                seen.add(row[0])
                deduped.append(row)

        num_minio = len(files_in_minio)
        num_audit = len(deduped)

        if num_minio < expected:
            raise RuntimeError(
                f"Input validation failed for {source_name}: "
                f"expected at least {expected} file(s) in MinIO, found {num_minio}: {files_in_minio}"
            )

        if num_audit < expected:
            raise RuntimeError(
                f"Input validation failed for {source_name}: "
                f"expected at least {expected} audit record(s), found {num_audit}"
            )

        if expected == 1:
            md5 = deduped[0][1]
            timestamp = deduped[0][2]
            log(f"✅ {source_name} validated: {timestamp} | md5: {md5}")
        else:
            filenames = [r[0] for r in deduped[:expected]]
            log(
                f"✅ {source_name} validated: {len(deduped)} file(s) — {', '.join(filenames)}"
            )
