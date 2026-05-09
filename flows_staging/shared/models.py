"""Data models — dataclasses shared across staging flows, scrapers, and parsers."""

from dataclasses import dataclass
from dataclasses import field


@dataclass
class KnownFileHash:
    """Hash information for a known file.

    Attributes:
        md5: MD5 checksum of the file contents.
        filename_timestamp: Timestamped filename stored in MinIO.
        file_location: Full MinIO key, or None if not yet uploaded.
    """

    md5: str
    filename_timestamp: str
    file_location: str | None = None


@dataclass
class StagingFlowParams:
    """Parameters for run_staging_flow.

    Attributes:
        domain: The domain name (e.g., 'demographics', 'geography').
        domain_download: The download item name from config.yaml (e.g., 'historical_population').
        technical_type: The technical type (default: 'DOWNLOAD').
    """

    domain: str
    domain_download: str
    technical_type: str = "DOWNLOAD"


@dataclass
class FileMetadataRecord:
    """Per-file metadata to persist to the database.

    Attributes:
        name: Full filename (e.g., 'populations_historiques.csv').
        filename_timestamp: Timestamped filename stored in MinIO.
        source_url: Download URL for provenance tracking.
        size_mb: File size in megabytes.
        md5_hash: MD5 checksum of file contents.
        bucket: MinIO bucket name where the file was uploaded.
        file_location: Full MinIO key (bucket path) for the uploaded file.
    """

    name: str
    filename_timestamp: str
    source_url: str | None
    size_mb: float
    md5_hash: str
    bucket: str
    file_location: str


@dataclass
class StageConfig:
    """Configuration for a staging operation — shared by downloaders and scrapers.

    Downloaders populate all fields. Scrapers only need name, url, target_folder,
    run_id, staging_bucket, and evidence_bucket — the list fields default to empty.

    Attributes:
        name: Item name from config.yaml (e.g., 'populations_historiques').
        url: Source URL (download URL or scraper entry URL).
        target_folder: Subfolder within the MinIO staging bucket.
        run_id: Unique ID for the current flow run.
        staging_bucket: MinIO staging bucket name.
        evidence_bucket: MinIO evidence archive bucket name.
        filename: Optional filename hint for the downloaded file (from config.yaml).
        source_file_patterns: Regex patterns matching files inside the archive (downloaders only).
        file_targets: Target filenames after extraction/renaming (downloaders only).
        extensions: File extensions for each target, e.g. ['.csv'] (downloaders only).
    """

    name: str
    url: str
    target_folder: str
    run_id: str
    staging_bucket: str
    evidence_bucket: str
    filename: str | None = None
    source_file_patterns: list[str] = field(default_factory=list)
    file_targets: list[str] = field(default_factory=list)
    extensions: list[str] = field(default_factory=list)
