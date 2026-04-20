from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import TypeAlias


@dataclass
class FileInfo:
    local_path: Path
    name: str | None = None
    source_url: str | None = None
    domain: str | None = None


@dataclass
class StagingResult:
    domain: str
    files: list[FileInfo] = field(default_factory=list)
    status: str = "SUCCESS"


@dataclass
class KnownFileHash:
    """Hash information for a known file."""

    md5: str
    filename_timestamp: str
    file_location: str | None = None


KnownHashes: TypeAlias = dict[str, KnownFileHash]


@dataclass
class StagingFlowParams:
    """Parameters for run_staging_flow.

    Attributes:
        domain: The domain name (e.g., 'demographics', 'geography').
        domain_downloads: List of download names for this domain.
        technical_type: The technical type (default: 'DOWNLOAD').
    """

    domain: str
    domain_downloads: list[str]
    technical_type: str = "DOWNLOAD"


@dataclass
class AsyncDownloadParams:
    """Parameters for run_async_downloads_to_minio.

    Attributes:
        downloads: List of download configuration dicts.
        temp_dir: Path to temporary directory for extracted files.
        known_hashes: Dict of known file hashes.
        minio_client: MinIO client for uploads.
        staging_bucket: Name of the staging bucket.
        concurrency: Max concurrent downloads (default: 3).
        timeout_seconds: HTTP timeout in seconds (default: 120).
    """

    downloads: list[dict]
    temp_dir: Path
    known_hashes: KnownHashes
    minio_client: Any
    staging_bucket: str
    concurrency: int = 3
    timeout_seconds: int = 120
