from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
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
