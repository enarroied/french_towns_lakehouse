from dataclasses import dataclass
from dataclasses import field
from pathlib import Path


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
