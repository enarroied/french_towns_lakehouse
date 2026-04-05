from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ScraperConfig:
    """Normalised configuration for a single scraper.

    Common fields are typed attributes; scraper-specific overrides (e.g.
    ``page_size``, ``concurrency``) live in ``extra`` and are accessed via
    ``config.extra.get("page_size", default)``.
    """

    name: str
    module: str
    url: str
    enabled: bool = True
    user_agent: str = "FrenchTownsBot/1.0"
    concurrency: int = 5
    target_folder: str = "labels"
    domain: str = "labels"
    extra: dict = field(default_factory=dict)

    @property
    def headers(self) -> dict:
        """Standard HTML browser headers derived from this config."""
        return {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        }

    @classmethod
    def from_dict(cls, data: dict) -> ScraperConfig:
        """Build a ``ScraperConfig`` from a raw YAML dict.

        Known keys become typed attributes; everything else goes into ``extra``.
        """
        known = {
            "name",
            "module",
            "url",
            "enabled",
            "user_agent",
            "concurrency",
            "target_folder",
            "domain",
        }
        base = {k: v for k, v in data.items() if k in known}
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(**base, extra=extra)


@dataclass
class ScraperResult:
    """Outcome of a single scraper run."""

    name: str
    module: str
    success: bool
    output_key: str | None = None
    error: str | None = None
    tb: str | None = None
