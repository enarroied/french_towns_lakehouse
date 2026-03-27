import logging
import traceback
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ScraperResult:
    name: str
    module: str
    success: bool
    output_path: Path | None = None
    error: str | None = None
    tb: str | None = None


async def run_all_scrapers(config: dict) -> list[ScraperResult]:
    results: list[ScraperResult] = []

    for scraper in config.get("scrapers", []):
        if not scraper.get("enabled", True):
            logger.info("⏭  %s — skipped (disabled)", scraper["name"])
            continue

        name = scraper["name"]
        module_path = scraper["module"]

        try:
            module = import_module(module_path)
            output_path = await module.run(config)
            results.append(
                ScraperResult(
                    name=name,
                    module=module_path,
                    success=True,
                    output_path=output_path,
                )
            )
            logger.info("✅ %s — OK → %s", name, output_path)

        except Exception as exc:
            full_tb = traceback.format_exc()
            results.append(
                ScraperResult(
                    name=name,
                    module=module_path,
                    success=False,
                    error=str(exc),
                    tb=full_tb,
                )
            )
            # Log the short error immediately so it appears inline in Prefect logs
            logger.error("❌ %s — FAILED: %s", name, exc)

    return results
