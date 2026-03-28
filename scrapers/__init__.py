import asyncio
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


async def _run_single_scraper(scraper: dict, config: dict) -> ScraperResult:
    name = scraper["name"]
    module_path = scraper["module"]
    try:
        module = import_module(module_path)
        output_path = await module.run(config)
        return ScraperResult(
            name=name,
            module=module_path,
            success=True,
            output_path=output_path,
        )
    except Exception as exc:
        full_tb = traceback.format_exc()
        return ScraperResult(
            name=name,
            module=module_path,
            success=False,
            error=str(exc),
            tb=full_tb,
        )


async def run_all_scrapers(config: dict) -> list[ScraperResult]:
    enabled_scrapers = [s for s in config.get("scrapers", []) if s.get("enabled", True)]
    skipped = [s for s in config.get("scrapers", []) if not s.get("enabled", True)]

    for s in skipped:
        logger.info("⏭  %s — skipped (disabled)", s["name"])

    tasks = [_run_single_scraper(s, config) for s in enabled_scrapers]
    results = await asyncio.gather(*tasks)

    for r in results:
        if r.success:
            logger.info("✅ %s — OK → %s", r.name, r.output_path)
        else:
            logger.error("❌ %s — FAILED: %s", r.name, r.error)

    return list(results)
