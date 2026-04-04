import asyncio
import logging
import traceback
from importlib import import_module

from scrapers.models import ScraperResult

logger = logging.getLogger(__name__)


async def _run_scraper(scraper: dict, config: dict) -> ScraperResult:
    """Import and execute a single scraper module, capturing any errors."""
    name = scraper["name"]
    module_path = scraper["module"]
    try:
        module = import_module(module_path)
        output_key = await module.run(config)
        return ScraperResult(name=name, module=module_path, success=True, output_key=output_key)
    except Exception as exc:
        return ScraperResult(
            name=name,
            module=module_path,
            success=False,
            error=str(exc),
            tb=traceback.format_exc(),
        )


async def run_all_scrapers(config: dict) -> list[ScraperResult]:
    """Run all enabled scrapers concurrently and return their results."""
    enabled = [s for s in config.get("scrapers", []) if s.get("enabled", True)]
    skipped = [s for s in config.get("scrapers", []) if not s.get("enabled", True)]

    for s in skipped:
        logger.info("Skipping %s (disabled)", s["name"])

    results = await asyncio.gather(*[_run_scraper(s, config) for s in enabled])

    for r in results:
        if r.success:
            logger.info("✅ %s → %s", r.name, r.output_key)
        else:
            logger.error("❌ %s: %s", r.name, r.error)

    return list(results)
