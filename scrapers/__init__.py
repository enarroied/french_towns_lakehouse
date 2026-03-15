from importlib import import_module
from pathlib import Path


async def run_all_scrapers(config: dict) -> list[Path]:
    output_paths = []
    for scraper in config.get("scrapers", []):
        if not scraper.get("enabled", True):
            continue
        module = import_module(scraper["module"])
        output_path = await module.run(config)
        output_paths.append(output_path)
    return output_paths
