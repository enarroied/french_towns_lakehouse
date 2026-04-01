import asyncio
import logging
import sys
import traceback
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path

from prefect import flow
from prefect import task


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flows.shared import get_config
from flows.shared import get_custom_parsers
from flows.shared import get_directories
from flows.shared import get_paths
from flows.shared import get_scrapers


logger = logging.getLogger(__name__)


@dataclass
class ScraperResult:
    name: str
    module: str
    success: bool
    minio_key: str | None = None
    error: str | None = None
    tb: str | None = None


async def _run_single_scraper(scraper: dict, config: dict) -> ScraperResult:
    name = scraper["name"]
    module_path = scraper["module"]
    try:
        module = import_module(module_path)
        minio_key = await module.run(config)
        return ScraperResult(
            name=name,
            module=module_path,
            success=True,
            minio_key=minio_key,
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


async def _run_async_scrapers() -> list[ScraperResult]:
    config = get_config()
    enabled_scrapers = [s for s in get_scrapers() if s.get("enabled", True)]
    skipped = [s for s in get_scrapers() if not s.get("enabled", True)]

    for s in skipped:
        logger.info("⏭ %s — skipped (disabled)", s["name"])

    tasks = [_run_single_scraper(s, config) for s in enabled_scrapers]
    results = await asyncio.gather(*tasks)

    succeeded = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    width = 50
    print("\n" + "=" * width)
    print(" SCRAPER RUN SUMMARY")
    print("=" * width)
    for r in results:
        status = "OK" if r.success else "FAILED"
        key_info = f" → {r.minio_key}" if r.minio_key else ""
        print(f" {r.name:<30} {status}{key_info}")
        if not r.success:
            short = (r.error or "unknown error").splitlines()[0][:60]
            print(f"     - {short}")
    print("-" * width)
    print(f" {len(succeeded)}/{len(results)} scrapers succeeded.")
    print("=" * width + "\n")

    if failed:
        log_path = Path("logs/scraper_errors.log")
        with log_path.open("w", encoding="utf-8") as f:
            for r in failed:
                f.write(f"\n{'=' * 60}\n")
                f.write(f"SCRAPER : {r.name}\n")
                f.write(f"MODULE  : {r.module}\n")
                f.write(f"ERROR   : {r.error}\n")
                f.write(f"{'─' * 60}\n")
                f.write(r.tb or "No traceback available.")
                f.write("\n")
        logger.warning("Full error details written to %s", log_path)

    if len(failed) == len(results):
        raise RuntimeError(
            f"All {len(results)} scrapers failed. "
            "Check logs/scraper_errors.log for details."
        )

    return list(results)


@task
def create_required_dirs() -> None:
    for dir_key in get_directories():
        Path(get_paths()[dir_key]).mkdir(exist_ok=True, parents=True)
    Path("logs").mkdir(exist_ok=True)


@task
def run_scrapers() -> list[ScraperResult]:
    return asyncio.run(_run_async_scrapers())


@dataclass
class ParserResult:
    name: str
    module: str
    success: bool
    minio_key: str | None = None
    error: str | None = None
    tb: str | None = None


@task
def run_parsers() -> list[ParserResult]:
    config = get_config()
    results: list[ParserResult] = []
    skipped: list[dict] = []

    for parser in get_custom_parsers():
        if not parser.get("enabled", True):
            skipped.append(parser)
            continue

        name = parser["name"]
        module_path = parser["module"]

        try:
            module = import_module(module_path)
            minio_key = module.run(config)
            results.append(
                ParserResult(
                    name=name,
                    module=module_path,
                    success=True,
                    minio_key=minio_key,
                )
            )
            logger.info("OK %s — %s", name, minio_key)

        except Exception as exc:
            full_tb = traceback.format_exc()
            results.append(
                ParserResult(
                    name=name,
                    module=module_path,
                    success=False,
                    error=str(exc),
                    tb=full_tb,
                )
            )
            logger.error("FAILED %s — %s", name, exc)

    succeeded = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    width = 50
    print("\n" + "=" * width)
    print(" PARSER RUN SUMMARY")
    print("=" * width)

    for s in skipped:
        print(f" {s['name']:<30} SKIPPED (disabled)")

    for r in results:
        status = "OK" if r.success else "FAILED"
        key_info = f" → {r.minio_key}" if r.minio_key else ""
        print(f" {r.name:<30} {status}{key_info}")
        if not r.success:
            short = (r.error or "unknown error").splitlines()[0][:60]
            print(f"     - {short}")

    print("-" * width)
    print(f" {len(succeeded)}/{len(results)} parsers succeeded.")
    print("=" * width + "\n")

    if failed:
        log_path = Path("logs/parser_errors.log")
        with log_path.open("w", encoding="utf-8") as f:
            for r in failed:
                f.write(f"\n{'=' * 60}\n")
                f.write(f"PARSER : {r.name}\n")
                f.write(f"MODULE : {r.module}\n")
                f.write(f"ERROR  : {r.error}\n")
                f.write(f"{'─' * 60}\n")
                f.write(r.tb or "No traceback available.")
                f.write("\n")
        logger.warning("Full error details written to %s", log_path)

    if len(failed) == len(results):
        raise RuntimeError(
            f"All {len(results)} parsers failed. "
            "Check logs/parser_errors.log for details."
        )

    return results


@flow(name="staging_current_labels")
def staging_current_labels() -> None:
    create_required_dirs()
    run_scrapers()
    run_parsers()


if __name__ == "__main__":
    staging_current_labels()
