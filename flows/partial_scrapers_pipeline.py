import asyncio
import logging
from pathlib import Path

import yaml
from prefect import flow, task

from scrapers import run_all_scrapers

logger = logging.getLogger(__name__)

with open("config.yaml") as f:
    config = yaml.safe_load(f)

PATHS = config["paths"]
DIRECTORIES = config.get("directories", [])


@task
def create_required_dirs() -> None:
    for dir_key in DIRECTORIES:
        Path(config["paths"][dir_key]).mkdir(exist_ok=True, parents=True)
    # Make sure we have a logs dir for error dumps
    Path("logs").mkdir(exist_ok=True)


@task
def run_scrapers() -> None:
    results = asyncio.run(run_all_scrapers(config))

    succeeded = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    # ── Clean summary table ──────────────────────────────────────────────────
    width = 50
    print("\n" + "=" * width)
    print("  SCRAPER RUN SUMMARY")
    print("=" * width)
    for r in results:
        status = "✅ OK" if r.success else "❌ FAILED"
        print(f"  {r.name:<30} {status}")
        if not r.success:
            # One-liner reason — full trace goes to the log file
            short = (r.error or "unknown error").splitlines()[0][:60]
            print(f"      └─ {short}")
    print("-" * width)
    print(f"  {len(succeeded)}/{len(results)} scrapers succeeded.")
    print("=" * width + "\n")

    # ── Full tracebacks → logs/scraper_errors.log ────────────────────────────
    if failed:
        log_path = Path("logs/scraper_errors.log")
        with open(log_path, "w", encoding="utf-8") as f:
            for r in failed:
                f.write(f"\n{'=' * 60}\n")
                f.write(f"SCRAPER : {r.name}\n")
                f.write(f"MODULE  : {r.module}\n")
                f.write(f"ERROR   : {r.error}\n")
                f.write(f"{'─' * 60}\n")
                f.write(r.tb or "No traceback available.")
                f.write("\n")
        logger.warning("Full error details written to %s", log_path)

    # Only blow up the flow if every single scraper failed
    if len(failed) == len(results):
        raise RuntimeError(
            f"All {len(results)} scrapers failed. "
            "Check logs/scraper_errors.log for details."
        )


@flow
def partial_scrapers_pipeline() -> None:
    create_required_dirs()
    run_scrapers()


if __name__ == "__main__":
    partial_scrapers_pipeline()
