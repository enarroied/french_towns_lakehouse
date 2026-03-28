import logging
import sys
import traceback
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path

import yaml
from prefect import flow
from prefect import task


logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent))

with open("config.yaml") as f:
    config = yaml.safe_load(f)

DIRECTORIES = config.get("directories", [])


@dataclass
class ParserResult:
    name: str
    module: str
    success: bool
    output_path: Path | None = None
    error: str | None = None
    tb: str | None = None


@task
def create_required_dirs() -> None:
    for dir_key in DIRECTORIES:
        Path(config["paths"][dir_key]).mkdir(exist_ok=True, parents=True)
    Path("logs").mkdir(exist_ok=True)


@task
def run_parsers() -> list[ParserResult]:
    results: list[ParserResult] = []
    skipped: list[dict] = []

    for parser in config.get("custom_parsers", []):
        if not parser.get("enabled", True):
            skipped.append(parser)
            continue

        name = parser["name"]
        module_path = parser["module"]

        try:
            module = import_module(module_path)
            output_path = module.run(config)
            results.append(
                ParserResult(
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
                ParserResult(
                    name=name,
                    module=module_path,
                    success=False,
                    error=str(exc),
                    tb=full_tb,
                )
            )
            logger.error("❌ %s — FAILED: %s", name, exc)

    succeeded = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    width = 50
    print("\n" + "=" * width)
    print("  PARSER RUN SUMMARY")
    print("=" * width)

    for s in skipped:
        print(f"  {s['name']:<30} ⏭  SKIPPED (disabled)")

    for r in results:
        status = "✅ OK" if r.success else "❌ FAILED"
        print(f"  {r.name:<30} {status}")
        if not r.success:
            short = (r.error or "unknown error").splitlines()[0][:60]
            print(f"      └─ {short}")

    print("-" * width)
    print(f"  {len(succeeded)}/{len(results)} parsers succeeded.")
    print("=" * width + "\n")

    if failed:
        log_path = Path("logs/parser_errors.log")
        with open(log_path, "w", encoding="utf-8") as f:
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


@flow
def partial_parsers_pipeline() -> None:
    create_required_dirs()
    run_parsers()


if __name__ == "__main__":
    partial_parsers_pipeline()
