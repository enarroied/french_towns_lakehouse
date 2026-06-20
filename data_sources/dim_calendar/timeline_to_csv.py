"""Read timelines.yaml and output per-category enrichment CSV files.

Dynamically discovers all top-level keys in the YAML (french_presidents,
french_prime_ministers, french_legislatures, etc.) and writes one CSV
per category with columns start_date, end_date, name.
"""

import csv
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


OUTPUT_DIR = Path(__file__).parent
YAML_PATH = OUTPUT_DIR / "timelines.yaml"


class TimelineError(Exception):
    """Raised on invalid timeline YAML structure or content."""


def _parse_and_validate_date(value: Any, field_name: str, entry_name: str) -> str:
    """Coerce YAML date values (string or native date) into a strict YYYY-MM-DD string."""
    if value is None:
        return ""

    # Handle case where PyYAML auto-parses into a datetime.date object
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    if isinstance(value, str):
        try:
            # Validate format string explicitly
            datetime.strptime(value, "%Y-%m-%d")
            return value
        except ValueError as e:
            raise TimelineError(
                f"Invalid date format for '{field_name}' in '{entry_name}'. Got '{value}' — must be YYYY-MM-DD."
            ) from e

    raise TimelineError(
        f"Unexpected type for '{field_name}' in '{entry_name}'. Expected date string, got {type(value).__name__}."
    )


def write_timeline_csv_files() -> int:
    """Parse timelines.yaml and write one CSV per category. Returns file count."""
    if not YAML_PATH.exists():
        raise FileNotFoundError(f"timelines.yaml not found at {YAML_PATH}")

    raw = YAML_PATH.read_text(encoding="utf-8")
    if not raw.strip():
        raise TimelineError("timelines.yaml is empty")

    data = yaml.safe_load(raw)
    if data is None:
        raise TimelineError("timelines.yaml contains no data")
    if not isinstance(data, dict):
        raise TimelineError("timelines.yaml root must be a mapping (dictionary)")

    file_count = 0

    for category_key, entries in data.items():
        # Handle empty categories safely
        if entries is None:
            continue

        if not isinstance(entries, list):
            raise TimelineError(
                f"Category '{category_key}' must be a list, got {type(entries).__name__}"
            )

        rows: list[dict[str, str]] = []

        for entry in entries:
            if not isinstance(entry, dict):
                raise TimelineError(
                    f"Entry in '{category_key}' must be a mapping, got {type(entry).__name__}"
                )

            name = entry.get("name")
            if not name:
                raise TimelineError(
                    f"Entry in '{category_key}' missing required 'name' field"
                )

            # Ensure everything maps safely regardless of how PyYAML digested it
            start_val = entry.get("start")
            if start_val is None:
                raise TimelineError(
                    f"Entry '{name}' in '{category_key}' missing required 'start' field"
                )

            start_str = _parse_and_validate_date(start_val, "start", str(name))
            end_str = _parse_and_validate_date(entry.get("end"), "end", str(name))

            rows.append(
                {"start_date": start_str, "end_date": end_str, "name": str(name)}
            )

        # Skip writing empty CSVs if a category list explicitly exists but is empty
        if not rows:
            continue

        csv_path = OUTPUT_DIR / f"{category_key}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["start_date", "end_date", "name"])
            writer.writeheader()
            writer.writerows(rows)

        print(f"✅ {category_key}.csv: {len(rows)} entries")
        file_count += 1

    return file_count


if __name__ == "__main__":
    write_timeline_csv_files()
