"""Read config.yaml sources + source_links and write CSVs for dim_source.

Reads source metadata from:
  - downloads:   (11 INSEE/LaPoste datasets)
  - scrapers:    (7 label sources)
  - custom_parsers: (1 label source)
  - sources:     (non-download/scraper items e.g. dim_equipment)

Produces:
  data_sources/dim_source/sources.csv
  data_sources/dim_source/bridge_model_sources.csv
"""

import csv
from pathlib import Path

import yaml


OUTPUT_DIR = Path(__file__).parent
CONFIG_PATH = OUTPUT_DIR / ".." / ".." / "config.yaml"

SOURCES_COLUMNS = [
    "source_id",
    "source_name",
    "source_label",
    "organization",
    "domain",
    "reference_url",
    "license",
    "description",
]

BRIDGE_COLUMNS = ["model_name", "source_id"]


def _collect_sources(config: dict) -> list[dict]:
    sources = []

    for entry in config.get("downloads", []):
        sources.append(
            {
                "source_id": str(entry.get("source_id", "")),
                "source_name": entry["name"],
                "source_label": entry.get("source_label", ""),
                "organization": entry.get("organization", ""),
                "domain": entry.get("domain", ""),
                "reference_url": entry.get("reference_url", ""),
                "license": entry.get("license", ""),
                "description": entry.get("description", ""),
            }
        )

    for entry in config.get("scrapers", []):
        sources.append(
            {
                "source_id": str(entry.get("source_id", "")),
                "source_name": entry["name"],
                "source_label": entry.get("source_label", ""),
                "organization": entry.get("organization", ""),
                "domain": entry.get("domain", ""),
                "reference_url": entry.get("reference_url", ""),
                "license": entry.get("license", ""),
                "description": entry.get("description", ""),
            }
        )

    for entry in config.get("custom_parsers", []):
        sources.append(
            {
                "source_id": str(entry.get("source_id", "")),
                "source_name": entry["name"],
                "source_label": entry.get("source_label", ""),
                "organization": entry.get("organization", ""),
                "domain": entry.get("domain", ""),
                "reference_url": entry.get("reference_url", ""),
                "license": entry.get("license", ""),
                "description": entry.get("description", ""),
            }
        )

    for entry in config.get("sources", []):
        sources.append(
            {
                "source_id": str(entry.get("source_id", "")),
                "source_name": entry.get("source_name", ""),
                "source_label": entry.get("source_label", ""),
                "organization": entry.get("organization", ""),
                "domain": entry.get("domain", ""),
                "reference_url": entry.get("reference_url", ""),
                "license": entry.get("license", ""),
                "description": entry.get("description", ""),
            }
        )

    sources.sort(key=lambda s: int(s["source_id"]) if s["source_id"].isdigit() else 0)
    return sources


def generate_sources_csv() -> int:
    with CONFIG_PATH.open(encoding="utf-8") as f:
        config = yaml.safe_load(f)

    sources = _collect_sources(config)
    csv_path = OUTPUT_DIR / "sources.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SOURCES_COLUMNS)
        writer.writeheader()
        writer.writerows(sources)
    print(f"✅ sources.csv: {len(sources)} entries")
    return len(sources)


def generate_bridge_csv() -> int:
    with CONFIG_PATH.open(encoding="utf-8") as f:
        config = yaml.safe_load(f)

    links = config.get("source_links", [])
    rows = []
    for link in links:
        model = link["model"]
        for sid in link["sources"]:
            rows.append({"model_name": model, "source_id": str(sid)})

    csv_path = OUTPUT_DIR / "bridge_model_sources.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=BRIDGE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ bridge_model_sources.csv: {len(rows)} entries")
    return len(rows)


if __name__ == "__main__":
    n_src = generate_sources_csv()
    n_bridge = generate_bridge_csv()
    print(f"Done — {n_src} sources, {n_bridge} bridge links")
