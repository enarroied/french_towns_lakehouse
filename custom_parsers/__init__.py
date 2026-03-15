from importlib import import_module
from pathlib import Path


def run_all_custom_parsers(config: dict) -> list[Path]:
    output_paths = []
    for parser in config.get("custom_parsers", []):
        if not parser.get("enabled", True):
            continue
        module = import_module(parser["module"])
        output_path = module.run(config)
        output_paths.append(output_path)
    return output_paths
