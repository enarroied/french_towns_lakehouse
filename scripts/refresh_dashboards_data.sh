#!/bin/bash
# ==============================================================
#  Refresh all dashboard datasets from the gold layer.
#
#  Runs every .py in scripts/dashboards_data/ and writes CSVs
#  to blog/data/.
# ==============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "── Refreshing dashboard data ──"

for script in "$SCRIPT_DIR"/dashboards_data/*.py; do
    module="scripts.dashboards_data.$(basename "$script" .py)"
    echo "  Running $module..."
    uv run python -m "$module"
done

echo "✓ Done"
