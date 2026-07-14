#!/bin/bash
# ==============================================================
#  Refresh the visited-towns dashboard data + render.
# ==============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(realpath "$0")")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"

echo "=== Step 1: Generate photo thumbnails → blog/data/img/ ==="
uv run python scripts/dashboard/generate_thumbnails.py

echo ""
echo "=== Step 2: Generate master parquet ==="
uv run python scripts/dashboard/generate_master_parquet.py

echo ""
echo "=== Step 3: Copy data into blog/ for Quarto ==="
mkdir -p "$PROJECT_ROOT/blog/data/dashboard"
cp "$PROJECT_ROOT/data/dashboard/visited_towns.parquet" "$PROJECT_ROOT/blog/data/dashboard/"

echo ""
echo "=== Step 4: Render dashboard ==="
QUARTO_PYTHON="$PROJECT_ROOT/.venv/bin/python" \
  quarto render blog/dashboards/visited-towns/

echo ""
echo "✅ Dashboard refreshed"
