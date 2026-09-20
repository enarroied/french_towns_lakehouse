#!/bin/bash
# ==============================================================
#  Install / refresh the QGIS+QField "communes" project.
#
#  Source of truth:
#    - commit:  generate_qfield/communes/communes_qfield.qgs  (project template)
#    - commit:  generate_qfield/communes/generate_communes_gpkg.py (baseline gpkg)
#    - runtime: $PROJECT_DIR (default ~/qgis_projects/communes) — field data lives HERE
#  The project template is copied into place; existing field data is NEVER overwritten.
# ==============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(realpath "$0")")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
QGS_TEMPLATE="$SCRIPT_DIR/communes_qfield.qgs"

PROJECT_DIR="${QFIELD_PROJECT_DIR:-$HOME/qgis_projects/communes}"

echo "=== QGIS/QField project directory: $PROJECT_DIR ==="
mkdir -p "$PROJECT_DIR"

if [ ! -f "$PROJECT_DIR/communes.gpkg" ]; then
  echo "=== Generating baseline communes.gpkg from the lakehouse ==="
  cd "$PROJECT_ROOT"
  uv run python "$SCRIPT_DIR/generate_communes_gpkg.py" --project-dir "$PROJECT_DIR"
else
  echo "=== communes.gpkg already exists — keeping it (field data preserved) ==="
fi

if [ ! -f "$PROJECT_DIR/communes_qfield.qgs" ] || [ "${1:-}" = "--force" ]; then
  echo "=== Installing project template → communes_qfield.qgs ==="
  cp "$QGS_TEMPLATE" "$PROJECT_DIR/communes_qfield.qgs"
else
  echo "=== communes_qfield.qgs already exists — keeping it ==="
fi

echo ""
echo "✅ Project ready at $PROJECT_DIR"
echo "   Next steps:"
echo "   1. Open $PROJECT_DIR/communes_qfield.qgs in QGIS"
echo "   2. QFieldSync: package to QField / upload to QFieldCloud"
echo "   3. After field work, refresh the dashboard:"
echo "        scripts/dashboard/refresh_dashboard.sh"
echo "      then commit & push, then:"
echo "        uv run python scripts/dashboard/promote_photos.py"
