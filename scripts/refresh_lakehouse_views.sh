#!/bin/bash
# ==============================================================
#  Refresh lakehouse.duckdb gold + silver views
#
#  Substitutes {{VAR}} placeholders in the SQL template from
#  .env, then re-applies the DDL so that any new tables or
#  schema changes are reflected in the .duckdb file used by
#  DBeaver / DuckDB CLI.
#
#  Idempotent — safe to run any time.
# ==============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LAKEHOUSE_DB="${LAKEHOUSE_DB:-$HOME/Documents/lakehouse.duckdb}"

source "$PROJECT_ROOT/.env"

echo "── Refreshing views in $LAKEHOUSE_DB ──"

sed \
  -e "s/{{POLARIS_CLIENT_ID}}/$POLARIS_CLIENT_ID/g" \
  -e "s/{{POLARIS_CLIENT_SECRET}}/$POLARIS_CLIENT_SECRET/g" \
  -e "s/{{MINIO_ROOT_USER}}/$MINIO_ROOT_USER/g" \
  -e "s/{{MINIO_ROOT_PASSWORD}}/$MINIO_ROOT_PASSWORD/g" \
  "$PROJECT_ROOT/scripts/init_duckdb.sql.template" \
  | duckdb "$LAKEHOUSE_DB"

echo ""
echo "✓ Done. Re-open the file in DBeaver (right-click → Refresh)."
