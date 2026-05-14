#!/bin/bash
# ==============================================================
#  Render the blog with the correct Python environment.
#
#  blog/_quarto.yml sets execute-dir: project so blog_utils
#  resolves via CWD — no PYTHONPATH needed.
#  blog_utils.py loads .env via python-dotenv at import time,
#  so no need to source .env here.
# ==============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

QUARTO_PYTHON="$PROJECT_ROOT/.venv/bin/python" \
  quarto render "$PROJECT_ROOT/blog/" "$@"
