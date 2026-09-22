#!/bin/bash
# ==============================================================
#  Build a time-lapse movie from the dated France map snapshots.
#
#  Frames:  data/dashboard/snapshots/*.png  (kept locally, gitignored)
#  Output:  data/dashboard/snapshots/france_visited_movie.mp4
#
#  Run after a few refreshes have accumulated snapshots:
#    bash scripts/dashboard/build_movie.sh
# ==============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(realpath "$0")")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SNAPSHOT_DIR="$PROJECT_ROOT/data/dashboard/snapshots"

if ! command -v ffmpeg >/dev/null 2>&1; then
    echo "❌ ffmpeg not found. Install it first, e.g.:"
    echo "     sudo apt install ffmpeg     (Debian/Ubuntu)"
    echo "     brew install ffmpeg         (macOS)"
    exit 1
fi

mapfile -t FRAMES < <(find "$SNAPSHOT_DIR" -name '*.png' -type f | sort)
if [ "${#FRAMES[@]}" -lt 2 ]; then
    echo "⚠️  Only ${#FRAMES[@]} snapshot(s) found in $SNAPSHOT_DIR."
    echo "   Need at least 2 to build a movie (or 1 to make a still)."
    exit 0
fi

echo "Frames: ${#FRAMES[@]}"
echo "First:  $(basename "${FRAMES[0]}")"
echo "Last:   $(basename "${FRAMES[-1]}")"

OUT="$SNAPSHOT_DIR/france_visited_movie.mp4"
ffmpeg -y \
    -framerate 2 \
    -pattern_type glob \
    -i "$SNAPSHOT_DIR/*.png" \
    -c:v libx264 -pix_fmt yuv420p -movflags +faststart \
    -vf "format=yuv420p" \
    "$OUT"

echo ""
echo "✅ Movie saved: $OUT"
echo "   Frames evolve chronologically because filenames are YYYY-MM-DD.png."
