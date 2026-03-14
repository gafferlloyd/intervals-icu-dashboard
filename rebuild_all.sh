#!/bin/bash
# rebuild_all.sh
# ──────────────
# Runs all data builders sequentially.
# Use after downloading new .fit files or changing any builder/parser.
#
# Usage:
#   ./rebuild_all.sh              # full rebuild
#   ./rebuild_all.sh --push       # full rebuild + push to GitHub
#   ./rebuild_all.sh --from 3     # restart from step 3
#   ./rebuild_all.sh --from 4 --push  # restart from step 4 and push

set -e   # stop on any error

# ── Parse arguments ───────────────────────────────────────────────────────────
FROM_STEP=1
PUSH=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --from) FROM_STEP="$2"; shift 2 ;;
        --push) PUSH=true; shift ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

echo ""
echo "══════════════════════════════════════════════"
echo "  Training Dashboard — Rebuild (from step ${FROM_STEP})"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "══════════════════════════════════════════════"

cd "$(dirname "$0")"

step() {
    local num=$1
    local label=$2
    if (( num < FROM_STEP )); then
        echo "── $num/6  $label [skipped]"
    else
        echo ""
        echo "── $num/6  $label ──────────────────────────"
    fi
}

run() {
    local num=$1
    local cmd=$2
    if (( num >= FROM_STEP )); then
        eval "$cmd"
    fi
}

step 1 "Reparse .fit files (add new fields)"
run  1 "python3 reparse_all.py"

step 2 "Running pace/HR cloud"
run  2 "python3 build_running_cloud.py"

step 3 "Cycling power/HR cloud"
run  3 "python3 build_cycling_cloud.py"

step 4 "Cycling power curve (MMP)"
run  4 "python3 build_cycling_curve.py"

step 5 "Running personal bests"
run  5 "python3 build_running_bests.py"

step 6 "Dashboard data"
run  6 "python3 build_data.py"

echo ""
echo "══════════════════════════════════════════════"
echo "  Rebuild complete  $(date '+%H:%M:%S')"
echo "══════════════════════════════════════════════"
echo ""

if [[ "$PUSH" == "true" ]]; then
    echo "Pushing to GitHub …"
    ./push_to_github.sh "Rebuild: $(date '+%Y-%m-%d %H:%M')"
fi
