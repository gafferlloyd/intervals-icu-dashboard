#!/bin/bash
# setup_cron.sh
# ─────────────
# Installs a nightly cron job to run the full dashboard rebuild locally.
# Runs at 23:30 local time (after Garmin syncs from your watch).
#
# Run once:
#   chmod +x setup_cron.sh && ./setup_cron.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/cron.log"
VENV="$SCRIPT_DIR/.venv/bin/python3"

# Cron line: 23:30 nightly
CRON_LINE="30 23 * * * cd $SCRIPT_DIR && $VENV -c 'import subprocess; subprocess.run([\"./rebuild_all.sh\", \"--push\"])' >> $LOG_FILE 2>&1"

# Check if already installed
if crontab -l 2>/dev/null | grep -q "rebuild_all.sh"; then
    echo "Cron job already installed:"
    crontab -l | grep "rebuild_all.sh"
    exit 0
fi

# Install
( crontab -l 2>/dev/null; echo "$CRON_LINE" ) | crontab -
echo "Cron job installed. Runs nightly at 23:30."
echo "Log file: $LOG_FILE"
echo ""
echo "To verify:"
echo "  crontab -l"
echo ""
echo "To remove:"
echo "  crontab -l | grep -v rebuild_all | crontab -"
