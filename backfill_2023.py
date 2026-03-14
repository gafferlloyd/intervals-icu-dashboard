"""
backfill_2023.py
────────────────
One-off script to download cycling and running .fit files from
2023-01-01 up to (but not overlapping) the existing archive.

Run once:
    python3 backfill_2023.py

Then rebuild clouds:
    rm running_cloud.json cycling_cloud.json cycling_curve.json running_bests.json
    python3 build_running_cloud.py
    python3 build_cycling_cloud.py
    python3 build_cycling_curve.py
    python3 build_running_bests.py
    python3 build_data.py
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

from garminconnect import Garmin, GarminConnectAuthenticationError
from fit_parser import parse_fit

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Config ────────────────────────────────────────────────────────────────────
GARMIN_EMAIL    = os.getenv("GARMIN_EMAIL", "")
GARMIN_PASSWORD = os.getenv("GARMIN_PASSWORD", "")
DOWNLOAD_DIR    = Path("fit_files")
START_DATE      = "2023-01-01"
END_DATE        = "2024-01-02"   # overlap by 1 day to ensure no gap
DELAY_BETWEEN   = 1.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def activity_category(activity: dict) -> str | None:
    type_key = (
        activity.get("activityType", {})
                .get("typeKey", "")
                .lower()
    )
    cycling_keywords = ("cycl", "bik", "ride")
    running_keywords = ("run", "jog", "treadmill")
    if any(k in type_key for k in cycling_keywords):
        return type_key
    if any(k in type_key for k in running_keywords):
        return type_key
    return None


def broad_category(type_key: str) -> str:
    if any(k in type_key for k in ("cycl", "bik", "ride")):
        return "cycling"
    return "running"


def download_fit(client, activity, dest_dir):
    activity_id   = activity["activityId"]
    activity_name = activity.get("activityName", "unknown")
    start_time    = activity.get("startTimeLocal", "unknown")
    type_key      = activity_category(activity)
    label         = broad_category(type_key)

    safe_time = str(start_time).replace(":", "-").replace(" ", "_")
    fit_path  = dest_dir / f"{safe_time}_{label}_{activity_id}.fit"

    if fit_path.exists():
        log.info("  ↳ already present: %s", fit_path.name)
        return fit_path, type_key

    log.info("  ↓ [%s] %-16s  %s", activity_id, type_key, activity_name)
    try:
        fit_data = client.download_activity(
            activity_id,
            dl_fmt=client.ActivityDownloadFormat.ORIGINAL,
        )
        fit_path.write_bytes(fit_data)
        return fit_path, type_key
    except Exception as exc:
        log.warning("    Download failed (%s): %s", activity_id, exc)
        return None, None


def parse_and_save(fit_path, type_key):
    json_path = fit_path.with_suffix(".json")
    if json_path.exists():
        return True
    try:
        metrics = parse_fit(fit_path, type_key)
        json_path.write_text(json.dumps(metrics, indent=2))
        log.info("    → parsed: %s", json_path.name)
        return True
    except Exception as exc:
        log.warning("    Parse failed (%s): %s", fit_path.name, exc)
        return False


def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    if not GARMIN_EMAIL or not GARMIN_PASSWORD:
        log.error("Credentials missing — check .env file")
        return

    log.info("Authenticating as %s …", GARMIN_EMAIL)
    client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
    client.login()
    log.info("Authenticated.")

    log.info("Fetching activities from %s to %s …", START_DATE, END_DATE)
    all_activities = client.get_activities_by_date(START_DATE, END_DATE)

    filtered = [
        (a, activity_category(a))
        for a in all_activities
        if activity_category(a) is not None
    ]
    log.info("Found %d cycling/running activities (%d total).",
             len(filtered), len(all_activities))

    downloaded, skipped, failed = [], [], []

    for i, (activity, type_key) in enumerate(filtered, start=1):
        log.info("Processing %d/%d  [%s] …", i, len(filtered), type_key)
        fit_path, tk = download_fit(client, activity, DOWNLOAD_DIR)

        if fit_path is None:
            failed.append(activity["activityId"])
        elif fit_path.stat().st_size == 0:
            skipped.append(activity["activityId"])
        else:
            downloaded.append(fit_path)
            parse_and_save(fit_path, tk)

        if i < len(filtered):
            time.sleep(DELAY_BETWEEN)

    cycling_count = sum(1 for f in downloaded if "_cycling_" in f.name)
    running_count = sum(1 for f in downloaded if "_running_" in f.name)

    print("\n" + "─" * 58)
    print(f"  Downloaded : {len(downloaded)} file(s)  →  {DOWNLOAD_DIR.resolve()}")
    print(f"             : {cycling_count} cycling  |  {running_count} running")
    print(f"  Skipped    : {len(skipped)}  (already present)")
    print(f"  Failed     : {len(failed)}")
    print("─" * 58)


if __name__ == "__main__":
    main()