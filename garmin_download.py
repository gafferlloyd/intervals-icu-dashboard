"""
garmin_download.py
──────────────────
Downloads the last N cycling and running .fit files from Garmin Connect,
then immediately parses each one and writes a companion .json file.

Directory layout:
    fit_files/
        2026-03-12_17-04-42_cycling_22150767637.fit
        2026-03-12_17-04-42_cycling_22150767637.json   ← parsed metrics
        ...

Requirements:
    pip install garminconnect python-dotenv fitparse

Credentials:
    Copy .env.template → .env and fill in GARMIN_EMAIL / GARMIN_PASSWORD.
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
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
DAYS_BACK       = 42        # fetch all cycling/running within this window
DELAY_BETWEEN   = 1.5       # seconds between downloads (be polite to Garmin)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def activity_category(activity: dict) -> str | None:
    """
    Return the Garmin typeKey if it's cycling or running, else None.
    We preserve the exact typeKey (e.g. 'indoor_cycling', 'road_biking')
    so fit_parser can select the correct FTP.
    """
    type_key = (
        activity.get("activityType", {})
                .get("typeKey", "")
                .lower()
    )

    cycling_keywords = ("cycl", "bik", "ride")
    running_keywords = ("run", "jog", "treadmill")

    if any(k in type_key for k in cycling_keywords):
        return type_key     # e.g. 'indoor_cycling', 'road_biking', 'cycling'
    if any(k in type_key for k in running_keywords):
        return type_key     # e.g. 'running', 'trail_running'
    return None


def broad_category(type_key: str) -> str:
    """Map a specific typeKey to 'cycling' or 'running' for filename use."""
    cycling_keywords = ("cycl", "bik", "ride")
    if any(k in type_key for k in cycling_keywords):
        return "cycling"
    return "running"


def authenticate() -> Garmin:
    if not GARMIN_EMAIL or not GARMIN_PASSWORD:
        raise ValueError(
            "Credentials missing. Set GARMIN_EMAIL and GARMIN_PASSWORD "
            "in your .env file."
        )
    log.info("Authenticating as %s …", GARMIN_EMAIL)
    client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
    client.login()
    log.info("Authenticated.")
    return client


def download_fit(client: Garmin, activity: dict, dest_dir: Path) -> tuple[Path | None, str | None]:
    """
    Download a .fit file.
    Returns (fit_path, type_key) or (None, None) on failure.
    """
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


def parse_and_save(fit_path: Path, type_key: str) -> bool:
    """
    Parse fit_path and write a companion .json file.
    Skips parsing if .json already exists and is newer than the .fit.
    Returns True on success.
    """
    json_path = fit_path.with_suffix(".json")

    if json_path.exists() and json_path.stat().st_mtime >= fit_path.stat().st_mtime:
        log.info("    .json already up to date: %s", json_path.name)
        return True

    try:
        metrics = parse_fit(fit_path, type_key)
        json_path.write_text(json.dumps(metrics, indent=2))
        log.info("    → parsed: %s", json_path.name)
        return True
    except Exception as exc:
        log.warning("    Parse failed (%s): %s", fit_path.name, exc)
        return False


def main() -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    try:
        client = authenticate()
    except (GarminConnectAuthenticationError, ValueError) as exc:
        log.error("%s", exc)
        return

    start_date = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    log.info("Fetching cycling/running activities since %s …",
             start_date.strftime("%Y-%m-%d"))

    # get_activities_by_date fetches all activities in a date range
    all_activities = client.get_activities_by_date(
        start_date.strftime("%Y-%m-%d"),
        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )

    filtered = [
        (a, activity_category(a))
        for a in all_activities
        if activity_category(a) is not None
    ]

    log.info("Found %d cycling/running activities in last %d days (from %d total).",
             len(filtered), DAYS_BACK, len(all_activities))

    downloaded, skipped, failed, parse_failed = [], [], [], []

    for i, (activity, type_key) in enumerate(filtered, start=1):
        log.info("Processing %d/%d  [%s] …", i, len(filtered), type_key)
        fit_path, tk = download_fit(client, activity, DOWNLOAD_DIR)

        if fit_path is None:
            failed.append(activity["activityId"])
        elif fit_path.stat().st_size == 0:
            skipped.append(activity["activityId"])
        else:
            downloaded.append(fit_path)
            ok = parse_and_save(fit_path, tk)
            if not ok:
                parse_failed.append(fit_path.name)

        if i < len(filtered):
            time.sleep(DELAY_BETWEEN)

    cycling_count = sum(1 for f in downloaded if "_cycling_" in f.name)
    running_count = sum(1 for f in downloaded if "_running_" in f.name)

    print("\n" + "─" * 58)
    print(f"  Downloaded : {len(downloaded)} file(s)  →  {DOWNLOAD_DIR.resolve()}")
    print(f"             : {cycling_count} cycling  |  {running_count} running")
    print(f"  Skipped    : {len(skipped)}  (already present)")
    print(f"  Failed     : {len(failed)}")
    if parse_failed:
        print(f"  Parse errs : {parse_failed}")
    print("─" * 58)


if __name__ == "__main__":
    main()
