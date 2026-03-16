"""
icu_download.py
───────────────
Downloads .fit files from intervals.icu for all cycling and running
activities not already present locally.

Replaces garmin_download.py — no Garmin credentials needed.

Run:
    python3 icu_download.py

Requires in .env:
    ICU_API_KEY=...
    ICU_ATHLETE_ID=i128043
"""

from __future__ import annotations
import os
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fit_parser import parse_fit

# ── Config ────────────────────────────────────────────────────────────────────
ICU_API_KEY    = os.getenv("ICU_API_KEY", "")
ICU_ATHLETE_ID = os.getenv("ICU_ATHLETE_ID", "")
BASE_URL       = "https://intervals.icu/api/v1"
FIT_DIR        = Path("fit_files")
DELAY          = 0.3   # seconds between requests

# Activity types to download
CYCLING_TYPES = {"ride", "virtualride", "gravelride", "mountainbikeride",
                 "ebikeride", "emountainbikeride", "indoorcycling"}
RUNNING_TYPES = {"run", "trailrun", "virtualrun", "treadmill"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def activity_label(type_key: str) -> str | None:
    t = type_key.lower()
    if t in CYCLING_TYPES: return "cycling"
    if t in RUNNING_TYPES: return "running"
    return None


def existing_icu_ids() -> set[str]:
    """Return set of intervals.icu activity IDs already downloaded."""
    ids = set()
    for f in FIT_DIR.glob("*.json"):
        try:
            data = json.loads(f.read_text())
            icu_id = data.get("icu_activity_id")
            if icu_id:
                ids.add(icu_id)
        except Exception:
            pass
    return ids


def fetch_activities(oldest: str, newest: str) -> list[dict]:
    auth = HTTPBasicAuth("API_KEY", ICU_API_KEY)
    resp = requests.get(
        f"{BASE_URL}/athlete/{ICU_ATHLETE_ID}/activities",
        auth=auth,
        params={
            "oldest": oldest,
            "newest": newest,
            "fields": "id,start_date_local,type,name,trainer",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def download_fit(activity_id: str) -> bytes | None:
    auth = HTTPBasicAuth("API_KEY", ICU_API_KEY)
    resp = requests.get(
        f"{BASE_URL}/activity/{activity_id}/fit-file",
        auth=auth,
        timeout=30,
    )
    if resp.status_code == 200:
        return resp.content
    log.warning("  fit-file returned %d for %s", resp.status_code, activity_id)
    return None


def main():
    if not ICU_API_KEY or not ICU_ATHLETE_ID:
        log.error("ICU_API_KEY and ICU_ATHLETE_ID must be set in .env")
        return

    FIT_DIR.mkdir(parents=True, exist_ok=True)

    # Download last 42 days by default — enough to catch any missed sessions
    newest = datetime.now().strftime("%Y-%m-%d")
    oldest = (datetime.now() - timedelta(days=42)).strftime("%Y-%m-%d")

    log.info("Fetching activity list %s → %s …", oldest, newest)
    activities = fetch_activities(oldest, newest)
    log.info("Found %d activities.", len(activities))

    # Filter to cycling + running only
    targets = [
        a for a in activities
        if activity_label(a.get("type", "")) is not None
    ]
    log.info("%d cycling/running activities.", len(targets))

    # Find already-downloaded IDs
    existing = existing_icu_ids()
    to_download = [a for a in targets if a["id"] not in existing]
    log.info("%d new activities to download.", len(to_download))

    downloaded, failed = [], []

    for i, activity in enumerate(to_download, 1):
        icu_id    = activity["id"]
        type_key  = activity.get("type", "")
        label     = activity_label(type_key)
        name      = activity.get("name", "unknown")
        start     = activity.get("start_date_local", "")[:19].replace("T","_").replace(":","_").replace(" ","_")
        is_indoor = activity.get("trainer", False) or type_key.lower() in {"virtualride","indoorcycling","virtualrun"}

        safe_start = start.replace(":", "-")
        fit_path   = FIT_DIR / f"{safe_start[:10]}_{safe_start[11:]}_{label}_{icu_id}.fit"
        json_path  = fit_path.with_suffix(".json")

        log.info("  [%d/%d] %s  %s  %s", i, len(to_download), icu_id, label, name)

        fit_data = download_fit(icu_id)
        if not fit_data:
            failed.append(icu_id)
            continue

        fit_path.write_bytes(fit_data)

        # Parse immediately
        try:
            metrics = parse_fit(fit_path, type_key)
            metrics["icu_activity_id"] = icu_id   # store for dedup
            metrics["is_indoor"]       = is_indoor
            json_path.write_text(json.dumps(metrics, indent=2))
            downloaded.append(fit_path)
            log.info("    → parsed OK")
        except Exception as exc:
            log.warning("    Parse failed: %s", exc)
            failed.append(icu_id)

        if i < len(to_download):
            time.sleep(DELAY)

    cycling = sum(1 for f in downloaded if "_cycling_" in f.name)
    running = sum(1 for f in downloaded if "_running_" in f.name)

    print(f"\n{'─'*55}")
    print(f"  Downloaded : {len(downloaded)}  ({cycling} cycling / {running} running)")
    print(f"  Failed     : {len(failed)}")
    print(f"  Skipped    : {len(to_download) - len(downloaded) - len(failed)} (parse errors)")
    print(f"{'─'*55}")


if __name__ == "__main__":
    main()
