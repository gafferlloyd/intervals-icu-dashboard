"""
fetch_icu_data.py
─────────────────
Fetches data from intervals.icu API and saves to icu_data.json.

Currently fetches:
  - Wellness / PMC (CTL, ATL, TSB, ramp rate) — daily, all dates
  - Power/HR curves — per year + recent 42d
  - Activities list — for TRIMP, calories, zones by year

Run:
    python3 fetch_icu_data.py

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

# ── Config ────────────────────────────────────────────────────────────────────
ICU_API_KEY    = os.getenv("ICU_API_KEY", "")
ICU_ATHLETE_ID = os.getenv("ICU_ATHLETE_ID", "")
BASE_URL       = "https://intervals.icu/api/v1"
OUTPUT_FILE    = Path("icu_data.json")
DELAY          = 0.5   # seconds between requests — be polite

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def icu_get(path: str, params: dict = {}) -> dict | list:
    """Make an authenticated GET request to the intervals.icu API."""
    url  = f"{BASE_URL}/{path}"
    auth = HTTPBasicAuth("API_KEY", ICU_API_KEY)
    resp = requests.get(url, auth=auth, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_wellness(oldest: str, newest: str) -> list[dict]:
    """Fetch daily wellness/PMC records."""
    log.info("Fetching wellness %s → %s …", oldest, newest)
    data = icu_get(
        f"athlete/{ICU_ATHLETE_ID}/wellness.json",
        params={
            "oldest": oldest,
            "newest": newest,
            "fields": "id,ctl,atl,rampRate,ctlLoad,atlLoad",
        }
    )
    log.info("  → %d wellness records", len(data))
    return data


def fetch_power_hr_curve(start: str, end: str) -> dict:
    """Fetch power vs HR curve for a date range."""
    log.info("Fetching power/HR curve %s → %s …", start, end)
    data = icu_get(
        f"athlete/{ICU_ATHLETE_ID}/power-hr-curve",
        params={"start": start, "end": end}
    )
    return data


def fetch_activities(oldest: str, newest: str) -> list[dict]:
    """Fetch activity list with key metrics."""
    log.info("Fetching activities %s → %s …", oldest, newest)
    data = icu_get(
        f"athlete/{ICU_ATHLETE_ID}/activities",
        params={
            "oldest"  : oldest,
            "newest"  : newest,
            "fields"  : "id,start_date_local,type,name,moving_time,distance,"
                        "total_elevation_gain,average_heartrate,max_heartrate,"
                        "calories,trimp,icu_training_load,icu_atl,icu_ctl,"
                        "trainer,average_watts,icu_weighted_avg_watts,"
                        "icu_intensity,average_cadence",
        }
    )
    log.info("  → %d activities", len(data))
    return data


def main():
    if not ICU_API_KEY or not ICU_ATHLETE_ID:
        log.error("ICU_API_KEY and ICU_ATHLETE_ID must be set in .env")
        return

    log.info("Fetching data from intervals.icu for athlete %s …", ICU_ATHLETE_ID)

    # Date ranges
    today      = datetime.now().strftime("%Y-%m-%d")
    start_2023 = "2023-01-01"

    output = {
        "generated"    : datetime.now().strftime("%Y-%m-%d %H:%M"),
        "athlete_id"   : ICU_ATHLETE_ID,
    }

    # ── 1. Wellness / PMC ─────────────────────────────────────────────────────
    wellness = fetch_wellness(start_2023, today)
    output["wellness"] = wellness
    time.sleep(DELAY)

    # ── 2. Power/HR curves — one per year + recent 42d ───────────────────────
    years = ["2023", "2024", "2025"]
    power_hr_curves = {}

    for year in years:
        curve = fetch_power_hr_curve(f"{year}-01-01", f"{year}-12-31")
        power_hr_curves[year] = curve
        log.info("  → %s: %d power buckets", year, len(curve.get("bpm", [])))
        time.sleep(DELAY)

    # 2026 year to date
    curve = fetch_power_hr_curve("2026-01-01", today)
    power_hr_curves["2026"] = curve
    log.info("  → 2026 YTD: %d power buckets", len(curve.get("bpm", [])))
    time.sleep(DELAY)

    # Recent 42 days
    cutoff_42d = (datetime.now() - timedelta(days=42)).strftime("%Y-%m-%d")
    curve = fetch_power_hr_curve(cutoff_42d, today)
    power_hr_curves["recent"] = curve
    log.info("  → recent 42d: %d power buckets", len(curve.get("bpm", [])))
    time.sleep(DELAY)

    output["power_hr_curves"] = power_hr_curves

    # ── 3. Activities ─────────────────────────────────────────────────────────
    activities = fetch_activities(start_2023, today)
    output["activities"] = activities
    time.sleep(DELAY)

    # ── Save ──────────────────────────────────────────────────────────────────
    OUTPUT_FILE.write_text(json.dumps(output, indent=2))
    size_kb = OUTPUT_FILE.stat().st_size // 1024
    log.info("Written → %s  (%d KB)", OUTPUT_FILE, size_kb)

    # ── Quick summary ─────────────────────────────────────────────────────────
    latest = wellness[-1] if wellness else {}
    print(f"\n{'─'*50}")
    print(f"  Wellness records : {len(wellness)}")
    print(f"  Latest CTL       : {latest.get('ctl', '?'):.1f}" if latest.get('ctl') else "  Latest CTL       : ?")
    print(f"  Latest ATL       : {latest.get('atl', '?'):.1f}" if latest.get('atl') else "  Latest ATL       : ?")
    print(f"  Activities       : {len(activities)}")
    print(f"  Power/HR curves  : {list(power_hr_curves.keys())}")
    print(f"{'─'*50}")


if __name__ == "__main__":
    main()
