"""
fetch_icu_data.py
─────────────────
Fetches data from intervals.icu API and saves to icu_data.json.

Fetches:
  - Wellness / PMC (CTL, ATL, TSB, ramp rate) — daily
  - Power/HR curves — per year + recent 42d (all cycling)
  - MMP power curves — per year, split indoor/outdoor, + recent 42d
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
DELAY          = 0.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

AUTH = None   # set in main()


def icu_get(path: str, params: dict = {}) -> dict | list:
    url  = f"{BASE_URL}/{path}"
    resp = requests.get(url, auth=AUTH, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_wellness(oldest: str, newest: str) -> list[dict]:
    log.info("Fetching wellness %s → %s …", oldest, newest)
    data = icu_get(
        f"athlete/{ICU_ATHLETE_ID}/wellness.json",
        params={
            "oldest": oldest, "newest": newest,
            "fields": "id,ctl,atl,rampRate,ctlLoad,atlLoad",
        }
    )
    log.info("  → %d wellness records", len(data))
    return data


def fetch_power_hr_curve(start: str, end: str, label: str = "") -> dict:
    """Fetch binned power vs HR curve (all cycling)."""
    log.info("Fetching power/HR curve %s %s → %s …", label, start, end)
    data = icu_get(
        f"athlete/{ICU_ATHLETE_ID}/power-hr-curve",
        params={"start": start, "end": end}
    )
    log.info("  → %s: %d buckets", label or "curve", len(data.get("bpm", [])))
    return data


def fetch_mmp_curves(start: str, end: str, label: str,
                     indoor: bool | None = None) -> dict | None:
    """
    Fetch MMP (mean maximal power) curves from intervals.icu.
    Uses f1/f2 filter params for indoor/outdoor split.
    """
    range_id = f"r.{start}.{end}"

    # Base params — no indoor filter means all cycling
    params = [
        ("curves", range_id),
        ("type",   "Ride"),
    ]

    # Indoor/outdoor filter — value must be string "indoor" or "outdoor"
    if indoor is True:
        params.append(("f1", json.dumps([{"field_id": "indoor", "operator": "is", "value": "indoor"}])))
    elif indoor is False:
        params.append(("f1", json.dumps([{"field_id": "indoor", "operator": "is", "value": "outdoor"}])))

    try:
        log.info("Fetching MMP %s (indoor=%s) %s → %s …", label, indoor, start, end)
        url  = f"{BASE_URL}/athlete/{ICU_ATHLETE_ID}/power-curves.json"
        resp = requests.get(url, auth=AUTH, params=params, timeout=30)
        resp.raise_for_status()
        data   = resp.json()
        curves = data.get("list", [])
        if curves:
            secs = curves[0].get("secs", [])
            vals = curves[0].get("values", [])
            log.info("  → %s: %d curve(s), %d durations, max %dW",
                     label, len(curves), len(secs), max(vals) if vals else 0)
        else:
            log.info("  → %s: 0 curves", label)
        return data
    except Exception as exc:
        log.warning("  MMP fetch failed for %s: %s", label, exc)
        # Log response body for debugging
        try:
            log.warning("  Response: %s", exc.response.text[:200] if hasattr(exc, 'response') else "no response")
        except Exception:
            pass
        return None


def fetch_activities(oldest: str, newest: str) -> list[dict]:
    log.info("Fetching activities %s → %s …", oldest, newest)
    data = icu_get(
        f"athlete/{ICU_ATHLETE_ID}/activities",
        params={
            "oldest" : oldest,
            "newest" : newest,
            "fields" : "id,start_date_local,type,name,moving_time,distance,"
                       "total_elevation_gain,average_heartrate,max_heartrate,"
                       "calories,trimp,icu_training_load,icu_atl,icu_ctl,"
                       "trainer,average_watts,icu_weighted_avg_watts,"
                       "icu_intensity,average_cadence",
        }
    )
    log.info("  → %d activities", len(data))
    return data


def main():
    global AUTH
    if not ICU_API_KEY or not ICU_ATHLETE_ID:
        log.error("ICU_API_KEY and ICU_ATHLETE_ID must be set in .env")
        return

    AUTH = HTTPBasicAuth("API_KEY", ICU_API_KEY)
    log.info("Fetching data from intervals.icu for athlete %s …", ICU_ATHLETE_ID)

    today      = datetime.now().strftime("%Y-%m-%d")
    start_2023 = "2023-01-01"
    cutoff_42d = (datetime.now() - timedelta(days=42)).strftime("%Y-%m-%d")

    output = {
        "generated"    : datetime.now().strftime("%Y-%m-%d %H:%M"),
        "athlete_id"   : ICU_ATHLETE_ID,
    }

    # ── 1. Wellness / PMC ─────────────────────────────────────────────────────
    output["wellness"] = fetch_wellness(start_2023, today)
    time.sleep(DELAY)

    # ── 2. Power/HR curves (binned, for power vs HR chart) ────────────────────
    power_hr = {}
    year_ranges = [("2023","2023-01-01","2023-12-31"),
                   ("2024","2024-01-01","2024-12-31"),
                   ("2025","2025-01-01","2025-12-31"),
                   ("2026","2026-01-01",today),
                   ("recent",cutoff_42d,today)]
    for label, start, end in year_ranges:
        power_hr[label] = fetch_power_hr_curve(start, end, label)
        time.sleep(DELAY)
    output["power_hr_curves"] = power_hr

    # ── 3. MMP curves — per year, indoor + outdoor separately ────────────────
    mmp_curves = {}
    for year, start, end in [("2023","2023-01-01","2023-12-31"),
                               ("2024","2024-01-01","2024-12-31"),
                               ("2025","2025-01-01","2025-12-31"),
                               ("2026","2026-01-01",today)]:
        for context, indoor_flag in [("indoor", True), ("outdoor", False)]:
            key = f"{context}_{year}"
            result = fetch_mmp_curves(start, end, key, indoor=indoor_flag)
            if result:
                mmp_curves[key] = result
            time.sleep(DELAY)

    # Recent 42d — both contexts
    for context, indoor_flag in [("indoor_recent", True), ("outdoor_recent", False)]:
        result = fetch_mmp_curves(cutoff_42d, today, context, indoor=indoor_flag)
        if result:
            mmp_curves[context] = result
        time.sleep(DELAY)

    output["mmp_curves"] = mmp_curves

    # ── 4. Activities ─────────────────────────────────────────────────────────
    output["activities"] = fetch_activities(start_2023, today)
    time.sleep(DELAY)

    # ── Save ──────────────────────────────────────────────────────────────────
    OUTPUT_FILE.write_text(json.dumps(output, indent=2))
    size_kb = OUTPUT_FILE.stat().st_size // 1024
    log.info("Written → %s  (%d KB)", OUTPUT_FILE, size_kb)

    wellness = output["wellness"]
    latest   = wellness[-1] if wellness else {}
    ctl = latest.get("ctl", 0) or 0
    atl = latest.get("atl", 0) or 0
    print(f"\n{'─'*55}")
    print(f"  Wellness records : {len(wellness)}")
    print(f"  Latest CTL       : {ctl:.1f}  ATL: {atl:.1f}  TSB: {ctl-atl:.1f}")
    print(f"  Activities       : {len(output['activities'])}")
    print(f"  Power/HR curves  : {list(power_hr.keys())}")
    print(f"  MMP curves       : {list(mmp_curves.keys())}")
    print(f"{'─'*55}")


if __name__ == "__main__":
    main()
