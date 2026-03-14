"""
build_running_cloud.py
──────────────────────
Extracts 1-minute windowed pace + HR data from outdoor running .fit files
and builds an incremental binned dataset (running_cloud.json).

Series produced:
  year_YYYY   — one per calendar year present in data
  recent      — last 42 days

Each series contains:
  - Binned avg HR per pace bucket (5 sec/km wide, 3:00–7:00/km)
  - Linear fit with HRR marker and VT1/threshold extrapolations
  - Raw scatter points for recent window

Run after garmin_download.py:
    python build_running_cloud.py

To rebuild from scratch:
    rm running_cloud.json && python build_running_cloud.py
"""

from __future__ import annotations
import io
import json
import math
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from athlete_config import (
    HR_REST, HR_MAX, HRR, THRESHOLD_HR, VT1_HR,
    HRR_MARKERS_PCT, hrr_to_bpm,
)

# ── Config ────────────────────────────────────────────────────────────────────
FIT_DIR       = Path("fit_files")
CLOUD_FILE    = Path("running_cloud.json")
WINDOW_SECS   = 60
PACE_MIN_SEC  = 180    # 3:00 /km
PACE_MAX_SEC  = 420    # 7:00 /km
BUCKET_SEC    = 5      # 5 sec/km per bucket
HR_MIN        = 60
RECENT_DAYS   = 42
MIN_POINTS    = 5


# ── Helpers ───────────────────────────────────────────────────────────────────

def pace_label(sec_per_km: float) -> str:
    m, s = divmod(int(sec_per_km), 60)
    return f"{m}:{s:02d}"


from fit_window_extractor import extract_clean_windows


def bucket_index(pace_sec: float) -> int | None:
    if not (PACE_MIN_SEC <= pace_sec <= PACE_MAX_SEC):
        return None
    return int((pace_sec - PACE_MIN_SEC) // BUCKET_SEC)


def bucket_centre(idx: int) -> float:
    return PACE_MIN_SEC + idx * BUCKET_SEC + BUCKET_SEC / 2


def compute_bucket_stats(raw_buckets: dict) -> dict:
    stats = {}
    for key, b in raw_buckets.items():
        count = b["count"]
        if count < MIN_POINTS:
            continue
        idx     = int(key)
        centre  = bucket_centre(idx)
        avg_hr  = b["sum_hr"] / count
        variance = max(0, b["sum_sq_hr"] / count - avg_hr ** 2)
        std_hr  = math.sqrt(variance)
        stats[key] = {
            "pace_sec"  : round(centre, 1),
            "pace_label": pace_label(centre),
            "avg_hr"    : round(avg_hr, 2),
            "std_hr"    : round(std_hr, 2),
            "count"     : count,
        }
    return stats


# HR range for regression fitting
FIT_HR_MIN_PRIMARY  = 120.0   # bpm — primary fit lower bound
FIT_HR_MAX_PRIMARY  = 150.0   # bpm — primary fit upper bound
FIT_MIN_BUCKETS     = 4       # minimum buckets needed to fit
FIT_MIN_R2          = 0.70    # minimum R² to accept fit


def _do_fit(filtered: list) -> dict | None:
    """Run linear regression on a filtered bucket list. Returns fit dict or None."""
    if len(filtered) < FIT_MIN_BUCKETS:
        return None
    paces = np.array([b["pace_sec"] for b in filtered])
    hrs   = np.array([b["avg_hr"]   for b in filtered])
    coeffs = np.polyfit(paces, hrs, 1)
    y_pred = np.polyval(coeffs, paces)
    ss_res = np.sum((hrs - y_pred) ** 2)
    ss_tot = np.sum((hrs - np.mean(hrs)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    if r2 < FIT_MIN_R2:
        return None
    return {"coeffs": coeffs, "r2": r2, "hrs": hrs, "paces": paces}


def fit_linear(bucket_stats: dict) -> dict | None:
    """
    Two-tier linear regression:
      1. Primary: fit over HR 120–150 bpm (clean aerobic linear region)
      2. Fallback: fit over all available buckets if primary has < 4 points
    Reports actual fit HR range so caller knows which tier was used.
    Extrapolates to VT1, threshold, and all HRR% markers.
    """
    all_buckets = sorted(bucket_stats.values(),
                         key=lambda b: b["pace_sec"], reverse=True)

    # Tier 1: primary HR range
    primary = [b for b in all_buckets
               if FIT_HR_MIN_PRIMARY <= b["avg_hr"] <= FIT_HR_MAX_PRIMARY]
    fit_result = _do_fit(primary)
    fit_tier   = "primary"
    fit_hr_min = FIT_HR_MIN_PRIMARY
    fit_hr_max = FIT_HR_MAX_PRIMARY

    # Tier 2: fallback — use all buckets
    if fit_result is None:
        fit_result = _do_fit(all_buckets)
        fit_tier   = "fallback"
        fit_hr_min = float(min(b["avg_hr"] for b in all_buckets)) if all_buckets else 0
        fit_hr_max = float(max(b["avg_hr"] for b in all_buckets)) if all_buckets else 0

    if fit_result is None:
        return None

    coeffs   = fit_result["coeffs"]
    best_r2  = fit_result["r2"]
    slope, intercept = coeffs
    plateau_pace = all_buckets[0]["pace_sec"] if all_buckets else 400.0

    def hr_to_pace(hr_target: float) -> dict:
        pace_sec = (hr_target - intercept) / slope
        return {"pace_sec": round(float(pace_sec), 1),
                "pace_label": pace_label(pace_sec),
                "hr": round(hr_target, 1)}

    # HRR markers — all percentages
    markers = {}
    for pct in HRR_MARKERS_PCT:
        bpm = hrr_to_bpm(pct)
        markers[str(pct)] = hr_to_pace(bpm)
    markers["vt1"]       = hr_to_pace(VT1_HR)
    markers["threshold"] = hr_to_pace(THRESHOLD_HR)

    # Regression line drawn across actual fit HR range
    hr_line  = np.linspace(fit_hr_min, fit_hr_max, 50)
    x_line   = (hr_line - intercept) / slope
    y_line   = hr_line

    return {
        "slope"           : round(float(slope), 4),
        "intercept"       : round(float(intercept), 2),
        "r2"              : round(float(best_r2), 4),
        "plateau_pace_sec": round(float(plateau_pace), 1),
        "fit_hr_range"    : [round(fit_hr_min, 1), round(fit_hr_max, 1)],
        "fit_tier"        : fit_tier,
        "markers"         : markers,
        "regression_line" : [
            {"pace_sec": round(float(x), 1), "hr": round(float(y), 2)}
            for x, y in zip(x_line, y_line)
            if PACE_MIN_SEC <= x <= PACE_MAX_SEC
        ],
    }


def load_cloud() -> dict:
    if CLOUD_FILE.exists():
        return json.loads(CLOUD_FILE.read_text())
    return {
        "_raw_series"     : {},
        "processed_files" : [],
        "last_updated"    : "",
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def build_running_cloud() -> None:
    cloud     = load_cloud()
    processed = set(cloud["processed_files"])
    cutoff_recent = datetime.now() - timedelta(days=RECENT_DAYS)
    cutoff_str    = cutoff_recent.strftime("%Y-%m-%d")

    new_files = [
        f for f in sorted(FIT_DIR.glob("*.fit"))
        if "_running_" in f.name and f.name not in processed
    ]
    print(f"Found {len(new_files)} new running .fit files to process.")

    raw_series = cloud.get("_raw_series", {})

    for fit_path in new_files:
        name     = fit_path.name
        date_str = name[:10]
        year     = name[:4]
        sk_year  = f"year_{year}"

        try:
            windows = extract_clean_windows(
                fit_path,
                is_indoor   = False,   # running is always outdoor
                effort_field= "pace",
                effort_min  = PACE_MIN_SEC,
                effort_max  = PACE_MAX_SEC,
            )
        except Exception as exc:
            print(f"  ✗ {name}: {exc}")
            processed.add(name)
            continue

        if not windows:
            print(f"  – {name}: no valid windows")
            processed.add(name)
            continue

        print(f"  ✓ {name}: {len(windows)} windows  [{sk_year}]")

        is_recent = date_str >= cutoff_str

        for sk in [sk_year, "recent"] if is_recent else [sk_year]:
            if sk not in raw_series:
                raw_series[sk] = {"raw_buckets": {}, "recent_points": []}

        for pace, hr in windows:
            idx = bucket_index(pace)
            if idx is None:
                continue
            key = str(idx)

            for sk in [sk_year] + (["recent"] if is_recent else []):
                if sk not in raw_series:
                    raw_series[sk] = {"raw_buckets": {}, "recent_points": []}
                b = raw_series[sk]["raw_buckets"].setdefault(
                    key, {"sum_hr": 0.0, "sum_sq_hr": 0.0, "count": 0})
                b["sum_hr"]    += hr
                b["sum_sq_hr"] += hr * hr
                b["count"]     += 1

        # Raw scatter for recent
        if is_recent:
            pts = raw_series.setdefault("recent", {
                "raw_buckets": {}, "recent_points": []})["recent_points"]
            for pace, hr in windows[:50]:
                pts.append({"date": date_str, "pace_sec": pace, "hr": hr})

        processed.add(name)

    # Prune recent points
    if "recent" in raw_series:
        raw_series["recent"]["recent_points"] = [
            p for p in raw_series["recent"].get("recent_points", [])
            if p.get("date", "") >= cutoff_str
        ]

    # Compute derived stats and fits
    output_series = {}
    for sk, s in raw_series.items():
        stats = compute_bucket_stats(s["raw_buckets"])
        fit   = fit_linear(stats)
        output_series[sk] = {
            "bucket_stats"  : stats,
            "linear_fit"    : fit,
            "recent_points" : s.get("recent_points", []),
            "n_windows"     : sum(b["count"] for b in stats.values()),
        }

    # HRR reference lines
    hrr_markers = {str(pct): round(hrr_to_bpm(pct), 1) for pct in HRR_MARKERS_PCT}
    hrr_markers["vt1"]       = round(float(VT1_HR), 1)
    hrr_markers["threshold"] = round(float(THRESHOLD_HR), 1)

    cloud["series"]          = output_series
    cloud["_raw_series"]     = raw_series
    cloud["processed_files"] = sorted(processed)
    cloud["hrr_markers"]     = hrr_markers
    cloud["last_updated"]    = datetime.now().strftime("%Y-%m-%d %H:%M")

    CLOUD_FILE.write_text(json.dumps(cloud, indent=2))
    size_kb = CLOUD_FILE.stat().st_size // 1024
    print(f"\nWritten → {CLOUD_FILE}  ({size_kb} KB)")
    print(f"  Series: {sorted(output_series.keys())}")
    for sk, s in output_series.items():
        fit = s.get("linear_fit")
        r2  = f"R²={fit['r2']:.3f}" if fit else "no fit"
        print(f"  {sk:<12} {s['n_windows']:>6} windows  {r2}")


if __name__ == "__main__":
    build_running_cloud()
