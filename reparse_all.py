"""
reparse_all.py
──────────────
Re-parses all .fit files and updates companion .json files.
Use this when fit_parser.py gains new fields (e.g. calories).
Only updates files where the target field is missing.

Usage:
    python3 reparse_all.py
"""

import json
import zipfile
import io
from pathlib import Path
from fit_parser import parse_fit

FIT_DIR      = Path("fit_files")
CHECK_FIELDS = ["calories_kcal", "tss"]   # fields to check — reparse if any are missing


def needs_reparse(json_path: Path) -> bool:
    try:
        data = json.loads(json_path.read_text())
        return any(f not in data for f in CHECK_FIELDS)
    except Exception:
        return True


def get_activity_type(json_path: Path) -> str:
    try:
        return json.loads(json_path.read_text()).get("activity_type", "cycling")
    except Exception:
        return "cycling"


def main():
    fit_files = sorted(FIT_DIR.glob("*.fit"))
    to_reparse = []

    for fit_path in fit_files:
        json_path = fit_path.with_suffix(".json")
        if not json_path.exists():
            to_reparse.append(fit_path)
        elif needs_reparse(json_path):
            to_reparse.append(fit_path)

    print(f"Found {len(to_reparse)} files needing reparse (of {len(fit_files)} total).")

    ok, failed = 0, 0
    for i, fit_path in enumerate(to_reparse, 1):
        json_path    = fit_path.with_suffix(".json")
        activity_type = get_activity_type(json_path) if json_path.exists() else "cycling"

        try:
            metrics = parse_fit(fit_path, activity_type)
            json_path.write_text(json.dumps(metrics, indent=2))
            ok += 1
            if i % 50 == 0:
                print(f"  {i}/{len(to_reparse)} done …")
        except Exception as exc:
            print(f"  ✗ {fit_path.name}: {exc}")
            failed += 1

    print(f"\nDone. {ok} updated, {failed} failed.")


if __name__ == "__main__":
    main()
