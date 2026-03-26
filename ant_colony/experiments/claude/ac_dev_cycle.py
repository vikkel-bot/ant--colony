"""
AC_DEV - Cycle Runner
=====================
Voert Phase 2 en Phase 3 in volgorde uit.
Schrijft een gecombineerde summary naar ANT_OUT.

Gebruik:
    python ac_dev_cycle.py
    python ac_dev_cycle.py --phase 2
    python ac_dev_cycle.py --phase 3
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import traceback
from datetime import datetime, timezone

ROOT    = r"C:\Trading\AC_DEV"
ANT_OUT = os.path.join(ROOT, "ANT_OUT")
SCRIPTS = os.path.dirname(os.path.abspath(__file__))

PHASES = [
    ("phase2_colony_risk",  "queen_colony_risk_runner.py",   "Colony Risk Layer"),
    ("phase3_backtest",     "research_backtest_runner.py",   "Research Backtest"),
]

def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def run_script(script_name: str) -> tuple[bool, str]:
    path = os.path.join(SCRIPTS, script_name)
    if not os.path.exists(path):
        return False, f"SCRIPT_NOT_FOUND: {path}"
    try:
        spec   = importlib.util.spec_from_file_location("_mod", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        rc = module.main()
        return (rc == 0), f"returncode={rc}"
    except SystemExit as e:
        return (int(str(e)) == 0), f"exit={e}"
    except Exception:
        return False, traceback.format_exc()[-500:]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", type=int, choices=[2, 3], default=None,
                        help="Draai alleen deze phase (2 of 3). Default: beide.")
    args = parser.parse_args()

    ts = utc_now()
    os.makedirs(ANT_OUT, exist_ok=True)

    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║         AC_DEV — Ant Colony Development Cycle       ║")
    print("╠══════════════════════════════════════════════════════╣")
    print(f"║  {ts}                              ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    steps = []
    overall_ok = True

    for key, script, label in PHASES:
        phase_num = 2 if "risk" in key else 3
        if args.phase is not None and args.phase != phase_num:
            continue

        print(f"▶ Phase {phase_num}: {label}")
        print("─" * 55)
        ok, msg = run_script(script)
        print("─" * 55)
        status = "OK" if ok else "FAIL"
        icon   = "✓" if ok else "✗"
        print(f"{icon} Phase {phase_num} {status}  ({msg[:80]})")
        print()

        steps.append({"phase": phase_num, "label": label,
                      "script": script, "ok": ok, "msg": msg[:200]})
        if not ok:
            overall_ok = False
            break

    summary = {
        "version":    "ac_dev_cycle_v1",
        "ts_utc":     ts,
        "overall_ok": overall_ok,
        "phases_run": len(steps),
        "steps":      steps,
    }

    summary_path = os.path.join(ANT_OUT, "ac_dev_cycle_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("═" * 55)
    print(f"  Summary: {'ALL OK' if overall_ok else 'FAILED'}")
    print(f"  Written: {summary_path}")
    print()

    sys.exit(0 if overall_ok else 1)

if __name__ == "__main__":
    main()
