"""
AC-112: Manual Refresh Check Runner (No Execution)

Orchestrates AC-103 → AC-105 → AC-109 in sequence and prints a compact
one-screen summary. No data refresh, no broker calls, no new logic.

Pipeline:
  1. build_marketdata_scenario_adapter_lite  (AC-103) — classify markets
  2. build_source_health_review_lite         (AC-105) — assess source health
  3. build_source_freshness_recovery_plan_lite (AC-109) — build recovery plan
  4. print compact summary

All output files go to the same directory as the individual builders by
default; override via keyword args for testing.

No execution. No API calls. Paper-only. Non-binding. Simulation-only.
live_activation_allowed=False always.

Usage:
    python ant_colony/run_manual_refresh_check_lite.py
"""
from __future__ import annotations

import datetime
import importlib.util
from pathlib import Path

# ---------------------------------------------------------------------------
# Default paths (same as underlying builders)
# ---------------------------------------------------------------------------

_OUT = Path(r"C:\Trading\ANT_OUT")

DEFAULT_MD_PATH       = _OUT / "worker_market_data.json"
DEFAULT_INTENTS_PATH  = _OUT / "rebalance_intents.json"
DEFAULT_ADAPTER_PATH  = _OUT / "marketdata_scenario_adapter.json"
DEFAULT_HEALTH_PATH   = _OUT / "source_health_review.json"
DEFAULT_RECOVERY_PATH = _OUT / "source_freshness_recovery_plan.json"

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

def _load(mod_name: str, filename: str):
    path = Path(__file__).parent / filename
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_adapter_mod  = _load("_adapter_mod",  "build_marketdata_scenario_adapter_lite.py")
_health_mod   = _load("_health_mod",   "build_source_health_review_lite.py")
_recovery_mod = _load("_recovery_mod", "build_source_freshness_recovery_plan_lite.py")

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(
    md_path:       Path = DEFAULT_MD_PATH,
    intents_path:  Path = DEFAULT_INTENTS_PATH,
    adapter_path:  Path = DEFAULT_ADAPTER_PATH,
    health_path:   Path = DEFAULT_HEALTH_PATH,
    recovery_path: Path = DEFAULT_RECOVERY_PATH,
    now_utc:       datetime.datetime | None = None,
) -> dict:
    """
    Run AC-103 → AC-105 → AC-109 in sequence.
    Returns summary dict; writes outputs via each builder's own write function.
    Fail-closed: any builder failure is caught, pipeline continues with None result.
    """
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    # ── Step 1: adapter (AC-103) ─────────────────────────────────────────────
    try:
        adapter_result = _adapter_mod.run_adapter(
            md_path      = md_path,
            intents_path = intents_path,
            output_path  = adapter_path,
            now_utc      = now_utc,
        )
    except Exception:
        adapter_result = None

    # ── Step 2: source health review (AC-105) ────────────────────────────────
    try:
        health_result = _health_mod.run_health_review(
            adapter_path = adapter_path,
            md_path      = md_path,
            output_path  = health_path,
            now_utc      = now_utc,
        )
    except Exception:
        health_result = None

    # ── Step 3: recovery plan (AC-109) ───────────────────────────────────────
    try:
        recovery_result = _recovery_mod.run_recovery_plan(
            health_path  = health_path,
            adapter_path = adapter_path,
            output_path  = recovery_path,
            now_utc      = now_utc,
        )
    except Exception:
        recovery_result = None

    return {
        "adapter":  adapter_result,
        "health":   health_result,
        "recovery": recovery_result,
    }


# ---------------------------------------------------------------------------
# CLI output
# ---------------------------------------------------------------------------

def _print_summary(result: dict) -> None:
    print("=== ANT MANUAL REFRESH CHECK ===")

    # Adapter
    ar = result.get("adapter")
    if ar and isinstance(ar, dict):
        n = len(ar.get("markets") or [])
        print(f"adapter  : {n} markets classified")
    else:
        print("adapter  : NO DATA")

    # Health
    hr = result.get("health")
    if hr and isinstance(hr, dict):
        sh_status = hr.get("source_health_status", "?")
        sh_fresh  = hr.get("markets_fresh",   0)
        sh_stale  = hr.get("markets_stale",   0)
        sh_miss   = hr.get("markets_missing", 0)
        print(f"health   : {sh_status} | fresh={sh_fresh} stale={sh_stale} missing={sh_miss}")
    else:
        print("health   : NO DATA")

    # Recovery
    rr = result.get("recovery")
    if rr and isinstance(rr, dict):
        rp_status = rr.get("recovery_status", "?")
        rp_sm     = rr.get("summary", {})
        rp_req    = rp_sm.get("markets_requiring_recovery", 0) if isinstance(rp_sm, dict) else 0
        rp_po     = rr.get("priority_order") or []
        top_mkts  = ", ".join(e["market"] for e in rp_po[:3]) if rp_po else "—"
        print(f"recovery : {rp_status} | requiring={rp_req}")
        print(f"top      : {top_mkts}")
    else:
        print("recovery : NO DATA")


def main() -> None:
    result = run()
    _print_summary(result)


if __name__ == "__main__":
    main()
