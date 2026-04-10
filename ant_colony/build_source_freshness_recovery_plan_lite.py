"""
AC-109: Source Freshness Recovery Plan (No Execution)

Reads existing local outputs and produces a compact, prioritised recovery
plan for stale/missing market data sources. No execution, no broker calls.

Sources:
  PRIMARY   : C:\\Trading\\ANT_OUT\\source_health_review.json    (AC-105)
  PRIMARY   : C:\\Trading\\ANT_OUT\\marketdata_scenario_adapter.json (AC-103)
  OPTIONAL  : C:\\Trading\\ANT_OUT\\worker_market_data.json

Output:
  C:\\Trading\\ANT_OUT\\source_freshness_recovery_plan.json

Recovery status:
  NONE        — no stale or missing markets
  PLAN_READY  — stale markets present, none missing
  URGENT      — any missing, or source health is CRITICAL

Priority order:
  1. RESTORE_MISSING (DATA_MISSING) — HIGH priority, alphabetical
  2. REFRESH_STALE   (DATA_STALE)   — MEDIUM priority, alphabetical

No execution. No API calls. Paper-only. Non-binding. Simulation-only.
live_activation_allowed=False always.

Usage:
    python ant_colony/build_source_freshness_recovery_plan_lite.py
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

HEALTH_PATH  = Path(r"C:\Trading\ANT_OUT\source_health_review.json")
ADAPTER_PATH = Path(r"C:\Trading\ANT_OUT\marketdata_scenario_adapter.json")
OUTPUT_PATH  = Path(r"C:\Trading\ANT_OUT\source_freshness_recovery_plan.json")

VERSION   = "source_freshness_recovery_plan_v1"
COMPONENT = "build_source_freshness_recovery_plan_lite"

FLAGS = {
    "non_binding":             True,
    "simulation_only":         True,
    "paper_only":              True,
    "live_activation_allowed": False,
}

_STALE_CLASSES   = {"DATA_STALE"}
_MISSING_CLASSES = {"DATA_MISSING"}


# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------

def build_recovery_plan(
    health_data:  dict | None,
    adapter_data: dict | None,
    now_utc:      datetime.datetime,
) -> dict:
    """
    Build source freshness recovery plan from loaded source dicts.
    Pure function — no I/O, no side effects.

    Args:
        health_data:  parsed source_health_review.json, or None
        adapter_data: parsed marketdata_scenario_adapter.json, or None
        now_utc:      current UTC datetime

    Returns:
        source_freshness_recovery_plan dict
    """
    # ── Collect per-market classification from adapter ────────────────────────
    stale_markets:   list[str] = []
    missing_markets: list[str] = []

    if adapter_data and isinstance(adapter_data, dict):
        for m in adapter_data.get("markets") or []:
            if not isinstance(m, dict):
                continue
            name       = str(m.get("market") or "UNKNOWN")
            seed_class = str(m.get("review_seed_class") or "")
            if seed_class in _MISSING_CLASSES:
                missing_markets.append(name)
            elif seed_class in _STALE_CLASSES:
                stale_markets.append(name)

    # ── Cross-check with source health (optional reinforcement) ──────────────
    # If health_data says CRITICAL but adapter found nothing → trust health
    sh_status = ""
    if health_data and isinstance(health_data, dict):
        sh_status = str(health_data.get("source_health_status") or "")

    # ── Recovery status ───────────────────────────────────────────────────────
    n_miss  = len(missing_markets)
    n_stale = len(stale_markets)

    if n_miss > 0 or sh_status == "CRITICAL":
        recovery_status      = "URGENT"
        recovery_reason_code = ("SOURCES_MISSING"    if n_miss > 0
                                else "SOURCE_HEALTH_CRITICAL")
    elif n_stale > 0:
        recovery_status      = "PLAN_READY"
        recovery_reason_code = "SOURCES_STALE"
    else:
        recovery_status      = "NONE"
        recovery_reason_code = "ALL_SOURCES_FRESH"

    # ── Priority order: RESTORE_MISSING first, then REFRESH_STALE ─────────────
    priority_order: list[dict] = []

    for market in sorted(missing_markets):
        priority_order.append({
            "market":         market,
            "recovery_class": "RESTORE_MISSING",
            "priority":       "HIGH",
            "reason_code":    "DATA_MISSING",
        })

    for market in sorted(stale_markets):
        priority_order.append({
            "market":         market,
            "recovery_class": "REFRESH_STALE",
            "priority":       "MEDIUM",
            "reason_code":    "DATA_STALE",
        })

    markets_total              = n_miss + n_stale
    markets_requiring_recovery = markets_total

    return {
        "version":               VERSION,
        "component":             COMPONENT,
        "ts_utc":                now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "recovery_status":       recovery_status,
        "recovery_reason_code":  recovery_reason_code,
        "summary": {
            "markets_total":              markets_total,
            "markets_requiring_recovery": markets_requiring_recovery,
            "markets_stale":              n_stale,
            "markets_missing":            n_miss,
        },
        "priority_order": priority_order,
        "sources": {
            "health_loaded":   health_data   is not None,
            "adapter_loaded":  adapter_data  is not None,
        },
        "flags": dict(FLAGS),
    }


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict | None:
    """Load JSON. Returns None if absent or corrupt (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_recovery_plan(result: dict, path: Path = OUTPUT_PATH) -> None:
    """Write recovery plan to JSON file, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")


def run_recovery_plan(
    health_path:  Path = HEALTH_PATH,
    adapter_path: Path = ADAPTER_PATH,
    output_path:  Path = OUTPUT_PATH,
    now_utc:      datetime.datetime | None = None,
) -> dict:
    """Load sources → build plan → write output → return result."""
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    health_data  = _load_json(health_path)
    adapter_data = _load_json(adapter_path)

    result = build_recovery_plan(health_data, adapter_data, now_utc)
    write_recovery_plan(result, output_path)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    result = run_recovery_plan()
    sm = result["summary"]
    print(f"Recovery status   : {result['recovery_status']}")
    print(f"Reason code       : {result['recovery_reason_code']}")
    print(f"Markets requiring : {sm['markets_requiring_recovery']} "
          f"(stale={sm['markets_stale']} missing={sm['markets_missing']})")
    print()
    if result["priority_order"]:
        print("Priority order:")
        for item in result["priority_order"]:
            print(f"  [{item['priority']:<6}]  {item['recovery_class']:<16}  {item['market']}")
    else:
        print("Priority order: (empty — no recovery needed)")
    print(f"\nOutput            : {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
