"""
AC-114: Semi-Automatic Refresh Trigger (No Execution)

Reads existing local outputs and produces a compact trigger object that
indicates whether and how urgently the operator should run a manual
refresh check. No execution, no broker calls, no scheduler.

Sources:
  PRIMARY : C:\\Trading\\ANT_OUT\\source_health_review.json         (AC-105)
  PRIMARY : C:\\Trading\\ANT_OUT\\source_freshness_recovery_plan.json (AC-109)
  OPTIONAL: C:\\Trading\\ANT_OUT\\combined_review_snapshot.json     (AC-107)

Output:
  C:\\Trading\\ANT_OUT\\refresh_trigger.json

Trigger status:
  NONE   — health HEALTHY + recovery NONE
  WATCH  — health DEGRADED or recovery PLAN_READY
  DUE    — health DEGRADED + recovery PLAN_READY (both present)
  URGENT — health CRITICAL or recovery URGENT

Operator guidance:
  NONE    / NONE   — no action needed
  MONITOR / LATER  — worth watching, re-check when convenient
  MONITOR / SOON   — DUE: re-check soon
  RUN_MANUAL_REFRESH_CHECK_NOW / NOW — URGENT: run immediately

No execution. No API calls. Paper-only. Non-binding. Simulation-only.
live_activation_allowed=False always.

Usage:
    python ant_colony/build_refresh_trigger_lite.py
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

HEALTH_PATH   = Path(r"C:\Trading\ANT_OUT\source_health_review.json")
RECOVERY_PATH = Path(r"C:\Trading\ANT_OUT\source_freshness_recovery_plan.json")
SNAPSHOT_PATH = Path(r"C:\Trading\ANT_OUT\combined_review_snapshot.json")
OUTPUT_PATH   = Path(r"C:\Trading\ANT_OUT\refresh_trigger.json")

VERSION   = "refresh_trigger_v1"
COMPONENT = "build_refresh_trigger_lite"

FLAGS = {
    "non_binding":             True,
    "simulation_only":         True,
    "paper_only":              True,
    "live_activation_allowed": False,
}

# ---------------------------------------------------------------------------
# Reason codes
# ---------------------------------------------------------------------------

_REASON_HEALTHY        = "SOURCE_HEALTHY_RECOVERY_NONE"
_REASON_DEGRADED       = "SOURCE_DEGRADED"
_REASON_PLAN_READY     = "RECOVERY_PLAN_READY"
_REASON_DEGRADED_PLAN  = "SOURCE_DEGRADED_AND_RECOVERY_PLAN_READY"
_REASON_CRITICAL       = "SOURCE_CRITICAL"
_REASON_URGENT         = "RECOVERY_URGENT"
_REASON_NO_DATA        = "NO_SOURCE_DATA"


# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------

def build_trigger(
    health_data:   dict | None,
    recovery_data: dict | None,
    now_utc:       datetime.datetime,
) -> dict:
    """
    Build refresh trigger from loaded source dicts.
    Pure function — no I/O, no side effects.

    Args:
        health_data:   parsed source_health_review.json, or None
        recovery_data: parsed source_freshness_recovery_plan.json, or None
        now_utc:       current UTC datetime

    Returns:
        refresh_trigger dict
    """
    sh_status = None
    rp_status = None
    markets_requiring = 0

    if health_data and isinstance(health_data, dict):
        sh_status = health_data.get("source_health_status")

    if recovery_data and isinstance(recovery_data, dict):
        rp_status = recovery_data.get("recovery_status")
        sm = recovery_data.get("summary", {})
        if isinstance(sm, dict):
            markets_requiring = sm.get("markets_requiring_recovery", 0) or 0

    # ── Trigger logic ──────────────────────────────────────────────────────────
    # Priority: URGENT > DUE > WATCH > NONE
    # URGENT: CRITICAL health or URGENT recovery
    # DUE:    DEGRADED health AND PLAN_READY recovery (both signals present)
    # WATCH:  DEGRADED health OR PLAN_READY recovery (one signal)
    # NONE:   HEALTHY health AND NONE recovery (or no data → fail-closed WATCH)

    if sh_status == "CRITICAL" or rp_status == "URGENT":
        trigger_status        = "URGENT"
        refresh_required      = True
        reason_code           = _REASON_CRITICAL if sh_status == "CRITICAL" else _REASON_URGENT
        recommended_action    = "RUN_MANUAL_REFRESH_CHECK_NOW"
        recommended_window    = "NOW"

    elif sh_status == "DEGRADED" and rp_status == "PLAN_READY":
        trigger_status        = "DUE"
        refresh_required      = True
        reason_code           = _REASON_DEGRADED_PLAN
        recommended_action    = "MONITOR"
        recommended_window    = "SOON"

    elif sh_status == "DEGRADED" or rp_status == "PLAN_READY":
        trigger_status        = "WATCH"
        refresh_required      = True
        reason_code           = _REASON_DEGRADED if sh_status == "DEGRADED" else _REASON_PLAN_READY
        recommended_action    = "MONITOR"
        recommended_window    = "LATER"

    elif sh_status == "HEALTHY" and rp_status == "NONE":
        trigger_status        = "NONE"
        refresh_required      = False
        reason_code           = _REASON_HEALTHY
        recommended_action    = "NONE"
        recommended_window    = "NONE"

    else:
        # No data or unrecognised statuses → fail-closed: WATCH
        trigger_status        = "WATCH"
        refresh_required      = True
        reason_code           = _REASON_NO_DATA
        recommended_action    = "MONITOR"
        recommended_window    = "LATER"

    return {
        "version":   VERSION,
        "component": COMPONENT,
        "ts_utc":    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),

        "refresh_check_required": refresh_required,
        "trigger_status":         trigger_status,
        "trigger_reason_code":    reason_code,

        "summary": {
            "source_health_status":       sh_status or "UNKNOWN",
            "recovery_status":            rp_status or "UNKNOWN",
            "markets_requiring_recovery": markets_requiring,
        },

        "operator_guidance": {
            "recommended_action": recommended_action,
            "recommended_window": recommended_window,
        },

        "flags": dict(FLAGS),
    }


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load(path: Path) -> dict | None:
    """Load JSON from path. Returns None on any error (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def run_trigger(
    health_path:   Path = HEALTH_PATH,
    recovery_path: Path = RECOVERY_PATH,
    output_path:   Path = OUTPUT_PATH,
    now_utc:       datetime.datetime | None = None,
) -> dict:
    """
    Load sources, build trigger, write output.
    Returns the trigger dict.
    """
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    health_data   = _load(health_path)
    recovery_data = _load(recovery_path)

    result = build_trigger(health_data, recovery_data, now_utc)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_trigger(result: dict) -> None:
    ts  = result.get("ts_utc", "?")
    st  = result.get("trigger_status", "?")
    req = result.get("refresh_check_required", "?")
    rc  = result.get("trigger_reason_code", "?")
    sm  = result.get("summary", {})
    og  = result.get("operator_guidance", {})

    print("=== ANT REFRESH TRIGGER ===")
    print(f"ts           : {ts}")
    print(f"status       : {st}  (required={req})")
    print(f"reason       : {rc}")
    print(f"source_health: {sm.get('source_health_status', '?')}")
    print(f"recovery     : {sm.get('recovery_status', '?')}  "
          f"requiring={sm.get('markets_requiring_recovery', 0)}")
    print(f"action       : {og.get('recommended_action', '?')}  "
          f"window={og.get('recommended_window', '?')}")


def main() -> None:
    result = run_trigger()
    _print_trigger(result)


if __name__ == "__main__":
    main()
