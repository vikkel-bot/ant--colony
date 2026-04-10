"""
AC-120: Execution Readiness Gate (Pre-Execution, No Execution)

Reads system readiness score, refresh trigger, and source health review.
Produces a gate JSON that tells the operator whether the system is
structurally cleared for evaluation — NOT for live trading.

execution_allowed is ALWAYS False. This module never enables execution.

Sources (read-only):
  C:\\Trading\\ANT_OUT\\system_readiness_score.json      (AC-117)
  C:\\Trading\\ANT_OUT\\refresh_trigger.json             (AC-114)
  C:\\Trading\\ANT_OUT\\source_health_review.json        (AC-105)

Output:
  C:\\Trading\\ANT_OUT\\execution_readiness_gate.json

Gate logic:
  BLOCKED  : SOURCE_HEALTH_CRITICAL or TRIGGER_URGENT or readiness NOT_READY
  LIMITED  : readiness LIMITED or trigger DUE or trigger WATCH
  OPEN     : readiness READY + trigger NONE + health HEALTHY/DEGRADED

  Missing / corrupt input → BLOCKED (fail-closed)

Reason code priority (first match wins):
  SOURCE_HEALTH_CRITICAL  : source_health_status == CRITICAL
  TRIGGER_URGENT          : trigger_status == URGENT
  NOT_READY               : readiness_status == NOT_READY
  LIMITED_STATE           : gate is LIMITED
  READY_BUT_LOCKED        : gate is OPEN

execution_allowed is ALWAYS False.

No execution. No API calls. Read-only sources. UTF-8 output only.

Usage:
    python ant_colony/build_execution_readiness_gate_lite.py
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

READINESS_PATH = Path(r"C:\Trading\ANT_OUT\system_readiness_score.json")
TRIGGER_PATH   = Path(r"C:\Trading\ANT_OUT\refresh_trigger.json")
HEALTH_PATH    = Path(r"C:\Trading\ANT_OUT\source_health_review.json")
OUTPUT_PATH    = Path(r"C:\Trading\ANT_OUT\execution_readiness_gate.json")

COMPONENT = "build_execution_readiness_gate_lite"

# Fail-closed fallbacks for missing / unrecognised values
_RD_FALLBACK = "NOT_READY"
_TR_FALLBACK = "URGENT"
_SH_FALLBACK = "CRITICAL"

# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------

def build_execution_gate(
    readiness_data: dict | None,
    trigger_data:   dict | None,
    health_data:    dict | None,
    now_utc:        datetime.datetime,
) -> dict:
    """
    Build the execution readiness gate dict.
    Pure function — no I/O, no side effects.

    None inputs are treated as fail-closed worst-case values.
    execution_allowed is ALWAYS False.
    """
    ts_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Extract values (fail-closed on None / missing keys) ──────────────────
    if readiness_data and isinstance(readiness_data, dict):
        rd_status = readiness_data.get("readiness_status", _RD_FALLBACK)
        if rd_status not in ("NOT_READY", "LIMITED", "READY"):
            rd_status = _RD_FALLBACK
    else:
        rd_status = _RD_FALLBACK

    if trigger_data and isinstance(trigger_data, dict):
        tr_status = trigger_data.get("trigger_status", _TR_FALLBACK)
        if tr_status not in ("NONE", "WATCH", "DUE", "URGENT"):
            tr_status = _TR_FALLBACK
    else:
        tr_status = _TR_FALLBACK

    if health_data and isinstance(health_data, dict):
        sh_status = health_data.get("source_health_status", _SH_FALLBACK)
        if sh_status not in ("HEALTHY", "DEGRADED", "CRITICAL"):
            sh_status = _SH_FALLBACK
    else:
        sh_status = _SH_FALLBACK

    # ── Gate logic ────────────────────────────────────────────────────────────
    if sh_status == "CRITICAL" or tr_status == "URGENT" or rd_status == "NOT_READY":
        gate_status = "BLOCKED"
    elif rd_status == "LIMITED" or tr_status in ("DUE", "WATCH"):
        gate_status = "LIMITED"
    else:
        gate_status = "OPEN"

    # ── Reason code (first match wins) ────────────────────────────────────────
    if sh_status == "CRITICAL":
        reason_code = "SOURCE_HEALTH_CRITICAL"
    elif tr_status == "URGENT":
        reason_code = "TRIGGER_URGENT"
    elif rd_status == "NOT_READY":
        reason_code = "NOT_READY"
    elif gate_status == "LIMITED":
        reason_code = "LIMITED_STATE"
    else:
        reason_code = "READY_BUT_LOCKED"

    return {
        "version":           "execution_readiness_gate_v1",
        "ts_utc":            ts_str,
        "gate_status":       gate_status,
        "execution_allowed": False,
        "reason_code":       reason_code,
        "conditions": {
            "readiness_status":    rd_status,
            "trigger_status":      tr_status,
            "source_health_status": sh_status,
        },
        "flags": {
            "paper_only":        True,
            "execution_disabled": True,
        },
    }


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict | None:
    """Load JSON file. Returns dict or None (missing / corrupt)."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def run_execution_gate(
    readiness_path: Path = READINESS_PATH,
    trigger_path:   Path = TRIGGER_PATH,
    health_path:    Path = HEALTH_PATH,
    output_path:    Path = OUTPUT_PATH,
    now_utc:        datetime.datetime | None = None,
) -> dict:
    """Load sources, build gate dict, write to output_path. Returns dict."""
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    readiness = _load_json(readiness_path)
    trigger   = _load_json(trigger_path)
    health    = _load_json(health_path)

    result = build_execution_gate(readiness, trigger, health, now_utc)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    result = run_execution_gate()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
