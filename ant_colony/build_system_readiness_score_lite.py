"""
AC-117: System Readiness Score (No Execution)

Reads existing local outputs and produces a single readiness score that
indicates whether the system is "ready to be reviewed". Purely
observational — no execution, no decision logic, no broker calls.

Sources (read-only):
  C:\\Trading\\ANT_OUT\\combined_review_snapshot.json      (AC-107)
  C:\\Trading\\ANT_OUT\\source_health_review.json          (AC-105)
  C:\\Trading\\ANT_OUT\\source_freshness_recovery_plan.json (AC-109)
  C:\\Trading\\ANT_OUT\\refresh_trigger.json               (AC-114)

Output:
  C:\\Trading\\ANT_OUT\\system_readiness_score.json

Score mapping (start=100, clamp min=0):
  source_health CRITICAL       → -50
  source_health DEGRADED       → -20
  recovery_status URGENT       → -30
  recovery_status PLAN_READY   → -10
  trigger_status URGENT        → -30
  trigger_status DUE           → -15
  review_alignment LOW         → -25
  review_alignment MEDIUM      → -10
  missing/unknown input        → worst-case penalty for that component

Status:
  score  < 40  → NOT_READY
  40 ≤ score < 70  → LIMITED
  score ≥ 70   → READY

Blocking (hard stop for review):
  source_health == CRITICAL  OR  recovery_status == URGENT

No execution. No API calls. Read-only. Non-binding. Simulation-only.
live_activation_allowed=False always.

Usage:
    python ant_colony/build_system_readiness_score_lite.py
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SNAPSHOT_PATH  = Path(r"C:\Trading\ANT_OUT\combined_review_snapshot.json")
HEALTH_PATH    = Path(r"C:\Trading\ANT_OUT\source_health_review.json")
RECOVERY_PATH  = Path(r"C:\Trading\ANT_OUT\source_freshness_recovery_plan.json")
TRIGGER_PATH   = Path(r"C:\Trading\ANT_OUT\refresh_trigger.json")
OUTPUT_PATH    = Path(r"C:\Trading\ANT_OUT\system_readiness_score.json")

VERSION   = "system_readiness_score_v1"
COMPONENT = "build_system_readiness_score_lite"

FLAGS = {
    "non_execution": True,
    "read_only":     True,
}

# ---------------------------------------------------------------------------
# Deduction table
# ---------------------------------------------------------------------------

_SH_DEDUCTIONS  = {"CRITICAL": 50, "DEGRADED": 20}
_RS_DEDUCTIONS  = {"URGENT": 30,   "PLAN_READY": 10}
_TS_DEDUCTIONS  = {"URGENT": 30,   "DUE": 15}
_RA_DEDUCTIONS  = {"LOW": 25,      "MEDIUM": 10}

# Fail-closed worst-case values when a source is missing/corrupt
_SH_FALLBACK = "CRITICAL"
_RS_FALLBACK = "URGENT"
_TS_FALLBACK = "URGENT"
_RA_FALLBACK = "LOW"

# Reason code priority (first match wins)
_REASON_PRIORITY = [
    ("NO_SOURCE_DATA",         lambda sh, rs, ts, ra: False),   # handled separately
    ("SOURCE_HEALTH_CRITICAL", lambda sh, rs, ts, ra: sh == "CRITICAL"),
    ("RECOVERY_URGENT",        lambda sh, rs, ts, ra: rs == "URGENT"),
    ("TRIGGER_URGENT",         lambda sh, rs, ts, ra: ts == "URGENT"),
    ("REVIEW_ALIGNMENT_LOW",   lambda sh, rs, ts, ra: ra == "LOW"),
    ("TRIGGER_DUE",            lambda sh, rs, ts, ra: ts == "DUE"),
    ("SOURCE_HEALTH_DEGRADED", lambda sh, rs, ts, ra: sh == "DEGRADED"),
    ("RECOVERY_PLAN_READY",    lambda sh, rs, ts, ra: rs == "PLAN_READY"),
    ("REVIEW_ALIGNMENT_MEDIUM",lambda sh, rs, ts, ra: ra == "MEDIUM"),
    ("SYSTEM_READY",           lambda sh, rs, ts, ra: True),
]


# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------

def build_readiness_score(
    snapshot_data:  dict | None,
    health_data:    dict | None,
    recovery_data:  dict | None,
    trigger_data:   dict | None,
    now_utc:        datetime.datetime,
) -> dict:
    """
    Compute system readiness score from loaded source dicts.
    Pure function — no I/O, no side effects.
    Fail-closed: None inputs use worst-case penalty values.
    """
    all_none = (snapshot_data is None and health_data is None
                and recovery_data is None and trigger_data is None)

    # ── Extract raw component values ─────────────────────────────────────────
    source_health = None
    if health_data and isinstance(health_data, dict):
        source_health = health_data.get("source_health_status")

    recovery_status = None
    if recovery_data and isinstance(recovery_data, dict):
        recovery_status = recovery_data.get("recovery_status")

    trigger_status = None
    if trigger_data and isinstance(trigger_data, dict):
        trigger_status = trigger_data.get("trigger_status")

    review_alignment = None
    if snapshot_data and isinstance(snapshot_data, dict):
        rh = snapshot_data.get("review_health", {})
        if isinstance(rh, dict):
            review_alignment = rh.get("alignment")

    # ── Compute deductions (fail-closed: None → worst-case) ──────────────────
    sh = source_health    or _SH_FALLBACK
    rs = recovery_status  or _RS_FALLBACK
    ts = trigger_status   or _TS_FALLBACK
    ra = review_alignment or _RA_FALLBACK

    score = 100
    score -= _SH_DEDUCTIONS.get(sh, 0)
    score -= _RS_DEDUCTIONS.get(rs, 0)
    score -= _TS_DEDUCTIONS.get(ts, 0)
    score -= _RA_DEDUCTIONS.get(ra, 0)
    score  = max(0, score)

    # ── Status ────────────────────────────────────────────────────────────────
    if score < 40:
        readiness_status = "NOT_READY"
    elif score < 70:
        readiness_status = "LIMITED"
    else:
        readiness_status = "READY"

    # ── Blocking (based on actual values, not fail-closed substitutions) ──────
    blocking = (source_health == "CRITICAL" or recovery_status == "URGENT")

    # ── Reason code ──────────────────────────────────────────────────────────
    if all_none:
        reason_code = "NO_SOURCE_DATA"
    else:
        reason_code = "SYSTEM_READY"
        for code, predicate in _REASON_PRIORITY[1:]:  # skip NO_SOURCE_DATA
            if predicate(sh, rs, ts, ra):
                reason_code = code
                break

    return {
        "version":          VERSION,
        "component":        COMPONENT,
        "ts_utc":           now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "readiness_score":  score,
        "readiness_status": readiness_status,
        "components": {
            "source_health":    source_health    or "UNKNOWN",
            "review_alignment": review_alignment or "UNKNOWN",
            "recovery_status":  recovery_status  or "UNKNOWN",
            "trigger_status":   trigger_status   or "UNKNOWN",
        },
        "blocking":    blocking,
        "reason_code": reason_code,
        "flags":       dict(FLAGS),
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


def run_readiness_score(
    snapshot_path:  Path = SNAPSHOT_PATH,
    health_path:    Path = HEALTH_PATH,
    recovery_path:  Path = RECOVERY_PATH,
    trigger_path:   Path = TRIGGER_PATH,
    output_path:    Path = OUTPUT_PATH,
    now_utc:        datetime.datetime | None = None,
) -> dict:
    """Load sources, compute score, write output. Returns result dict."""
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    snapshot_data  = _load(snapshot_path)
    health_data    = _load(health_path)
    recovery_data  = _load(recovery_path)
    trigger_data   = _load(trigger_path)

    result = build_readiness_score(
        snapshot_data, health_data, recovery_data, trigger_data, now_utc
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_score(result: dict) -> None:
    score  = result.get("readiness_score",  "?")
    status = result.get("readiness_status", "?")
    block  = result.get("blocking",         "?")
    reason = result.get("reason_code",      "?")
    comps  = result.get("components",       {})
    print("=== ANT SYSTEM READINESS SCORE ===")
    print(f"score    : {score}/100  ({status})")
    print(f"blocking : {block}")
    print(f"reason   : {reason}")
    print(f"sh={comps.get('source_health','?')}  "
          f"rs={comps.get('recovery_status','?')}  "
          f"ts={comps.get('trigger_status','?')}  "
          f"ra={comps.get('review_alignment','?')}")


def main() -> None:
    result = run_readiness_score()
    _print_score(result)


if __name__ == "__main__":
    main()
