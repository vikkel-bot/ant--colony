"""
AC-119: Daily Status Writer (Compact Operator File)

Reads existing local outputs and writes one ultracompact plain-text file
for quick human check outside Python. No new logic — only bundling and
formatting existing fields.

Sources (read-only):
  C:\\Trading\\ANT_OUT\\combined_review_snapshot.json      (AC-107)
  C:\\Trading\\ANT_OUT\\system_readiness_score.json        (AC-117)
  C:\\Trading\\ANT_OUT\\refresh_trigger.json               (AC-114)
  C:\\Trading\\ANT_OUT\\source_health_review.json          (AC-105)
  C:\\Trading\\ANT_OUT\\source_freshness_recovery_plan.json (AC-109)

Output:
  C:\\Trading\\ANT_OUT\\daily_status.txt  (UTF-8, plain text, no colours)

Format (exact, fixed):
  === ANT DAILY STATUS ===
  ts        : <ts_utc>

  overview  : <CRITICAL|ATTENTION|WATCH|HEALTHY>
  readiness : <status> (<score>/100)
  trigger   : <status> (<action>/<window>)

  source    : <status> (fresh=<n> stale=<n> missing=<n>)
  recovery  : <status> (req=<n> top=<m1,m2,m3>)

  risk      : <top_risk>
  human     : <human_context>

Fail-closed:
  missing source  → field = "NO DATA"
  corrupt source  → field = "ERROR"
  never crashes

No execution. No API calls. Read-only sources. UTF-8 output only.

Usage:
    python ant_colony/build_daily_status_file_lite.py
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SNAPSHOT_PATH  = Path(r"C:\Trading\ANT_OUT\combined_review_snapshot.json")
READINESS_PATH = Path(r"C:\Trading\ANT_OUT\system_readiness_score.json")
TRIGGER_PATH   = Path(r"C:\Trading\ANT_OUT\refresh_trigger.json")
HEALTH_PATH    = Path(r"C:\Trading\ANT_OUT\source_health_review.json")
RECOVERY_PATH  = Path(r"C:\Trading\ANT_OUT\source_freshness_recovery_plan.json")
OUTPUT_PATH    = Path(r"C:\Trading\ANT_OUT\daily_status.txt")

COMPONENT = "build_daily_status_file_lite"

# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------

# Each source is passed as (dict | None, error_str | None).
# error_str is "NO DATA" for missing, "ERROR" for corrupt.
SourceTuple = tuple["dict | None", "str | None"]


def build_daily_status(
    snapshot:  SourceTuple,
    readiness: SourceTuple,
    trigger:   SourceTuple,
    health:    SourceTuple,
    recovery:  SourceTuple,
    now_utc:   datetime.datetime,
) -> str:
    """
    Build the daily status string from loaded source tuples.
    Pure function — no I/O, no side effects.

    Each source is (data_dict, error_msg).
      data_dict is None when missing or corrupt.
      error_msg is "NO DATA" (missing) or "ERROR" (corrupt).
    """

    def _err(t: SourceTuple) -> str:
        return t[1] or "NO DATA"

    ts_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Snapshot fields ───────────────────────────────────────────────────────
    snap_data = snapshot[0]
    if snap_data and isinstance(snap_data, dict):
        overview = snap_data.get("overview_status", "?")
        sm       = snap_data.get("summary", {})
        risk     = sm.get("top_risk",      "?") if isinstance(sm, dict) else "?"
        human    = sm.get("human_context", "?") if isinstance(sm, dict) else "?"
    else:
        _e       = _err(snapshot)
        overview = _e
        risk     = _e
        human    = _e

    # ── Readiness fields ──────────────────────────────────────────────────────
    rd_data = readiness[0]
    if rd_data and isinstance(rd_data, dict):
        rd_status = rd_data.get("readiness_status", "?")
        rd_score  = rd_data.get("readiness_score",  "?")
        rd_val    = f"{rd_status} ({rd_score}/100)"
    else:
        rd_val = _err(readiness)

    # ── Trigger fields ────────────────────────────────────────────────────────
    tr_data = trigger[0]
    if tr_data and isinstance(tr_data, dict):
        tr_status = tr_data.get("trigger_status", "?")
        og        = tr_data.get("operator_guidance", {})
        tr_action = og.get("recommended_action", "?") if isinstance(og, dict) else "?"
        tr_window = og.get("recommended_window", "?") if isinstance(og, dict) else "?"
        tr_val    = f"{tr_status} ({tr_action}/{tr_window})"
    else:
        tr_val = _err(trigger)

    # ── Source health fields ──────────────────────────────────────────────────
    hd_data = health[0]
    if hd_data and isinstance(hd_data, dict):
        sh_status = hd_data.get("source_health_status", "?")
        sh_fresh  = hd_data.get("markets_fresh",   0)
        sh_stale  = hd_data.get("markets_stale",   0)
        sh_miss   = hd_data.get("markets_missing", 0)
        sh_val    = f"{sh_status} (fresh={sh_fresh} stale={sh_stale} missing={sh_miss})"
    else:
        sh_val = _err(health)

    # ── Recovery fields ───────────────────────────────────────────────────────
    rc_data = recovery[0]
    if rc_data and isinstance(rc_data, dict):
        rc_status = rc_data.get("recovery_status", "?")
        rc_sm     = rc_data.get("summary", {})
        rc_req    = rc_sm.get("markets_requiring_recovery", 0) if isinstance(rc_sm, dict) else 0
        rc_po     = rc_data.get("priority_order") or []
        rc_top    = ", ".join(e["market"] for e in rc_po[:3]) if rc_po else "—"
        rc_val    = f"{rc_status} (req={rc_req} top={rc_top})"
    else:
        rc_val = _err(recovery)

    # ── Assemble ──────────────────────────────────────────────────────────────
    lines = [
        "=== ANT DAILY STATUS ===",
        f"ts        : {ts_str}",
        "",
        f"overview  : {overview}",
        f"readiness : {rd_val}",
        f"trigger   : {tr_val}",
        "",
        f"source    : {sh_val}",
        f"recovery  : {rc_val}",
        "",
        f"risk      : {risk}",
        f"human     : {human}",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_for_status(path: Path) -> SourceTuple:
    """Load JSON for status file. Returns (data, None) or (None, error_str)."""
    if not path.exists():
        return None, "NO DATA"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (OSError, json.JSONDecodeError):
        return None, "ERROR"


def run_daily_status(
    snapshot_path:  Path = SNAPSHOT_PATH,
    readiness_path: Path = READINESS_PATH,
    trigger_path:   Path = TRIGGER_PATH,
    health_path:    Path = HEALTH_PATH,
    recovery_path:  Path = RECOVERY_PATH,
    output_path:    Path = OUTPUT_PATH,
    now_utc:        datetime.datetime | None = None,
) -> str:
    """Load sources, build status string, write to output_path. Returns content."""
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    snapshot  = _load_for_status(snapshot_path)
    readiness = _load_for_status(readiness_path)
    trigger   = _load_for_status(trigger_path)
    health    = _load_for_status(health_path)
    recovery  = _load_for_status(recovery_path)

    content = build_daily_status(snapshot, readiness, trigger, health, recovery, now_utc)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return content


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    content = run_daily_status()
    print(content)


if __name__ == "__main__":
    main()
