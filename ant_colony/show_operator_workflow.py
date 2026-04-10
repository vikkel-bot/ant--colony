"""
AC-113: Operator Workflow (Daily Routine)
AC-116: Refresh Trigger Integration

Prints a compact, repeatable daily operator workflow.
AC-116 adds live refresh trigger status from refresh_trigger.json (AC-114).
Read-only — no execution, no file writes.

Usage:
    python ant_colony/show_operator_workflow.py
"""

import json
from pathlib import Path

TRIGGER_PATH = Path(r"C:\Trading\ANT_OUT\refresh_trigger.json")

_WORKFLOW = """\
=== ANT DAILY WORKFLOW ===

1. Refresh check
   python ant_colony/run_manual_refresh_check_lite.py

   IF health=CRITICAL  → STOP — restore sources before reviewing
   IF health=DEGRADED  → MONITOR — proceed with caution, flag stale markets
   IF health=HEALTHY   → OK — continue to step 2

2. Operator summary
   python ant_colony/show_operator_summary.py

   IF overview=CRITICAL  → STOP — source or review chain needs attention
   IF overview=ATTENTION → REVIEW — human disagreement signal, inspect cases
   IF overview=WATCH     → MONITOR — alignment medium or partial staleness
   IF overview=HEALTHY   → OK — system and human aligned, no action needed

3. Dashboard (detail)
   python ant_colony/show_feedback_dashboard.py

   Use for deeper inspection of:
   - feedback distribution per action class / urgency
   - source health detail (fresh/stale/missing counts)
   - recovery plan top priorities
   - flags (non_binding / simulation_only / paper_only)

── Interpretation guide ──────────────────────────────────────────────────────

  STOP conditions:
    health=CRITICAL    — majority or all markets stale/missing
    overview=CRITICAL  — source health critical (data blocks review)

  MONITOR conditions:
    health=DEGRADED    — some markets stale, none missing
    overview=WATCH     — alignment MEDIUM or partial staleness

  REVIEW conditions:
    overview=ATTENTION — needs_attention=True (e.g. CRITICAL_DISAGREE)
    REVIEW_BLOCKING_FINDINGS with disagree > 30%

  CLEAR conditions:
    health=HEALTHY + overview=HEALTHY — no source or review issues

── Trigger interpretation ────────────────────────────────────────────────────

  trigger=NONE    → proceed normally — no refresh needed
  trigger=WATCH   → monitor — re-run operator summary later
  trigger=DUE     → run step 1 (refresh check) soon
  trigger=URGENT  → run step 1 (refresh check) immediately

── Notes ─────────────────────────────────────────────────────────────────────

  All outputs are:  non_binding=True  simulation_only=True
                    paper_only=True   live_activation_allowed=False

  No action in this workflow triggers execution or broker calls.
"""


def _load_trigger(path: Path) -> dict | None:
    """Load refresh_trigger.json. Returns None on missing/corrupt (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _trigger_block(trigger: dict | None, path: Path) -> str:
    """
    Build the live current-trigger section as a string.
    Pure — no I/O.
    """
    lines = ["── Current trigger ─────────────────────────────────────────────────────────"]
    if trigger and isinstance(trigger, dict):
        st  = trigger.get("trigger_status", "?")
        rc  = trigger.get("trigger_reason_code", "?")
        og  = trigger.get("operator_guidance", {})
        act = og.get("recommended_action", "?") if isinstance(og, dict) else "?"
        win = og.get("recommended_window", "?") if isinstance(og, dict) else "?"
        lines.append(f"  trigger : {st}  ({rc})")
        lines.append(f"  action  : {act}")
        lines.append(f"  window  : {win}")
    else:
        lines.append(f"  NO DATA — refresh_trigger.json not found or corrupt")
        lines.append(f"  Run: python ant_colony/build_refresh_trigger_lite.py")
    return "\n".join(lines) + "\n"


def show(trigger_path: Path = TRIGGER_PATH) -> None:
    """Print the daily operator workflow with live trigger status. No file writes."""
    print(_WORKFLOW)
    trigger = _load_trigger(trigger_path)
    print(_trigger_block(trigger, trigger_path))


if __name__ == "__main__":
    show()
