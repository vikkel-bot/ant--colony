"""
AC-113: Operator Workflow (Daily Routine)

Prints a compact, repeatable daily operator workflow.
Read-only — no execution, no file writes, no external dependencies.

Usage:
    python ant_colony/show_operator_workflow.py
"""

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

── Notes ─────────────────────────────────────────────────────────────────────

  All outputs are:  non_binding=True  simulation_only=True
                    paper_only=True   live_activation_allowed=False

  No action in this workflow triggers execution or broker calls.
"""


def show() -> None:
    """Print the daily operator workflow. No file writes."""
    print(_WORKFLOW)


if __name__ == "__main__":
    show()
