"""
AC-100: Feedback Analysis Layer

Analyses human_feedback_log.jsonl (AC-99) entries.
No execution impact. No feedback → decision coupling. No model training.

Inputs:
  path to human_feedback_log.jsonl (AC-99)
  OR list of pre-loaded entry dicts (for pure-function testing)

Design principles:
  - non_binding=True always.
  - simulation_only=True always.
  - paper_only=True always.
  - live_activation_allowed=False always.
  - Fail-closed: absent file → empty analysis (all zeros). Corrupted lines → skip.
  - Deterministic: same entries → same analysis every call.
  - Pure core (analyse_feedback) — no I/O, no side effects.
  - No execution, no alerts, no state machine, no model training.
  - AC-99 module untouched.

Alignment signal:
  disagree_rate < 0.2  → HIGH
  0.2 ≤ rate ≤ 0.5    → MEDIUM
  > 0.5               → LOW
  (0 valid entries)   → HIGH  (no evidence of misalignment)

needs_attention=True when (first matching rule wins):
  CRITICAL urgency group has any DISAGREE entry  → CRITICAL_DISAGREE
  disagree_rate > 0.3                            → HIGH_DISAGREE_RATE
  valid entries > 0 and confirm_rate < 0.2       → LOW_CONFIRM_RATE

Rates use valid entry count (confirm + disagree + uncertain) as denominator.
Rates are 0.0 when denominator is 0.
Rates are rounded to 4 decimal places.

Known group keys — always present in output (pre-seeded to zero):
  by_action_class: NO_ACTION, REVIEW_STATUS, REVIEW_CONFLICT,
                   REVIEW_BLOCKING_FINDINGS, REVIEW_MISSING_INPUT,
                   REVIEW_CRITICAL_STATE
  by_urgency:      NONE, LOW, MEDIUM, HIGH, CRITICAL

Output file: C:\\Trading\\ANT_OUT\\feedback_analysis.json
"""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path

VERSION   = "feedback_analysis_v1"
COMPONENT = "build_feedback_analysis_lite"
OUT_DIR   = Path(r"C:\Trading\ANT_OUT")
ANALYSIS_PATH  = OUT_DIR / "feedback_analysis.json"

# Default log path (from AC-99)
DEFAULT_LOG_PATH = OUT_DIR / "human_feedback_log.jsonl"

# Feedback action mirrors
_CONFIRM   = "CONFIRM"
_DISAGREE  = "DISAGREE"
_UNCERTAIN = "UNCERTAIN"
_INVALID   = "INVALID"

# Alignment levels
ALIGN_HIGH   = "HIGH"
ALIGN_MEDIUM = "MEDIUM"
ALIGN_LOW    = "LOW"

# Attention reason codes
ATTN_CRITICAL_DISAGREE  = "CRITICAL_DISAGREE"
ATTN_HIGH_DISAGREE_RATE = "HIGH_DISAGREE_RATE"
ATTN_LOW_CONFIRM_RATE   = "LOW_CONFIRM_RATE"
ATTN_NONE               = "NONE"

# Pre-seeded group keys — always present in output
_KNOWN_ACTION_CLASSES = (
    "NO_ACTION",
    "REVIEW_STATUS",
    "REVIEW_CONFLICT",
    "REVIEW_BLOCKING_FINDINGS",
    "REVIEW_MISSING_INPUT",
    "REVIEW_CRITICAL_STATE",
)
_KNOWN_URGENCIES = ("NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL")


# ---------------------------------------------------------------------------
# Core analysis function (pure, no I/O)
# ---------------------------------------------------------------------------

def analyse_feedback(entries: list) -> dict:
    """
    Analyse a list of feedback entry dicts (as loaded from JSONL log).

    Pure function — no file reads or writes.
    Corrupted / non-dict entries in the list are skipped silently.

    Returns feedback_analysis dict.
    """
    ts = _utc_ts()

    # --- Count totals ---
    n_confirm = n_disagree = n_uncertain = n_invalid = 0

    # --- Group accumulators ---
    by_ac  = {k: _zero_group() for k in _KNOWN_ACTION_CLASSES}
    by_urg = {k: _zero_group() for k in _KNOWN_URGENCIES}

    for entry in entries:
        if not isinstance(entry, dict):
            continue  # corrupted entry — skip

        action  = str(entry.get("feedback_action", ""))
        act_ctx = entry.get("action_context", {})
        if not isinstance(act_ctx, dict):
            act_ctx = {}

        ac_key  = str(act_ctx.get("action_class", ""))
        urg_key = str(act_ctx.get("urgency", ""))

        # Ensure dynamic keys exist (entries from unknown action_classes / urgencies)
        if ac_key not in by_ac:
            by_ac[ac_key] = _zero_group()
        if urg_key not in by_urg:
            by_urg[urg_key] = _zero_group()

        # Increment group totals
        by_ac[ac_key]["entries"]  += 1
        by_urg[urg_key]["entries"] += 1

        if action == _CONFIRM:
            n_confirm += 1
            by_ac[ac_key]["confirm"]  += 1
            by_urg[urg_key]["confirm"] += 1
        elif action == _DISAGREE:
            n_disagree += 1
            by_ac[ac_key]["disagree"]  += 1
            by_urg[urg_key]["disagree"] += 1
        elif action == _UNCERTAIN:
            n_uncertain += 1
            by_ac[ac_key]["uncertain"]  += 1
            by_urg[urg_key]["uncertain"] += 1
        else:
            n_invalid += 1
            # invalid entries still counted in group totals but not in
            # confirm/disagree/uncertain — they don't affect rates

    total_entries = n_confirm + n_disagree + n_uncertain + n_invalid
    valid_entries = n_confirm + n_disagree + n_uncertain  # denominator for rates

    # --- Rates ---
    confirm_rate  = _rate(n_confirm,  valid_entries)
    disagree_rate = _rate(n_disagree, valid_entries)
    uncertain_rate = _rate(n_uncertain, valid_entries)

    # --- Alignment ---
    alignment = _alignment(disagree_rate)

    # --- needs_attention ---
    critical_disagree = by_urg.get("CRITICAL", {}).get("disagree", 0)

    if critical_disagree > 0:
        attn_code = ATTN_CRITICAL_DISAGREE
    elif disagree_rate > 0.3:
        attn_code = ATTN_HIGH_DISAGREE_RATE
    elif valid_entries > 0 and confirm_rate < 0.2:
        attn_code = ATTN_LOW_CONFIRM_RATE
    else:
        attn_code = ATTN_NONE

    needs_attention = attn_code != ATTN_NONE

    return {
        "version":   VERSION,
        "component": COMPONENT,
        "ts_utc":    ts,
        "totals": {
            "entries":   total_entries,
            "confirm":   n_confirm,
            "disagree":  n_disagree,
            "uncertain": n_uncertain,
            "invalid":   n_invalid,
        },
        "rates": {
            "confirm_rate":   confirm_rate,
            "disagree_rate":  disagree_rate,
            "uncertain_rate": uncertain_rate,
        },
        "by_action_class": by_ac,
        "by_urgency":      by_urg,
        "signals": {
            "system_human_alignment": alignment,
            "needs_attention":        needs_attention,
            "attention_reason_code":  attn_code,
        },
        "flags": {
            "non_binding":             True,
            "simulation_only":         True,
            "paper_only":              True,
            "live_activation_allowed": False,
        },
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _zero_group() -> dict:
    return {"entries": 0, "confirm": 0, "disagree": 0, "uncertain": 0}


def _rate(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round(count / total, 4)


def _alignment(disagree_rate: float) -> str:
    if disagree_rate < 0.2:
        return ALIGN_HIGH
    if disagree_rate <= 0.5:
        return ALIGN_MEDIUM
    return ALIGN_LOW


def _utc_ts() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# File I/O (separated from pure core)
# ---------------------------------------------------------------------------

def load_feedback_log(path: Path = DEFAULT_LOG_PATH) -> list[dict]:
    """
    Load entries from JSONL log. Returns [] if file is absent.
    Corrupted lines are skipped silently.
    """
    path = Path(path)
    if not path.exists():
        return []
    entries = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass  # corrupted line — skip
    return entries


def write_feedback_analysis(
    analysis: dict,
    path: Path = ANALYSIS_PATH,
) -> None:
    """Write feedback analysis to JSON file. Overwrites on each call."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(analysis, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Convenience: load + analyse + optionally write
# ---------------------------------------------------------------------------

def analyse_from_log(
    log_path:      Path = DEFAULT_LOG_PATH,
    output_path:   Path = ANALYSIS_PATH,
    write_output:  bool = False,
) -> dict:
    """
    Load feedback log from JSONL, analyse, and optionally write result.

    Returns feedback_analysis dict.
    """
    entries  = load_feedback_log(log_path)
    analysis = analyse_feedback(entries)
    if write_output:
        write_feedback_analysis(analysis, output_path)
    return analysis


# ---------------------------------------------------------------------------
# Optional main (CLI demo)
# ---------------------------------------------------------------------------

def main() -> None:
    result = analyse_from_log(write_output=True)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
