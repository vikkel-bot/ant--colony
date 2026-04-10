"""
AC-99: Human Feedback Capture (Non-Binding)

Captures operator feedback against anomaly_action_queue (AC-98) entries.
Append-only JSONL log. No execution impact. No state machine.
No feedback → decision coupling.

Inputs:
  feedback             — dict with feedback_action, feedback_note, operator_id
  anomaly_action_queue — dict from build_anomaly_action_queue() (AC-98)
  review_packet        — optional dict from build_review_packet_lite (AC-96)

Design principles:
  - non_binding=True always.
  - simulation_only=True always.
  - paper_only=True always.
  - live_activation_allowed=False always.
  - Append-only: entries are never overwritten or deleted.
  - Fail-closed: invalid input → skip + log invalid entry, never crash.
  - Deterministic structure: same valid input → same entry shape.
  - Pure core (build_feedback_entry) — no I/O, no side effects.
  - File I/O separated: append_feedback_entry / capture_and_append.
  - No execution, no allocation, no approval flow.
  - No coupling to pipeline state or decision logic.

Valid feedback_action values:
  CONFIRM   — operator agrees with escalation / action queue
  DISAGREE  — operator disagrees
  UNCERTAIN — operator is unsure

Invalid feedback → entry_valid=False written to log; function returns entry dict, no crash.

Output file: C:\\Trading\\ANT_OUT\\human_feedback_log.jsonl (append-only)

Entry schema:
  ts_utc          — ISO-8601 UTC timestamp
  feedback_action — CONFIRM | DISAGREE | UNCERTAIN | INVALID
  feedback_note   — free-text note (may be empty)
  operator_id     — optional operator identifier (may be empty)
  entry_valid     — True for valid feedback; False for invalid/skipped entries
  action_context  — {action_class, urgency, reason_code} from anomaly_action_queue
  source_context  — {anomaly_level, promotion_status, dossier_status, review_status}
  flags           — {non_binding, simulation_only, paper_only, live_activation_allowed}
"""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path

VERSION   = "human_feedback_capture_v1"
COMPONENT = "build_human_feedback_capture_lite"
OUT_DIR   = Path(r"C:\Trading\ANT_OUT")
LOG_PATH  = OUT_DIR / "human_feedback_log.jsonl"

# Valid feedback actions
FEEDBACK_CONFIRM   = "CONFIRM"
FEEDBACK_DISAGREE  = "DISAGREE"
FEEDBACK_UNCERTAIN = "UNCERTAIN"
FEEDBACK_INVALID   = "INVALID"

_VALID_ACTIONS = {FEEDBACK_CONFIRM, FEEDBACK_DISAGREE, FEEDBACK_UNCERTAIN}


# ---------------------------------------------------------------------------
# Core entry builder (pure, no I/O)
# ---------------------------------------------------------------------------

def build_feedback_entry(
    feedback:             object,
    anomaly_action_queue: object = None,
    review_packet:        object = None,
) -> dict:
    """
    Build one feedback log entry.

    Pure function — no file writes, no side effects.
    Invalid input → entry_valid=False entry, no exception raised.

    Args:
        feedback:             dict with feedback_action, feedback_note, operator_id.
        anomaly_action_queue: dict from build_anomaly_action_queue() (AC-98).
        review_packet:        optional dict from AC-96 (context only, not required).

    Returns:
        Feedback entry dict.
    """
    ts = _utc_ts()

    # Validate feedback input
    if not isinstance(feedback, dict):
        return _invalid_entry(ts, "feedback is not a dict", anomaly_action_queue)

    feedback_action = str(feedback.get("feedback_action", ""))
    if feedback_action not in _VALID_ACTIONS:
        return _invalid_entry(
            ts,
            f"invalid feedback_action={feedback_action!r} "
            f"(expected CONFIRM|DISAGREE|UNCERTAIN)",
            anomaly_action_queue,
        )

    feedback_note = str(feedback.get("feedback_note", ""))
    operator_id   = str(feedback.get("operator_id", ""))

    action_ctx  = _action_context(anomaly_action_queue)
    source_ctx  = _source_context(anomaly_action_queue)

    return {
        "ts_utc":          ts,
        "feedback_action": feedback_action,
        "feedback_note":   feedback_note,
        "operator_id":     operator_id,
        "entry_valid":     True,
        "action_context":  action_ctx,
        "source_context":  source_ctx,
        "flags": {
            "non_binding":             True,
            "simulation_only":         True,
            "paper_only":              True,
            "live_activation_allowed": False,
        },
    }


# ---------------------------------------------------------------------------
# Context extractors (non-mutating)
# ---------------------------------------------------------------------------

def _action_context(queue: object) -> dict:
    if not isinstance(queue, dict):
        return {"action_class": "", "urgency": "", "reason_code": ""}
    return {
        "action_class": str(queue.get("action_class", "")),
        "urgency":      str(queue.get("urgency", "")),
        "reason_code":  str(queue.get("reason_code", "")),
    }


def _source_context(queue: object) -> dict:
    sc: object = {}
    if isinstance(queue, dict):
        sc = queue.get("source_context", {})
    if not isinstance(sc, dict):
        sc = {}
    return {
        "anomaly_level":    str(sc.get("anomaly_level", "")),
        "promotion_status": str(sc.get("promotion_status", "")),
        "dossier_status":   str(sc.get("dossier_status", "")),
        "review_status":    str(sc.get("review_status", "")),
    }


# ---------------------------------------------------------------------------
# Fail-closed invalid entry
# ---------------------------------------------------------------------------

def _invalid_entry(ts: str, reason: str, queue: object) -> dict:
    return {
        "ts_utc":          ts,
        "feedback_action": FEEDBACK_INVALID,
        "feedback_note":   reason,
        "operator_id":     "",
        "entry_valid":     False,
        "action_context":  _action_context(queue),
        "source_context":  _source_context(queue),
        "flags": {
            "non_binding":             True,
            "simulation_only":         True,
            "paper_only":              True,
            "live_activation_allowed": False,
        },
    }


def _utc_ts() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# File I/O — append-only JSONL
# ---------------------------------------------------------------------------

def append_feedback_entry(
    entry: dict,
    path:  Path = LOG_PATH,
) -> None:
    """Append one feedback entry to the JSONL log. Creates dirs if needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def read_feedback_log(path: Path = LOG_PATH) -> list[dict]:
    """Read all entries from the JSONL log. Returns [] if file absent."""
    path = Path(path)
    if not path.exists():
        return []
    entries = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass  # corrupted line — skip silently
    return entries


# ---------------------------------------------------------------------------
# Convenience: build + append in one call
# ---------------------------------------------------------------------------

def capture_and_append(
    feedback:             object,
    anomaly_action_queue: object = None,
    review_packet:        object = None,
    path:                 Path   = LOG_PATH,
) -> dict:
    """
    Build a feedback entry and append it to the JSONL log.

    Always appends (even for invalid feedback — fail-closed logging).
    Returns the entry dict.
    """
    entry = build_feedback_entry(feedback, anomaly_action_queue, review_packet)
    append_feedback_entry(entry, path)
    return entry
