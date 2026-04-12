"""
AC-161: Queen Feedback Intake

Accepts a closed trade feedback record and verifies the Queen may safely
receive it — acting as the queen's gate before any memory is written.

One sentence: Accepts a closed trade feedback record and checks whether the
Queen may safely receive it, blocking incomplete or mismatched records.

The intake does not modify the record. It only validates and accepts.
No learning, no allocation change, no strategy update occurs here.

Fail-closed: invalid, incomplete, or mismatched records are rejected.
No broker calls. No file IO. No paper pipeline imports. Never raises.
"""
from __future__ import annotations

from typing import Any

from ant_colony.live.live_feedback_schema import validate_live_feedback_record

_VALID_LANES = {"live_test"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}

# Causal fields that must be present in every record the queen accepts.
# Absence of any of these means the queen cannot learn from the record.
_CAUSAL_FIELDS = (
    "market_regime_at_entry",
    "volatility_at_entry",
    "signal_strength",
    "signal_key",
    "slippage_vs_expected_eur",
    "entry_latency_ms",
)


def intake_feedback_for_queen(live_feedback_record: Any) -> dict[str, Any]:
    """
    Validate and accept a live feedback record for the Queen.

    Parameters:
        live_feedback_record — AC-159 validated feedback record dict

    Returns:
        {
            "ok": bool,
            "reason": str,
            "accepted_feedback": dict | None  # normalized record when ok=True
        }

    Never raises. Fail-closed on any invalid or incomplete input.
    """
    try:
        return _intake(live_feedback_record)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected intake error: {exc}",
            "accepted_feedback": None,
        }


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "accepted_feedback": None}


def _intake(record: Any) -> dict[str, Any]:
    # --- AC-159 schema validation ---
    schema_result = validate_live_feedback_record(record)
    if not schema_result["ok"]:
        return _fail(f"feedback schema invalid: {schema_result['reason']}")

    nr = schema_result["normalized_record"]

    # --- queen-specific scope constraints ---
    if nr["lane"] not in _VALID_LANES:
        return _fail(f"lane not accepted by queen: {nr['lane']!r}")

    if nr["market"] not in _VALID_MARKETS:
        return _fail(f"market not accepted by queen: {nr['market']!r}")

    if nr["strategy_key"] not in _VALID_STRATEGIES:
        return _fail(f"strategy_key not accepted by queen: {nr['strategy_key']!r}")

    # --- causal context must be present and non-trivially available ---
    # (AC-159 schema already validates types; here we confirm all six fields exist
    #  in the normalized record, which they must because they are required fields)
    for field in _CAUSAL_FIELDS:
        if field not in nr:
            return _fail(f"causal field missing from normalized record: {field}")

    return {
        "ok": True,
        "reason": "QUEEN_FEEDBACK_ACCEPTED",
        "accepted_feedback": nr,
    }
