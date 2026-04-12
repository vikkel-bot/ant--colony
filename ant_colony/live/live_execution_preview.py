"""
AC-149: Live Execution Preview (Dry Integration)

Accepts a minimal live intent, builds a deterministic mock execution result,
and validates it against the AC-148 schema.

Hard constraints:
- No broker calls
- No file IO
- No ANT_OUT writes
- No paper pipeline imports
- No random / uuid4
- Deterministic: caller supplies timestamps via _now_utc to keep tests stable
- Fail-closed on any invalid input or schema violation
"""
from __future__ import annotations

from typing import Any

from ant_colony.live.live_execution_result_schema import validate_live_execution_result

# ---------------------------------------------------------------------------
# Intent field requirements
# ---------------------------------------------------------------------------

_REQUIRED_INTENT_FIELDS = (
    "lane",
    "market",
    "strategy_key",
    "position_side",
    "qty",
    "entry_price",
    "exit_price",
    "exit_reason",
)

_VALID_INTENT_LANES = {"live_test"}
_VALID_INTENT_MARKETS = {"BNB-EUR"}
_VALID_INTENT_STRATEGIES = {"EDGE3"}
_VALID_INTENT_SIDES = {"long", "short"}
_VALID_INTENT_EXIT_REASONS = {
    "SL", "TP", "SIGNAL", "OPERATOR_KILL", "MANUAL", "UNKNOWN"
}

# Fixed fallback timestamp used when caller passes no _now_utc.
# Tests must pass _now_utc explicitly to stay deterministic.
_FALLBACK_TS = "2026-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def preview_live_execution(
    intent: dict[str, Any],
    _now_utc: str | None = None,
) -> dict[str, Any]:
    """
    Build and validate a dry execution result from a live intent.

    Args:
        intent:    Minimal trade intent dict (see module docstring).
        _now_utc:  UTC timestamp string used for all three timestamp fields.
                   Pass this in tests to keep results deterministic.

    Returns:
        {
            "ok": True,
            "reason": "PREVIEW_OK",
            "execution_result": { ...AC-148 compatible record... }
        }
        or on failure:
        {
            "ok": False,
            "reason": "<explanation>",
            "execution_result": None
        }

    Never raises.
    """
    try:
        return _build_preview(intent, _now_utc or _FALLBACK_TS)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"unexpected error: {exc}", "execution_result": None}


# ---------------------------------------------------------------------------
# Internal logic
# ---------------------------------------------------------------------------

def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "execution_result": None}


def _build_preview(intent: Any, ts: str) -> dict[str, Any]:
    if not isinstance(intent, dict):
        return _fail("intent must be a dict")

    # --- required fields ---
    for field in _REQUIRED_INTENT_FIELDS:
        if field not in intent:
            return _fail(f"missing required intent field: {field}")

    # --- lane ---
    if intent["lane"] not in _VALID_INTENT_LANES:
        return _fail(f"lane must be one of {sorted(_VALID_INTENT_LANES)}, got {intent['lane']!r}")

    # --- market ---
    if intent["market"] not in _VALID_INTENT_MARKETS:
        return _fail(
            f"market must be one of {sorted(_VALID_INTENT_MARKETS)}, got {intent['market']!r}"
        )

    # --- strategy_key ---
    if intent["strategy_key"] not in _VALID_INTENT_STRATEGIES:
        return _fail(
            f"strategy_key must be one of {sorted(_VALID_INTENT_STRATEGIES)}, "
            f"got {intent['strategy_key']!r}"
        )

    # --- position_side ---
    side = intent["position_side"]
    if side not in _VALID_INTENT_SIDES:
        return _fail(
            f"position_side must be one of {sorted(_VALID_INTENT_SIDES)}, got {side!r}"
        )

    # --- qty ---
    qty = intent["qty"]
    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty <= 0:
        return _fail(f"qty must be numeric > 0, got {qty!r}")

    # --- prices ---
    entry_price = intent["entry_price"]
    exit_price = intent["exit_price"]
    for name, val in (("entry_price", entry_price), ("exit_price", exit_price)):
        if not isinstance(val, (int, float)) or isinstance(val, bool) or val <= 0:
            return _fail(f"{name} must be numeric > 0, got {val!r}")

    # --- exit_reason ---
    if intent["exit_reason"] not in _VALID_INTENT_EXIT_REASONS:
        return _fail(
            f"exit_reason must be one of {sorted(_VALID_INTENT_EXIT_REASONS)}, "
            f"got {intent['exit_reason']!r}"
        )

    # --- pnl calculation ---
    if side == "long":
        pnl = round((exit_price - entry_price) * qty, 8)
    else:  # short
        pnl = round((entry_price - exit_price) * qty, 8)

    # --- deterministic identifiers derived from intent fields ---
    id_base = f"{intent['market']}-{intent['strategy_key']}-{side}"
    trade_id = f"PREVIEW-{id_base}"
    broker_entry_id = f"MOCK-ENTRY-{id_base}"
    broker_exit_id = f"MOCK-EXIT-{id_base}"

    record = {
        "trade_id": trade_id,
        "lane": intent["lane"],
        "market": intent["market"],
        "strategy_key": intent["strategy_key"],
        "position_side": side,
        "qty": qty,
        "entry_ts_utc": ts,
        "exit_ts_utc": ts,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "realized_pnl_eur": pnl,
        "slippage_eur": 0.0,
        "hold_duration_minutes": 0.0,
        "exit_reason": intent["exit_reason"],
        "execution_quality_flag": "OK",
        "broker_order_id_entry": broker_entry_id,
        "broker_order_id_exit": broker_exit_id,
        "ts_recorded_utc": ts,
    }

    validation = validate_live_execution_result(record)
    if not validation["ok"]:
        return _fail(f"schema validation failed: {validation['reason']}")

    return {
        "ok": True,
        "reason": "PREVIEW_OK",
        "execution_result": validation["normalized_record"],
    }
