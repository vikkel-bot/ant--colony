"""
AC-158: Live Exit Intent

Determines whether an open live position should be closed and why, producing
a validated exit intent record for consumption by the exit executor.

One sentence: Determines whether the open live position should be closed and
builds a validated exit intent record, blocking on anything other than OPEN_POSITION.

Only OPEN_POSITION positions may generate an exit intent.
Short exit is not supported in this phase — only long (market sell).
Fail-closed: any invalid input returns ok=False. Never raises.
No broker calls. No file IO. No paper pipeline imports.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

_VALID_EXIT_REASONS = {"SL", "TP", "SIGNAL", "OPERATOR_KILL", "MANUAL"}
_VALID_LANES = {"live_test"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}


def build_live_exit_intent(
    position_state_record: Any,
    exit_reason: Any,
    operator_approved: Any,
) -> dict[str, Any]:
    """
    Build a validated exit intent from a position state record.

    Parameters:
        position_state_record — AC-157 validated position state record
        exit_reason           — one of: SL, TP, SIGNAL, OPERATOR_KILL, MANUAL
        operator_approved     — must be True to proceed

    Returns:
        {
            "ok": bool,
            "reason": str,
            "exit_intent": dict | None
        }

    Never raises. Fail-closed on any invalid input or unsafe state.
    """
    try:
        return _build(position_state_record, exit_reason, operator_approved)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"unexpected error: {exc}", "exit_intent": None}


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "exit_intent": None}


def _build(record: Any, exit_reason: Any, operator_approved: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        return _fail("position_state_record must be a dict")

    position_state = record.get("position_state")
    if position_state != "OPEN_POSITION":
        return _fail(
            f"exit intent requires OPEN_POSITION, got {position_state!r}"
        )

    lane = record.get("lane")
    market = record.get("market")
    strategy_key = record.get("strategy_key")
    qty = record.get("qty")
    position_side = record.get("position_side")
    entry_order_id = record.get("entry_order_id")
    entry_price = record.get("entry_price")

    if lane not in _VALID_LANES:
        return _fail(f"lane not allowed: {lane!r}")
    if market not in _VALID_MARKETS:
        return _fail(f"market not allowed: {market!r}")
    if strategy_key not in _VALID_STRATEGIES:
        return _fail(f"strategy_key not allowed: {strategy_key!r}")

    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty <= 0:
        return _fail(f"qty must be numeric > 0, got {qty!r}")
    if not isinstance(entry_order_id, str) or not entry_order_id.strip():
        return _fail("entry_order_id must be non-empty")
    if not isinstance(entry_price, (int, float)) or isinstance(entry_price, bool) \
            or entry_price < 0:
        return _fail(f"entry_price must be numeric >= 0, got {entry_price!r}")

    # Determine close order side
    if position_side == "long":
        order_side = "sell"
    elif position_side == "short":
        return _fail("short position exits are not supported in this phase")
    else:
        return _fail(f"position_side must be 'long' or 'short', got {position_side!r}")

    # exit_reason whitelist
    if exit_reason not in _VALID_EXIT_REASONS:
        return _fail(
            f"exit_reason must be one of {sorted(_VALID_EXIT_REASONS)}, "
            f"got {exit_reason!r}"
        )

    # operator_approved
    if not isinstance(operator_approved, bool):
        return _fail(
            f"operator_approved must be bool, got {type(operator_approved).__name__}"
        )
    if not operator_approved:
        return _fail("operator_approved must be True to build exit intent")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "ok": True,
        "reason": "EXIT_INTENT_READY",
        "exit_intent": {
            "lane": lane,
            "market": market,
            "strategy_key": strategy_key,
            "position_side": position_side,
            "order_side": order_side,
            "qty": qty,
            "exit_reason": exit_reason,
            "operator_approved": operator_approved,
            "entry_order_id": entry_order_id,
            "entry_price": entry_price,
            "ts_intent_utc": now_iso,
        },
    }
