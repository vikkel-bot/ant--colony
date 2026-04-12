"""
AC-150: Broker Execution Intake Contract

Defines and validates the canonical intake record shape that future broker
execution must satisfy before any order is placed.

This module is a pure validator/normalizer. It:
- defines what a valid intake looks like
- rejects anything that does not meet the contract
- never calls a broker
- never performs file IO
- never imports paper pipeline modules

Fail-closed: any invalid or unsafe input is rejected with a reason string.
No exceptions leak to the caller.
"""
from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Whitelists
# ---------------------------------------------------------------------------

_VALID_LANES = {"live_test"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}
_VALID_POSITION_SIDES = {"long", "short"}
_VALID_ORDER_SIDES = {"buy", "sell"}
_VALID_ORDER_TYPES = {"market", "limit"}
_VALID_RISK_STATES = {"NORMAL", "CAUTION", "FREEZE"}

# Canonical mapping: position side → expected order side
_SIDE_TO_ORDER: dict[str, str] = {"long": "buy", "short": "sell"}

_REQUIRED_FIELDS = (
    "lane",
    "market",
    "strategy_key",
    "position_side",
    "order_side",
    "qty",
    "intended_entry_price",
    "order_type",
    "max_notional_eur",
    "allow_broker_execution",
    "risk_state",
    "freeze_new_entries",
    "operator_approved",
    "ts_intake_utc",
)

_NORMALIZED_KEY_ORDER = _REQUIRED_FIELDS


# ---------------------------------------------------------------------------
# Timestamp validation (no external deps)
# ---------------------------------------------------------------------------

def _is_valid_utc_ts(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    from datetime import datetime
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.utcoffset().total_seconds() == 0
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_broker_execution_intake(record: Any) -> dict[str, Any]:
    """
    Validate a broker execution intake record.

    Returns:
        {
            "ok": bool,
            "reason": str,
            "normalized_record": dict | None
        }

    Never raises. Fail-closed on any validation or constraint violation.
    """
    try:
        return _validate(record)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"unexpected validation error: {exc}", "normalized_record": None}


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "normalized_record": None}


def _validate(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        return _fail("record must be a dict")

    # --- required fields present ---
    for field in _REQUIRED_FIELDS:
        if field not in record:
            return _fail(f"missing required field: {field}")

    # --- lane ---
    if record["lane"] not in _VALID_LANES:
        return _fail(f"lane must be one of {sorted(_VALID_LANES)}, got {record['lane']!r}")

    # --- market ---
    if record["market"] not in _VALID_MARKETS:
        return _fail(f"market must be one of {sorted(_VALID_MARKETS)}, got {record['market']!r}")

    # --- strategy_key ---
    if record["strategy_key"] not in _VALID_STRATEGIES:
        return _fail(
            f"strategy_key must be one of {sorted(_VALID_STRATEGIES)}, "
            f"got {record['strategy_key']!r}"
        )

    # --- position_side ---
    position_side = record["position_side"]
    if position_side not in _VALID_POSITION_SIDES:
        return _fail(
            f"position_side must be one of {sorted(_VALID_POSITION_SIDES)}, "
            f"got {position_side!r}"
        )

    # --- order_side ---
    order_side = record["order_side"]
    if order_side not in _VALID_ORDER_SIDES:
        return _fail(
            f"order_side must be one of {sorted(_VALID_ORDER_SIDES)}, got {order_side!r}"
        )

    # --- cross-field: position_side / order_side mapping ---
    expected_order_side = _SIDE_TO_ORDER[position_side]
    if order_side != expected_order_side:
        return _fail(
            f"position_side '{position_side}' requires order_side '{expected_order_side}', "
            f"got '{order_side}'"
        )

    # --- qty ---
    qty = record["qty"]
    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty <= 0:
        return _fail(f"qty must be numeric > 0, got {qty!r}")

    # --- intended_entry_price ---
    price = record["intended_entry_price"]
    if not isinstance(price, (int, float)) or isinstance(price, bool) or price <= 0:
        return _fail(f"intended_entry_price must be numeric > 0, got {price!r}")

    # --- order_type ---
    if record["order_type"] not in _VALID_ORDER_TYPES:
        return _fail(
            f"order_type must be one of {sorted(_VALID_ORDER_TYPES)}, "
            f"got {record['order_type']!r}"
        )

    # --- max_notional_eur ---
    max_notional = record["max_notional_eur"]
    if not isinstance(max_notional, (int, float)) or isinstance(max_notional, bool) \
            or max_notional <= 0 or max_notional > 50:
        return _fail(f"max_notional_eur must be numeric > 0 and <= 50, got {max_notional!r}")

    # --- allow_broker_execution must be bool ---
    # False  → dry intake (no broker call)
    # True   → live-capable shape; final execution permission is granted by
    #           evaluate_controlled_live_intake() (AC-162), not here.
    if not isinstance(record["allow_broker_execution"], bool):
        return _fail(
            f"allow_broker_execution must be bool, "
            f"got {type(record['allow_broker_execution']).__name__}"
        )

    # --- risk_state ---
    risk_state = record["risk_state"]
    if not isinstance(risk_state, str) or risk_state not in _VALID_RISK_STATES:
        return _fail(
            f"risk_state must be one of {sorted(_VALID_RISK_STATES)}, got {risk_state!r}"
        )

    # --- cross-field: risk_state == FREEZE → block ---
    if risk_state == "FREEZE":
        return _fail("risk_state is FREEZE; intake blocked")

    # --- freeze_new_entries must be bool ---
    freeze = record["freeze_new_entries"]
    if not isinstance(freeze, bool):
        return _fail(f"freeze_new_entries must be bool, got {type(freeze).__name__}")

    # --- cross-field: freeze_new_entries == true → block ---
    if freeze is True:
        return _fail("freeze_new_entries is true; intake blocked")

    # --- operator_approved must be bool ---
    if not isinstance(record["operator_approved"], bool):
        return _fail(
            f"operator_approved must be bool, got {type(record['operator_approved']).__name__}"
        )

    # --- ts_intake_utc ---
    if not _is_valid_utc_ts(record["ts_intake_utc"]):
        return _fail(
            f"ts_intake_utc must be a valid UTC timestamp string, "
            f"got {record['ts_intake_utc']!r}"
        )

    # --- cross-field: notional check ---
    notional = qty * price
    if notional > max_notional:
        return _fail(
            f"qty * intended_entry_price ({notional:.4f}) exceeds "
            f"max_notional_eur ({max_notional})"
        )

    # --- normalize ---
    normalized = {k: record[k] for k in _NORMALIZED_KEY_ORDER}
    return {"ok": True, "reason": "INTAKE_OK", "normalized_record": normalized}
