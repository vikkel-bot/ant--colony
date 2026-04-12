"""
AC-151: Broker Request Builder (dry only)

Converts a validated AC-150 intake record into a canonical broker request
payload. This is a pure mapping layer — no broker calls, no file IO, no
network activity, no paper pipeline imports.

The output is the standardised request form that a future broker adapter
may consume. It is NOT a broker-specific payload and contains no signed
requests or HTTP details.

Fail-closed: any invalid intake is rejected before a payload is built.
No exceptions leak to the caller.
"""
from __future__ import annotations

from typing import Any

from ant_colony.live.broker_execution_intake_contract import validate_broker_execution_intake

_VALID_ORDER_TYPES = {"market", "limit"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}

# Canonical output key order
_PAYLOAD_KEYS = (
    "lane",
    "market",
    "strategy_key",
    "order_side",
    "order_type",
    "qty",
    "intended_entry_price",
    "max_notional_eur",
    "client_request_id",
    "ts_request_utc",
    "operator_approved",
)


def build_broker_request(intake_record: Any) -> dict[str, Any]:
    """
    Build a canonical broker request payload from a validated intake record.

    Returns:
        {
            "ok": True,
            "reason": "BROKER_REQUEST_READY",
            "broker_request": { ...canonical payload... }
        }
        or on failure:
        {
            "ok": False,
            "reason": "<explanation>",
            "broker_request": None
        }

    Never raises. Fail-closed on invalid or unsafe input.
    """
    try:
        return _build(intake_record)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"unexpected error: {exc}", "broker_request": None}


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "broker_request": None}


def _build(intake_record: Any) -> dict[str, Any]:
    # Gate 1: run AC-150 intake validation
    intake = validate_broker_execution_intake(intake_record)
    if not intake["ok"]:
        return _fail(f"intake validation failed: {intake['reason']}")

    nr = intake["normalized_record"]

    # Gate 2: post-normalization safety checks (defence-in-depth)
    if nr["market"] not in _VALID_MARKETS:
        return _fail(f"market not allowed: {nr['market']!r}")

    if nr["strategy_key"] not in _VALID_STRATEGIES:
        return _fail(f"strategy_key not allowed: {nr['strategy_key']!r}")

    if nr["order_type"] not in _VALID_ORDER_TYPES:
        return _fail(f"order_type not allowed: {nr['order_type']!r}")

    qty = nr["qty"]
    price = nr["intended_entry_price"]
    max_notional = nr["max_notional_eur"]
    if qty * price > max_notional:
        return _fail(
            f"notional {qty * price:.4f} exceeds max_notional_eur {max_notional}"
        )

    if not isinstance(nr["operator_approved"], bool):
        return _fail("operator_approved must be bool")

    # Build deterministic client_request_id from stable intake fields
    client_request_id = "_".join([
        "REQ",
        nr["lane"],
        nr["market"].replace("-", ""),
        nr["strategy_key"],
        nr["order_side"],
        nr["order_type"],
        nr["ts_intake_utc"].replace(":", "").replace("-", "").replace("+", ""),
    ])

    payload = {
        "lane": nr["lane"],
        "market": nr["market"],
        "strategy_key": nr["strategy_key"],
        "order_side": nr["order_side"],
        "order_type": nr["order_type"],
        "qty": nr["qty"],
        "intended_entry_price": nr["intended_entry_price"],
        "max_notional_eur": nr["max_notional_eur"],
        "client_request_id": client_request_id,
        "ts_request_utc": nr["ts_intake_utc"],
        "operator_approved": nr["operator_approved"],
    }

    return {"ok": True, "reason": "BROKER_REQUEST_READY", "broker_request": payload}
