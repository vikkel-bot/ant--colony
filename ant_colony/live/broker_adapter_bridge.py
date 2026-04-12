"""
AC-152: Broker Adapter Bridge (dry only)

Converts a validated AC-151 broker_request into a broker-ready command
envelope. This is a pure mapping layer — the output describes what a real
broker adapter would receive, but no adapter is called, no HTTP is made,
and no order is placed.

mode="dry" is hardcoded. Changing it requires an explicit architecture step.

Fail-closed: any invalid input or constraint violation returns ok=False.
No exceptions leak to the caller.
"""
from __future__ import annotations

from typing import Any

from ant_colony.live.broker_request_builder import build_broker_request

_ADAPTER_NAME = "bitvavo"
_MODE = "dry"
_REQUEST_TYPE = "place_order"
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}


def build_broker_adapter_command(intake_record: Any) -> dict[str, Any]:
    """
    Build a dry broker adapter command envelope from an intake record.

    Returns:
        {
            "ok": True,
            "reason": "BROKER_ADAPTER_COMMAND_READY",
            "adapter_command": { ... }
        }
        or on failure:
        {
            "ok": False,
            "reason": "<explanation>",
            "adapter_command": None
        }

    Never raises.
    """
    try:
        return _build(intake_record)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"unexpected error: {exc}", "adapter_command": None}


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "adapter_command": None}


def _build(intake_record: Any) -> dict[str, Any]:
    # Gate: run AC-151 broker request builder
    req_result = build_broker_request(intake_record)
    if not req_result["ok"]:
        return _fail(f"broker request build failed: {req_result['reason']}")

    br = req_result["broker_request"]

    # Defence-in-depth checks on the normalised broker_request
    if br["market"] not in _VALID_MARKETS:
        return _fail(f"market not allowed: {br['market']!r}")

    if br["strategy_key"] not in _VALID_STRATEGIES:
        return _fail(f"strategy_key not allowed: {br['strategy_key']!r}")

    qty = br["qty"]
    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty <= 0:
        return _fail(f"qty must be numeric > 0, got {qty!r}")

    price = br["intended_entry_price"]
    if not isinstance(price, (int, float)) or isinstance(price, bool) or price <= 0:
        return _fail(f"intended_entry_price must be numeric > 0, got {price!r}")

    max_notional = br["max_notional_eur"]
    if not isinstance(max_notional, (int, float)) or isinstance(max_notional, bool) \
            or max_notional > 50:
        return _fail(f"max_notional_eur must be <= 50, got {max_notional!r}")

    client_request_id = br["client_request_id"]
    if not isinstance(client_request_id, str) or not client_request_id.strip():
        return _fail("client_request_id must be a non-empty string")

    adapter_command = {
        "adapter": _ADAPTER_NAME,
        "mode": _MODE,
        "request_type": _REQUEST_TYPE,
        "payload": {
            "market": br["market"],
            "side": br["order_side"],
            "order_type": br["order_type"],
            "qty": qty,
            "intended_entry_price": price,
            "max_notional_eur": max_notional,
            "strategy_key": br["strategy_key"],
            "operator_approved": br["operator_approved"],
        },
        "client_request_id": client_request_id,
        "ts_command_utc": br["ts_request_utc"],
    }

    return {
        "ok": True,
        "reason": "BROKER_ADAPTER_COMMAND_READY",
        "adapter_command": adapter_command,
    }
