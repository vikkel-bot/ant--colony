"""
AC-154: Live Order Reconciler

Converts a Bitvavo broker response and the original intake record into a
canonical AC-148 entry execution record with sentinel values for exit fields
that have not yet occurred (position still open).

One sentence: Maps a BitvavoAdapter place_order response to an AC-148-compatible
entry execution record, using safe sentinel values for open exit fields.

No broker calls. No file IO. No paper pipeline imports.
Fail-closed: invalid input is always rejected. Never raises.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def reconcile_live_order(
    intake_record: Any,
    broker_response: Any,
) -> dict[str, Any]:
    """
    Reconcile a broker adapter response against the intake record.

    Returns:
        {
            "ok": bool,
            "reason": str,
            "execution_result": dict | None  # AC-148 compatible if ok=True
        }

    Never raises. Fail-closed on any invalid input.
    """
    try:
        return _reconcile(intake_record, broker_response)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"unexpected reconcile error: {exc}", "execution_result": None}


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "execution_result": None}


def _reconcile(intake_record: Any, broker_response: Any) -> dict[str, Any]:
    if not isinstance(intake_record, dict):
        return _fail("intake_record must be a dict")
    if not isinstance(broker_response, dict):
        return _fail("broker_response must be a dict")

    if not broker_response.get("ok"):
        err = broker_response.get("error") or {}
        msg = err.get("message", "unknown") if isinstance(err, dict) else str(err)
        return _fail(f"broker_response not ok: {msg}")

    data = broker_response.get("data")
    if not isinstance(data, dict):
        return _fail("broker_response.data must be a dict")

    raw = data.get("raw") if isinstance(data.get("raw"), dict) else {}

    # broker_order_id: prefer data.order_id, fall back to raw.orderId
    broker_order_id = data.get("order_id") or str(raw.get("orderId", ""))
    if not broker_order_id:
        return _fail("broker_response missing order_id")

    # qty and intended price from intake (already validated by AC-150 upstream)
    qty = intake_record.get("qty")
    intended_price = intake_record.get("intended_entry_price")

    if not isinstance(qty, (int, float)) or qty <= 0:
        return _fail(f"intake qty invalid: {qty!r}")
    if not isinstance(intended_price, (int, float)) or intended_price <= 0:
        return _fail(f"intake intended_entry_price invalid: {intended_price!r}")

    # Determine fill price.
    # AC-192: priority: fills[0]["price"] → raw["price"] → fail-closed.
    # intended_entry_price is never used as entry_price — it is only a
    # pre-trade estimate; the actual fill price must come from the broker.
    fill_price: float | None = None

    fills = raw.get("fills")
    if isinstance(fills, list) and fills:
        try:
            candidate = float(fills[0].get("price") or 0)
            if candidate > 0:
                fill_price = candidate
        except (ValueError, TypeError):
            pass

    if fill_price is None:
        price_raw = raw.get("price")
        if price_raw is not None:
            try:
                candidate = float(price_raw)
                if candidate > 0:
                    fill_price = candidate
            except (ValueError, TypeError):
                pass

    if fill_price is None:
        return _fail("broker_response has no fill price (fills[0].price and raw.price both absent)")

    # Determine execution quality flag
    quality_flag = "OK"
    status = data.get("status", "")
    filled_raw = raw.get("filledAmount") or raw.get("amountFilled")
    if filled_raw is not None:
        try:
            filled = float(filled_raw)
            if filled < float(qty) * 0.99:
                quality_flag = "PARTIAL_FILL"
        except (ValueError, TypeError):
            quality_flag = "MISMATCH"

    if status not in ("filled", "new", "partiallyFilled", ""):
        quality_flag = "MISMATCH"

    # Entry timestamp: prefer broker creation time, fall back to now
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entry_ts = now_iso
    created_raw = raw.get("created")
    if created_raw is not None:
        try:
            entry_ts = datetime.fromtimestamp(
                float(created_raw) / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError, OSError):
            pass

    # Deterministic trade_id from stable intake fields
    market_slug = str(intake_record.get("market", "UNKNOWN")).replace("-", "")
    strategy = str(intake_record.get("strategy_key", "UNKNOWN"))
    side = str(intake_record.get("position_side", "UNKNOWN")).upper()
    ts_slug = (
        str(intake_record.get("ts_intake_utc", now_iso))
        .replace(":", "")
        .replace("-", "")
        .replace("Z", "")
        .replace("T", "")
    )
    trade_id = f"LIVE-{market_slug}-{strategy}-{side}-{ts_slug}"

    execution_result = {
        "trade_id": trade_id,
        "lane": intake_record.get("lane", "live_test"),
        "market": intake_record.get("market"),
        "strategy_key": intake_record.get("strategy_key"),
        "position_side": intake_record.get("position_side"),
        "qty": qty,
        "entry_ts_utc": entry_ts,
        "exit_ts_utc": entry_ts,             # sentinel: position still open
        "entry_price": fill_price,
        "exit_price": fill_price,            # sentinel: same as entry until exit
        "realized_pnl_eur": 0.0,             # sentinel: no realized PNL yet
        "slippage_eur": 0.0,                 # computed on exit
        "hold_duration_minutes": 0.0,        # sentinel: just entered
        "exit_reason": "UNKNOWN",            # sentinel: position still open
        "execution_quality_flag": quality_flag,
        "broker_order_id_entry": broker_order_id,
        "broker_order_id_exit": None,  # AC-190: null until a proven exit exists
        "ts_recorded_utc": now_iso,
    }

    return {"ok": True, "reason": "RECONCILE_OK", "execution_result": execution_result}
