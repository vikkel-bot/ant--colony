"""
AC-158: Live Exit Reconciler

Verifies that the broker executed the close order and converts the outcome
into a fully-closed AC-148 compatible trade record with computed PnL.

One sentence: Verifies the broker executed the close and converts the result
into a fully-closed AC-148 trade record with realized PnL and hold duration.

PnL rules:
  long:  realized_pnl_eur = (exit_price - entry_price) * qty
  short: realized_pnl_eur = (entry_price - exit_price) * qty

slippage_eur = 0.0 (market close order has no pre-specified target price)
hold_duration_minutes = max(0.0, (exit_ts - entry_ts).total_seconds() / 60)

The output is validated against validate_live_execution_result() (AC-148)
before being returned. Fail-closed if schema validation fails.

No broker calls. No file IO. No paper pipeline imports. Never raises.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ant_colony.live.live_execution_result_schema import validate_live_execution_result

_VALID_EXIT_REASONS = {"SL", "TP", "SIGNAL", "OPERATOR_KILL", "MANUAL"}
_VALID_LANES = {"live_test"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}

_ENTRY_REQUIRED = (
    "trade_id",
    "lane",
    "market",
    "strategy_key",
    "position_side",
    "qty",
    "entry_ts_utc",
    "entry_price",
    "broker_order_id_entry",
)

_EXIT_INTENT_REQUIRED = ("exit_reason", "qty")


def reconcile_live_exit(
    entry_execution_result: Any,
    position_state_record: Any,
    exit_intent: Any,
    broker_response: Any,
) -> dict[str, Any]:
    """
    Reconcile a broker close response into a fully-closed AC-148 trade record.

    Parameters:
        entry_execution_result — AC-148/AC-154 entry record
        position_state_record  — AC-157 position state (must be OPEN_POSITION)
        exit_intent            — AC-158 exit intent (from build_live_exit_intent)
        broker_response        — BitvavoAdapter.place_order result for the exit order

    Returns:
        {
            "ok": bool,
            "reason": str,
            "closed_trade_result": dict | None  # AC-148 closed record when ok=True
        }

    Never raises. Fail-closed on any validation or schema failure.
    """
    try:
        return _reconcile(
            entry_execution_result, position_state_record, exit_intent, broker_response
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected reconcile error: {exc}",
            "closed_trade_result": None,
        }


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "closed_trade_result": None}


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.utcoffset().total_seconds() == 0:
            return dt.astimezone(timezone.utc)
    except Exception:
        pass
    return None


def _reconcile(
    entry_result: Any,
    position_state_record: Any,
    exit_intent: Any,
    broker_response: Any,
) -> dict[str, Any]:
    # --- validate entry_execution_result ---
    if not isinstance(entry_result, dict):
        return _fail("entry_execution_result must be a dict")
    for field in _ENTRY_REQUIRED:
        if field not in entry_result:
            return _fail(f"entry_execution_result missing field: {field}")

    entry_market = entry_result["market"]
    if entry_market not in _VALID_MARKETS:
        return _fail(f"entry market not allowed: {entry_market!r}")

    broker_order_id_entry = entry_result["broker_order_id_entry"]
    if not isinstance(broker_order_id_entry, str) or not broker_order_id_entry.strip():
        return _fail("entry broker_order_id_entry must be non-empty")
    if broker_order_id_entry == "ENTRY_ONLY_PENDING_EXIT":
        return _fail(
            "entry broker_order_id_entry is the sentinel value; "
            "a real entry order ID is required for exit reconcile"
        )

    # --- validate position_state_record ---
    if not isinstance(position_state_record, dict):
        return _fail("position_state_record must be a dict")
    if position_state_record.get("position_state") != "OPEN_POSITION":
        return _fail(
            f"position_state must be OPEN_POSITION for exit reconcile, "
            f"got {position_state_record.get('position_state')!r}"
        )
    if position_state_record.get("market") != entry_market:
        return _fail(
            f"position_state market {position_state_record.get('market')!r} "
            f"!= entry market {entry_market!r}"
        )

    # --- validate exit_intent ---
    if not isinstance(exit_intent, dict):
        return _fail("exit_intent must be a dict")
    for field in _EXIT_INTENT_REQUIRED:
        if field not in exit_intent:
            return _fail(f"exit_intent missing field: {field}")

    exit_reason = exit_intent.get("exit_reason", "UNKNOWN")
    if exit_reason not in _VALID_EXIT_REASONS:
        exit_reason = "UNKNOWN"

    # --- validate broker_response ---
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

    # --- extract exit order ID ---
    broker_order_id_exit = data.get("order_id") or str(raw.get("orderId", ""))
    if not broker_order_id_exit:
        return _fail("broker_response missing exit order_id")

    # --- extract fields from entry ---
    trade_id = str(entry_result.get("trade_id", ""))
    lane = entry_result["lane"]
    strategy_key = entry_result["strategy_key"]
    position_side = entry_result["position_side"]
    qty = entry_result["qty"]
    entry_ts_utc = entry_result["entry_ts_utc"]
    entry_price = entry_result["entry_price"]

    # --- exit price: prefer raw.price, fall back to entry_price ---
    exit_price = entry_price
    price_raw = raw.get("price")
    if price_raw is not None:
        try:
            candidate = float(price_raw)
            if candidate > 0:
                exit_price = candidate
        except (ValueError, TypeError):
            pass

    # --- exit timestamp ---
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    exit_ts_utc = now_iso
    created_raw = raw.get("created")
    if created_raw is not None:
        try:
            exit_ts_utc = datetime.fromtimestamp(
                float(created_raw) / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError, OSError):
            pass

    # --- hold duration ---
    entry_dt = _parse_iso(entry_ts_utc)
    exit_dt = _parse_iso(exit_ts_utc)
    hold_duration_minutes = 0.0
    if entry_dt is not None and exit_dt is not None:
        diff_s = (exit_dt - entry_dt).total_seconds()
        hold_duration_minutes = max(0.0, diff_s / 60.0)

    # --- realized PnL ---
    if position_side == "long":
        realized_pnl_eur = (exit_price - entry_price) * qty
    elif position_side == "short":
        realized_pnl_eur = (entry_price - exit_price) * qty
    else:
        realized_pnl_eur = 0.0
    realized_pnl_eur = round(realized_pnl_eur, 8)

    # --- execution quality flag ---
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

    closed_record = {
        "trade_id": trade_id,
        "lane": lane,
        "market": entry_market,
        "strategy_key": strategy_key,
        "position_side": position_side,
        "qty": qty,
        "entry_ts_utc": entry_ts_utc,
        "exit_ts_utc": exit_ts_utc,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "realized_pnl_eur": realized_pnl_eur,
        "slippage_eur": 0.0,
        "hold_duration_minutes": hold_duration_minutes,
        "exit_reason": exit_reason,
        "execution_quality_flag": quality_flag,
        "broker_order_id_entry": broker_order_id_entry,
        "broker_order_id_exit": broker_order_id_exit,
        "ts_recorded_utc": now_iso,
    }

    schema_result = validate_live_execution_result(closed_record)
    if not schema_result["ok"]:
        return _fail(f"AC-148 schema validation failed: {schema_result['reason']}")

    return {
        "ok": True,
        "reason": "LIVE_EXIT_RECONCILED",
        "closed_trade_result": schema_result["normalized_record"],
    }
