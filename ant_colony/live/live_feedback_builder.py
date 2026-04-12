"""
AC-159: Live Feedback Builder

Builds a validated causally-rich feedback record from a closed trade result
and the market/signal context that existed when the trade was entered.

One sentence: Merges a closed trade result (AC-158) with the causal market
and signal context at entry time into a single validated feedback record for
the queen.

The causal_context must contain all six required causal fields. Records
without causal context are rejected — the queen cannot learn from them.

Causal context keys (all required):
  market_regime_at_entry   — "BULL" | "BEAR" | "SIDEWAYS" | "UNKNOWN"
  volatility_at_entry      — "LOW" | "MID" | "HIGH" | "UNKNOWN"
  signal_strength          — float 0.0–1.0 (strength); -1.0 = not available
  signal_key               — str identifying which signal triggered the entry
  slippage_vs_expected_eur — float: actual slippage minus expected slippage
  entry_latency_ms         — int/float >= 0: signal-to-fill latency in ms

No broker calls. No file IO. No paper pipeline imports.
Fail-closed: any invalid input returns ok=False. Never raises.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ant_colony.live.live_feedback_schema import validate_live_feedback_record

_CAUSAL_REQUIRED = (
    "market_regime_at_entry",
    "volatility_at_entry",
    "signal_strength",
    "signal_key",
    "slippage_vs_expected_eur",
    "entry_latency_ms",
)

_CLOSED_TRADE_REQUIRED = (
    "trade_id",
    "lane",
    "market",
    "strategy_key",
    "position_side",
    "qty",
    "entry_ts_utc",
    "exit_ts_utc",
    "entry_price",
    "exit_price",
    "realized_pnl_eur",
    "slippage_eur",
    "hold_duration_minutes",
    "exit_reason",
    "execution_quality_flag",
    "broker_order_id_entry",
    "broker_order_id_exit",
    "ts_recorded_utc",
)


def build_live_feedback_record(
    closed_trade_result: Any,
    causal_context: Any,
) -> dict[str, Any]:
    """
    Build a validated causally-rich feedback record.

    Parameters:
        closed_trade_result — AC-148 / AC-158 validated closed trade dict
        causal_context      — dict with six required causal fields (see module docstring)

    Returns:
        {
            "ok": bool,
            "reason": str,
            "feedback_record": dict | None  # validated 26-field record when ok=True
        }

    Never raises. Fail-closed on any invalid input or missing causal context.
    """
    try:
        return _build(closed_trade_result, causal_context)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected error: {exc}",
            "feedback_record": None,
        }


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "feedback_record": None}


def _build(closed_trade: Any, causal: Any) -> dict[str, Any]:
    # --- validate closed trade ---
    if not isinstance(closed_trade, dict):
        return _fail("closed_trade_result must be a dict")
    for field in _CLOSED_TRADE_REQUIRED:
        if field not in closed_trade:
            return _fail(f"closed_trade_result missing required field: {field}")

    # --- validate causal context ---
    if not isinstance(causal, dict):
        return _fail("causal_context must be a dict")
    for field in _CAUSAL_REQUIRED:
        if field not in causal:
            return _fail(f"causal_context missing required field: {field}")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Merge: closed trade fields + causal fields + feedback metadata
    record = {
        # AC-148 closed trade fields
        "trade_id": closed_trade["trade_id"],
        "lane": closed_trade["lane"],
        "market": closed_trade["market"],
        "strategy_key": closed_trade["strategy_key"],
        "position_side": closed_trade["position_side"],
        "qty": closed_trade["qty"],
        "entry_ts_utc": closed_trade["entry_ts_utc"],
        "exit_ts_utc": closed_trade["exit_ts_utc"],
        "entry_price": closed_trade["entry_price"],
        "exit_price": closed_trade["exit_price"],
        "realized_pnl_eur": closed_trade["realized_pnl_eur"],
        "slippage_eur": closed_trade["slippage_eur"],
        "hold_duration_minutes": closed_trade["hold_duration_minutes"],
        "exit_reason": closed_trade["exit_reason"],
        "execution_quality_flag": closed_trade["execution_quality_flag"],
        "broker_order_id_entry": closed_trade["broker_order_id_entry"],
        "broker_order_id_exit": closed_trade["broker_order_id_exit"],
        "ts_recorded_utc": closed_trade["ts_recorded_utc"],
        # Causal context
        "market_regime_at_entry": causal["market_regime_at_entry"],
        "volatility_at_entry": causal["volatility_at_entry"],
        "signal_strength": causal["signal_strength"],
        "signal_key": causal["signal_key"],
        "slippage_vs_expected_eur": causal["slippage_vs_expected_eur"],
        "entry_latency_ms": causal["entry_latency_ms"],
        # Feedback metadata
        "feedback_ts_utc": now_iso,
        "feedback_version": "1",
    }

    result = validate_live_feedback_record(record)
    if not result["ok"]:
        return _fail(f"feedback validation failed: {result['reason']}")

    return {
        "ok": True,
        "reason": "FEEDBACK_RECORD_BUILT",
        "feedback_record": result["normalized_record"],
    }
