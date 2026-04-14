"""
AC-161: Queen Memory Store

Converts a valid feedback record into a compact, causally-rich memory entry
that the Queen can read — without changing any behavior, weights, or settings.

One sentence: Converts a valid feedback record into a compact queen memory
entry that preserves causal context and flags whether queen attention is needed.

Memory entry contains:
  - trade outcome (pnl, win/loss label, hold duration, exit reason)
  - execution quality (anomaly_flag, execution_quality_flag)
  - causal context (regime, volatility, signal, slippage_vs_expected, latency)
  - derived flags (queen_action_required)

Deliberately excluded from the memory entry:
  - raw broker order IDs (broker detail not needed for queen learning)
  - raw slippage_eur (slippage_vs_expected_eur is more informative)
  - ts_recorded_utc (internal execution timestamp)

queen_action_required = True when any of:
  - anomaly_flag is True (execution_quality_flag != "OK")
  - market_regime_at_entry == "UNKNOWN"  (queen cannot interpret this trade)
  - volatility_at_entry == "UNKNOWN"     (queen cannot interpret this trade)

This step is intake + memory formation only.
No learning, no weight updates, no strategy changes, no allocation changes.

No broker calls. No file IO. No paper pipeline imports. Never raises.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ant_colony.live.live_feedback_schema import validate_live_feedback_record


def build_queen_memory_entry(live_feedback_record: Any) -> dict[str, Any]:
    """
    Build a compact queen memory entry from a live feedback record.

    Parameters:
        live_feedback_record — AC-159 validated feedback record dict

    Returns:
        {
            "ok": bool,
            "reason": str,
            "memory_entry": dict | None
        }

    Never raises. Fail-closed on any invalid input.
    """
    try:
        return _build(live_feedback_record)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected memory store error: {exc}",
            "memory_entry": None,
        }


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "memory_entry": None}


def _build(record: Any) -> dict[str, Any]:
    # Validate against AC-159 schema
    schema_result = validate_live_feedback_record(record)
    if not schema_result["ok"]:
        return _fail(f"feedback schema invalid: {schema_result['reason']}")

    nr = schema_result["normalized_record"]

    # --- win/loss label ---
    pnl = nr["realized_pnl_eur"]
    if pnl > 0:
        win_loss_label = "WIN"
    elif pnl < 0:
        win_loss_label = "LOSS"
    else:
        win_loss_label = "FLAT"

    # --- anomaly flag ---
    # An anomaly is any execution that did not fill cleanly.
    anomaly_flag = nr["execution_quality_flag"] != "OK"

    # --- queen action required ---
    # The queen must look at this record if execution was anomalous,
    # or if the causal context is incomplete (UNKNOWN regime or volatility
    # means the queen cannot fully interpret the trade).
    queen_action_required = (
        anomaly_flag
        or nr["market_regime_at_entry"] == "UNKNOWN"
        or nr["volatility_at_entry"] == "UNKNOWN"
    )

    memory_ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # AC-189: A trade is only closed when a proven exit exists.
    # broker_order_id_exit == "ENTRY_ONLY_PENDING_EXIT" means the entry has
    # no corresponding exit yet — do not label it as a closed trade.
    _exit_id = nr.get("broker_order_id_exit", "")
    _is_closed = bool(_exit_id) and _exit_id != "ENTRY_ONLY_PENDING_EXIT"
    record_type = "closed_trade_memory" if _is_closed else "open_trade_memory"

    memory_entry = {
        "memory_version": "1",
        "record_type": record_type,
        "lane": nr["lane"],
        "market": nr["market"],
        "strategy_key": nr["strategy_key"],
        "trade_id": nr["trade_id"],
        "entry_ts_utc": nr["entry_ts_utc"],
        "exit_ts_utc": nr["exit_ts_utc"],
        "hold_duration_minutes": nr["hold_duration_minutes"],
        "realized_pnl_eur": nr["realized_pnl_eur"],
        "win_loss_label": win_loss_label,
        "exit_reason": nr["exit_reason"],
        "anomaly_flag": anomaly_flag,
        "execution_quality_flag": nr["execution_quality_flag"],
        "market_regime_at_entry": nr["market_regime_at_entry"],
        "volatility_at_entry": nr["volatility_at_entry"],
        "signal_strength": nr["signal_strength"],
        "signal_key": nr["signal_key"],
        "slippage_vs_expected_eur": nr["slippage_vs_expected_eur"],
        "entry_latency_ms": nr["entry_latency_ms"],
        "feedback_ts_utc": nr["feedback_ts_utc"],
        "memory_ts_utc": memory_ts_utc,
        "queen_action_required": queen_action_required,
    }

    return {
        "ok": True,
        "reason": "QUEEN_MEMORY_READY",
        "memory_entry": memory_entry,
    }
