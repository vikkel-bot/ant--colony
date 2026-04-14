"""
AC-191: EDGE3 Exit Signal

Evaluates take-profit and stop-loss conditions for an open long position.
Pure computation — no IO, no broker calls, no file access.

Rules (long position only):
  TP: current_price > entry_price * (1 + tp_pct)  →  exit_reason = "TP"
  SL: current_price < entry_price * (1 - sl_pct)  →  exit_reason = "SL"
  Otherwise: no exit (return None)

Config keys consumed: sl_pct, tp_pct
Fail-closed: invalid input returns {"ok": False, "reason": str}. Never raises.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def evaluate_exit_signal(
    execution_artifact: dict[str, Any],
    config: dict[str, Any],
    current_price: float,
) -> dict[str, Any] | None:
    """
    Evaluate TP/SL for an open long position.

    Parameters:
        execution_artifact — execution artifact dict; must contain entry_price,
                             qty, position_side, market, strategy_key, lane,
                             broker_order_id_entry
        config             — live lane config; must contain sl_pct and tp_pct
        current_price      — current market price (float > 0)

    Returns:
        exit_intent dict   when TP or SL is triggered
        None               when price is within range (no exit)
        {"ok": False, ...} on invalid input (fail-closed)

    Never raises.
    """
    try:
        return _evaluate(execution_artifact, config, current_price)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"exit signal error: {exc}"}


def _evaluate(
    artifact: Any,
    config: Any,
    current_price: Any,
) -> dict[str, Any] | None:
    if not isinstance(artifact, dict):
        return {"ok": False, "reason": "execution_artifact must be a dict"}
    if not isinstance(config, dict):
        return {"ok": False, "reason": "config must be a dict"}
    if (
        not isinstance(current_price, (int, float))
        or isinstance(current_price, bool)
        or current_price <= 0
    ):
        return {"ok": False, "reason": f"current_price must be numeric > 0, got {current_price!r}"}

    for field in (
        "entry_price", "qty", "position_side",
        "market", "strategy_key", "lane", "broker_order_id_entry",
    ):
        if field not in artifact:
            return {"ok": False, "reason": f"execution_artifact missing field: {field}"}

    entry_price = artifact["entry_price"]
    if (
        not isinstance(entry_price, (int, float))
        or isinstance(entry_price, bool)
        or entry_price <= 0
    ):
        return {"ok": False, "reason": f"entry_price must be numeric > 0, got {entry_price!r}"}

    # Only long positions are supported.
    if artifact.get("position_side") != "long":
        return None

    sl_pct = config.get("sl_pct")
    tp_pct = config.get("tp_pct")
    if (
        not isinstance(sl_pct, (int, float))
        or isinstance(sl_pct, bool)
        or sl_pct <= 0
    ):
        return {"ok": False, "reason": f"sl_pct must be numeric > 0, got {sl_pct!r}"}
    if (
        not isinstance(tp_pct, (int, float))
        or isinstance(tp_pct, bool)
        or tp_pct <= 0
    ):
        return {"ok": False, "reason": f"tp_pct must be numeric > 0, got {tp_pct!r}"}

    sl_threshold = entry_price * (1.0 - sl_pct)
    tp_threshold = entry_price * (1.0 + tp_pct)

    if current_price > tp_threshold:
        exit_reason = "TP"
    elif current_price < sl_threshold:
        exit_reason = "SL"
    else:
        return None  # within range — no exit

    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "lane":             artifact["lane"],
        "market":           artifact["market"],
        "strategy_key":     artifact["strategy_key"],
        "position_side":    artifact["position_side"],
        "order_side":       "sell",
        "qty":              artifact["qty"],
        "exit_reason":      exit_reason,
        "operator_approved": True,
        "entry_order_id":   artifact["broker_order_id_entry"],
        "entry_price":      entry_price,
        "ts_intent_utc":    ts_now,
    }
