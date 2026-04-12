"""
AC-158: Bitvavo Live Exit Executor

Closes one existing live position only when all safety gates are open, using
a market sell order placed through the existing BitvavoAdapter.

One sentence: Closes one existing live position only when every safety gate is
open, blocking duplicate close attempts and failing closed on any gate failure.

Gate order:
  1. exit_intent structure and content validation
  2. Live execution gate (AC-153): lane enabled, live_enabled, allow_broker_execution,
     market, strategy, notional, positions, isolation flags, macro clear
  3. Explicit macro freeze check (defense-in-depth)
  4. Auto-freeze check (AC-155)
  5. Duplicate exit guard (session-scoped, keyed on entry_order_id)
  6. Real broker call (market sell via BitvavoAdapter)

No hidden state machine. No partial close. No staged exits.
Only market orders (simplest safe close). No short exit in this phase.
Fail-closed. Never raises.
No paper pipeline imports. No file IO beyond adapter ops log.
"""
from __future__ import annotations

from typing import Any

from ant_colony.live.live_execution_gate import evaluate_live_execution_gate

_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}

_EXIT_INTENT_REQUIRED = (
    "lane", "market", "strategy_key", "position_side", "order_side",
    "qty", "exit_reason", "operator_approved", "entry_order_id",
    "entry_price", "ts_intent_utc",
)

# Session-scoped duplicate exit guard: maps entry_order_id → True once closed.
# Prevents double-close within a single process session.
# Reset via _reset_exit_dedup_for_testing() in test suites only.
_CLOSED_ENTRY_ORDER_IDS: set[str] = set()


def _reset_exit_dedup_for_testing() -> None:
    """Reset the session-scoped duplicate exit guard. For use in tests only."""
    _CLOSED_ENTRY_ORDER_IDS.clear()


def execute_live_exit(
    exit_intent: Any,
    live_lane_config: Any,
    macro_freeze_config: Any,
    auto_freeze_result: Any,
    *,
    _adapter: Any = None,
) -> dict[str, Any]:
    """
    Execute a live position close through the full gate chain.

    Parameters:
        exit_intent         — AC-158 exit intent dict (from build_live_exit_intent)
        live_lane_config    — lane config with live_enabled=True, allow_broker_execution=True
        macro_freeze_config — AC-147 macro freeze config
        auto_freeze_result  — pre-evaluated AC-155 result dict
        _adapter            — injectable broker adapter (BitvavoAdapter by default)

    Returns:
        {
            "ok": bool,
            "reason": str,
            "exit_execution_raw": dict | None  # broker response when ok=True
        }

    Never raises. Fail-closed on any gate failure or unexpected error.
    """
    try:
        return _execute(
            exit_intent, live_lane_config, macro_freeze_config, auto_freeze_result, _adapter
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected executor error: {exc}",
            "exit_execution_raw": None,
        }


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "exit_execution_raw": None}


def _execute(
    exit_intent: Any,
    live_lane_config: Any,
    macro_freeze_config: Any,
    auto_freeze_result: Any,
    adapter: Any,
) -> dict[str, Any]:
    # --- Gate 1: exit_intent structure ---
    if not isinstance(exit_intent, dict):
        return _fail("exit_intent must be a dict")

    for field in _EXIT_INTENT_REQUIRED:
        if field not in exit_intent:
            return _fail(f"exit_intent missing required field: {field}")

    if exit_intent.get("market") not in _VALID_MARKETS:
        return _fail(f"exit market not allowed: {exit_intent.get('market')!r}")
    if exit_intent.get("strategy_key") not in _VALID_STRATEGIES:
        return _fail(f"exit strategy_key not allowed: {exit_intent.get('strategy_key')!r}")
    if exit_intent.get("operator_approved") is not True:
        return _fail("operator_approved must be True")

    qty = exit_intent.get("qty")
    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty <= 0:
        return _fail(f"qty must be numeric > 0, got {qty!r}")

    entry_order_id = exit_intent.get("entry_order_id", "")
    if not isinstance(entry_order_id, str) or not entry_order_id.strip():
        return _fail("entry_order_id must be non-empty")

    # --- Gate 2: live execution gate (covers lane, macro, isolation) ---
    gate_result = evaluate_live_execution_gate(live_lane_config, macro_freeze_config)
    if not gate_result["allow"]:
        return _fail(f"LIVE_GATE_BLOCKED: {gate_result['reason']}")

    # --- Gate 3: explicit macro freeze (defense-in-depth) ---
    if not isinstance(macro_freeze_config, dict):
        return _fail("MACRO_CONFIG_INVALID: must be a dict")
    if macro_freeze_config.get("risk_state") == "FREEZE":
        return _fail("MACRO_FREEZE_ACTIVE: risk_state is FREEZE")
    if macro_freeze_config.get("freeze_new_entries") is True:
        return _fail("MACRO_FREEZE_ACTIVE: freeze_new_entries is true")

    # --- Gate 4: auto-freeze ---
    if not isinstance(auto_freeze_result, dict):
        return _fail("AUTO_FREEZE_RESULT_INVALID: must be a dict")
    if not auto_freeze_result.get("allow"):
        reason = auto_freeze_result.get("reason", "UNKNOWN")
        return _fail(f"AUTO_FREEZE_ACTIVE: {reason}")

    # --- Gate 5: duplicate exit guard ---
    if entry_order_id in _CLOSED_ENTRY_ORDER_IDS:
        return _fail(
            f"DUPLICATE_EXIT_BLOCKED: entry_order_id {entry_order_id!r} "
            "already closed this session"
        )

    # --- Build close order (market sell only) ---
    ts_slug = (
        exit_intent["ts_intent_utc"]
        .replace(":", "")
        .replace("-", "")
        .replace("Z", "")
        .replace("T", "")
    )
    client_request_id = (
        f"EXIT_{exit_intent['lane']}_"
        f"{exit_intent['market'].replace('-', '')}_"
        f"{exit_intent['strategy_key']}_"
        f"{exit_intent['order_side']}_"
        f"{ts_slug}"
    )

    order_request = {
        "market": exit_intent["market"],
        "side": exit_intent["order_side"],
        "order_type": "market",
        "qty": qty,
        "client_request_id": client_request_id,
    }

    if adapter is None:
        from ant_colony.broker_adapters.bitvavo_adapter import BitvavoAdapter
        adapter = BitvavoAdapter()

    broker_response = adapter.place_order(order_request)

    if not broker_response.get("ok"):
        err = broker_response.get("error") or {}
        msg = err.get("message", "unknown") if isinstance(err, dict) else str(err)
        return _fail(f"BROKER_CALL_FAILED: {msg}")

    # Mark closed — prevents duplicate exit for this entry_order_id
    _CLOSED_ENTRY_ORDER_IDS.add(entry_order_id)

    return {
        "ok": True,
        "reason": "LIVE_EXIT_EXECUTED",
        "exit_execution_raw": broker_response,
    }
