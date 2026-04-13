"""
AC-154 / AC-162: Bitvavo Live Executor

Executes one live entry order through the complete gate chain and, if all
gates pass, places a real order on Bitvavo and returns an AC-148-compatible
entry execution record.

One sentence: Runs the complete gate chain and, when every gate is open,
places a real order on Bitvavo and returns a validated AC-148 entry record.

Gate order (mandatory):
  A.  Intake shape validation (AC-150)
  A'. Controlled live intake gate (AC-162) — all live conditions must be true
  B.  Broker request builder (AC-151)
  C.  Adapter bridge (AC-152)
  G.  Real broker call (BitvavoAdapter.place_order)
  H.  Order reconciliation
  I.  AC-148 schema validation

Fail-closed: any gate failure stops execution and returns ok=False.
No paper pipeline imports. No file IO beyond the adapter ops log.
Never raises.
"""
from __future__ import annotations

import os
from typing import Any

from ant_colony.live.broker_execution_intake_contract import validate_broker_execution_intake
from ant_colony.live.broker_request_builder import build_broker_request
from ant_colony.live.broker_adapter_bridge import build_broker_adapter_command
from ant_colony.live.controlled_live_intake_gate import evaluate_controlled_live_intake
from ant_colony.live.live_order_reconciler import reconcile_live_order
from ant_colony.live.live_execution_result_schema import validate_live_execution_result


def execute_first_live_order(
    intake_record: Any,
    live_lane_config: Any,
    macro_freeze_config: Any,
    auto_freeze_result: Any,
    *,
    _adapter: Any = None,
) -> dict[str, Any]:
    """
    Execute one live entry order through the full A-I gate chain.

    Parameters:
        intake_record       — AC-150 validated intake dict
        live_lane_config    — lane config with live_enabled=True, allow_broker_execution=True
        macro_freeze_config — AC-147 macro freeze config
        auto_freeze_result  — pre-evaluated AC-155 result dict
        _adapter            — injectable broker adapter (BitvavoAdapter by default)

    Returns:
        {
            "ok": bool,
            "reason": str,
            "gate": str,                  # last gate reached (or gate that blocked)
            "execution_result": dict | None  # AC-148 record when ok=True
        }

    Never raises. Fail-closed on any gate failure or unexpected error.
    """
    try:
        return _execute(
            intake_record, live_lane_config, macro_freeze_config, auto_freeze_result, _adapter
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected executor error: {exc}",
            "gate": "EXECUTOR",
            "execution_result": None,
        }


def _fail(reason: str, gate: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "gate": gate, "execution_result": None}


def _execute(
    intake_record: Any,
    live_lane_config: Any,
    macro_freeze_config: Any,
    auto_freeze_result: Any,
    adapter: Any,
) -> dict[str, Any]:
    # --- Gate A: intake shape validation (AC-150) ---
    intake_result = validate_broker_execution_intake(intake_record)
    if not intake_result["ok"]:
        return _fail(f"INTAKE_INVALID: {intake_result['reason']}", "A_INTAKE")

    # --- Gate A': controlled live intake (AC-162) ---
    # All live conditions must be simultaneously true before broker call.
    live_gate = evaluate_controlled_live_intake(
        intake_record, live_lane_config, macro_freeze_config, auto_freeze_result
    )
    if not live_gate["allow"]:
        return _fail(f"CONTROLLED_LIVE_GATE_BLOCKED: {live_gate['reason']}", "A_CONTROLLED_LIVE")

    # --- Gate B: broker request builder (AC-151) ---
    req_result = build_broker_request(intake_record)
    if not req_result["ok"]:
        return _fail(f"REQUEST_BUILD_FAILED: {req_result['reason']}", "B_REQUEST_BUILDER")

    # --- Gate C: adapter bridge (AC-152) ---
    cmd_result = build_broker_adapter_command(intake_record)
    if not cmd_result["ok"]:
        return _fail(f"ADAPTER_BRIDGE_FAILED: {cmd_result['reason']}", "C_ADAPTER_BRIDGE")

    # --- Gate G: real broker call ---
    adapter_command = cmd_result["adapter_command"]
    payload = adapter_command["payload"]

    # Resolve operatorId before broker call: intake payload first, then env var.
    # Fail-closed: block with a clear reason if neither source provides a value.
    operator_id = intake_record.get("operator_id") or os.getenv("BITVAVO_OPERATOR_ID")
    if not operator_id:
        return _fail("BROKER_CONFIG_INVALID: operatorId missing", "G_BROKER_CALL")

    order_request = {
        "market": payload["market"],
        "side": payload["side"],
        "order_type": payload["order_type"],
        "qty": payload["qty"],
        "intended_entry_price": payload["intended_entry_price"],
        "max_notional_eur": payload["max_notional_eur"],
        "client_request_id": adapter_command["client_request_id"],
        "operator_id": operator_id,
    }

    if adapter is None:
        from ant_colony.broker_adapters.bitvavo_adapter import BitvavoAdapter
        adapter = BitvavoAdapter()

    broker_response = adapter.place_order(order_request)

    if not broker_response.get("ok"):
        err = broker_response.get("error") or {}
        msg = err.get("message", "unknown") if isinstance(err, dict) else str(err)
        return _fail(f"BROKER_CALL_FAILED: {msg}", "G_BROKER_CALL")

    # --- Gate H: reconcile ---
    reconcile_result = reconcile_live_order(intake_record, broker_response)
    if not reconcile_result["ok"]:
        return _fail(f"RECONCILE_FAILED: {reconcile_result['reason']}", "H_RECONCILE")

    # --- Gate I: AC-148 schema validation ---
    schema_result = validate_live_execution_result(reconcile_result["execution_result"])
    if not schema_result["ok"]:
        return _fail(f"SCHEMA_INVALID: {schema_result['reason']}", "I_SCHEMA")

    return {
        "ok": True,
        "reason": "ORDER_EXECUTED",
        "gate": "I_SCHEMA",
        "execution_result": schema_result["normalized_record"],
    }
