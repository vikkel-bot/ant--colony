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
        result = _execute(
            intake_record, live_lane_config, macro_freeze_config, auto_freeze_result, _adapter
        )
        result.pop("_broker_response", None)  # strip internal key; not part of public API
        return result
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected executor error: {exc}",
            "gate": "EXECUTOR",
            "execution_result": None,
        }


def execute_and_persist_live_order(
    intake_record: Any,
    live_lane_config: Any,
    macro_freeze_config: Any,
    auto_freeze_result: Any,
    *,
    _adapter: Any = None,
    _writer: Any = None,
) -> dict[str, Any]:
    """
    Execute one live entry order and persist artifacts to disk (AC-167).

    Identical gate chain to execute_first_live_order (A → I), then writes:
        {base_output_dir}/{lane}/execution/{trade_id}.json
        {base_output_dir}/{lane}/broker/{trade_id}.json

    Parameters:
        intake_record       — AC-150 validated intake dict
        live_lane_config    — lane config; must contain "base_output_dir"
        macro_freeze_config — AC-147 macro freeze config
        auto_freeze_result  — pre-evaluated AC-155 result dict
        _adapter            — injectable broker adapter (BitvavoAdapter by default)
        _writer             — injectable artifact writer for tests; defaults to
                              live_artifact_writer.write_entry_artifacts

    Returns:
        {
            "ok": bool,
            "reason": str,
            "gate": str,
            "execution_result": dict | None,
            "artifacts": {"execution": str, "broker": str}  # only when ok=True and base_output_dir set
        }

    Fail-closed: if persistence fails, ok=False, gate="J_PERSIST", execution_result preserved.
    If base_output_dir is absent from live_lane_config, execution succeeds without persisting.
    Never raises.
    """
    try:
        return _execute_and_persist(
            intake_record, live_lane_config, macro_freeze_config, auto_freeze_result,
            _adapter, _writer
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


def _execute_and_persist(
    intake_record: Any,
    live_lane_config: Any,
    macro_freeze_config: Any,
    auto_freeze_result: Any,
    adapter: Any,
    writer: Any,
) -> dict[str, Any]:
    inner = _execute(
        intake_record, live_lane_config, macro_freeze_config, auto_freeze_result, adapter
    )
    broker_response = inner.pop("_broker_response", None)

    if not inner["ok"]:
        return inner

    base_output_dir = (
        live_lane_config.get("base_output_dir")
        if isinstance(live_lane_config, dict)
        else None
    )
    if not base_output_dir:
        return inner  # no output dir configured — not a failure

    lane = (
        (intake_record.get("lane") if isinstance(intake_record, dict) else None)
        or (live_lane_config.get("lane") if isinstance(live_lane_config, dict) else None)
        or "live"
    )

    if writer is None:
        from ant_colony.live.live_artifact_writer import write_entry_artifacts as writer  # type: ignore[assignment]

    write_result = writer(base_output_dir, lane, inner["execution_result"], broker_response or {})

    if not write_result["ok"]:
        return {
            "ok": False,
            "reason": f"ORDER_EXECUTED_PERSISTENCE_FAILED: {write_result['reason']}",
            "gate": "J_PERSIST",
            "execution_result": inner["execution_result"],
        }

    # --- AC-173: feedback + memory artifacts ---
    # Causal context is not available at entry time; use explicit sentinel values.
    # All sentinels are valid per live_feedback_schema.py:
    #   UNKNOWN regime/volatility → queen_action_required=True (correct: review needed)
    #   signal_strength -1.0      → schema explicitly allows "not available"
    _sentinel_causal = {
        "market_regime_at_entry": "UNKNOWN",
        "volatility_at_entry": "UNKNOWN",
        "signal_strength": -1.0,
        "signal_key": "UNKNOWN",
        "slippage_vs_expected_eur": 0.0,
        "entry_latency_ms": 0,
    }

    from ant_colony.live.live_feedback_builder import build_live_feedback_record
    from ant_colony.live.queen_feedback_intake import intake_feedback_for_queen
    from ant_colony.live.queen_memory_store import build_queen_memory_entry
    from ant_colony.live.live_artifact_writer import (
        write_feedback_artifact,
        write_memory_artifact,
    )

    fb_build = build_live_feedback_record(inner["execution_result"], _sentinel_causal)
    if not fb_build["ok"]:
        return {
            "ok": False,
            "reason": f"FEEDBACK_BUILD_FAILED: {fb_build['reason']}",
            "gate": "K_FEEDBACK",
            "execution_result": inner["execution_result"],
        }

    queen_intake = intake_feedback_for_queen(fb_build["feedback_record"])
    if not queen_intake["ok"]:
        return {
            "ok": False,
            "reason": f"QUEEN_INTAKE_FAILED: {queen_intake['reason']}",
            "gate": "K_FEEDBACK",
            "execution_result": inner["execution_result"],
        }

    fb_write = write_feedback_artifact(base_output_dir, lane, queen_intake["accepted_feedback"])
    if not fb_write["ok"]:
        return {
            "ok": False,
            "reason": f"FEEDBACK_WRITE_FAILED: {fb_write['reason']}",
            "gate": "K_FEEDBACK",
            "execution_result": inner["execution_result"],
        }

    mem_build = build_queen_memory_entry(queen_intake["accepted_feedback"])
    if not mem_build["ok"]:
        return {
            "ok": False,
            "reason": f"MEMORY_BUILD_FAILED: {mem_build['reason']}",
            "gate": "K_FEEDBACK",
            "execution_result": inner["execution_result"],
        }

    mem_write = write_memory_artifact(base_output_dir, lane, mem_build["memory_entry"])
    if not mem_write["ok"]:
        return {
            "ok": False,
            "reason": f"MEMORY_WRITE_FAILED: {mem_write['reason']}",
            "gate": "K_FEEDBACK",
            "execution_result": inner["execution_result"],
        }

    all_artifacts = {
        **write_result["paths"],
        **fb_write["paths"],
        **mem_write["paths"],
    }
    return {**inner, "artifacts": all_artifacts}


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
        "_broker_response": broker_response,  # consumed by _execute_and_persist; stripped by execute_first_live_order
    }
