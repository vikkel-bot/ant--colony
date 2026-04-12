"""
AC-162: Controlled Live Intake Gate

Opens the final intake gate only when every live condition is simultaneously
and explicitly true — blocking accidental or partial live activation.

One sentence: Opens the final intake gate only when all live conditions are
explicitly true, making accidental live activation technically impossible.

This gate is the last check before broker execution is permitted. It requires:
- intake record is structurally valid and live-capable
- lane, market, strategy_key, notional, operator approval all correct
- live_lane_config has all three activation sentinels set to True
- isolation flags confirm no shared state and no paper inputs
- macro freeze is clear
- auto-freeze is clear
- max_positions == 1

Dry mode (allow_broker_execution=False in intake) is always blocked here —
this gate is only for controlled live execution.

Fail-closed: any missing, invalid, or unsafe condition blocks execution.
No broker calls. No file IO. No paper pipeline imports. Never raises.
"""
from __future__ import annotations

from typing import Any

from ant_colony.live.broker_execution_intake_contract import validate_broker_execution_intake

_VALID_RISK_STATES = {"NORMAL", "CAUTION", "FREEZE"}


def evaluate_controlled_live_intake(
    intake_record: Any,
    live_lane_config: Any,
    macro_freeze_config: Any,
    auto_freeze_result: Any,
) -> dict[str, Any]:
    """
    Evaluate all conditions required for controlled live broker execution.

    Parameters:
        intake_record       — broker execution intake record (AC-150 shape)
        live_lane_config    — lane config dict with activation sentinels
        macro_freeze_config — macro freeze config dict
        auto_freeze_result  — pre-evaluated auto-freeze result dict

    Returns:
        {
            "allow": bool,
            "reason": str,
            "mode": "live" | "blocked"
        }

    Never raises. Fail-closed on any condition failure.
    """
    try:
        return _evaluate(intake_record, live_lane_config, macro_freeze_config, auto_freeze_result)
    except Exception as exc:  # noqa: BLE001
        return {
            "allow": False,
            "reason": f"unexpected controlled live intake error: {exc}",
            "mode": "blocked",
        }


def _block(reason: str) -> dict[str, Any]:
    return {"allow": False, "reason": reason, "mode": "blocked"}


def _evaluate(
    intake_record: Any,
    cfg: Any,
    macro: Any,
    auto_freeze: Any,
) -> dict[str, Any]:
    # --- intake shape validation (AC-150) ---
    shape_result = validate_broker_execution_intake(intake_record)
    if not shape_result["ok"]:
        return _block(f"intake shape invalid: {shape_result['reason']}")

    nr = shape_result["normalized_record"]

    # --- must be live-capable (not dry mode) ---
    if nr["allow_broker_execution"] is not True:
        return _block("allow_broker_execution must be true for live execution")

    # --- intake-level constraints ---
    if nr["lane"] != "live_test":
        return _block(f"lane must be 'live_test', got {nr['lane']!r}")

    if nr["market"] != "BNB-EUR":
        return _block(f"market must be 'BNB-EUR', got {nr['market']!r}")

    if nr["strategy_key"] != "EDGE3":
        return _block(f"strategy_key must be 'EDGE3', got {nr['strategy_key']!r}")

    if nr["qty"] <= 0:
        return _block(f"qty must be > 0, got {nr['qty']!r}")

    if nr["intended_entry_price"] <= 0:
        return _block(f"intended_entry_price must be > 0, got {nr['intended_entry_price']!r}")

    notional = nr["qty"] * nr["intended_entry_price"]
    if notional > 50:
        return _block(
            f"notional {notional:.4f} EUR exceeds 50 EUR limit"
        )

    if nr["operator_approved"] is not True:
        return _block("operator_approved must be true")

    if nr["risk_state"] == "FREEZE":
        return _block("INTAKE_FREEZE: risk_state is FREEZE")

    if nr["freeze_new_entries"] is True:
        return _block("INTAKE_FREEZE: freeze_new_entries is true")

    # --- live_lane_config ---
    if not isinstance(cfg, dict):
        return _block("live_lane_config must be a dict")

    if cfg.get("enabled") is not True:
        return _block("LANE_DISABLED: enabled must be true")

    if cfg.get("live_enabled") is not True:
        return _block("LIVE_DISABLED: live_enabled must be true")

    if cfg.get("allow_broker_execution") is not True:
        return _block("BROKER_EXECUTION_DISABLED: allow_broker_execution must be true")

    if cfg.get("allow_shared_state") is not False:
        return _block("ISOLATION_VIOLATION: allow_shared_state must be false")

    if cfg.get("allow_paper_inputs") is not False:
        return _block("ISOLATION_VIOLATION: allow_paper_inputs must be false")

    if cfg.get("max_positions") != 1:
        return _block(f"max_positions must be 1, got {cfg.get('max_positions')!r}")

    # --- macro freeze config ---
    if not isinstance(macro, dict):
        return _block("macro_freeze_config must be a dict")

    risk_state = macro.get("risk_state")
    if not isinstance(risk_state, str) or risk_state not in _VALID_RISK_STATES:
        return _block(f"macro_freeze_config invalid risk_state: {risk_state!r}")

    if risk_state == "FREEZE":
        return _block("MACRO_FREEZE_ACTIVE: risk_state is FREEZE")

    freeze_flag = macro.get("freeze_new_entries")
    if not isinstance(freeze_flag, bool):
        return _block(
            f"macro_freeze_config freeze_new_entries must be bool, "
            f"got {type(freeze_flag).__name__}"
        )

    if freeze_flag is True:
        return _block("MACRO_FREEZE_ACTIVE: freeze_new_entries is true")

    # --- auto-freeze result ---
    if not isinstance(auto_freeze, dict):
        return _block("auto_freeze_result must be a dict")

    if not auto_freeze.get("allow"):
        reason = auto_freeze.get("reason", "UNKNOWN")
        return _block(f"AUTO_FREEZE_ACTIVE: {reason}")

    return {
        "allow": True,
        "reason": "CONTROLLED_LIVE_INTAKE_ALLOWED",
        "mode": "live",
    }
