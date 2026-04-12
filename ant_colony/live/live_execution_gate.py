"""
AC-153: Live Execution Gate

Hard gate that must be fully open before live broker execution is allowed.
Every condition must be true simultaneously — no partial allows.

Fail-closed: any missing, invalid, or unsafe condition blocks execution.
No broker calls. No file IO. No paper pipeline imports.

The repo default is live_enabled=false. Changing this to true is an explicit
operator action. Accidental activation is technically impossible within this
gate chain.
"""
from __future__ import annotations

from typing import Any

from ant_colony.live.broker_execution_intake_contract import validate_broker_execution_intake

_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}
_VALID_RISK_STATES = {"NORMAL", "CAUTION", "FREEZE"}


def evaluate_live_execution_gate(
    live_lane_config: dict[str, Any],
    macro_freeze_config: dict[str, Any],
    intake_record: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Evaluate all live execution gate conditions.

    Returns:
        {
            "allow": bool,
            "reason": str,
            "live_enabled": bool | None,
            "allow_broker_execution": bool | None,
            "risk_state": str
        }

    Never raises. Fail-closed on any violation.
    """
    try:
        return _evaluate(live_lane_config, macro_freeze_config, intake_record)
    except Exception as exc:  # noqa: BLE001
        return {
            "allow": False,
            "reason": f"unexpected gate error: {exc}",
            "live_enabled": None,
            "allow_broker_execution": None,
            "risk_state": "UNKNOWN",
        }


def _block(
    reason: str,
    live_enabled: Any = False,
    allow_broker_execution: Any = False,
    risk_state: str = "UNKNOWN",
) -> dict[str, Any]:
    return {
        "allow": False,
        "reason": reason,
        "live_enabled": live_enabled,
        "allow_broker_execution": allow_broker_execution,
        "risk_state": risk_state,
    }


def _evaluate(
    cfg: Any,
    macro: Any,
    intake: dict[str, Any] | None,
) -> dict[str, Any]:
    # --- live_lane_config must be a dict ---
    if not isinstance(cfg, dict):
        return _block("live_lane_config must be a dict")

    # --- lane config: enabled ---
    if not isinstance(cfg.get("enabled"), bool):
        return _block("enabled must be bool", live_enabled=cfg.get("live_enabled"))
    if not cfg["enabled"]:
        return _block(
            "LANE_DISABLED",
            live_enabled=cfg.get("live_enabled", False),
            allow_broker_execution=cfg.get("allow_broker_execution", False),
        )

    # --- live_enabled: must exist and be explicitly true ---
    if "live_enabled" not in cfg:
        return _block("LIVE_DISABLED: live_enabled field missing")
    if not isinstance(cfg["live_enabled"], bool):
        return _block(
            f"live_enabled must be bool, got {type(cfg['live_enabled']).__name__}"
        )
    if not cfg["live_enabled"]:
        return _block(
            "LIVE_DISABLED",
            live_enabled=False,
            allow_broker_execution=cfg.get("allow_broker_execution", False),
        )

    # --- allow_broker_execution: must be explicitly true ---
    if cfg.get("allow_broker_execution") is not True:
        return _block(
            "BROKER_EXECUTION_DISABLED",
            live_enabled=True,
            allow_broker_execution=cfg.get("allow_broker_execution", False),
        )

    # --- market ---
    if cfg.get("market") not in _VALID_MARKETS:
        return _block(
            f"invalid market: {cfg.get('market')!r}",
            live_enabled=True,
            allow_broker_execution=True,
        )

    # --- strategy ---
    if cfg.get("strategy") not in _VALID_STRATEGIES:
        return _block(
            f"invalid strategy: {cfg.get('strategy')!r}",
            live_enabled=True,
            allow_broker_execution=True,
        )

    # --- max_notional_eur ---
    notional = cfg.get("max_notional_eur")
    if not isinstance(notional, (int, float)) or isinstance(notional, bool) \
            or notional <= 0 or notional > 50:
        return _block(
            f"max_notional_eur must be > 0 and <= 50, got {notional!r}",
            live_enabled=True,
            allow_broker_execution=True,
        )

    # --- max_positions ---
    if cfg.get("max_positions") != 1:
        return _block(
            f"max_positions must be 1, got {cfg.get('max_positions')!r}",
            live_enabled=True,
            allow_broker_execution=True,
        )

    # --- isolation flags ---
    if cfg.get("allow_shared_state") is not False:
        return _block(
            "allow_shared_state must be false",
            live_enabled=True,
            allow_broker_execution=True,
        )
    if cfg.get("allow_paper_inputs") is not False:
        return _block(
            "allow_paper_inputs must be false",
            live_enabled=True,
            allow_broker_execution=True,
        )

    # --- macro freeze config ---
    if not isinstance(macro, dict):
        return _block(
            "macro_freeze_config must be a dict",
            live_enabled=True,
            allow_broker_execution=True,
        )

    risk_state = macro.get("risk_state", "UNKNOWN")
    if not isinstance(risk_state, str) or risk_state not in _VALID_RISK_STATES:
        return _block(
            f"invalid risk_state: {risk_state!r}",
            live_enabled=True,
            allow_broker_execution=True,
            risk_state=str(risk_state),
        )
    if risk_state == "FREEZE":
        return _block(
            "MACRO_FREEZE_ACTIVE: risk_state is FREEZE",
            live_enabled=True,
            allow_broker_execution=True,
            risk_state=risk_state,
        )

    freeze_flag = macro.get("freeze_new_entries")
    if not isinstance(freeze_flag, bool):
        return _block(
            f"freeze_new_entries must be bool, got {type(freeze_flag).__name__}",
            live_enabled=True,
            allow_broker_execution=True,
            risk_state=risk_state,
        )
    if freeze_flag:
        return _block(
            "MACRO_FREEZE_ACTIVE: freeze_new_entries is true",
            live_enabled=True,
            allow_broker_execution=True,
            risk_state=risk_state,
        )

    # --- optional intake record ---
    if intake is not None:
        intake_result = validate_broker_execution_intake(intake)
        if not intake_result["ok"]:
            return _block(
                f"intake invalid: {intake_result['reason']}",
                live_enabled=True,
                allow_broker_execution=True,
                risk_state=risk_state,
            )
        operator_approved = intake.get("operator_approved")
        if not isinstance(operator_approved, bool):
            return _block(
                "operator_approved must be bool",
                live_enabled=True,
                allow_broker_execution=True,
                risk_state=risk_state,
            )
        if not operator_approved:
            return _block(
                "OPERATOR_NOT_APPROVED",
                live_enabled=True,
                allow_broker_execution=True,
                risk_state=risk_state,
            )

    return {
        "allow": True,
        "reason": "LIVE_EXECUTION_GATE_OPEN",
        "live_enabled": True,
        "allow_broker_execution": True,
        "risk_state": risk_state,
    }
