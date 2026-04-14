"""
AC-153 / AC-188: Live Execution Gate

Hard gate that must be fully open before live broker execution is allowed.
Every condition must be true simultaneously — no partial allows.

Fail-closed: any missing, invalid, or unsafe condition blocks execution.
No broker calls. No paper pipeline imports.

AC-188: File IO added for open-position guard only. Scans
{base_output_dir}/{lane}/execution/ for existing artifacts matching the
current market/strategy pair; cross-references {lane}/exit/ for closed
positions. If an execution exists with no corresponding exit →
OPEN_POSITION_EXISTS (blocks new entries). Fail-closed: unreadable
artifacts block immediately.

The repo default is live_enabled=false. Changing this to true is an explicit
operator action. Accidental activation is technically impossible within this
gate chain.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_SAFE_NAME_RE = re.compile(r"[^a-zA-Z0-9_\-]")

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


def _check_open_position(
    cfg: dict[str, Any],
    risk_state: str,
) -> dict[str, Any] | None:
    """
    AC-188: Block new entries when an open position already exists.

    Scans {base_output_dir}/{lane}/execution/ for JSON files whose market and
    strategy_key match the current config. For each match, checks whether a
    corresponding exit artifact exists in {lane}/exit/ (keyed by the execution's
    broker_order_id_entry). If an execution has no matching exit, the position
    is still open → return OPEN_POSITION_EXISTS block.

    Returns a _block() dict when blocked, None when the entry may proceed.
    Fail-closed: unreadable execution artifacts, or artifacts with missing
    broker_order_id_entry, block immediately.
    Never raises.
    """
    try:
        base_output_dir = cfg.get("base_output_dir")
        if not base_output_dir:
            return None   # no artifact dir configured — guard skipped

        lane     = str(cfg.get("lane") or "live")
        market   = str(cfg.get("market") or "")
        strategy = str(cfg.get("strategy") or "")

        exec_dir = Path(base_output_dir) / lane / "execution"
        exit_dir = Path(base_output_dir) / lane / "exit"

        if not exec_dir.exists():
            return None   # no executions yet → no open position

        for exec_file in sorted(exec_dir.glob("*.json")):
            # --- read execution artifact ---
            try:
                data = json.loads(exec_file.read_text(encoding="utf-8"))
            except Exception:
                return _block(
                    "OPEN_POSITION_EXISTS: execution artifact unreadable",
                    live_enabled=True,
                    allow_broker_execution=True,
                    risk_state=risk_state,
                )

            file_market   = str(data.get("market") or "")
            file_strategy = str(data.get("strategy_key") or data.get("strategy") or "")

            if file_market != market or file_strategy != strategy:
                continue   # different pair — ignore

            # --- execution matches this market/strategy ---
            broker_order_id = str(data.get("broker_order_id_entry") or "").strip()
            if not broker_order_id:
                return _block(
                    "OPEN_POSITION_EXISTS: execution artifact missing broker_order_id_entry",
                    live_enabled=True,
                    allow_broker_execution=True,
                    risk_state=risk_state,
                )

            safe_id   = _SAFE_NAME_RE.sub("_", broker_order_id)
            exit_file = exit_dir / f"{safe_id}.json"
            if not exit_file.exists():
                return _block(
                    "OPEN_POSITION_EXISTS",
                    live_enabled=True,
                    allow_broker_execution=True,
                    risk_state=risk_state,
                )

        return None   # all executions have exits — entry is safe
    except Exception:  # noqa: BLE001
        return _block(
            "OPEN_POSITION_EXISTS: position check failed unexpectedly",
            live_enabled=True,
            allow_broker_execution=True,
            risk_state=risk_state,
        )


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

    # --- AC-188: open position guard (entry path only) ---
    # Only runs when intake_record is present (i.e., an actual entry execution
    # attempt). Gate-check calls (no intake) and exit-executor calls (no intake)
    # are not affected.
    if intake is not None:
        pos_block = _check_open_position(cfg, risk_state)
        if pos_block is not None:
            return pos_block

    return {
        "allow": True,
        "reason": "LIVE_EXECUTION_GATE_OPEN",
        "live_enabled": True,
        "allow_broker_execution": True,
        "risk_state": risk_state,
    }
