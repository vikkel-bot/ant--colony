"""
AC-147: Macro Freeze Guard

Validates macro_freeze_config.json and decides whether new lane activity
is allowed. Fail-closed: invalid config or any freeze condition blocks.

No broker calls. No external feeds. No paper pipeline imports.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_VALID_RISK_STATES = {"NORMAL", "CAUTION", "FREEZE"}
_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "macro_freeze_config.json"


def load_macro_config(path: Path = _DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load macro freeze config from JSON. Returns empty dict on error (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def check(config: dict[str, Any]) -> dict[str, Any]:
    """
    Validate macro freeze config and return an allow/block decision.

    Returns:
        {
            "allow": bool,
            "reason": str,
            "risk_state": str,
            "component": "macro_freeze_guard"
        }
    """
    def _block(reason: str, risk_state: str = "UNKNOWN") -> dict[str, Any]:
        return {
            "allow": False,
            "reason": reason,
            "risk_state": risk_state,
            "component": "macro_freeze_guard",
        }

    # risk_state must be present and valid
    risk_state = config.get("risk_state")
    if not isinstance(risk_state, str) or risk_state not in _VALID_RISK_STATES:
        return _block(
            f"invalid risk_state: {risk_state!r}; must be one of {sorted(_VALID_RISK_STATES)}",
            risk_state=str(risk_state) if risk_state is not None else "MISSING",
        )

    # freeze_new_entries must be bool
    freeze_flag = config.get("freeze_new_entries")
    if not isinstance(freeze_flag, bool):
        return _block(
            f"freeze_new_entries must be bool, got {type(freeze_flag).__name__}",
            risk_state=risk_state,
        )

    # Decision: FREEZE state always blocks
    if risk_state == "FREEZE":
        reason = config.get("reason") or "risk_state is FREEZE"
        return _block(f"macro freeze active: {reason}", risk_state=risk_state)

    # Decision: explicit freeze flag blocks regardless of risk_state
    if freeze_flag is True:
        reason = config.get("reason") or "freeze_new_entries is true"
        return _block(f"macro freeze active: {reason}", risk_state=risk_state)

    return {
        "allow": True,
        "reason": "no freeze active",
        "risk_state": risk_state,
        "component": "macro_freeze_guard",
    }
