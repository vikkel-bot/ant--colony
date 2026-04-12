"""
AC-146/AC-147/AC-153: Live Lane Runner

Loads live lane config + macro freeze config, runs all guards, emits JSON.

Gate order (fail-closed at each step):
  1. Live lane guard (isolation constraints)
  2. Enabled flag
  3. Macro freeze guard
  4. Live execution gate (live_enabled + allow_broker_execution)

Constraints (hard):
- No broker calls
- No reads from paper/simulation artefacts
- No writes outside own lane scope
- Output is JSON only
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ant_colony.live.live_lane_guard import validate
from ant_colony.live.macro_freeze_guard import check as macro_check
from ant_colony.live.macro_freeze_guard import load_macro_config
from ant_colony.live.live_execution_gate import evaluate_live_execution_gate

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "live_lane_config.json"


def load_config(path: Path = _DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load lane config from JSON. Returns empty dict on error (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def run(
    config: dict[str, Any] | None = None,
    macro_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Run all live lane guards and return a status dict.
    Does not perform broker calls or read paper artefacts.
    """
    if config is None:
        config = load_config()

    guard = validate(config)
    market = config.get("market", "unknown")
    strategy = config.get("strategy", "unknown")
    lane = config.get("lane", "unknown")

    # Gate 1: lane isolation constraints
    if not guard["allow"]:
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": guard["reason"],
            "live_enabled": config.get("live_enabled", False),
            "allow_broker_execution": False,
            "market": market,
            "strategy": strategy,
        }

    # Gate 2: lane must be explicitly enabled
    if not config.get("enabled", False):
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": "LANE_DISABLED",
            "live_enabled": config.get("live_enabled", False),
            "allow_broker_execution": False,
            "market": market,
            "strategy": strategy,
        }

    # Gate 3: macro freeze / risk override
    if macro_config is None:
        macro_config = load_macro_config()

    macro = macro_check(macro_config)
    risk_state = macro.get("risk_state", "UNKNOWN")
    if not macro["allow"]:
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": "MACRO_FREEZE_ACTIVE",
            "live_enabled": config.get("live_enabled", False),
            "allow_broker_execution": False,
            "risk_state": risk_state,
            "market": market,
            "strategy": strategy,
        }

    # Gate 4: live execution gate (live_enabled + allow_broker_execution)
    gate = evaluate_live_execution_gate(config, macro_config)
    live_enabled = gate.get("live_enabled", False)
    allow_broker = gate.get("allow_broker_execution", False)

    if not gate["allow"]:
        return {
            "component": "live_lane_runner",
            "lane": lane,
            "state": "BLOCKED",
            "reason": gate["reason"],
            "live_enabled": live_enabled,
            "allow_broker_execution": allow_broker,
            "risk_state": risk_state,
            "market": market,
            "strategy": strategy,
        }

    return {
        "component": "live_lane_runner",
        "lane": lane,
        "state": "LIVE_GATE_READY",
        "live_enabled": True,
        "allow_broker_execution": True,
        "risk_state": risk_state,
        "market": market,
        "strategy": strategy,
        "note": "gate open; no execution in AC-153",
    }


def main() -> None:
    result = run()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
