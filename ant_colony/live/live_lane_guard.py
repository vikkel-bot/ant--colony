"""
AC-146: Live Lane Guard

Validates live_lane_config.json before any lane operation is allowed.
Fail-closed: any constraint violation blocks the lane.

No imports from paper/simulation modules.
No shared state with the paper pipeline.
"""
from __future__ import annotations

from typing import Any


def validate(config: dict[str, Any]) -> dict[str, Any]:
    """
    Validate live lane config. Returns allow/block decision.

    Returns:
        {
            "allow": bool,
            "reason": str,
            "lane": str
        }
    """
    lane = config.get("lane", "unknown")

    def _block(reason: str) -> dict[str, Any]:
        return {"allow": False, "reason": reason, "lane": lane}

    # enabled must be present and bool
    if "enabled" not in config:
        return _block("missing field: enabled")
    if not isinstance(config["enabled"], bool):
        return _block("enabled must be bool")

    # market
    if config.get("market") != "BNB-EUR":
        return _block("market must be BNB-EUR")

    # strategy
    if config.get("strategy") != "EDGE3":
        return _block("strategy must be EDGE3")

    # max_notional_eur
    notional = config.get("max_notional_eur")
    if not isinstance(notional, (int, float)) or notional <= 0 or notional > 50:
        return _block("max_notional_eur must be > 0 and <= 50")

    # max_positions
    if config.get("max_positions") != 1:
        return _block("max_positions must be 1")

    # hard isolation flags — all must be false
    if config.get("allow_broker_execution") is not False:
        return _block("allow_broker_execution must be false")
    if config.get("allow_shared_state") is not False:
        return _block("allow_shared_state must be false")
    if config.get("allow_paper_inputs") is not False:
        return _block("allow_paper_inputs must be false")

    # base_output_dir must not be empty
    base_dir = config.get("base_output_dir", "")
    if not isinstance(base_dir, str) or not base_dir.strip():
        return _block("base_output_dir must not be empty")

    return {"allow": True, "reason": "all checks passed", "lane": lane}
