"""
AC-155: External Auto-Freeze Guard

Evaluates a market snapshot against configured thresholds and automatically
activates a freeze when extreme conditions are detected. This is the
automatic risk layer that complements the manual macro freeze (AC-147).

The output shape (risk_state / freeze_new_entries) is intentionally
compatible with the live execution gate (AC-153) so both can be composed
without glue code.

No broker calls. No external paid APIs required. No paper pipeline imports.
No file IO beyond config loading. Fail-closed on any invalid input.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "external_auto_freeze_config.json"

_FREEZE_RESULT = {
    "allow": False,
    "risk_state": "FREEZE",
    "freeze_new_entries": True,
}
_CLEAR_RESULT = {
    "allow": True,
    "reason": "AUTO_FREEZE_CLEAR",
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
}


def load_auto_freeze_config(path: Path = _DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load auto-freeze config from JSON. Returns empty dict on error (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def evaluate_external_auto_freeze(
    market_snapshot: Any,
    config: Any,
) -> dict[str, Any]:
    """
    Evaluate auto-freeze conditions against a market snapshot.

    Returns:
        {
            "allow": bool,
            "reason": str,
            "risk_state": "NORMAL" | "FREEZE",
            "freeze_new_entries": bool
        }

    Fail-closed: invalid or missing data → freeze.
    Never raises.
    """
    try:
        return _evaluate(market_snapshot, config)
    except Exception as exc:  # noqa: BLE001
        return {**_FREEZE_RESULT, "reason": f"unexpected error: {exc}"}


def _freeze(reason: str) -> dict[str, Any]:
    return {**_FREEZE_RESULT, "reason": reason}


def _evaluate(snapshot: Any, config: Any) -> dict[str, Any]:
    # config must be a dict
    if not isinstance(config, dict):
        return _freeze("config must be a dict")

    # if disabled, pass through without evaluating
    enabled = config.get("enabled")
    if not isinstance(enabled, bool):
        return _freeze("config.enabled must be bool")
    if not enabled:
        return {**_CLEAR_RESULT, "reason": "AUTO_FREEZE_DISABLED"}

    configured_market = config.get("market")
    freeze_on_missing = config.get("freeze_on_market_data_missing")
    stale_threshold = config.get("stale_market_data_seconds")
    max_single = config.get("max_single_move_pct")
    max_abs = config.get("max_abs_move_pct")

    # validate config fields
    if not isinstance(freeze_on_missing, bool):
        return _freeze("config.freeze_on_market_data_missing must be bool")
    if not isinstance(stale_threshold, (int, float)) or stale_threshold <= 0:
        return _freeze("config.stale_market_data_seconds must be numeric > 0")
    if not isinstance(max_single, (int, float)) or max_single <= 0:
        return _freeze("config.max_single_move_pct must be numeric > 0")
    if not isinstance(max_abs, (int, float)) or max_abs <= 0:
        return _freeze("config.max_abs_move_pct must be numeric > 0")

    # snapshot must be a dict
    if not isinstance(snapshot, dict):
        if freeze_on_missing:
            return _freeze("market_snapshot missing or invalid")
        return {**_CLEAR_RESULT, "reason": "AUTO_FREEZE_DISABLED_FOR_MISSING"}

    # market must match config
    if snapshot.get("market") != configured_market:
        return _freeze(
            f"market mismatch: expected {configured_market!r}, "
            f"got {snapshot.get('market')!r}"
        )

    # market_data_ok
    market_data_ok = snapshot.get("market_data_ok")
    if market_data_ok is not True:
        if freeze_on_missing:
            return _freeze("market_data_ok is not true")
        # if freeze_on_missing is false, allow through — but this path is
        # currently unreachable given the default config; kept for correctness.
        return {**_CLEAR_RESULT, "reason": "AUTO_FREEZE_CLEAR_MISSING_TOLERATED"}

    # ts_utc must be present and parseable
    ts_raw = snapshot.get("ts_utc")
    if not ts_raw:
        return _freeze("ts_utc missing in market snapshot")

    ts = _parse_ts(ts_raw)
    if ts is None:
        return _freeze(f"ts_utc unreadable: {ts_raw!r}")

    # staleness check — use a fixed "now" if injected for testability
    now = snapshot.get("_now_utc_override") or datetime.now(tz=timezone.utc)
    if isinstance(now, str):
        now = _parse_ts(now)
        if now is None:
            return _freeze("_now_utc_override unreadable")

    age_seconds = (now - ts).total_seconds()
    if age_seconds > stale_threshold:
        return _freeze(
            f"market data stale: {age_seconds:.0f}s > {stale_threshold}s"
        )

    # price_now / price_ref must be numeric > 0
    price_now = snapshot.get("price_now")
    price_ref = snapshot.get("price_ref")

    if not isinstance(price_now, (int, float)) or isinstance(price_now, bool) \
            or price_now <= 0:
        return _freeze(f"price_now must be numeric > 0, got {price_now!r}")
    if not isinstance(price_ref, (int, float)) or isinstance(price_ref, bool) \
            or price_ref <= 0:
        return _freeze(f"price_ref must be numeric > 0, got {price_ref!r}")

    # single move check (from snapshot field)
    move_pct = snapshot.get("move_pct")
    if move_pct is not None:
        if not isinstance(move_pct, (int, float)) or isinstance(move_pct, bool):
            return _freeze(f"move_pct must be numeric, got {move_pct!r}")
        if abs(move_pct) >= max_single:
            return _freeze(
                f"extreme single move: abs({move_pct:.2f}%) >= {max_single}%"
            )

    # absolute move check (computed from prices, authoritative)
    abs_move = abs((price_now - price_ref) / price_ref * 100)
    if abs_move >= max_abs:
        return _freeze(
            f"extreme absolute move: {abs_move:.2f}% >= {max_abs}%"
        )

    return dict(_CLEAR_RESULT)


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.utcoffset().total_seconds() == 0:
            return dt.astimezone(timezone.utc)
    except Exception:
        pass
    return None
