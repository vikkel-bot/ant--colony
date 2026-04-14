"""
AC-148: Live Execution Result Schema

Defines and validates the canonical record shape for live/test trade outcomes.
This schema is the contract that future broker execution must satisfy.

No broker calls. No file IO. No paper pipeline imports.
Fail-closed: invalid input is always rejected with a reason string.
"""
from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Whitelists
# ---------------------------------------------------------------------------

_VALID_LANES = {"live_test"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}
_VALID_POSITION_SIDES = {"long", "short"}
_VALID_EXIT_REASONS = {"SL", "TP", "SIGNAL", "OPERATOR_KILL", "MANUAL", "UNKNOWN"}
_VALID_QUALITY_FLAGS = {
    "OK", "PARTIAL_FILL", "HIGH_SLIPPAGE", "TIMEOUT_RECOVERED", "MISMATCH"
}

_REQUIRED_FIELDS = (
    "trade_id",
    "lane",
    "market",
    "strategy_key",
    "position_side",
    "qty",
    "entry_ts_utc",
    "exit_ts_utc",
    "entry_price",
    "exit_price",
    "realized_pnl_eur",
    "slippage_eur",
    "hold_duration_minutes",
    "exit_reason",
    "execution_quality_flag",
    "broker_order_id_entry",
    "broker_order_id_exit",
    "ts_recorded_utc",
)

# Canonical output key order matches _REQUIRED_FIELDS declaration order.
_NORMALIZED_KEY_ORDER = _REQUIRED_FIELDS


# ---------------------------------------------------------------------------
# Timestamp validation
# ---------------------------------------------------------------------------

def _is_valid_utc_ts(value: Any) -> bool:
    """Accept ISO-8601 UTC strings ending in Z or +00:00."""
    if not isinstance(value, str) or not value.strip():
        return False
    from datetime import datetime, timezone
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            dt = datetime.strptime(value, fmt)
            return True
        except ValueError:
            pass
    # Also accept +00:00 offset form
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.utcoffset().total_seconds() == 0
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Public validator
# ---------------------------------------------------------------------------

def validate_live_execution_result(record: Any) -> dict[str, Any]:
    """
    Validate a live execution result record.

    Returns:
        {
            "ok": bool,
            "reason": str,
            "normalized_record": dict  # only present when ok=True
        }

    Never raises. Fail-closed on any validation error.
    """
    try:
        return _validate(record)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"unexpected validation error: {exc}"}


def _validate(record: Any) -> dict[str, Any]:
    def _fail(reason: str) -> dict[str, Any]:
        return {"ok": False, "reason": reason}

    if not isinstance(record, dict):
        return _fail("record must be a dict")

    # --- required fields present ---
    for field in _REQUIRED_FIELDS:
        if field not in record:
            return _fail(f"missing required field: {field}")

    # --- trade_id ---
    if not isinstance(record["trade_id"], str) or not record["trade_id"].strip():
        return _fail("trade_id must be a non-empty string")

    # --- lane ---
    if record["lane"] not in _VALID_LANES:
        return _fail(f"lane must be one of {sorted(_VALID_LANES)}, got {record['lane']!r}")

    # --- market ---
    if record["market"] not in _VALID_MARKETS:
        return _fail(f"market must be one of {sorted(_VALID_MARKETS)}, got {record['market']!r}")

    # --- strategy_key ---
    if record["strategy_key"] not in _VALID_STRATEGIES:
        return _fail(
            f"strategy_key must be one of {sorted(_VALID_STRATEGIES)}, "
            f"got {record['strategy_key']!r}"
        )

    # --- position_side ---
    if record["position_side"] not in _VALID_POSITION_SIDES:
        return _fail(
            f"position_side must be one of {sorted(_VALID_POSITION_SIDES)}, "
            f"got {record['position_side']!r}"
        )

    # --- qty ---
    qty = record["qty"]
    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty <= 0:
        return _fail(f"qty must be numeric > 0, got {qty!r}")

    # --- timestamps ---
    for ts_field in ("entry_ts_utc", "exit_ts_utc", "ts_recorded_utc"):
        if not _is_valid_utc_ts(record[ts_field]):
            return _fail(f"{ts_field} must be a valid UTC timestamp string, got {record[ts_field]!r}")

    # --- prices ---
    for price_field in ("entry_price", "exit_price"):
        v = record[price_field]
        if not isinstance(v, (int, float)) or isinstance(v, bool) or v <= 0:
            return _fail(f"{price_field} must be numeric > 0, got {v!r}")

    # --- realized_pnl_eur (may be negative) ---
    pnl = record["realized_pnl_eur"]
    if not isinstance(pnl, (int, float)) or isinstance(pnl, bool):
        return _fail(f"realized_pnl_eur must be numeric, got {pnl!r}")

    # --- slippage_eur (may be positive or negative) ---
    slip = record["slippage_eur"]
    if not isinstance(slip, (int, float)) or isinstance(slip, bool):
        return _fail(f"slippage_eur must be numeric, got {slip!r}")

    # --- hold_duration_minutes (>= 0) ---
    hdm = record["hold_duration_minutes"]
    if not isinstance(hdm, (int, float)) or isinstance(hdm, bool) or hdm < 0:
        return _fail(f"hold_duration_minutes must be numeric >= 0, got {hdm!r}")

    # --- exit_reason ---
    if record["exit_reason"] not in _VALID_EXIT_REASONS:
        return _fail(
            f"exit_reason must be one of {sorted(_VALID_EXIT_REASONS)}, "
            f"got {record['exit_reason']!r}"
        )

    # --- execution_quality_flag ---
    if record["execution_quality_flag"] not in _VALID_QUALITY_FLAGS:
        return _fail(
            f"execution_quality_flag must be one of {sorted(_VALID_QUALITY_FLAGS)}, "
            f"got {record['execution_quality_flag']!r}"
        )

    # --- broker order ids ---
    v = record["broker_order_id_entry"]
    if not isinstance(v, str) or not v.strip():
        return _fail("broker_order_id_entry must be a non-empty string")

    # AC-190: broker_order_id_exit is null while a position is still open.
    v = record["broker_order_id_exit"]
    if v is not None and (not isinstance(v, str) or not v.strip()):
        return _fail("broker_order_id_exit must be a non-empty string or null")

    # --- normalize ---
    normalized = {k: record[k] for k in _NORMALIZED_KEY_ORDER}

    return {"ok": True, "reason": "all checks passed", "normalized_record": normalized}
