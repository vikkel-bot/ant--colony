"""
AC-159: Live Feedback Schema

Validates causally-rich trade feedback records so the queen can understand
not just what happened in a trade, but why it worked or failed.

One sentence: Validates a causally-rich trade feedback record containing both
the closed trade outcome (AC-148) and the market/signal context at entry time.

Causal fields (required, not optional):
  market_regime_at_entry  — market regime when the ant entered
  volatility_at_entry     — volatility level when the ant entered
  signal_strength         — entry signal strength (0.0–1.0; -1.0 = not available)
  signal_key              — which signal triggered the entry
  slippage_vs_expected_eur — actual execution deviation from expected
  entry_latency_ms        — signal-to-fill latency in milliseconds

A feedback record without these six fields is rejected. The queen cannot learn
from records that only say what happened without explaining why.

No broker calls. No file IO. No paper pipeline imports.
Fail-closed: any invalid input is rejected. Never raises.
"""
from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Whitelists (inherited from AC-148 where applicable)
# ---------------------------------------------------------------------------

_VALID_LANES = {"live_test"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}
_VALID_POSITION_SIDES = {"long", "short"}
_VALID_EXIT_REASONS = {"SL", "TP", "SIGNAL", "OPERATOR_KILL", "MANUAL", "UNKNOWN"}
_VALID_QUALITY_FLAGS = {"OK", "PARTIAL_FILL", "HIGH_SLIPPAGE", "TIMEOUT_RECOVERED", "MISMATCH"}
_VALID_MARKET_REGIMES = {"BULL", "BEAR", "SIDEWAYS", "UNKNOWN"}
_VALID_VOLATILITY_LEVELS = {"LOW", "MID", "HIGH", "UNKNOWN"}
_VALID_FEEDBACK_VERSIONS = {"1"}

_REQUIRED_FIELDS = (
    # --- AC-148 closed trade fields ---
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
    # --- causal context (required, not optional) ---
    "market_regime_at_entry",
    "volatility_at_entry",
    "signal_strength",
    "signal_key",
    "slippage_vs_expected_eur",
    "entry_latency_ms",
    "feedback_ts_utc",
    "feedback_version",
)

_NORMALIZED_KEY_ORDER = _REQUIRED_FIELDS


def validate_live_feedback_record(record: Any) -> dict[str, Any]:
    """
    Validate a causally-rich live feedback record.

    Returns:
        {
            "ok": bool,
            "reason": str,
            "normalized_record": dict | None
        }

    Never raises. Fail-closed on any validation error.
    """
    try:
        return _validate(record)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected validation error: {exc}",
            "normalized_record": None,
        }


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "normalized_record": None}


def _is_valid_utc_ts(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    from datetime import datetime
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.utcoffset().total_seconds() == 0
    except Exception:
        pass
    return False


def _validate(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        return _fail("record must be a dict")

    # --- required fields present ---
    for field in _REQUIRED_FIELDS:
        if field not in record:
            return _fail(f"missing required field: {field}")

    # -----------------------------------------------------------------------
    # AC-148 fields
    # -----------------------------------------------------------------------

    if not isinstance(record["trade_id"], str) or not record["trade_id"].strip():
        return _fail("trade_id must be a non-empty string")

    if record["lane"] not in _VALID_LANES:
        return _fail(f"lane must be one of {sorted(_VALID_LANES)}, got {record['lane']!r}")

    if record["market"] not in _VALID_MARKETS:
        return _fail(f"market must be one of {sorted(_VALID_MARKETS)}, got {record['market']!r}")

    if record["strategy_key"] not in _VALID_STRATEGIES:
        return _fail(f"strategy_key must be one of {sorted(_VALID_STRATEGIES)}, got {record['strategy_key']!r}")

    if record["position_side"] not in _VALID_POSITION_SIDES:
        return _fail(f"position_side must be one of {sorted(_VALID_POSITION_SIDES)}, got {record['position_side']!r}")

    qty = record["qty"]
    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty <= 0:
        return _fail(f"qty must be numeric > 0, got {qty!r}")

    for ts_field in ("entry_ts_utc", "exit_ts_utc", "ts_recorded_utc"):
        if not _is_valid_utc_ts(record[ts_field]):
            return _fail(f"{ts_field} must be a valid UTC timestamp string, got {record[ts_field]!r}")

    for price_field in ("entry_price", "exit_price"):
        v = record[price_field]
        if not isinstance(v, (int, float)) or isinstance(v, bool) or v <= 0:
            return _fail(f"{price_field} must be numeric > 0, got {v!r}")

    pnl = record["realized_pnl_eur"]
    if not isinstance(pnl, (int, float)) or isinstance(pnl, bool):
        return _fail(f"realized_pnl_eur must be numeric, got {pnl!r}")

    slip = record["slippage_eur"]
    if not isinstance(slip, (int, float)) or isinstance(slip, bool):
        return _fail(f"slippage_eur must be numeric, got {slip!r}")

    hdm = record["hold_duration_minutes"]
    if not isinstance(hdm, (int, float)) or isinstance(hdm, bool) or hdm < 0:
        return _fail(f"hold_duration_minutes must be numeric >= 0, got {hdm!r}")

    if record["exit_reason"] not in _VALID_EXIT_REASONS:
        return _fail(f"exit_reason must be one of {sorted(_VALID_EXIT_REASONS)}, got {record['exit_reason']!r}")

    if record["execution_quality_flag"] not in _VALID_QUALITY_FLAGS:
        return _fail(f"execution_quality_flag must be one of {sorted(_VALID_QUALITY_FLAGS)}, got {record['execution_quality_flag']!r}")

    v = record["broker_order_id_entry"]
    if not isinstance(v, str) or not v.strip():
        return _fail("broker_order_id_entry must be a non-empty string")

    # AC-190: broker_order_id_exit is null while a position is still open;
    # it is only set to a real order ID when the exit is proven.
    v = record["broker_order_id_exit"]
    if v is not None and (not isinstance(v, str) or not v.strip()):
        return _fail("broker_order_id_exit must be a non-empty string or null")

    # -----------------------------------------------------------------------
    # Causal context fields
    # -----------------------------------------------------------------------

    if record["market_regime_at_entry"] not in _VALID_MARKET_REGIMES:
        return _fail(
            f"market_regime_at_entry must be one of {sorted(_VALID_MARKET_REGIMES)}, "
            f"got {record['market_regime_at_entry']!r}"
        )

    if record["volatility_at_entry"] not in _VALID_VOLATILITY_LEVELS:
        return _fail(
            f"volatility_at_entry must be one of {sorted(_VALID_VOLATILITY_LEVELS)}, "
            f"got {record['volatility_at_entry']!r}"
        )

    # signal_strength: float in [0.0, 1.0] OR -1.0 (not available)
    ss = record["signal_strength"]
    if not isinstance(ss, (int, float)) or isinstance(ss, bool):
        return _fail(f"signal_strength must be numeric, got {ss!r}")
    if not (ss == -1.0 or 0.0 <= ss <= 1.0):
        return _fail(
            f"signal_strength must be in [0.0, 1.0] or -1.0 (not available), got {ss!r}"
        )

    if not isinstance(record["signal_key"], str) or not record["signal_key"].strip():
        return _fail("signal_key must be a non-empty string")

    svs = record["slippage_vs_expected_eur"]
    if not isinstance(svs, (int, float)) or isinstance(svs, bool):
        return _fail(f"slippage_vs_expected_eur must be numeric, got {svs!r}")

    latency = record["entry_latency_ms"]
    if not isinstance(latency, (int, float)) or isinstance(latency, bool) or latency < 0:
        return _fail(f"entry_latency_ms must be numeric >= 0, got {latency!r}")

    if not _is_valid_utc_ts(record["feedback_ts_utc"]):
        return _fail(f"feedback_ts_utc must be a valid UTC timestamp string, got {record['feedback_ts_utc']!r}")

    if record["feedback_version"] not in _VALID_FEEDBACK_VERSIONS:
        return _fail(f"feedback_version must be one of {sorted(_VALID_FEEDBACK_VERSIONS)}, got {record['feedback_version']!r}")

    normalized = {k: record[k] for k in _NORMALIZED_KEY_ORDER}
    return {"ok": True, "reason": "FEEDBACK_RECORD_OK", "normalized_record": normalized}
