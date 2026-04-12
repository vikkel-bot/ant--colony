"""
AC-157: Live Position State

Validator and normalizer for live position state records.
A position state record answers: is the first live ant in a flat,
open, mismatched, or unknown state right now?

One sentence: Validates and normalizes a live position state record so the
colony always has an explicit, structured answer to "is there an open position?"

No broker calls. No file IO. No paper pipeline imports.
Fail-closed: any invalid input is rejected with a reason string. Never raises.
"""
from __future__ import annotations

from typing import Any

_VALID_LANES = {"live_test"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}
_VALID_POSITION_STATES = {"FLAT", "OPEN_POSITION", "POSITION_MISMATCH", "UNKNOWN"}
_VALID_POSITION_SIDES = {"long", "short", "none"}

_REQUIRED_FIELDS = (
    "lane",
    "market",
    "strategy_key",
    "position_state",
    "entry_order_id",
    "entry_price",
    "qty",
    "position_side",
    "ts_observed_utc",
    "reason",
)

_NORMALIZED_KEY_ORDER = _REQUIRED_FIELDS


def validate_live_position_state(record: Any) -> dict[str, Any]:
    """
    Validate a live position state record.

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
        return {"ok": False, "reason": f"unexpected validation error: {exc}", "normalized_record": None}


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

    for field in _REQUIRED_FIELDS:
        if field not in record:
            return _fail(f"missing required field: {field}")

    # lane
    if record["lane"] not in _VALID_LANES:
        return _fail(f"lane must be one of {sorted(_VALID_LANES)}, got {record['lane']!r}")

    # market
    if record["market"] not in _VALID_MARKETS:
        return _fail(f"market must be one of {sorted(_VALID_MARKETS)}, got {record['market']!r}")

    # strategy_key
    if record["strategy_key"] not in _VALID_STRATEGIES:
        return _fail(
            f"strategy_key must be one of {sorted(_VALID_STRATEGIES)}, "
            f"got {record['strategy_key']!r}"
        )

    # position_state
    position_state = record["position_state"]
    if position_state not in _VALID_POSITION_STATES:
        return _fail(
            f"position_state must be one of {sorted(_VALID_POSITION_STATES)}, "
            f"got {position_state!r}"
        )

    # qty >= 0
    qty = record["qty"]
    if not isinstance(qty, (int, float)) or isinstance(qty, bool) or qty < 0:
        return _fail(f"qty must be numeric >= 0, got {qty!r}")

    # entry_price >= 0
    price = record["entry_price"]
    if not isinstance(price, (int, float)) or isinstance(price, bool) or price < 0:
        return _fail(f"entry_price must be numeric >= 0, got {price!r}")

    # position_side
    if record["position_side"] not in _VALID_POSITION_SIDES:
        return _fail(
            f"position_side must be one of {sorted(_VALID_POSITION_SIDES)}, "
            f"got {record['position_side']!r}"
        )

    # entry_order_id: required non-empty when OPEN_POSITION or POSITION_MISMATCH
    entry_order_id = record["entry_order_id"]
    if not isinstance(entry_order_id, str):
        return _fail("entry_order_id must be a string")
    if position_state in ("OPEN_POSITION", "POSITION_MISMATCH") and not entry_order_id.strip():
        return _fail(
            f"entry_order_id must be non-empty when position_state is {position_state!r}"
        )

    # ts_observed_utc
    if not _is_valid_utc_ts(record["ts_observed_utc"]):
        return _fail(
            f"ts_observed_utc must be a valid UTC timestamp string, "
            f"got {record['ts_observed_utc']!r}"
        )

    # reason
    if not isinstance(record["reason"], str) or not record["reason"].strip():
        return _fail("reason must be a non-empty string")

    normalized = {k: record[k] for k in _NORMALIZED_KEY_ORDER}
    return {"ok": True, "reason": "POSITION_STATE_OK", "normalized_record": normalized}
