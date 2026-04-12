"""
AC-157: Live Outcome Capture

Translates broker position information into an explicit colony position status
without executing any exit order.

One sentence: Translates broker position information into clear colony status
(FLAT / OPEN_POSITION / POSITION_MISMATCH / UNKNOWN) without placing any exit.

Position states:
  FLAT             — broker reports zero quantity for this market
  OPEN_POSITION    — broker confirms a live position consistent with the entry record
  POSITION_MISMATCH — broker data contradicts what the colony expects (order id,
                       side, market, or non-zero qty when we expect flat, etc.)
  UNKNOWN          — broker snapshot unreadable or missing; cannot determine state

No broker calls. No file IO. No paper pipeline imports.
Fail-closed: invalid input returns ok=False. Never raises.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ant_colony.live.live_position_state import validate_live_position_state

_VALID_LANES = {"live_test"}
_VALID_MARKETS = {"BNB-EUR"}
_VALID_STRATEGIES = {"EDGE3"}

# Minimum fields required from the AC-154 entry execution result
_ENTRY_REQUIRED_FIELDS = (
    "lane",
    "market",
    "strategy_key",
    "broker_order_id_entry",
    "entry_price",
    "qty",
    "position_side",
)

# Minimum fields required from the broker position snapshot
_SNAPSHOT_REQUIRED_FIELDS = (
    "market",
    "position_qty",
    "avg_entry_price",
    "side",
)


def capture_live_position_state(
    entry_execution_result: Any,
    broker_position_snapshot: Any,
) -> dict[str, Any]:
    """
    Capture current live position state by comparing the entry execution result
    against a broker position snapshot.

    Parameters:
        entry_execution_result   — AC-154 / AC-148 compatible entry record
        broker_position_snapshot — dict with current broker position data:
            {
                "market": "BNB-EUR",
                "position_qty": float,   # 0 = flat
                "avg_entry_price": float,
                "side": "long" | "short" | "none",
                "broker_order_id_entry": str  # optional, for mismatch detection
            }

    Returns:
        {
            "ok": bool,
            "reason": str,
            "position_state_record": dict | None
        }

    Never raises. Fail-closed on any invalid input.
    """
    try:
        return _capture(entry_execution_result, broker_position_snapshot)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected capture error: {exc}",
            "position_state_record": None,
        }


def _fail(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason, "position_state_record": None}


def _capture(entry_result: Any, snapshot: Any) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- validate entry execution result ---
    if not isinstance(entry_result, dict):
        return _fail("entry_execution_result must be a dict")

    for field in _ENTRY_REQUIRED_FIELDS:
        if field not in entry_result:
            return _fail(f"entry_execution_result missing required field: {field}")

    lane = entry_result["lane"]
    market = entry_result["market"]
    strategy_key = entry_result["strategy_key"]
    entry_order_id = entry_result["broker_order_id_entry"]
    entry_price = entry_result["entry_price"]
    entry_qty = entry_result["qty"]
    position_side = entry_result["position_side"]

    if lane not in _VALID_LANES:
        return _fail(f"entry lane not allowed: {lane!r}")
    if market not in _VALID_MARKETS:
        return _fail(f"entry market not allowed: {market!r}")
    if strategy_key not in _VALID_STRATEGIES:
        return _fail(f"entry strategy_key not allowed: {strategy_key!r}")

    if not isinstance(entry_order_id, str) or not entry_order_id.strip():
        return _fail("broker_order_id_entry must be a non-empty string")
    if not isinstance(entry_price, (int, float)) or isinstance(entry_price, bool) or entry_price < 0:
        return _fail(f"entry_price must be numeric >= 0, got {entry_price!r}")
    if not isinstance(entry_qty, (int, float)) or isinstance(entry_qty, bool) or entry_qty < 0:
        return _fail(f"qty must be numeric >= 0, got {entry_qty!r}")

    # --- broker snapshot: missing or unreadable → UNKNOWN ---
    if not isinstance(snapshot, dict):
        return _build_state(
            lane=lane, market=market, strategy_key=strategy_key,
            position_state="UNKNOWN",
            entry_order_id=entry_order_id,
            entry_price=entry_price,
            qty=entry_qty,
            position_side=position_side,
            ts_observed_utc=now_iso,
            reason="broker_position_snapshot missing or unreadable",
        )

    for field in _SNAPSHOT_REQUIRED_FIELDS:
        if field not in snapshot:
            return _build_state(
                lane=lane, market=market, strategy_key=strategy_key,
                position_state="UNKNOWN",
                entry_order_id=entry_order_id,
                entry_price=entry_price,
                qty=entry_qty,
                position_side=position_side,
                ts_observed_utc=now_iso,
                reason=f"broker_position_snapshot missing required field: {field}",
            )

    snap_market = snapshot.get("market")
    snap_qty_raw = snapshot.get("position_qty")
    snap_side = snapshot.get("side")
    snap_order_id = snapshot.get("broker_order_id_entry")

    # --- market mismatch → POSITION_MISMATCH ---
    if snap_market != market:
        return _build_state(
            lane=lane, market=market, strategy_key=strategy_key,
            position_state="POSITION_MISMATCH",
            entry_order_id=entry_order_id,
            entry_price=entry_price,
            qty=entry_qty,
            position_side=position_side,
            ts_observed_utc=now_iso,
            reason=f"broker snapshot market {snap_market!r} != expected {market!r}",
        )

    # --- parse broker qty ---
    try:
        snap_qty = float(snap_qty_raw)
    except (TypeError, ValueError):
        return _build_state(
            lane=lane, market=market, strategy_key=strategy_key,
            position_state="UNKNOWN",
            entry_order_id=entry_order_id,
            entry_price=entry_price,
            qty=entry_qty,
            position_side=position_side,
            ts_observed_utc=now_iso,
            reason=f"broker snapshot position_qty unreadable: {snap_qty_raw!r}",
        )

    # --- FLAT: broker reports zero qty ---
    if snap_qty == 0:
        return _build_state(
            lane=lane, market=market, strategy_key=strategy_key,
            position_state="FLAT",
            entry_order_id="",
            entry_price=0.0,
            qty=0.0,
            position_side="none",
            ts_observed_utc=now_iso,
            reason="broker reports position_qty == 0",
        )

    # --- broker has a non-zero position: check consistency ---
    if snap_qty < 0:
        return _build_state(
            lane=lane, market=market, strategy_key=strategy_key,
            position_state="POSITION_MISMATCH",
            entry_order_id=entry_order_id,
            entry_price=entry_price,
            qty=entry_qty,
            position_side=position_side,
            ts_observed_utc=now_iso,
            reason=f"broker reports negative position_qty: {snap_qty}",
        )

    # --- side mismatch ---
    if isinstance(snap_side, str) and snap_side != position_side and snap_side != "none":
        return _build_state(
            lane=lane, market=market, strategy_key=strategy_key,
            position_state="POSITION_MISMATCH",
            entry_order_id=entry_order_id,
            entry_price=entry_price,
            qty=entry_qty,
            position_side=position_side,
            ts_observed_utc=now_iso,
            reason=f"broker side {snap_side!r} != expected {position_side!r}",
        )

    # --- broker_order_id mismatch (if provided) ---
    if snap_order_id is not None and isinstance(snap_order_id, str) \
            and snap_order_id.strip() \
            and snap_order_id != entry_order_id:
        return _build_state(
            lane=lane, market=market, strategy_key=strategy_key,
            position_state="POSITION_MISMATCH",
            entry_order_id=entry_order_id,
            entry_price=entry_price,
            qty=entry_qty,
            position_side=position_side,
            ts_observed_utc=now_iso,
            reason=(
                f"broker_order_id_entry {snap_order_id!r} != "
                f"entry record {entry_order_id!r}"
            ),
        )

    # --- all checks pass: OPEN_POSITION ---
    return _build_state(
        lane=lane, market=market, strategy_key=strategy_key,
        position_state="OPEN_POSITION",
        entry_order_id=entry_order_id,
        entry_price=entry_price,
        qty=snap_qty,
        position_side=position_side,
        ts_observed_utc=now_iso,
        reason="broker confirms open position consistent with entry record",
    )


def _build_state(
    *,
    lane: str,
    market: str,
    strategy_key: str,
    position_state: str,
    entry_order_id: str,
    entry_price: float,
    qty: float,
    position_side: str,
    ts_observed_utc: str,
    reason: str,
) -> dict[str, Any]:
    record = {
        "lane": lane,
        "market": market,
        "strategy_key": strategy_key,
        "position_state": position_state,
        "entry_order_id": entry_order_id,
        "entry_price": entry_price,
        "qty": qty,
        "position_side": position_side,
        "ts_observed_utc": ts_observed_utc,
        "reason": reason,
    }
    validation = validate_live_position_state(record)
    if not validation["ok"]:
        return {
            "ok": False,
            "reason": f"internal position state invalid: {validation['reason']}",
            "position_state_record": None,
        }
    return {
        "ok": True,
        "reason": "LIVE_POSITION_CAPTURED",
        "position_state_record": validation["normalized_record"],
    }
