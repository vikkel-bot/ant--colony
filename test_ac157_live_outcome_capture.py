"""
AC-157: Tests for Live Outcome Capture and Position State

Verifies:
  A. Valid open position snapshot → ok=True, OPEN_POSITION
  B. position_qty == 0 → FLAT
  C. Broker snapshot missing / None → UNKNOWN (fail-closed)
  D. Market mismatch in snapshot → POSITION_MISMATCH
  E. broker_order_id mismatch → POSITION_MISMATCH
  F. Side mismatch → POSITION_MISMATCH
  G. Invalid entry_execution_result → fail-closed
  H. Missing required fields in entry result → fail-closed
  I. Missing required fields in snapshot → UNKNOWN
  J. Output shape (ok, reason, position_state_record)
  K. position_state_record shape (all AC-157 fields present)
  L. FLAT record uses sentinel values (qty=0, entry_price=0, side="none")
  M. position_state validator: valid record passes
  N. position_state validator: invalid position_state blocked
  O. position_state validator: missing required fields blocked
  P. position_state validator: entry_order_id empty for OPEN_POSITION blocked
  Q. position_state validator: qty < 0 blocked
  R. position_state validator: invalid ts_utc blocked
  S. No exceptions leak (fail-closed for all paths)
  T. No paper/network markers in source files
  U. position_qty negative → POSITION_MISMATCH
  V. Unreadable position_qty → UNKNOWN
  W. Non-dict snapshot types → UNKNOWN
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_outcome_capture import capture_live_position_state
from ant_colony.live.live_position_state import validate_live_position_state

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = "2026-04-12T12:00:00Z"

_ENTRY_RESULT = {
    "trade_id": "LIVE-BNBEUR-EDGE3-LONG-20260412T120000",
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "qty": 0.08,
    "entry_ts_utc": _NOW,
    "exit_ts_utc": _NOW,
    "entry_price": 600.0,
    "exit_price": 600.0,
    "realized_pnl_eur": 0.0,
    "slippage_eur": 0.0,
    "hold_duration_minutes": 0.0,
    "exit_reason": "UNKNOWN",
    "execution_quality_flag": "OK",
    "broker_order_id_entry": "BTV-ORDER-001",
    "broker_order_id_exit": "ENTRY_ONLY_PENDING_EXIT",
    "ts_recorded_utc": _NOW,
}

_OPEN_SNAPSHOT = {
    "market": "BNB-EUR",
    "position_qty": 0.08,
    "avg_entry_price": 600.0,
    "side": "long",
    "broker_order_id_entry": "BTV-ORDER-001",
}

_FLAT_SNAPSHOT = {
    "market": "BNB-EUR",
    "position_qty": 0,
    "avg_entry_price": 0.0,
    "side": "none",
    "broker_order_id_entry": "",
}


def _cap(entry=None, snap=None, **entry_overrides):
    e = dict(_ENTRY_RESULT)
    e.update(entry_overrides)
    if entry is not None:
        e = entry
    s = snap if snap is not None else dict(_OPEN_SNAPSHOT)
    return capture_live_position_state(e, s)


# ---------------------------------------------------------------------------
# A. Valid open position → OPEN_POSITION
# ---------------------------------------------------------------------------

class TestOpenPosition:
    def test_ok_true(self):
        assert _cap()["ok"] is True

    def test_reason_live_position_captured(self):
        assert _cap()["reason"] == "LIVE_POSITION_CAPTURED"

    def test_position_state_open(self):
        assert _cap()["position_state_record"]["position_state"] == "OPEN_POSITION"

    def test_entry_order_id_correct(self):
        assert _cap()["position_state_record"]["entry_order_id"] == "BTV-ORDER-001"

    def test_qty_from_snapshot(self):
        assert _cap()["position_state_record"]["qty"] == 0.08

    def test_market_correct(self):
        assert _cap()["position_state_record"]["market"] == "BNB-EUR"

    def test_lane_correct(self):
        assert _cap()["position_state_record"]["lane"] == "live_test"

    def test_strategy_correct(self):
        assert _cap()["position_state_record"]["strategy_key"] == "EDGE3"


# ---------------------------------------------------------------------------
# B. position_qty == 0 → FLAT
# ---------------------------------------------------------------------------

class TestFlatPosition:
    def test_flat_ok_true(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["ok"] is True

    def test_flat_position_state(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["position_state"] == "FLAT"

    def test_flat_qty_zero(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["qty"] == 0.0

    def test_flat_entry_price_zero(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["entry_price"] == 0.0

    def test_flat_side_none(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["position_side"] == "none"

    def test_flat_entry_order_id_empty(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["entry_order_id"] == ""

    def test_flat_reason_mentions_qty(self):
        record = _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]
        assert "0" in record["reason"]


# ---------------------------------------------------------------------------
# C. Missing / None snapshot → UNKNOWN
# ---------------------------------------------------------------------------

class TestMissingSnapshot:
    def test_none_snapshot_ok_true_unknown(self):
        r = capture_live_position_state(_ENTRY_RESULT, None)
        assert r["ok"] is True
        assert r["position_state_record"]["position_state"] == "UNKNOWN"

    def test_non_dict_snapshot_unknown(self):
        r = capture_live_position_state(_ENTRY_RESULT, "bad")
        assert r["ok"] is True
        assert r["position_state_record"]["position_state"] == "UNKNOWN"

    def test_unknown_reason_mentions_missing(self):
        r = capture_live_position_state(_ENTRY_RESULT, None)
        assert "missing" in r["position_state_record"]["reason"].lower() or \
               "unreadable" in r["position_state_record"]["reason"].lower()


# ---------------------------------------------------------------------------
# D. Market mismatch → POSITION_MISMATCH
# ---------------------------------------------------------------------------

class TestMarketMismatch:
    def test_market_mismatch_ok_true(self):
        snap = dict(_OPEN_SNAPSHOT, market="BTC-EUR")
        r = _cap(snap=snap)
        assert r["ok"] is True

    def test_market_mismatch_state(self):
        snap = dict(_OPEN_SNAPSHOT, market="BTC-EUR")
        r = _cap(snap=snap)
        assert r["position_state_record"]["position_state"] == "POSITION_MISMATCH"

    def test_market_mismatch_reason_mentions_market(self):
        snap = dict(_OPEN_SNAPSHOT, market="BTC-EUR")
        r = _cap(snap=snap)
        assert "market" in r["position_state_record"]["reason"].lower()


# ---------------------------------------------------------------------------
# E. broker_order_id mismatch → POSITION_MISMATCH
# ---------------------------------------------------------------------------

class TestOrderIdMismatch:
    def test_order_id_mismatch_state(self):
        snap = dict(_OPEN_SNAPSHOT, broker_order_id_entry="DIFFERENT-ORDER")
        r = _cap(snap=snap)
        assert r["position_state_record"]["position_state"] == "POSITION_MISMATCH"

    def test_order_id_mismatch_reason(self):
        snap = dict(_OPEN_SNAPSHOT, broker_order_id_entry="DIFFERENT-ORDER")
        r = _cap(snap=snap)
        assert "DIFFERENT-ORDER" in r["position_state_record"]["reason"] or \
               "order" in r["position_state_record"]["reason"].lower()

    def test_missing_order_id_in_snapshot_still_open(self):
        # If broker doesn't provide order_id, we can't mismatch — still OPEN
        snap = {k: v for k, v in _OPEN_SNAPSHOT.items() if k != "broker_order_id_entry"}
        r = _cap(snap=snap)
        assert r["position_state_record"]["position_state"] == "OPEN_POSITION"

    def test_empty_order_id_in_snapshot_not_mismatch(self):
        # Empty string in snapshot = broker did not report order id — no mismatch
        snap = dict(_OPEN_SNAPSHOT, broker_order_id_entry="")
        r = _cap(snap=snap)
        assert r["position_state_record"]["position_state"] == "OPEN_POSITION"


# ---------------------------------------------------------------------------
# F. Side mismatch → POSITION_MISMATCH
# ---------------------------------------------------------------------------

class TestSideMismatch:
    def test_side_mismatch_state(self):
        snap = dict(_OPEN_SNAPSHOT, side="short")
        r = _cap(snap=snap)
        assert r["position_state_record"]["position_state"] == "POSITION_MISMATCH"

    def test_side_mismatch_reason_mentions_side(self):
        snap = dict(_OPEN_SNAPSHOT, side="short")
        r = _cap(snap=snap)
        assert "side" in r["position_state_record"]["reason"].lower()

    def test_side_none_in_snapshot_not_mismatch(self):
        # "none" side in snapshot = broker doesn't report side specifically
        snap = dict(_OPEN_SNAPSHOT, side="none")
        r = _cap(snap=snap)
        assert r["position_state_record"]["position_state"] == "OPEN_POSITION"


# ---------------------------------------------------------------------------
# G. Invalid entry_execution_result → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidEntryResult:
    def test_none_entry_result_fails(self):
        r = capture_live_position_state(None, _OPEN_SNAPSHOT)
        assert r["ok"] is False

    def test_non_dict_entry_result_fails(self):
        r = capture_live_position_state("bad", _OPEN_SNAPSHOT)
        assert r["ok"] is False

    def test_wrong_lane_fails(self):
        entry = dict(_ENTRY_RESULT, lane="wrong")
        r = capture_live_position_state(entry, _OPEN_SNAPSHOT)
        assert r["ok"] is False

    def test_wrong_market_fails(self):
        entry = dict(_ENTRY_RESULT, market="BTC-EUR")
        r = capture_live_position_state(entry, _OPEN_SNAPSHOT)
        assert r["ok"] is False

    def test_wrong_strategy_fails(self):
        entry = dict(_ENTRY_RESULT, strategy_key="RSI")
        r = capture_live_position_state(entry, _OPEN_SNAPSHOT)
        assert r["ok"] is False

    def test_empty_order_id_fails(self):
        entry = dict(_ENTRY_RESULT, broker_order_id_entry="")
        r = capture_live_position_state(entry, _OPEN_SNAPSHOT)
        assert r["ok"] is False

    def test_negative_entry_price_fails(self):
        entry = dict(_ENTRY_RESULT, entry_price=-1.0)
        r = capture_live_position_state(entry, _OPEN_SNAPSHOT)
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# H. Missing required fields in entry result → fail-closed
# ---------------------------------------------------------------------------

class TestMissingEntryFields:
    @pytest.mark.parametrize("field", [
        "lane", "market", "strategy_key",
        "broker_order_id_entry", "entry_price", "qty", "position_side",
    ])
    def test_missing_field_fails(self, field):
        entry = dict(_ENTRY_RESULT)
        del entry[field]
        r = capture_live_position_state(entry, _OPEN_SNAPSHOT)
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# I. Missing required snapshot fields → UNKNOWN
# ---------------------------------------------------------------------------

class TestMissingSnapshotFields:
    @pytest.mark.parametrize("field", ["market", "position_qty", "avg_entry_price", "side"])
    def test_missing_snapshot_field_unknown(self, field):
        snap = dict(_OPEN_SNAPSHOT)
        del snap[field]
        r = _cap(snap=snap)
        assert r["ok"] is True
        assert r["position_state_record"]["position_state"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# J. Output shape
# ---------------------------------------------------------------------------

class TestOutputShape:
    _REQUIRED_KEYS = ("ok", "reason", "position_state_record")

    def test_open_result_has_required_keys(self):
        r = _cap()
        for k in self._REQUIRED_KEYS:
            assert k in r, f"missing key: {k}"

    def test_flat_result_has_required_keys(self):
        r = _cap(snap=_FLAT_SNAPSHOT)
        for k in self._REQUIRED_KEYS:
            assert k in r

    def test_mismatch_result_has_required_keys(self):
        r = _cap(snap=dict(_OPEN_SNAPSHOT, market="BTC-EUR"))
        for k in self._REQUIRED_KEYS:
            assert k in r

    def test_fail_result_has_required_keys(self):
        r = capture_live_position_state(None, _OPEN_SNAPSHOT)
        for k in self._REQUIRED_KEYS:
            assert k in r


# ---------------------------------------------------------------------------
# K. position_state_record shape (all fields present)
# ---------------------------------------------------------------------------

class TestPositionStateRecordShape:
    _FIELDS = (
        "lane", "market", "strategy_key", "position_state",
        "entry_order_id", "entry_price", "qty", "position_side",
        "ts_observed_utc", "reason",
    )

    def test_open_record_has_all_fields(self):
        rec = _cap()["position_state_record"]
        for f in self._FIELDS:
            assert f in rec, f"missing field: {f}"

    def test_flat_record_has_all_fields(self):
        rec = _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]
        for f in self._FIELDS:
            assert f in rec, f"missing field: {f}"

    def test_unknown_record_has_all_fields(self):
        rec = capture_live_position_state(_ENTRY_RESULT, None)["position_state_record"]
        for f in self._FIELDS:
            assert f in rec, f"missing field: {f}"


# ---------------------------------------------------------------------------
# L. FLAT sentinel values
# ---------------------------------------------------------------------------

class TestFlatSentinels:
    def test_flat_entry_order_id_empty(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["entry_order_id"] == ""

    def test_flat_entry_price_zero(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["entry_price"] == 0.0

    def test_flat_qty_zero(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["qty"] == 0.0

    def test_flat_side_none(self):
        assert _cap(snap=_FLAT_SNAPSHOT)["position_state_record"]["position_side"] == "none"


# ---------------------------------------------------------------------------
# M. Validator: valid record passes
# ---------------------------------------------------------------------------

class TestValidatorValid:
    _VALID_RECORD = {
        "lane": "live_test",
        "market": "BNB-EUR",
        "strategy_key": "EDGE3",
        "position_state": "OPEN_POSITION",
        "entry_order_id": "BTV-ORDER-001",
        "entry_price": 600.0,
        "qty": 0.08,
        "position_side": "long",
        "ts_observed_utc": "2026-04-12T12:00:00Z",
        "reason": "broker confirms open position",
    }

    def test_valid_open_position_ok(self):
        r = validate_live_position_state(self._VALID_RECORD)
        assert r["ok"] is True

    def test_valid_flat_ok(self):
        rec = dict(self._VALID_RECORD,
                   position_state="FLAT",
                   entry_order_id="",
                   entry_price=0.0,
                   qty=0.0,
                   position_side="none",
                   reason="flat")
        r = validate_live_position_state(rec)
        assert r["ok"] is True

    def test_valid_unknown_ok(self):
        rec = dict(self._VALID_RECORD,
                   position_state="UNKNOWN",
                   entry_order_id="",
                   reason="cannot determine")
        r = validate_live_position_state(rec)
        assert r["ok"] is True

    def test_normalized_record_present(self):
        r = validate_live_position_state(self._VALID_RECORD)
        assert r["normalized_record"] is not None

    def test_reason_position_state_ok(self):
        r = validate_live_position_state(self._VALID_RECORD)
        assert r["reason"] == "POSITION_STATE_OK"


# ---------------------------------------------------------------------------
# N. Validator: invalid position_state blocked
# ---------------------------------------------------------------------------

class TestValidatorPositionState:
    _BASE = {
        "lane": "live_test",
        "market": "BNB-EUR",
        "strategy_key": "EDGE3",
        "position_state": "OPEN_POSITION",
        "entry_order_id": "X",
        "entry_price": 600.0,
        "qty": 0.08,
        "position_side": "long",
        "ts_observed_utc": "2026-04-12T12:00:00Z",
        "reason": "ok",
    }

    def _v(self, **overrides):
        r = dict(self._BASE)
        r.update(overrides)
        return validate_live_position_state(r)

    def test_unknown_position_state_blocked(self):
        assert self._v(position_state="PANIC")["ok"] is False

    def test_none_position_state_blocked(self):
        assert self._v(position_state=None)["ok"] is False

    def test_wrong_lane_blocked(self):
        assert self._v(lane="wrong")["ok"] is False

    def test_wrong_market_blocked(self):
        assert self._v(market="BTC-EUR")["ok"] is False

    def test_wrong_strategy_blocked(self):
        assert self._v(strategy_key="RSI")["ok"] is False

    def test_wrong_side_blocked(self):
        assert self._v(position_side="buy")["ok"] is False


# ---------------------------------------------------------------------------
# O. Validator: missing required fields blocked
# ---------------------------------------------------------------------------

class TestValidatorMissingFields:
    _BASE = {
        "lane": "live_test",
        "market": "BNB-EUR",
        "strategy_key": "EDGE3",
        "position_state": "FLAT",
        "entry_order_id": "",
        "entry_price": 0.0,
        "qty": 0.0,
        "position_side": "none",
        "ts_observed_utc": "2026-04-12T12:00:00Z",
        "reason": "flat",
    }

    @pytest.mark.parametrize("field", [
        "lane", "market", "strategy_key", "position_state",
        "entry_order_id", "entry_price", "qty", "position_side",
        "ts_observed_utc", "reason",
    ])
    def test_missing_field_blocked(self, field):
        rec = dict(self._BASE)
        del rec[field]
        r = validate_live_position_state(rec)
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# P. Validator: empty entry_order_id for OPEN_POSITION blocked
# ---------------------------------------------------------------------------

class TestValidatorOrderIdRequired:
    def test_empty_order_id_open_position_blocked(self):
        r = validate_live_position_state({
            "lane": "live_test", "market": "BNB-EUR", "strategy_key": "EDGE3",
            "position_state": "OPEN_POSITION", "entry_order_id": "",
            "entry_price": 600.0, "qty": 0.08, "position_side": "long",
            "ts_observed_utc": "2026-04-12T12:00:00Z", "reason": "open",
        })
        assert r["ok"] is False

    def test_empty_order_id_mismatch_blocked(self):
        r = validate_live_position_state({
            "lane": "live_test", "market": "BNB-EUR", "strategy_key": "EDGE3",
            "position_state": "POSITION_MISMATCH", "entry_order_id": "",
            "entry_price": 600.0, "qty": 0.08, "position_side": "long",
            "ts_observed_utc": "2026-04-12T12:00:00Z", "reason": "mismatch",
        })
        assert r["ok"] is False

    def test_empty_order_id_flat_allowed(self):
        r = validate_live_position_state({
            "lane": "live_test", "market": "BNB-EUR", "strategy_key": "EDGE3",
            "position_state": "FLAT", "entry_order_id": "",
            "entry_price": 0.0, "qty": 0.0, "position_side": "none",
            "ts_observed_utc": "2026-04-12T12:00:00Z", "reason": "flat",
        })
        assert r["ok"] is True


# ---------------------------------------------------------------------------
# Q. Validator: qty < 0 blocked
# ---------------------------------------------------------------------------

class TestValidatorQty:
    def test_negative_qty_blocked(self):
        r = validate_live_position_state({
            "lane": "live_test", "market": "BNB-EUR", "strategy_key": "EDGE3",
            "position_state": "OPEN_POSITION", "entry_order_id": "X",
            "entry_price": 600.0, "qty": -1.0, "position_side": "long",
            "ts_observed_utc": "2026-04-12T12:00:00Z", "reason": "x",
        })
        assert r["ok"] is False

    def test_bool_qty_blocked(self):
        r = validate_live_position_state({
            "lane": "live_test", "market": "BNB-EUR", "strategy_key": "EDGE3",
            "position_state": "OPEN_POSITION", "entry_order_id": "X",
            "entry_price": 600.0, "qty": True, "position_side": "long",
            "ts_observed_utc": "2026-04-12T12:00:00Z", "reason": "x",
        })
        assert r["ok"] is False

    def test_zero_qty_flat_allowed(self):
        r = validate_live_position_state({
            "lane": "live_test", "market": "BNB-EUR", "strategy_key": "EDGE3",
            "position_state": "FLAT", "entry_order_id": "",
            "entry_price": 0.0, "qty": 0.0, "position_side": "none",
            "ts_observed_utc": "2026-04-12T12:00:00Z", "reason": "flat",
        })
        assert r["ok"] is True


# ---------------------------------------------------------------------------
# R. Validator: invalid ts_utc blocked
# ---------------------------------------------------------------------------

class TestValidatorTimestamp:
    def test_bad_ts_string_blocked(self):
        r = validate_live_position_state({
            "lane": "live_test", "market": "BNB-EUR", "strategy_key": "EDGE3",
            "position_state": "FLAT", "entry_order_id": "",
            "entry_price": 0.0, "qty": 0.0, "position_side": "none",
            "ts_observed_utc": "not-a-date", "reason": "flat",
        })
        assert r["ok"] is False

    def test_empty_ts_blocked(self):
        r = validate_live_position_state({
            "lane": "live_test", "market": "BNB-EUR", "strategy_key": "EDGE3",
            "position_state": "FLAT", "entry_order_id": "",
            "entry_price": 0.0, "qty": 0.0, "position_side": "none",
            "ts_observed_utc": "", "reason": "flat",
        })
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# S. No exceptions leak
# ---------------------------------------------------------------------------

class TestNoExceptions:
    @pytest.mark.parametrize("bad", [None, 42, "x", [], True, {}])
    def test_no_exception_bad_entry(self, bad):
        r = capture_live_position_state(bad, _OPEN_SNAPSHOT)
        assert isinstance(r, dict)
        assert "ok" in r

    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_no_exception_bad_snapshot(self, bad):
        r = capture_live_position_state(_ENTRY_RESULT, bad)
        assert isinstance(r, dict)
        assert "ok" in r

    @pytest.mark.parametrize("bad", [None, 42, "x", [], True, {}])
    def test_validator_no_exception_bad_input(self, bad):
        r = validate_live_position_state(bad)
        assert isinstance(r, dict)
        assert "ok" in r

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0, True):
            r = capture_live_position_state(v, v)
            assert isinstance(r, dict)


# ---------------------------------------------------------------------------
# T. No paper/network markers in source
# ---------------------------------------------------------------------------

_PAPER_MARKERS = (
    "build_execution_bridge",
    "build_paper",
    "dry_run_ledger",
    "paper_runner",
    "paper_intent",
    "ANT_OUT",
)

_NET_MARKERS = (
    "import requests",
    "urllib",
    "http.client",
    "python_bitvavo_api",
)


class TestSourceMarkers:
    def _src(self, filename: str) -> str:
        return (_REPO_ROOT / "ant_colony" / "live" / filename).read_text(encoding="utf-8")

    def test_outcome_capture_no_paper_markers(self):
        src = self._src("live_outcome_capture.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"live_outcome_capture.py contains: {m!r}"

    def test_position_state_no_paper_markers(self):
        src = self._src("live_position_state.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"live_position_state.py contains: {m!r}"

    def test_outcome_capture_no_network(self):
        src = self._src("live_outcome_capture.py")
        for m in _NET_MARKERS:
            assert m not in src, f"live_outcome_capture.py contains network marker: {m!r}"

    def test_position_state_no_network(self):
        src = self._src("live_position_state.py")
        for m in _NET_MARKERS:
            assert m not in src, f"live_position_state.py contains network marker: {m!r}"


# ---------------------------------------------------------------------------
# U. Negative position_qty → POSITION_MISMATCH
# ---------------------------------------------------------------------------

class TestNegativeQty:
    def test_negative_position_qty_mismatch(self):
        snap = dict(_OPEN_SNAPSHOT, position_qty=-0.08)
        r = _cap(snap=snap)
        assert r["ok"] is True
        assert r["position_state_record"]["position_state"] == "POSITION_MISMATCH"

    def test_negative_qty_reason(self):
        snap = dict(_OPEN_SNAPSHOT, position_qty=-0.08)
        r = _cap(snap=snap)
        assert "negative" in r["position_state_record"]["reason"].lower()


# ---------------------------------------------------------------------------
# V. Unreadable position_qty → UNKNOWN
# ---------------------------------------------------------------------------

class TestUnreadableQty:
    def test_string_qty_unknown(self):
        snap = dict(_OPEN_SNAPSHOT, position_qty="bad")
        r = _cap(snap=snap)
        assert r["ok"] is True
        assert r["position_state_record"]["position_state"] == "UNKNOWN"

    def test_none_qty_unknown(self):
        snap = dict(_OPEN_SNAPSHOT, position_qty=None)
        r = _cap(snap=snap)
        assert r["ok"] is True
        assert r["position_state_record"]["position_state"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# W. Various non-dict snapshot types → UNKNOWN
# ---------------------------------------------------------------------------

class TestNonDictSnapshots:
    @pytest.mark.parametrize("bad_snap", [42, "x", [], True])
    def test_non_dict_returns_unknown(self, bad_snap):
        r = capture_live_position_state(_ENTRY_RESULT, bad_snap)
        assert r["ok"] is True
        assert r["position_state_record"]["position_state"] == "UNKNOWN"
