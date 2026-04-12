"""
AC-159: Tests for Live Feedback Schema and Builder

Verifies:
  SCHEMA VALIDATOR
  A. Valid feedback record with all causal fields → ok=True
  B. Missing any required field → blocked
  C. Missing causal fields specifically → blocked (not optional)
  D. Invalid market_regime_at_entry → blocked
  E. Invalid volatility_at_entry → blocked
  F. signal_strength out of range → blocked
  G. signal_strength = -1.0 (not available) → allowed
  H. signal_strength = 0.0 (minimum valid) → allowed
  I. signal_strength = 1.0 (maximum valid) → allowed
  J. signal_strength non-numeric → blocked
  K. entry_latency_ms < 0 → blocked
  L. entry_latency_ms = 0 → allowed
  M. Invalid feedback_version → blocked
  N. Invalid feedback_ts_utc → blocked
  O. Invalid AC-148 fields (price <= 0, qty <= 0, etc.) → blocked
  P. Normalized record has exact field order (all 26 fields)
  Q. No exceptions leak (fail-closed)

  BUILDER
  R. Valid closed trade + valid causal context → ok=True
  S. Missing closed trade fields → blocked
  T. Missing causal context fields → blocked (each one matters)
  U. Invalid causal_context type → blocked
  V. Invalid closed_trade_result type → blocked
  W. Builder output shape (ok, reason, feedback_record)
  X. Builder propagates all AC-148 fields correctly
  Y. Builder propagates all causal fields correctly
  Z. feedback_version is always "1"
  AA. feedback_ts_utc is a valid UTC timestamp
  AB. No exceptions leak from builder

  CAUSAL CONTEXT IS MANDATORY
  AC. Removing any single causal field blocks the builder
  AD. Validator rejects record with UNKNOWN in all causal fields still passes if schema-valid
  AE. Causal context with UNKNOWN regime + UNKNOWN volatility still accepted (explicit unknowns are valid)

  MARKER TESTS
  AF. No paper imports in feedback modules
  AG. No network/http markers in feedback modules
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_feedback_schema import validate_live_feedback_record
from ant_colony.live.live_feedback_builder import build_live_feedback_record

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = "2026-04-12T12:00:00Z"
_ENTRY_TS = "2025-01-01T10:00:00Z"
_EXIT_TS = "2025-01-01T10:30:00Z"

_VALID_FEEDBACK_RECORD = {
    # AC-148 closed trade fields
    "trade_id": "LIVE-BNBEUR-EDGE3-LONG-20250101T100000",
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "qty": 0.08,
    "entry_ts_utc": _ENTRY_TS,
    "exit_ts_utc": _EXIT_TS,
    "entry_price": 600.0,
    "exit_price": 620.0,
    "realized_pnl_eur": 1.6,
    "slippage_eur": 0.0,
    "hold_duration_minutes": 30.0,
    "exit_reason": "SIGNAL",
    "execution_quality_flag": "OK",
    "broker_order_id_entry": "BTV-ORDER-001",
    "broker_order_id_exit": "BTV-EXIT-002",
    "ts_recorded_utc": _EXIT_TS,
    # Causal context
    "market_regime_at_entry": "BULL",
    "volatility_at_entry": "MID",
    "signal_strength": 0.78,
    "signal_key": "EDGE3_BULL_CROSS",
    "slippage_vs_expected_eur": 0.12,
    "entry_latency_ms": 45,
    "feedback_ts_utc": _NOW,
    "feedback_version": "1",
}

_VALID_CLOSED_TRADE = {
    "trade_id": "LIVE-BNBEUR-EDGE3-LONG-20250101T100000",
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "qty": 0.08,
    "entry_ts_utc": _ENTRY_TS,
    "exit_ts_utc": _EXIT_TS,
    "entry_price": 600.0,
    "exit_price": 620.0,
    "realized_pnl_eur": 1.6,
    "slippage_eur": 0.0,
    "hold_duration_minutes": 30.0,
    "exit_reason": "SIGNAL",
    "execution_quality_flag": "OK",
    "broker_order_id_entry": "BTV-ORDER-001",
    "broker_order_id_exit": "BTV-EXIT-002",
    "ts_recorded_utc": _EXIT_TS,
}

_VALID_CAUSAL_CONTEXT = {
    "market_regime_at_entry": "BULL",
    "volatility_at_entry": "MID",
    "signal_strength": 0.78,
    "signal_key": "EDGE3_BULL_CROSS",
    "slippage_vs_expected_eur": 0.12,
    "entry_latency_ms": 45,
}


def _v(**overrides):
    r = dict(_VALID_FEEDBACK_RECORD)
    r.update(overrides)
    return validate_live_feedback_record(r)


def _build(closed=None, causal=None, **closed_overrides):
    c = dict(_VALID_CLOSED_TRADE)
    c.update(closed_overrides)
    if closed is not None:
        c = closed
    ctx = causal if causal is not None else dict(_VALID_CAUSAL_CONTEXT)
    return build_live_feedback_record(c, ctx)


# ---------------------------------------------------------------------------
# A. Valid feedback record → ok=True
# ---------------------------------------------------------------------------

class TestValidRecord:
    def test_ok_true(self):
        assert _v()["ok"] is True

    def test_reason_feedback_record_ok(self):
        assert _v()["reason"] == "FEEDBACK_RECORD_OK"

    def test_normalized_record_present(self):
        assert _v()["normalized_record"] is not None

    def test_normalized_record_is_dict(self):
        assert isinstance(_v()["normalized_record"], dict)


# ---------------------------------------------------------------------------
# B. Missing any required field → blocked
# ---------------------------------------------------------------------------

class TestMissingFields:
    _ALL_FIELDS = list(_VALID_FEEDBACK_RECORD.keys())

    @pytest.mark.parametrize("field", _ALL_FIELDS)
    def test_missing_field_blocked(self, field):
        rec = dict(_VALID_FEEDBACK_RECORD)
        del rec[field]
        r = validate_live_feedback_record(rec)
        assert r["ok"] is False, f"expected failure when {field!r} missing"


# ---------------------------------------------------------------------------
# C. Missing causal fields specifically → blocked (not optional)
# ---------------------------------------------------------------------------

class TestCausalFieldsMandatory:
    _CAUSAL_FIELDS = (
        "market_regime_at_entry",
        "volatility_at_entry",
        "signal_strength",
        "signal_key",
        "slippage_vs_expected_eur",
        "entry_latency_ms",
    )

    @pytest.mark.parametrize("field", _CAUSAL_FIELDS)
    def test_missing_causal_field_blocked(self, field):
        rec = dict(_VALID_FEEDBACK_RECORD)
        del rec[field]
        r = validate_live_feedback_record(rec)
        assert r["ok"] is False, f"expected failure when causal field {field!r} missing"


# ---------------------------------------------------------------------------
# D. Invalid market_regime_at_entry → blocked
# ---------------------------------------------------------------------------

class TestMarketRegime:
    def test_unknown_value_blocked(self):
        assert _v(market_regime_at_entry="CRASH")["ok"] is False

    def test_none_blocked(self):
        assert _v(market_regime_at_entry=None)["ok"] is False

    def test_bull_allowed(self):
        assert _v(market_regime_at_entry="BULL")["ok"] is True

    def test_bear_allowed(self):
        assert _v(market_regime_at_entry="BEAR")["ok"] is True

    def test_sideways_allowed(self):
        assert _v(market_regime_at_entry="SIDEWAYS")["ok"] is True

    def test_unknown_allowed(self):
        assert _v(market_regime_at_entry="UNKNOWN")["ok"] is True


# ---------------------------------------------------------------------------
# E. Invalid volatility_at_entry → blocked
# ---------------------------------------------------------------------------

class TestVolatility:
    def test_invalid_value_blocked(self):
        assert _v(volatility_at_entry="EXTREME")["ok"] is False

    def test_none_blocked(self):
        assert _v(volatility_at_entry=None)["ok"] is False

    def test_low_allowed(self):
        assert _v(volatility_at_entry="LOW")["ok"] is True

    def test_mid_allowed(self):
        assert _v(volatility_at_entry="MID")["ok"] is True

    def test_high_allowed(self):
        assert _v(volatility_at_entry="HIGH")["ok"] is True

    def test_unknown_allowed(self):
        assert _v(volatility_at_entry="UNKNOWN")["ok"] is True


# ---------------------------------------------------------------------------
# F. signal_strength out of range → blocked
# ---------------------------------------------------------------------------

class TestSignalStrengthRange:
    def test_above_1_blocked(self):
        assert _v(signal_strength=1.01)["ok"] is False

    def test_below_minus1_blocked(self):
        assert _v(signal_strength=-1.01)["ok"] is False

    def test_minus_half_blocked(self):
        assert _v(signal_strength=-0.5)["ok"] is False

    def test_reason_mentions_signal_strength(self):
        r = _v(signal_strength=2.0)
        assert "signal_strength" in r["reason"]


# ---------------------------------------------------------------------------
# G–I. signal_strength boundary values
# ---------------------------------------------------------------------------

class TestSignalStrengthBoundaries:
    def test_minus_one_allowed(self):
        assert _v(signal_strength=-1.0)["ok"] is True

    def test_zero_allowed(self):
        assert _v(signal_strength=0.0)["ok"] is True

    def test_one_allowed(self):
        assert _v(signal_strength=1.0)["ok"] is True

    def test_midpoint_allowed(self):
        assert _v(signal_strength=0.5)["ok"] is True

    def test_exact_78_allowed(self):
        assert _v(signal_strength=0.78)["ok"] is True


# ---------------------------------------------------------------------------
# J. signal_strength non-numeric → blocked
# ---------------------------------------------------------------------------

class TestSignalStrengthType:
    def test_string_blocked(self):
        assert _v(signal_strength="strong")["ok"] is False

    def test_none_blocked(self):
        assert _v(signal_strength=None)["ok"] is False

    def test_bool_blocked(self):
        assert _v(signal_strength=True)["ok"] is False


# ---------------------------------------------------------------------------
# K–L. entry_latency_ms
# ---------------------------------------------------------------------------

class TestEntryLatency:
    def test_negative_blocked(self):
        assert _v(entry_latency_ms=-1)["ok"] is False

    def test_zero_allowed(self):
        assert _v(entry_latency_ms=0)["ok"] is True

    def test_positive_allowed(self):
        assert _v(entry_latency_ms=120)["ok"] is True

    def test_float_allowed(self):
        assert _v(entry_latency_ms=45.5)["ok"] is True

    def test_bool_blocked(self):
        assert _v(entry_latency_ms=True)["ok"] is False

    def test_string_blocked(self):
        assert _v(entry_latency_ms="fast")["ok"] is False


# ---------------------------------------------------------------------------
# M. Invalid feedback_version → blocked
# ---------------------------------------------------------------------------

class TestFeedbackVersion:
    def test_version_2_blocked(self):
        assert _v(feedback_version="2")["ok"] is False

    def test_int_version_blocked(self):
        assert _v(feedback_version=1)["ok"] is False

    def test_empty_blocked(self):
        assert _v(feedback_version="")["ok"] is False

    def test_version_1_allowed(self):
        assert _v(feedback_version="1")["ok"] is True


# ---------------------------------------------------------------------------
# N. Invalid feedback_ts_utc → blocked
# ---------------------------------------------------------------------------

class TestFeedbackTimestamp:
    def test_bad_ts_blocked(self):
        assert _v(feedback_ts_utc="not-a-date")["ok"] is False

    def test_empty_ts_blocked(self):
        assert _v(feedback_ts_utc="")["ok"] is False

    def test_none_ts_blocked(self):
        assert _v(feedback_ts_utc=None)["ok"] is False

    def test_valid_ts_allowed(self):
        assert _v(feedback_ts_utc=_NOW)["ok"] is True


# ---------------------------------------------------------------------------
# O. Invalid AC-148 fields → blocked
# ---------------------------------------------------------------------------

class TestAC148Fields:
    def test_entry_price_zero_blocked(self):
        assert _v(entry_price=0)["ok"] is False

    def test_exit_price_negative_blocked(self):
        assert _v(exit_price=-1.0)["ok"] is False

    def test_qty_zero_blocked(self):
        assert _v(qty=0)["ok"] is False

    def test_hold_duration_negative_blocked(self):
        assert _v(hold_duration_minutes=-1.0)["ok"] is False

    def test_wrong_exit_reason_blocked(self):
        assert _v(exit_reason="PANIC")["ok"] is False

    def test_wrong_quality_flag_blocked(self):
        assert _v(execution_quality_flag="PERFECT")["ok"] is False

    def test_empty_broker_order_id_entry_blocked(self):
        assert _v(broker_order_id_entry="")["ok"] is False

    def test_empty_broker_order_id_exit_blocked(self):
        assert _v(broker_order_id_exit="")["ok"] is False


# ---------------------------------------------------------------------------
# P. Normalized record has all 26 fields
# ---------------------------------------------------------------------------

class TestNormalizedShape:
    _EXPECTED_FIELDS = (
        "trade_id", "lane", "market", "strategy_key", "position_side",
        "qty", "entry_ts_utc", "exit_ts_utc", "entry_price", "exit_price",
        "realized_pnl_eur", "slippage_eur", "hold_duration_minutes",
        "exit_reason", "execution_quality_flag", "broker_order_id_entry",
        "broker_order_id_exit", "ts_recorded_utc",
        "market_regime_at_entry", "volatility_at_entry", "signal_strength",
        "signal_key", "slippage_vs_expected_eur", "entry_latency_ms",
        "feedback_ts_utc", "feedback_version",
    )

    def test_all_26_fields_present(self):
        rec = _v()["normalized_record"]
        assert len(rec) == 26
        for f in self._EXPECTED_FIELDS:
            assert f in rec, f"missing field: {f}"

    def test_causal_fields_in_normalized(self):
        rec = _v()["normalized_record"]
        assert rec["market_regime_at_entry"] == "BULL"
        assert rec["volatility_at_entry"] == "MID"
        assert rec["signal_strength"] == 0.78
        assert rec["signal_key"] == "EDGE3_BULL_CROSS"


# ---------------------------------------------------------------------------
# Q. No exceptions leak
# ---------------------------------------------------------------------------

class TestSchemaNoExceptions:
    @pytest.mark.parametrize("bad", [None, 42, "x", [], True, {}])
    def test_no_exception_bad_input(self, bad):
        r = validate_live_feedback_record(bad)
        assert isinstance(r, dict)
        assert "ok" in r
        assert r["ok"] is False

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0, True):
            r = validate_live_feedback_record(v)
            assert isinstance(r, dict)


# ---------------------------------------------------------------------------
# R. Builder: valid inputs → ok=True
# ---------------------------------------------------------------------------

class TestBuilderValid:
    def test_ok_true(self):
        assert _build()["ok"] is True

    def test_reason(self):
        assert _build()["reason"] == "FEEDBACK_RECORD_BUILT"

    def test_feedback_record_present(self):
        assert _build()["feedback_record"] is not None

    def test_feedback_record_is_dict(self):
        assert isinstance(_build()["feedback_record"], dict)


# ---------------------------------------------------------------------------
# S. Builder: missing closed trade fields → blocked
# ---------------------------------------------------------------------------

class TestBuilderMissingClosedFields:
    @pytest.mark.parametrize("field", [
        "trade_id", "lane", "market", "strategy_key", "position_side",
        "qty", "entry_ts_utc", "exit_ts_utc", "entry_price", "exit_price",
        "realized_pnl_eur", "slippage_eur", "hold_duration_minutes",
        "exit_reason", "execution_quality_flag",
        "broker_order_id_entry", "broker_order_id_exit", "ts_recorded_utc",
    ])
    def test_missing_closed_field_blocked(self, field):
        closed = dict(_VALID_CLOSED_TRADE)
        del closed[field]
        r = build_live_feedback_record(closed, _VALID_CAUSAL_CONTEXT)
        assert r["ok"] is False, f"expected failure when {field!r} missing from closed trade"


# ---------------------------------------------------------------------------
# T. Builder: missing causal context fields → blocked (each one matters)
# ---------------------------------------------------------------------------

class TestBuilderMissingCausalFields:
    @pytest.mark.parametrize("field", [
        "market_regime_at_entry",
        "volatility_at_entry",
        "signal_strength",
        "signal_key",
        "slippage_vs_expected_eur",
        "entry_latency_ms",
    ])
    def test_missing_causal_field_blocked(self, field):
        causal = dict(_VALID_CAUSAL_CONTEXT)
        del causal[field]
        r = build_live_feedback_record(_VALID_CLOSED_TRADE, causal)
        assert r["ok"] is False, f"expected failure when causal field {field!r} missing"


# ---------------------------------------------------------------------------
# U–V. Builder: bad input types → blocked
# ---------------------------------------------------------------------------

class TestBuilderBadTypes:
    def test_none_closed_trade_blocked(self):
        assert build_live_feedback_record(None, _VALID_CAUSAL_CONTEXT)["ok"] is False

    def test_string_closed_trade_blocked(self):
        assert build_live_feedback_record("bad", _VALID_CAUSAL_CONTEXT)["ok"] is False

    def test_none_causal_context_blocked(self):
        assert build_live_feedback_record(_VALID_CLOSED_TRADE, None)["ok"] is False

    def test_string_causal_context_blocked(self):
        assert build_live_feedback_record(_VALID_CLOSED_TRADE, "bad")["ok"] is False

    def test_empty_causal_context_blocked(self):
        assert build_live_feedback_record(_VALID_CLOSED_TRADE, {})["ok"] is False


# ---------------------------------------------------------------------------
# W. Builder output shape
# ---------------------------------------------------------------------------

class TestBuilderOutputShape:
    _REQUIRED = ("ok", "reason", "feedback_record")

    def test_ok_result_has_required_keys(self):
        r = _build()
        for k in self._REQUIRED:
            assert k in r, f"missing key: {k}"

    def test_blocked_result_has_required_keys(self):
        r = build_live_feedback_record(None, _VALID_CAUSAL_CONTEXT)
        for k in self._REQUIRED:
            assert k in r, f"missing key: {k}"

    def test_blocked_feedback_record_is_none(self):
        r = build_live_feedback_record(None, _VALID_CAUSAL_CONTEXT)
        assert r["feedback_record"] is None


# ---------------------------------------------------------------------------
# X. Builder propagates all AC-148 fields correctly
# ---------------------------------------------------------------------------

class TestBuilderAC148Propagation:
    def test_trade_id_preserved(self):
        assert _build()["feedback_record"]["trade_id"] == _VALID_CLOSED_TRADE["trade_id"]

    def test_realized_pnl_preserved(self):
        assert _build()["feedback_record"]["realized_pnl_eur"] == 1.6

    def test_entry_price_preserved(self):
        assert _build()["feedback_record"]["entry_price"] == 600.0

    def test_exit_price_preserved(self):
        assert _build()["feedback_record"]["exit_price"] == 620.0

    def test_exit_reason_preserved(self):
        assert _build()["feedback_record"]["exit_reason"] == "SIGNAL"

    def test_broker_order_ids_preserved(self):
        fr = _build()["feedback_record"]
        assert fr["broker_order_id_entry"] == "BTV-ORDER-001"
        assert fr["broker_order_id_exit"] == "BTV-EXIT-002"

    def test_hold_duration_preserved(self):
        assert _build()["feedback_record"]["hold_duration_minutes"] == 30.0


# ---------------------------------------------------------------------------
# Y. Builder propagates all causal fields correctly
# ---------------------------------------------------------------------------

class TestBuilderCausalPropagation:
    def test_market_regime_propagated(self):
        assert _build()["feedback_record"]["market_regime_at_entry"] == "BULL"

    def test_volatility_propagated(self):
        assert _build()["feedback_record"]["volatility_at_entry"] == "MID"

    def test_signal_strength_propagated(self):
        assert _build()["feedback_record"]["signal_strength"] == 0.78

    def test_signal_key_propagated(self):
        assert _build()["feedback_record"]["signal_key"] == "EDGE3_BULL_CROSS"

    def test_slippage_vs_expected_propagated(self):
        assert _build()["feedback_record"]["slippage_vs_expected_eur"] == 0.12

    def test_entry_latency_ms_propagated(self):
        assert _build()["feedback_record"]["entry_latency_ms"] == 45

    def test_different_regime_propagated(self):
        causal = dict(_VALID_CAUSAL_CONTEXT, market_regime_at_entry="BEAR")
        fr = build_live_feedback_record(_VALID_CLOSED_TRADE, causal)["feedback_record"]
        assert fr["market_regime_at_entry"] == "BEAR"

    def test_not_available_signal_propagated(self):
        causal = dict(_VALID_CAUSAL_CONTEXT, signal_strength=-1.0)
        fr = build_live_feedback_record(_VALID_CLOSED_TRADE, causal)["feedback_record"]
        assert fr["signal_strength"] == -1.0


# ---------------------------------------------------------------------------
# Z. feedback_version is always "1"
# ---------------------------------------------------------------------------

class TestFeedbackVersionAlways1:
    def test_version_is_1(self):
        assert _build()["feedback_record"]["feedback_version"] == "1"

    def test_version_survives_all_valid_causal_combos(self):
        for regime in ("BULL", "BEAR", "SIDEWAYS", "UNKNOWN"):
            causal = dict(_VALID_CAUSAL_CONTEXT, market_regime_at_entry=regime)
            fr = build_live_feedback_record(_VALID_CLOSED_TRADE, causal)["feedback_record"]
            assert fr["feedback_version"] == "1"


# ---------------------------------------------------------------------------
# AA. feedback_ts_utc is a valid UTC timestamp
# ---------------------------------------------------------------------------

class TestFeedbackTimestampBuilt:
    def test_feedback_ts_utc_present(self):
        fr = _build()["feedback_record"]
        assert "feedback_ts_utc" in fr

    def test_feedback_ts_utc_is_string(self):
        assert isinstance(_build()["feedback_record"]["feedback_ts_utc"], str)

    def test_feedback_ts_utc_ends_with_z(self):
        ts = _build()["feedback_record"]["feedback_ts_utc"]
        assert ts.endswith("Z")


# ---------------------------------------------------------------------------
# AB. No exceptions leak from builder
# ---------------------------------------------------------------------------

class TestBuilderNoExceptions:
    @pytest.mark.parametrize("bad", [None, 42, "x", [], True, {}])
    def test_no_exception_bad_closed_trade(self, bad):
        r = build_live_feedback_record(bad, _VALID_CAUSAL_CONTEXT)
        assert isinstance(r, dict)
        assert "ok" in r

    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_no_exception_bad_causal_context(self, bad):
        r = build_live_feedback_record(_VALID_CLOSED_TRADE, bad)
        assert isinstance(r, dict)
        assert "ok" in r

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0, True):
            r = build_live_feedback_record(v, v)
            assert isinstance(r, dict)


# ---------------------------------------------------------------------------
# AC. Removing any single causal field blocks the builder
# ---------------------------------------------------------------------------

class TestCausalContextAllRequired:
    def test_all_six_causal_fields_required(self):
        for field in ("market_regime_at_entry", "volatility_at_entry",
                      "signal_strength", "signal_key",
                      "slippage_vs_expected_eur", "entry_latency_ms"):
            causal = dict(_VALID_CAUSAL_CONTEXT)
            del causal[field]
            r = build_live_feedback_record(_VALID_CLOSED_TRADE, causal)
            assert r["ok"] is False, f"expected block when {field!r} removed from causal context"
            assert r["feedback_record"] is None


# ---------------------------------------------------------------------------
# AD–AE. UNKNOWN values in causal context
# ---------------------------------------------------------------------------

class TestUnknownCausalValues:
    def test_all_unknown_causal_context_valid(self):
        causal = dict(
            _VALID_CAUSAL_CONTEXT,
            market_regime_at_entry="UNKNOWN",
            volatility_at_entry="UNKNOWN",
            signal_strength=-1.0,
            signal_key="UNKNOWN",
        )
        r = build_live_feedback_record(_VALID_CLOSED_TRADE, causal)
        assert r["ok"] is True

    def test_unknown_regime_explicit_is_valid(self):
        assert _v(market_regime_at_entry="UNKNOWN")["ok"] is True

    def test_unknown_volatility_explicit_is_valid(self):
        assert _v(volatility_at_entry="UNKNOWN")["ok"] is True

    def test_signal_strength_not_available_is_valid(self):
        assert _v(signal_strength=-1.0)["ok"] is True


# ---------------------------------------------------------------------------
# AF–AG. No paper/network markers in source
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

    def test_schema_no_paper_markers(self):
        src = self._src("live_feedback_schema.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"live_feedback_schema.py contains: {m!r}"

    def test_builder_no_paper_markers(self):
        src = self._src("live_feedback_builder.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"live_feedback_builder.py contains: {m!r}"

    def test_schema_no_network(self):
        src = self._src("live_feedback_schema.py")
        for m in _NET_MARKERS:
            assert m not in src, f"live_feedback_schema.py contains network marker: {m!r}"

    def test_builder_no_network(self):
        src = self._src("live_feedback_builder.py")
        for m in _NET_MARKERS:
            assert m not in src, f"live_feedback_builder.py contains network marker: {m!r}"
