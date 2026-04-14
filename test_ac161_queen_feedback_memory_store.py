"""
AC-161: Tests for Queen Feedback Intake and Memory Store

Verifies:
  INTAKE
  A. Valid AC-159 feedback record → QUEEN_FEEDBACK_ACCEPTED
  B. Invalid schema (missing causal field) → fail-closed
  C. Invalid market → fail-closed
  D. Invalid strategy_key → fail-closed
  E. Invalid lane → fail-closed
  F. Invalid signal_strength value → fail-closed
  G. Non-dict input → fail-closed
  H. Intake output shape (ok, reason, accepted_feedback)
  I. Accepted feedback is the normalized record from AC-159 schema
  J. No exceptions leak from intake

  MEMORY STORE
  K. Valid feedback → QUEEN_MEMORY_READY
  L. Memory entry contains all required compact fields
  M. Raw broker order IDs not present in memory entry
  N. win_loss_label = WIN for pnl > 0
  O. win_loss_label = LOSS for pnl < 0
  P. win_loss_label = FLAT for pnl == 0
  Q. anomaly_flag = True when execution_quality_flag != OK
  R. anomaly_flag = False when execution_quality_flag == OK
  S. queen_action_required = True when anomaly_flag is True
  T. queen_action_required = True when market_regime_at_entry == UNKNOWN
  U. queen_action_required = True when volatility_at_entry == UNKNOWN
  V. queen_action_required = False for clean normal trade
  W. All causal fields preserved in memory entry
  X. memory_version = "1"
  Y. record_type = "closed_trade_memory"
  Z. memory_ts_utc is a valid timestamp string
  AA. feedback_ts_utc preserved from input record
  AB. No exceptions leak from memory store

  NO-LEARNING MARKER TESTS
  AC. No paper imports in queen modules
  AD. No network/http markers
  AE. No learning/weight/allocation functions in source
  AF. No file IO markers (no ANT_OUT, no open(), no write())
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.queen_feedback_intake import intake_feedback_for_queen
from ant_colony.live.queen_memory_store import build_queen_memory_entry

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_NOW = "2026-04-12T12:00:00Z"
_ENTRY_TS = "2025-01-01T10:00:00Z"
_EXIT_TS = "2025-01-01T10:30:00Z"

_VALID_FEEDBACK = {
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
    "market_regime_at_entry": "BULL",
    "volatility_at_entry": "MID",
    "signal_strength": 0.78,
    "signal_key": "EDGE3_BULL_CROSS",
    "slippage_vs_expected_eur": 0.12,
    "entry_latency_ms": 45,
    "feedback_ts_utc": _NOW,
    "feedback_version": "1",
}


def _intake(**overrides):
    r = dict(_VALID_FEEDBACK)
    r.update(overrides)
    return intake_feedback_for_queen(r)


def _mem(**overrides):
    r = dict(_VALID_FEEDBACK)
    r.update(overrides)
    return build_queen_memory_entry(r)


# ---------------------------------------------------------------------------
# A. Valid record → QUEEN_FEEDBACK_ACCEPTED
# ---------------------------------------------------------------------------

class TestIntakeValid:
    def test_ok_true(self):
        assert _intake()["ok"] is True

    def test_reason(self):
        assert _intake()["reason"] == "QUEEN_FEEDBACK_ACCEPTED"

    def test_accepted_feedback_present(self):
        assert _intake()["accepted_feedback"] is not None

    def test_accepted_feedback_is_dict(self):
        assert isinstance(_intake()["accepted_feedback"], dict)


# ---------------------------------------------------------------------------
# B. Missing causal field → fail-closed
# ---------------------------------------------------------------------------

class TestIntakeMissingCausal:
    @pytest.mark.parametrize("field", [
        "market_regime_at_entry", "volatility_at_entry", "signal_strength",
        "signal_key", "slippage_vs_expected_eur", "entry_latency_ms",
    ])
    def test_missing_causal_field_blocked(self, field):
        r = dict(_VALID_FEEDBACK)
        del r[field]
        result = intake_feedback_for_queen(r)
        assert result["ok"] is False, f"expected fail when {field!r} missing"

    def test_missing_trade_id_blocked(self):
        r = dict(_VALID_FEEDBACK)
        del r["trade_id"]
        assert intake_feedback_for_queen(r)["ok"] is False


# ---------------------------------------------------------------------------
# C. Invalid market → fail-closed
# ---------------------------------------------------------------------------

class TestIntakeInvalidMarket:
    def test_wrong_market_blocked(self):
        assert _intake(market="BTC-EUR")["ok"] is False

    def test_empty_market_blocked(self):
        assert _intake(market="")["ok"] is False

    def test_reason_mentions_market(self):
        r = _intake(market="BTC-EUR")
        assert "market" in r["reason"].lower()


# ---------------------------------------------------------------------------
# D. Invalid strategy_key → fail-closed
# ---------------------------------------------------------------------------

class TestIntakeInvalidStrategy:
    def test_wrong_strategy_blocked(self):
        assert _intake(strategy_key="RSI_SIMPLE")["ok"] is False

    def test_empty_strategy_blocked(self):
        assert _intake(strategy_key="")["ok"] is False


# ---------------------------------------------------------------------------
# E. Invalid lane → fail-closed
# ---------------------------------------------------------------------------

class TestIntakeInvalidLane:
    def test_wrong_lane_blocked(self):
        assert _intake(lane="live_prod")["ok"] is False

    def test_empty_lane_blocked(self):
        assert _intake(lane="")["ok"] is False


# ---------------------------------------------------------------------------
# F. Invalid signal_strength → fail-closed
# ---------------------------------------------------------------------------

class TestIntakeInvalidSignalStrength:
    def test_out_of_range_blocked(self):
        assert _intake(signal_strength=1.5)["ok"] is False

    def test_negative_non_sentinel_blocked(self):
        assert _intake(signal_strength=-0.5)["ok"] is False

    def test_minus_one_accepted(self):
        assert _intake(signal_strength=-1.0)["ok"] is True

    def test_bool_blocked(self):
        assert _intake(signal_strength=True)["ok"] is False


# ---------------------------------------------------------------------------
# G. Non-dict input → fail-closed
# ---------------------------------------------------------------------------

class TestIntakeNonDict:
    @pytest.mark.parametrize("bad", [None, 42, "x", [], True, {}])
    def test_non_valid_input_blocked(self, bad):
        r = intake_feedback_for_queen(bad)
        assert r["ok"] is False

    def test_empty_dict_blocked(self):
        assert intake_feedback_for_queen({})["ok"] is False


# ---------------------------------------------------------------------------
# H. Intake output shape
# ---------------------------------------------------------------------------

class TestIntakeShape:
    _REQUIRED = ("ok", "reason", "accepted_feedback")

    def test_ok_result_has_required_keys(self):
        r = _intake()
        for k in self._REQUIRED:
            assert k in r, f"missing key: {k}"

    def test_blocked_result_has_required_keys(self):
        r = intake_feedback_for_queen(None)
        for k in self._REQUIRED:
            assert k in r, f"missing key: {k}"

    def test_blocked_accepted_feedback_is_none(self):
        assert intake_feedback_for_queen(None)["accepted_feedback"] is None


# ---------------------------------------------------------------------------
# I. Accepted feedback is the normalized AC-159 record
# ---------------------------------------------------------------------------

class TestIntakeNormalized:
    def test_lane_normalized(self):
        assert _intake()["accepted_feedback"]["lane"] == "live_test"

    def test_market_normalized(self):
        assert _intake()["accepted_feedback"]["market"] == "BNB-EUR"

    def test_causal_fields_present(self):
        af = _intake()["accepted_feedback"]
        for f in ("market_regime_at_entry", "volatility_at_entry", "signal_strength",
                  "signal_key", "slippage_vs_expected_eur", "entry_latency_ms"):
            assert f in af, f"missing causal field: {f}"


# ---------------------------------------------------------------------------
# J. No exceptions leak from intake
# ---------------------------------------------------------------------------

class TestIntakeNoExceptions:
    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_no_exception(self, bad):
        r = intake_feedback_for_queen(bad)
        assert isinstance(r, dict)
        assert "ok" in r

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0, True):
            r = intake_feedback_for_queen(v)
            assert isinstance(r, dict)


# ---------------------------------------------------------------------------
# K. Valid feedback → QUEEN_MEMORY_READY
# ---------------------------------------------------------------------------

class TestMemoryStoreValid:
    def test_ok_true(self):
        assert _mem()["ok"] is True

    def test_reason(self):
        assert _mem()["reason"] == "QUEEN_MEMORY_READY"

    def test_memory_entry_present(self):
        assert _mem()["memory_entry"] is not None

    def test_memory_entry_is_dict(self):
        assert isinstance(_mem()["memory_entry"], dict)


# ---------------------------------------------------------------------------
# L. Memory entry contains all required compact fields
# ---------------------------------------------------------------------------

class TestMemoryEntryFields:
    _REQUIRED_FIELDS = (
        "memory_version", "record_type", "lane", "market", "strategy_key",
        "trade_id", "entry_ts_utc", "exit_ts_utc", "hold_duration_minutes",
        "realized_pnl_eur", "win_loss_label", "exit_reason",
        "anomaly_flag", "execution_quality_flag",
        "market_regime_at_entry", "volatility_at_entry",
        "signal_strength", "signal_key", "slippage_vs_expected_eur",
        "entry_latency_ms", "feedback_ts_utc", "memory_ts_utc",
        "queen_action_required",
    )

    def test_all_required_fields_present(self):
        me = _mem()["memory_entry"]
        for f in self._REQUIRED_FIELDS:
            assert f in me, f"missing field: {f}"

    def test_field_count(self):
        me = _mem()["memory_entry"]
        assert len(me) == len(self._REQUIRED_FIELDS)


# ---------------------------------------------------------------------------
# M. Raw broker order IDs not present in memory entry
# ---------------------------------------------------------------------------

class TestMemoryNoBrokerIds:
    def test_broker_order_id_entry_not_in_memory(self):
        me = _mem()["memory_entry"]
        assert "broker_order_id_entry" not in me

    def test_broker_order_id_exit_not_in_memory(self):
        me = _mem()["memory_entry"]
        assert "broker_order_id_exit" not in me

    def test_ts_recorded_utc_not_in_memory(self):
        me = _mem()["memory_entry"]
        assert "ts_recorded_utc" not in me

    def test_slippage_eur_not_in_memory(self):
        # slippage_vs_expected_eur is the queen-useful metric; raw slippage_eur excluded
        me = _mem()["memory_entry"]
        assert "slippage_eur" not in me


# ---------------------------------------------------------------------------
# N–P. win_loss_label
# ---------------------------------------------------------------------------

class TestWinLossLabel:
    def test_positive_pnl_is_win(self):
        assert _mem(realized_pnl_eur=1.6)["memory_entry"]["win_loss_label"] == "WIN"

    def test_negative_pnl_is_loss(self):
        assert _mem(realized_pnl_eur=-0.5)["memory_entry"]["win_loss_label"] == "LOSS"

    def test_zero_pnl_is_flat(self):
        assert _mem(realized_pnl_eur=0.0)["memory_entry"]["win_loss_label"] == "FLAT"

    def test_small_positive_is_win(self):
        assert _mem(realized_pnl_eur=0.001)["memory_entry"]["win_loss_label"] == "WIN"

    def test_small_negative_is_loss(self):
        assert _mem(realized_pnl_eur=-0.001)["memory_entry"]["win_loss_label"] == "LOSS"


# ---------------------------------------------------------------------------
# Q–R. anomaly_flag
# ---------------------------------------------------------------------------

class TestAnomalyFlag:
    def test_partial_fill_is_anomaly(self):
        assert _mem(execution_quality_flag="PARTIAL_FILL")["memory_entry"]["anomaly_flag"] is True

    def test_mismatch_is_anomaly(self):
        assert _mem(execution_quality_flag="MISMATCH")["memory_entry"]["anomaly_flag"] is True

    def test_high_slippage_is_anomaly(self):
        assert _mem(execution_quality_flag="HIGH_SLIPPAGE")["memory_entry"]["anomaly_flag"] is True

    def test_timeout_recovered_is_anomaly(self):
        assert _mem(execution_quality_flag="TIMEOUT_RECOVERED")["memory_entry"]["anomaly_flag"] is True

    def test_ok_is_not_anomaly(self):
        assert _mem(execution_quality_flag="OK")["memory_entry"]["anomaly_flag"] is False

    def test_anomaly_flag_is_bool(self):
        assert isinstance(_mem()["memory_entry"]["anomaly_flag"], bool)


# ---------------------------------------------------------------------------
# S–V. queen_action_required
# ---------------------------------------------------------------------------

class TestQueenActionRequired:
    def test_anomaly_triggers_action_required(self):
        me = _mem(execution_quality_flag="PARTIAL_FILL")["memory_entry"]
        assert me["queen_action_required"] is True

    def test_unknown_regime_triggers_action_required(self):
        me = _mem(market_regime_at_entry="UNKNOWN")["memory_entry"]
        assert me["queen_action_required"] is True

    def test_unknown_volatility_triggers_action_required(self):
        me = _mem(volatility_at_entry="UNKNOWN")["memory_entry"]
        assert me["queen_action_required"] is True

    def test_clean_trade_no_action_required(self):
        me = _mem(
            execution_quality_flag="OK",
            market_regime_at_entry="BULL",
            volatility_at_entry="MID",
        )["memory_entry"]
        assert me["queen_action_required"] is False

    def test_bear_regime_no_action_required(self):
        # BEAR is a known regime — no action needed solely because it's bear
        me = _mem(market_regime_at_entry="BEAR", volatility_at_entry="LOW")["memory_entry"]
        assert me["queen_action_required"] is False

    def test_queen_action_required_is_bool(self):
        assert isinstance(_mem()["memory_entry"]["queen_action_required"], bool)

    def test_all_three_triggers_accumulate(self):
        # All three triggers → still True (not double-counted, just True)
        me = _mem(
            execution_quality_flag="MISMATCH",
            market_regime_at_entry="UNKNOWN",
            volatility_at_entry="UNKNOWN",
        )["memory_entry"]
        assert me["queen_action_required"] is True


# ---------------------------------------------------------------------------
# W. All causal fields preserved in memory entry
# ---------------------------------------------------------------------------

class TestCausalFieldsPreserved:
    def test_regime_preserved(self):
        assert _mem()["memory_entry"]["market_regime_at_entry"] == "BULL"

    def test_volatility_preserved(self):
        assert _mem()["memory_entry"]["volatility_at_entry"] == "MID"

    def test_signal_strength_preserved(self):
        assert _mem()["memory_entry"]["signal_strength"] == 0.78

    def test_signal_key_preserved(self):
        assert _mem()["memory_entry"]["signal_key"] == "EDGE3_BULL_CROSS"

    def test_slippage_vs_expected_preserved(self):
        assert _mem()["memory_entry"]["slippage_vs_expected_eur"] == 0.12

    def test_entry_latency_preserved(self):
        assert _mem()["memory_entry"]["entry_latency_ms"] == 45

    def test_not_available_signal_strength_preserved(self):
        me = _mem(signal_strength=-1.0)["memory_entry"]
        assert me["signal_strength"] == -1.0


# ---------------------------------------------------------------------------
# X–Y. memory_version and record_type
# ---------------------------------------------------------------------------

class TestMemoryMetadata:
    def test_memory_version_is_1(self):
        assert _mem()["memory_entry"]["memory_version"] == "1"

    def test_record_type_closed_when_exit_id_present(self):
        # Valid exit order ID → closed trade
        assert _mem()["memory_entry"]["record_type"] == "closed_trade_memory"

    def test_record_type_open_when_sentinel(self):
        # AC-189: ENTRY_ONLY_PENDING_EXIT sentinel → open trade
        result = _mem(broker_order_id_exit="ENTRY_ONLY_PENDING_EXIT")
        assert result["memory_entry"]["record_type"] == "open_trade_memory"

    def test_record_type_not_closed_when_sentinel(self):
        # AC-189: must never label a pending-exit trade as closed
        result = _mem(broker_order_id_exit="ENTRY_ONLY_PENDING_EXIT")
        assert result["memory_entry"]["record_type"] != "closed_trade_memory"

    def test_lane_correct(self):
        assert _mem()["memory_entry"]["lane"] == "live_test"

    def test_market_correct(self):
        assert _mem()["memory_entry"]["market"] == "BNB-EUR"

    def test_strategy_key_correct(self):
        assert _mem()["memory_entry"]["strategy_key"] == "EDGE3"


# ---------------------------------------------------------------------------
# Z. memory_ts_utc is a valid timestamp string
# ---------------------------------------------------------------------------

class TestMemoryTimestamp:
    def test_memory_ts_present(self):
        assert "memory_ts_utc" in _mem()["memory_entry"]

    def test_memory_ts_is_string(self):
        assert isinstance(_mem()["memory_entry"]["memory_ts_utc"], str)

    def test_memory_ts_ends_with_z(self):
        assert _mem()["memory_entry"]["memory_ts_utc"].endswith("Z")

    def test_memory_ts_has_date_format(self):
        ts = _mem()["memory_entry"]["memory_ts_utc"]
        assert "T" in ts and len(ts) >= 20


# ---------------------------------------------------------------------------
# AA. feedback_ts_utc preserved from input record
# ---------------------------------------------------------------------------

class TestFeedbackTsPreserved:
    def test_feedback_ts_preserved(self):
        assert _mem()["memory_entry"]["feedback_ts_utc"] == _NOW

    def test_hold_duration_preserved(self):
        assert _mem()["memory_entry"]["hold_duration_minutes"] == 30.0

    def test_trade_id_preserved(self):
        assert _mem()["memory_entry"]["trade_id"] == _VALID_FEEDBACK["trade_id"]


# ---------------------------------------------------------------------------
# AB. No exceptions leak from memory store
# ---------------------------------------------------------------------------

class TestMemoryStoreNoExceptions:
    @pytest.mark.parametrize("bad", [None, 42, "x", [], True, {}])
    def test_no_exception(self, bad):
        r = build_queen_memory_entry(bad)
        assert isinstance(r, dict)
        assert "ok" in r
        assert r["ok"] is False

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0, True):
            r = build_queen_memory_entry(v)
            assert isinstance(r, dict)


# ---------------------------------------------------------------------------
# AC–AF. Marker tests
# ---------------------------------------------------------------------------

_PAPER_MARKERS = (
    "build_execution_bridge", "build_paper", "dry_run_ledger",
    "paper_runner", "paper_intent", "ANT_OUT",
)

_NET_MARKERS = (
    "import requests", "urllib", "http.client", "python_bitvavo_api",
)

_LEARNING_MARKERS = (
    "update_weights", "update_strategy", "update_allocation",
    "adjust_regime", "backprop", "gradient",
    "fit(", ".fit(", "train(",
)

_FILE_IO_MARKERS = (
    "open(", ".write(", "ANT_OUT",
)


class TestSourceMarkers:
    def _src(self, filename: str) -> str:
        return (_REPO_ROOT / "ant_colony" / "live" / filename).read_text(encoding="utf-8")

    def test_intake_no_paper(self):
        src = self._src("queen_feedback_intake.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"queen_feedback_intake.py contains: {m!r}"

    def test_memory_store_no_paper(self):
        src = self._src("queen_memory_store.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"queen_memory_store.py contains: {m!r}"

    def test_intake_no_network(self):
        src = self._src("queen_feedback_intake.py")
        for m in _NET_MARKERS:
            assert m not in src

    def test_memory_store_no_network(self):
        src = self._src("queen_memory_store.py")
        for m in _NET_MARKERS:
            assert m not in src

    def test_intake_no_learning_functions(self):
        src = self._src("queen_feedback_intake.py")
        for m in _LEARNING_MARKERS:
            assert m not in src, f"queen_feedback_intake.py contains learning marker: {m!r}"

    def test_memory_store_no_learning_functions(self):
        src = self._src("queen_memory_store.py")
        for m in _LEARNING_MARKERS:
            assert m not in src, f"queen_memory_store.py contains learning marker: {m!r}"

    def test_memory_store_no_file_io(self):
        src = self._src("queen_memory_store.py")
        for m in _FILE_IO_MARKERS:
            assert m not in src, f"queen_memory_store.py contains file IO marker: {m!r}"

    def test_intake_no_file_io(self):
        src = self._src("queen_feedback_intake.py")
        for m in _FILE_IO_MARKERS:
            assert m not in src, f"queen_feedback_intake.py contains file IO marker: {m!r}"
