"""
AC-148: Tests for Live Execution Result Schema

Verifies:
  A. Valid record → ok=True, normalized_record present
  B. Missing any required field → fail-closed
  C. Invalid lane → fail-closed
  D. Invalid market → fail-closed
  E. Invalid strategy_key → fail-closed
  F. Invalid position_side → fail-closed
  G. qty <= 0 → fail-closed
  H. Invalid timestamps → fail-closed
  I. entry_price / exit_price <= 0 → fail-closed
  J. Invalid exit_reason → fail-closed
  K. Invalid execution_quality_flag → fail-closed
  L. Empty broker order ids → fail-closed
  M. Negative realized_pnl_eur allowed
  N. Negative slippage_eur allowed
  O. Zero hold_duration_minutes allowed
  P. Validator never raises exceptions
  Q. normalized_record preserves required key order
  R. Non-dict input → fail-closed
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_execution_result_schema import (
    validate_live_execution_result,
    _REQUIRED_FIELDS,
    _VALID_EXIT_REASONS,
    _VALID_QUALITY_FLAGS,
    _VALID_POSITION_SIDES,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_RECORD = {
    "trade_id": "TRADE-001",
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "qty": 1.5,
    "entry_ts_utc": "2026-04-01T10:00:00Z",
    "exit_ts_utc": "2026-04-01T10:45:00Z",
    "entry_price": 410.0,
    "exit_price": 418.5,
    "realized_pnl_eur": 12.75,
    "slippage_eur": -0.30,
    "hold_duration_minutes": 45,
    "exit_reason": "TP",
    "execution_quality_flag": "OK",
    "broker_order_id_entry": "BRK-ENTRY-001",
    "broker_order_id_exit": "BRK-EXIT-001",
    "ts_recorded_utc": "2026-04-01T10:45:05Z",
}


def _rec(**overrides) -> dict:
    r = dict(_VALID_RECORD)
    r.update(overrides)
    return r


def _rec_without(field: str) -> dict:
    r = dict(_VALID_RECORD)
    del r[field]
    return r


# ---------------------------------------------------------------------------
# A. Valid record → ok=True
# ---------------------------------------------------------------------------

class TestValidRecord:
    def test_valid_record_ok_true(self):
        result = validate_live_execution_result(_VALID_RECORD)
        assert result["ok"] is True

    def test_valid_record_has_normalized(self):
        result = validate_live_execution_result(_VALID_RECORD)
        assert "normalized_record" in result

    def test_valid_record_reason_present(self):
        result = validate_live_execution_result(_VALID_RECORD)
        assert isinstance(result["reason"], str)

    def test_valid_short_position(self):
        result = validate_live_execution_result(_rec(position_side="short"))
        assert result["ok"] is True

    def test_valid_all_exit_reasons(self):
        for reason in _VALID_EXIT_REASONS:
            result = validate_live_execution_result(_rec(exit_reason=reason))
            assert result["ok"] is True, f"exit_reason {reason!r} should be valid"

    def test_valid_all_quality_flags(self):
        for flag in _VALID_QUALITY_FLAGS:
            result = validate_live_execution_result(_rec(execution_quality_flag=flag))
            assert result["ok"] is True, f"quality flag {flag!r} should be valid"


# ---------------------------------------------------------------------------
# B. Missing required fields → fail-closed
# ---------------------------------------------------------------------------

class TestMissingFields:
    @pytest.mark.parametrize("field", _REQUIRED_FIELDS)
    def test_missing_field_blocked(self, field):
        result = validate_live_execution_result(_rec_without(field))
        assert result["ok"] is False
        assert field in result["reason"]

    def test_missing_field_no_normalized(self):
        result = validate_live_execution_result(_rec_without("trade_id"))
        assert "normalized_record" not in result


# ---------------------------------------------------------------------------
# C. Invalid lane
# ---------------------------------------------------------------------------

class TestInvalidLane:
    def test_wrong_lane_blocked(self):
        result = validate_live_execution_result(_rec(lane="paper"))
        assert result["ok"] is False
        assert "lane" in result["reason"]

    def test_empty_lane_blocked(self):
        result = validate_live_execution_result(_rec(lane=""))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# D. Invalid market
# ---------------------------------------------------------------------------

class TestInvalidMarket:
    def test_wrong_market_blocked(self):
        result = validate_live_execution_result(_rec(market="BTC-EUR"))
        assert result["ok"] is False
        assert "market" in result["reason"]

    def test_empty_market_blocked(self):
        result = validate_live_execution_result(_rec(market=""))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# E. Invalid strategy_key
# ---------------------------------------------------------------------------

class TestInvalidStrategy:
    def test_wrong_strategy_blocked(self):
        result = validate_live_execution_result(_rec(strategy_key="RSI_SIMPLE"))
        assert result["ok"] is False
        assert "strategy_key" in result["reason"]


# ---------------------------------------------------------------------------
# F. Invalid position_side
# ---------------------------------------------------------------------------

class TestInvalidPositionSide:
    def test_wrong_side_blocked(self):
        result = validate_live_execution_result(_rec(position_side="neutral"))
        assert result["ok"] is False
        assert "position_side" in result["reason"]

    def test_uppercase_side_blocked(self):
        # Contract requires lowercase
        result = validate_live_execution_result(_rec(position_side="LONG"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# G. qty <= 0
# ---------------------------------------------------------------------------

class TestQtyBounds:
    def test_zero_qty_blocked(self):
        result = validate_live_execution_result(_rec(qty=0))
        assert result["ok"] is False

    def test_negative_qty_blocked(self):
        result = validate_live_execution_result(_rec(qty=-1.0))
        assert result["ok"] is False

    def test_bool_qty_blocked(self):
        result = validate_live_execution_result(_rec(qty=True))
        assert result["ok"] is False

    def test_string_qty_blocked(self):
        result = validate_live_execution_result(_rec(qty="1.5"))
        assert result["ok"] is False

    def test_positive_float_qty_allowed(self):
        result = validate_live_execution_result(_rec(qty=0.001))
        assert result["ok"] is True


# ---------------------------------------------------------------------------
# H. Invalid timestamps
# ---------------------------------------------------------------------------

class TestInvalidTimestamps:
    @pytest.mark.parametrize("ts_field", ["entry_ts_utc", "exit_ts_utc", "ts_recorded_utc"])
    def test_empty_ts_blocked(self, ts_field):
        result = validate_live_execution_result(_rec(**{ts_field: ""}))
        assert result["ok"] is False

    @pytest.mark.parametrize("ts_field", ["entry_ts_utc", "exit_ts_utc", "ts_recorded_utc"])
    def test_bad_ts_string_blocked(self, ts_field):
        result = validate_live_execution_result(_rec(**{ts_field: "not-a-date"}))
        assert result["ok"] is False

    @pytest.mark.parametrize("ts_field", ["entry_ts_utc", "exit_ts_utc", "ts_recorded_utc"])
    def test_date_only_blocked(self, ts_field):
        result = validate_live_execution_result(_rec(**{ts_field: "2026-04-01"}))
        assert result["ok"] is False

    def test_valid_z_suffix_ts_accepted(self):
        result = validate_live_execution_result(_rec(entry_ts_utc="2026-04-01T10:00:00Z"))
        assert result["ok"] is True

    def test_valid_offset_ts_accepted(self):
        result = validate_live_execution_result(
            _rec(entry_ts_utc="2026-04-01T10:00:00+00:00")
        )
        assert result["ok"] is True


# ---------------------------------------------------------------------------
# I. entry_price / exit_price <= 0
# ---------------------------------------------------------------------------

class TestPriceBounds:
    def test_zero_entry_price_blocked(self):
        result = validate_live_execution_result(_rec(entry_price=0))
        assert result["ok"] is False

    def test_negative_entry_price_blocked(self):
        result = validate_live_execution_result(_rec(entry_price=-100.0))
        assert result["ok"] is False

    def test_zero_exit_price_blocked(self):
        result = validate_live_execution_result(_rec(exit_price=0))
        assert result["ok"] is False

    def test_string_price_blocked(self):
        result = validate_live_execution_result(_rec(entry_price="410.0"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# J. Invalid exit_reason
# ---------------------------------------------------------------------------

class TestInvalidExitReason:
    def test_unknown_exit_reason_blocked(self):
        result = validate_live_execution_result(_rec(exit_reason="EXPIRED"))
        assert result["ok"] is False
        assert "exit_reason" in result["reason"]

    def test_lowercase_exit_reason_blocked(self):
        result = validate_live_execution_result(_rec(exit_reason="sl"))
        assert result["ok"] is False

    def test_empty_exit_reason_blocked(self):
        result = validate_live_execution_result(_rec(exit_reason=""))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# K. Invalid execution_quality_flag
# ---------------------------------------------------------------------------

class TestInvalidQualityFlag:
    def test_unknown_flag_blocked(self):
        result = validate_live_execution_result(_rec(execution_quality_flag="PERFECT"))
        assert result["ok"] is False
        assert "execution_quality_flag" in result["reason"]

    def test_empty_flag_blocked(self):
        result = validate_live_execution_result(_rec(execution_quality_flag=""))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# L. Empty broker order ids
# ---------------------------------------------------------------------------

class TestBrokerOrderIds:
    def test_empty_entry_id_blocked(self):
        result = validate_live_execution_result(_rec(broker_order_id_entry=""))
        assert result["ok"] is False

    def test_empty_exit_id_blocked(self):
        result = validate_live_execution_result(_rec(broker_order_id_exit=""))
        assert result["ok"] is False

    def test_whitespace_entry_id_blocked(self):
        result = validate_live_execution_result(_rec(broker_order_id_entry="   "))
        assert result["ok"] is False

    def test_non_string_entry_id_blocked(self):
        result = validate_live_execution_result(_rec(broker_order_id_entry=12345))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# M. Negative realized_pnl_eur allowed
# ---------------------------------------------------------------------------

class TestPnlAllowsNegative:
    def test_negative_pnl_allowed(self):
        result = validate_live_execution_result(_rec(realized_pnl_eur=-50.0))
        assert result["ok"] is True

    def test_zero_pnl_allowed(self):
        result = validate_live_execution_result(_rec(realized_pnl_eur=0.0))
        assert result["ok"] is True

    def test_string_pnl_blocked(self):
        result = validate_live_execution_result(_rec(realized_pnl_eur="-50"))
        assert result["ok"] is False

    def test_bool_pnl_blocked(self):
        result = validate_live_execution_result(_rec(realized_pnl_eur=True))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# N. Negative slippage_eur allowed
# ---------------------------------------------------------------------------

class TestSlippageAllowsNegative:
    def test_negative_slippage_allowed(self):
        result = validate_live_execution_result(_rec(slippage_eur=-2.0))
        assert result["ok"] is True

    def test_positive_slippage_allowed(self):
        result = validate_live_execution_result(_rec(slippage_eur=1.5))
        assert result["ok"] is True

    def test_zero_slippage_allowed(self):
        result = validate_live_execution_result(_rec(slippage_eur=0.0))
        assert result["ok"] is True

    def test_string_slippage_blocked(self):
        result = validate_live_execution_result(_rec(slippage_eur="0.5"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# O. Zero hold_duration_minutes allowed
# ---------------------------------------------------------------------------

class TestHoldDuration:
    def test_zero_hold_allowed(self):
        result = validate_live_execution_result(_rec(hold_duration_minutes=0))
        assert result["ok"] is True

    def test_float_hold_allowed(self):
        result = validate_live_execution_result(_rec(hold_duration_minutes=1.5))
        assert result["ok"] is True

    def test_negative_hold_blocked(self):
        result = validate_live_execution_result(_rec(hold_duration_minutes=-1))
        assert result["ok"] is False

    def test_string_hold_blocked(self):
        result = validate_live_execution_result(_rec(hold_duration_minutes="45"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# P. Validator never raises exceptions
# ---------------------------------------------------------------------------

class TestNoExceptions:
    @pytest.mark.parametrize("bad_input", [
        None, 42, "string", [], True, object(),
        {k: None for k in _REQUIRED_FIELDS},
    ])
    def test_no_exception_on_bad_input(self, bad_input):
        result = validate_live_execution_result(bad_input)
        assert isinstance(result, dict)
        assert "ok" in result

    def test_returns_dict_always(self):
        for v in (None, {}, [], "x", 0):
            result = validate_live_execution_result(v)
            assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# Q. normalized_record key order matches contract
# ---------------------------------------------------------------------------

class TestNormalizedRecord:
    def test_normalized_contains_all_required_fields(self):
        result = validate_live_execution_result(_VALID_RECORD)
        normalized = result["normalized_record"]
        for field in _REQUIRED_FIELDS:
            assert field in normalized

    def test_normalized_key_order(self):
        result = validate_live_execution_result(_VALID_RECORD)
        keys = list(result["normalized_record"].keys())
        assert keys == list(_REQUIRED_FIELDS)

    def test_normalized_values_match_input(self):
        result = validate_live_execution_result(_VALID_RECORD)
        normalized = result["normalized_record"]
        for field in _REQUIRED_FIELDS:
            assert normalized[field] == _VALID_RECORD[field]

    def test_extra_fields_not_in_normalized(self):
        record = _rec(extra_unexpected_field="should_be_ignored")
        result = validate_live_execution_result(record)
        assert result["ok"] is True
        assert "extra_unexpected_field" not in result["normalized_record"]


# ---------------------------------------------------------------------------
# R. Non-dict input → fail-closed
# ---------------------------------------------------------------------------

class TestNonDictInput:
    def test_none_input_blocked(self):
        result = validate_live_execution_result(None)
        assert result["ok"] is False

    def test_list_input_blocked(self):
        result = validate_live_execution_result([_VALID_RECORD])
        assert result["ok"] is False

    def test_string_input_blocked(self):
        result = validate_live_execution_result("trade_id=001")
        assert result["ok"] is False

    def test_int_input_blocked(self):
        result = validate_live_execution_result(42)
        assert result["ok"] is False
