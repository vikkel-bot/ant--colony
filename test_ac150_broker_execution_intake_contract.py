"""
AC-150: Tests for Broker Execution Intake Contract

Verifies:
  A. Valid long intake  → ok=True
  B. Valid short intake → ok=True
  C. Missing required field → fail-closed
  D. Invalid lane → fail-closed
  E. Invalid market → fail-closed
  F. Invalid strategy_key → fail-closed
  G. Invalid position_side → fail-closed
  H. Invalid order_side → fail-closed
  I. position_side/order_side mismatch (long/sell) → fail-closed
  J. position_side/order_side mismatch (short/buy) → fail-closed
  K. qty <= 0 → fail-closed
  L. intended_entry_price <= 0 → fail-closed
  M. Invalid order_type → fail-closed
  N. max_notional_eur > 50 → fail-closed
  O. risk_state == FREEZE → fail-closed
  P. freeze_new_entries == true → fail-closed
  Q. allow_broker_execution == true → fail-closed
  R. qty * intended_entry_price > max_notional_eur → fail-closed
  S. Invalid timestamp → fail-closed
  T. Validator never raises exceptions
  U. normalized_record has correct key order and all required fields
  V. Non-dict input → fail-closed
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.broker_execution_intake_contract import (
    validate_broker_execution_intake,
    _REQUIRED_FIELDS,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_LONG = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "order_side": "buy",
    "qty": 0.08,
    "intended_entry_price": 600.0,
    "order_type": "market",
    "max_notional_eur": 50,
    "allow_broker_execution": False,
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
    "operator_approved": True,
    "ts_intake_utc": "2026-04-01T10:00:00Z",
}

_VALID_SHORT = {
    **_VALID_LONG,
    "position_side": "short",
    "order_side": "sell",
}


def _rec(**overrides) -> dict:
    r = dict(_VALID_LONG)
    r.update(overrides)
    return r


def _rec_without(field: str) -> dict:
    r = dict(_VALID_LONG)
    del r[field]
    return r


# ---------------------------------------------------------------------------
# A. Valid long intake
# ---------------------------------------------------------------------------

class TestValidLong:
    def test_ok_true(self):
        result = validate_broker_execution_intake(_VALID_LONG)
        assert result["ok"] is True

    def test_reason_intake_ok(self):
        result = validate_broker_execution_intake(_VALID_LONG)
        assert result["reason"] == "INTAKE_OK"

    def test_normalized_record_present(self):
        result = validate_broker_execution_intake(_VALID_LONG)
        assert result["normalized_record"] is not None
        assert isinstance(result["normalized_record"], dict)

    def test_limit_order_type_allowed(self):
        result = validate_broker_execution_intake(_rec(order_type="limit"))
        assert result["ok"] is True

    def test_caution_risk_state_allowed(self):
        result = validate_broker_execution_intake(_rec(risk_state="CAUTION"))
        assert result["ok"] is True

    def test_operator_not_approved_still_valid(self):
        # operator_approved is captured in the contract but not currently a block condition
        result = validate_broker_execution_intake(_rec(operator_approved=False))
        assert result["ok"] is True


# ---------------------------------------------------------------------------
# B. Valid short intake
# ---------------------------------------------------------------------------

class TestValidShort:
    def test_short_ok_true(self):
        result = validate_broker_execution_intake(_VALID_SHORT)
        assert result["ok"] is True

    def test_short_order_side_sell(self):
        result = validate_broker_execution_intake(_VALID_SHORT)
        assert result["normalized_record"]["order_side"] == "sell"


# ---------------------------------------------------------------------------
# C. Missing required field
# ---------------------------------------------------------------------------

class TestMissingFields:
    @pytest.mark.parametrize("field", _REQUIRED_FIELDS)
    def test_missing_field_blocked(self, field):
        result = validate_broker_execution_intake(_rec_without(field))
        assert result["ok"] is False
        assert field in result["reason"]
        assert result["normalized_record"] is None

    def test_missing_field_no_normalized(self):
        result = validate_broker_execution_intake(_rec_without("lane"))
        assert result["normalized_record"] is None


# ---------------------------------------------------------------------------
# D. Invalid lane
# ---------------------------------------------------------------------------

class TestInvalidLane:
    def test_wrong_lane_blocked(self):
        result = validate_broker_execution_intake(_rec(lane="paper"))
        assert result["ok"] is False
        assert "lane" in result["reason"]

    def test_empty_lane_blocked(self):
        result = validate_broker_execution_intake(_rec(lane=""))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# E. Invalid market
# ---------------------------------------------------------------------------

class TestInvalidMarket:
    def test_wrong_market_blocked(self):
        result = validate_broker_execution_intake(_rec(market="BTC-EUR"))
        assert result["ok"] is False
        assert "market" in result["reason"]


# ---------------------------------------------------------------------------
# F. Invalid strategy_key
# ---------------------------------------------------------------------------

class TestInvalidStrategy:
    def test_wrong_strategy_blocked(self):
        result = validate_broker_execution_intake(_rec(strategy_key="RSI_SIMPLE"))
        assert result["ok"] is False
        assert "strategy_key" in result["reason"]


# ---------------------------------------------------------------------------
# G. Invalid position_side
# ---------------------------------------------------------------------------

class TestInvalidPositionSide:
    def test_neutral_blocked(self):
        result = validate_broker_execution_intake(_rec(position_side="neutral"))
        assert result["ok"] is False
        assert "position_side" in result["reason"]

    def test_uppercase_long_blocked(self):
        result = validate_broker_execution_intake(_rec(position_side="LONG"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# H. Invalid order_side
# ---------------------------------------------------------------------------

class TestInvalidOrderSide:
    def test_wrong_order_side_blocked(self):
        result = validate_broker_execution_intake(_rec(order_side="hold"))
        assert result["ok"] is False
        assert "order_side" in result["reason"]

    def test_uppercase_buy_blocked(self):
        result = validate_broker_execution_intake(_rec(order_side="BUY"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# I. long + sell → mismatch
# ---------------------------------------------------------------------------

class TestLongSellMismatch:
    def test_long_sell_blocked(self):
        result = validate_broker_execution_intake(_rec(position_side="long", order_side="sell"))
        assert result["ok"] is False
        assert "long" in result["reason"]
        assert "buy" in result["reason"]


# ---------------------------------------------------------------------------
# J. short + buy → mismatch
# ---------------------------------------------------------------------------

class TestShortBuyMismatch:
    def test_short_buy_blocked(self):
        result = validate_broker_execution_intake(
            {**_VALID_SHORT, "order_side": "buy"}
        )
        assert result["ok"] is False
        assert "short" in result["reason"]
        assert "sell" in result["reason"]


# ---------------------------------------------------------------------------
# K. qty <= 0
# ---------------------------------------------------------------------------

class TestQtyBounds:
    def test_zero_qty_blocked(self):
        result = validate_broker_execution_intake(_rec(qty=0))
        assert result["ok"] is False

    def test_negative_qty_blocked(self):
        result = validate_broker_execution_intake(_rec(qty=-0.5))
        assert result["ok"] is False

    def test_bool_qty_blocked(self):
        result = validate_broker_execution_intake(_rec(qty=True))
        assert result["ok"] is False

    def test_string_qty_blocked(self):
        result = validate_broker_execution_intake(_rec(qty="0.1"))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# L. intended_entry_price <= 0
# ---------------------------------------------------------------------------

class TestPriceBounds:
    def test_zero_price_blocked(self):
        result = validate_broker_execution_intake(_rec(intended_entry_price=0))
        assert result["ok"] is False

    def test_negative_price_blocked(self):
        result = validate_broker_execution_intake(_rec(intended_entry_price=-100.0))
        assert result["ok"] is False

    def test_bool_price_blocked(self):
        result = validate_broker_execution_intake(_rec(intended_entry_price=True))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# M. Invalid order_type
# ---------------------------------------------------------------------------

class TestInvalidOrderType:
    def test_unknown_order_type_blocked(self):
        result = validate_broker_execution_intake(_rec(order_type="stop_limit"))
        assert result["ok"] is False
        assert "order_type" in result["reason"]

    def test_empty_order_type_blocked(self):
        result = validate_broker_execution_intake(_rec(order_type=""))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# N. max_notional_eur > 50 or <= 0
# ---------------------------------------------------------------------------

class TestMaxNotional:
    def test_above_50_blocked(self):
        result = validate_broker_execution_intake(_rec(max_notional_eur=51))
        assert result["ok"] is False
        assert "max_notional_eur" in result["reason"]

    def test_exactly_50_allowed(self):
        result = validate_broker_execution_intake(_rec(max_notional_eur=50))
        assert result["ok"] is True

    def test_zero_notional_blocked(self):
        result = validate_broker_execution_intake(_rec(max_notional_eur=0))
        assert result["ok"] is False

    def test_negative_notional_blocked(self):
        result = validate_broker_execution_intake(_rec(max_notional_eur=-10))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# O. risk_state == FREEZE
# ---------------------------------------------------------------------------

class TestRiskStateFreeze:
    def test_freeze_blocked(self):
        result = validate_broker_execution_intake(_rec(risk_state="FREEZE"))
        assert result["ok"] is False
        assert "FREEZE" in result["reason"]

    def test_invalid_risk_state_blocked(self):
        result = validate_broker_execution_intake(_rec(risk_state="PANIC"))
        assert result["ok"] is False
        assert "risk_state" in result["reason"]

    def test_none_risk_state_blocked(self):
        result = validate_broker_execution_intake(_rec(risk_state=None))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# P. freeze_new_entries == true
# ---------------------------------------------------------------------------

class TestFreezeNewEntries:
    def test_freeze_flag_true_blocked(self):
        result = validate_broker_execution_intake(_rec(freeze_new_entries=True))
        assert result["ok"] is False
        assert "freeze_new_entries" in result["reason"]

    def test_string_freeze_flag_blocked(self):
        result = validate_broker_execution_intake(_rec(freeze_new_entries="true"))
        assert result["ok"] is False

    def test_int_freeze_flag_blocked(self):
        result = validate_broker_execution_intake(_rec(freeze_new_entries=1))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# Q. allow_broker_execution — bool required; True = live-capable shape (AC-162)
# ---------------------------------------------------------------------------

class TestBrokerExecutionBool:
    def test_allow_broker_execution_true_live_capable_shape(self):
        # AC-162: True is now a valid shape for live-capable intake.
        # Final live permission is granted by evaluate_controlled_live_intake().
        result = validate_broker_execution_intake(_rec(allow_broker_execution=True))
        assert result["ok"] is True
        assert result["normalized_record"]["allow_broker_execution"] is True

    def test_allow_broker_execution_false_dry_mode(self):
        result = validate_broker_execution_intake(_rec(allow_broker_execution=False))
        assert result["ok"] is True
        assert result["normalized_record"]["allow_broker_execution"] is False

    def test_allow_broker_execution_string_blocked(self):
        result = validate_broker_execution_intake(_rec(allow_broker_execution="false"))
        assert result["ok"] is False

    def test_allow_broker_execution_none_blocked(self):
        result = validate_broker_execution_intake(_rec(allow_broker_execution=None))
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# R. qty * intended_entry_price > max_notional_eur
# ---------------------------------------------------------------------------

class TestNotionalBreachBlocked:
    def test_notional_breach_blocked(self):
        # 0.09 * 600 = 54 > 50
        result = validate_broker_execution_intake(
            _rec(qty=0.09, intended_entry_price=600.0, max_notional_eur=50)
        )
        assert result["ok"] is False
        assert "max_notional_eur" in result["reason"]

    def test_notional_exactly_at_limit_allowed(self):
        # 0.08333 * 600 = 49.998 <= 50
        result = validate_broker_execution_intake(
            _rec(qty=0.08333, intended_entry_price=600.0, max_notional_eur=50)
        )
        assert result["ok"] is True

    def test_notional_one_cent_over_blocked(self):
        # 0.0834 * 600 = 50.04 > 50
        result = validate_broker_execution_intake(
            _rec(qty=0.0834, intended_entry_price=600.0, max_notional_eur=50)
        )
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# S. Invalid timestamp
# ---------------------------------------------------------------------------

class TestInvalidTimestamp:
    def test_empty_ts_blocked(self):
        result = validate_broker_execution_intake(_rec(ts_intake_utc=""))
        assert result["ok"] is False

    def test_bad_ts_string_blocked(self):
        result = validate_broker_execution_intake(_rec(ts_intake_utc="not-a-date"))
        assert result["ok"] is False

    def test_date_only_blocked(self):
        result = validate_broker_execution_intake(_rec(ts_intake_utc="2026-04-01"))
        assert result["ok"] is False

    def test_valid_z_suffix_accepted(self):
        result = validate_broker_execution_intake(_rec(ts_intake_utc="2026-04-01T10:00:00Z"))
        assert result["ok"] is True

    def test_valid_offset_accepted(self):
        result = validate_broker_execution_intake(
            _rec(ts_intake_utc="2026-04-01T10:00:00+00:00")
        )
        assert result["ok"] is True


# ---------------------------------------------------------------------------
# T. Validator never raises exceptions
# ---------------------------------------------------------------------------

class TestNoExceptions:
    @pytest.mark.parametrize("bad_input", [
        None, 42, "string", [], True, {k: None for k in _REQUIRED_FIELDS},
    ])
    def test_no_exception_on_bad_input(self, bad_input):
        result = validate_broker_execution_intake(bad_input)
        assert isinstance(result, dict)
        assert "ok" in result

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0):
            result = validate_broker_execution_intake(v)
            assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# U. normalized_record shape
# ---------------------------------------------------------------------------

class TestNormalizedRecord:
    def test_all_required_fields_present(self):
        result = validate_broker_execution_intake(_VALID_LONG)
        nr = result["normalized_record"]
        for field in _REQUIRED_FIELDS:
            assert field in nr

    def test_key_order_matches_contract(self):
        result = validate_broker_execution_intake(_VALID_LONG)
        keys = list(result["normalized_record"].keys())
        assert keys == list(_REQUIRED_FIELDS)

    def test_values_match_input(self):
        result = validate_broker_execution_intake(_VALID_LONG)
        nr = result["normalized_record"]
        for field in _REQUIRED_FIELDS:
            assert nr[field] == _VALID_LONG[field]

    def test_extra_fields_stripped(self):
        record = _rec(unexpected_extra_field="noise")
        result = validate_broker_execution_intake(record)
        assert result["ok"] is True
        assert "unexpected_extra_field" not in result["normalized_record"]


# ---------------------------------------------------------------------------
# V. Non-dict input
# ---------------------------------------------------------------------------

class TestNonDictInput:
    def test_none_blocked(self):
        result = validate_broker_execution_intake(None)
        assert result["ok"] is False

    def test_list_blocked(self):
        result = validate_broker_execution_intake([_VALID_LONG])
        assert result["ok"] is False

    def test_int_blocked(self):
        result = validate_broker_execution_intake(42)
        assert result["ok"] is False
