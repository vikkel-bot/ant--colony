"""
AC-149: Tests for Live Execution Preview (Dry Integration)

Verifies:
  A. Valid long intent  → ok=True, schema-valid result
  B. Valid short intent → ok=True, correct pnl sign
  C. Invalid lane       → fail-closed
  D. Invalid market     → fail-closed
  E. Invalid strategy_key → fail-closed
  F. qty <= 0           → fail-closed
  G. Invalid position_side → fail-closed
  H. Invalid exit_reason   → fail-closed
  I. Missing field         → fail-closed
  J. Preview result has all 18 AC-148 required fields
  K. Execution result passes AC-148 validator directly
  L. Broker ids are non-empty deterministic strings
  M. Preview does no file IO
  N. Preview does no broker calls
  O. Exceptions never leak out
  P. PNL calculation correctness (long and short)
  Q. Determinism: same input → same output
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_execution_preview import (
    preview_live_execution,
    _REQUIRED_INTENT_FIELDS,
)
from ant_colony.live.live_execution_result_schema import (
    validate_live_execution_result,
    _REQUIRED_FIELDS as SCHEMA_REQUIRED_FIELDS,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = "2026-04-01T10:00:00Z"

_LONG_INTENT = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "qty": 0.25,
    "entry_price": 600.0,
    "exit_price": 606.0,
    "exit_reason": "SIGNAL",
}

_SHORT_INTENT = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "short",
    "qty": 0.25,
    "entry_price": 600.0,
    "exit_price": 594.0,
    "exit_reason": "TP",
}


def _intent(**overrides) -> dict:
    r = dict(_LONG_INTENT)
    r.update(overrides)
    return r


def _intent_without(field: str) -> dict:
    r = dict(_LONG_INTENT)
    del r[field]
    return r


def _preview(intent: dict | None = None, **overrides) -> dict:
    if intent is None:
        intent = _intent(**overrides)
    return preview_live_execution(intent, _now_utc=_TS)


# ---------------------------------------------------------------------------
# A. Valid long intent → ok=True
# ---------------------------------------------------------------------------

class TestValidLong:
    def test_ok_true(self):
        result = _preview()
        assert result["ok"] is True

    def test_reason_preview_ok(self):
        result = _preview()
        assert result["reason"] == "PREVIEW_OK"

    def test_execution_result_present(self):
        result = _preview()
        assert result["execution_result"] is not None
        assert isinstance(result["execution_result"], dict)

    def test_lane_correct(self):
        assert _preview()["execution_result"]["lane"] == "live_test"

    def test_market_correct(self):
        assert _preview()["execution_result"]["market"] == "BNB-EUR"

    def test_strategy_key_correct(self):
        assert _preview()["execution_result"]["strategy_key"] == "EDGE3"


# ---------------------------------------------------------------------------
# B. Valid short intent → ok=True, correct pnl sign
# ---------------------------------------------------------------------------

class TestValidShort:
    def test_short_ok_true(self):
        result = preview_live_execution(_SHORT_INTENT, _now_utc=_TS)
        assert result["ok"] is True

    def test_short_pnl_positive_when_price_drops(self):
        # short: entry 600, exit 594, qty 0.25 → pnl = (600-594)*0.25 = 1.5
        result = preview_live_execution(_SHORT_INTENT, _now_utc=_TS)
        assert result["execution_result"]["realized_pnl_eur"] == pytest.approx(1.5)

    def test_short_pnl_negative_when_price_rises(self):
        # short: entry 600, exit 612, qty 0.5 → pnl = (600-612)*0.5 = -6.0
        intent = _intent(position_side="short", entry_price=600.0, exit_price=612.0, qty=0.5)
        result = preview_live_execution(intent, _now_utc=_TS)
        assert result["ok"] is True
        assert result["execution_result"]["realized_pnl_eur"] == pytest.approx(-6.0)


# ---------------------------------------------------------------------------
# C. Invalid lane
# ---------------------------------------------------------------------------

class TestInvalidLane:
    def test_wrong_lane_blocked(self):
        result = _preview(lane="paper")
        assert result["ok"] is False
        assert "lane" in result["reason"]
        assert result["execution_result"] is None

    def test_empty_lane_blocked(self):
        result = _preview(lane="")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# D. Invalid market
# ---------------------------------------------------------------------------

class TestInvalidMarket:
    def test_wrong_market_blocked(self):
        result = _preview(market="BTC-EUR")
        assert result["ok"] is False
        assert "market" in result["reason"]

    def test_empty_market_blocked(self):
        result = _preview(market="")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# E. Invalid strategy_key
# ---------------------------------------------------------------------------

class TestInvalidStrategy:
    def test_wrong_strategy_blocked(self):
        result = _preview(strategy_key="RSI_SIMPLE")
        assert result["ok"] is False
        assert "strategy_key" in result["reason"]


# ---------------------------------------------------------------------------
# F. qty <= 0
# ---------------------------------------------------------------------------

class TestQtyBounds:
    def test_zero_qty_blocked(self):
        result = _preview(qty=0)
        assert result["ok"] is False

    def test_negative_qty_blocked(self):
        result = _preview(qty=-1.0)
        assert result["ok"] is False

    def test_bool_qty_blocked(self):
        result = _preview(qty=True)
        assert result["ok"] is False

    def test_string_qty_blocked(self):
        result = _preview(qty="0.25")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# G. Invalid position_side
# ---------------------------------------------------------------------------

class TestInvalidPositionSide:
    def test_neutral_side_blocked(self):
        result = _preview(position_side="neutral")
        assert result["ok"] is False
        assert "position_side" in result["reason"]

    def test_uppercase_long_blocked(self):
        result = _preview(position_side="LONG")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# H. Invalid exit_reason
# ---------------------------------------------------------------------------

class TestInvalidExitReason:
    def test_unknown_exit_reason_blocked(self):
        result = _preview(exit_reason="EXPIRED")
        assert result["ok"] is False
        assert "exit_reason" in result["reason"]

    def test_lowercase_exit_reason_blocked(self):
        result = _preview(exit_reason="tp")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# I. Missing intent field
# ---------------------------------------------------------------------------

class TestMissingIntentField:
    @pytest.mark.parametrize("field", _REQUIRED_INTENT_FIELDS)
    def test_missing_field_blocked(self, field):
        result = preview_live_execution(_intent_without(field), _now_utc=_TS)
        assert result["ok"] is False
        assert field in result["reason"]
        assert result["execution_result"] is None


# ---------------------------------------------------------------------------
# J. All 18 AC-148 fields present in execution result
# ---------------------------------------------------------------------------

class TestSchemaFieldsPresent:
    def test_all_required_fields_present(self):
        result = _preview()
        er = result["execution_result"]
        for field in SCHEMA_REQUIRED_FIELDS:
            assert field in er, f"missing field in execution_result: {field}"

    def test_exactly_18_fields(self):
        assert len(SCHEMA_REQUIRED_FIELDS) == 18


# ---------------------------------------------------------------------------
# K. Execution result passes AC-148 validator directly
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_execution_result_passes_schema(self):
        result = _preview()
        er = result["execution_result"]
        validation = validate_live_execution_result(er)
        assert validation["ok"] is True

    def test_short_execution_result_passes_schema(self):
        result = preview_live_execution(_SHORT_INTENT, _now_utc=_TS)
        er = result["execution_result"]
        validation = validate_live_execution_result(er)
        assert validation["ok"] is True


# ---------------------------------------------------------------------------
# L. Broker ids are non-empty deterministic strings
# ---------------------------------------------------------------------------

class TestBrokerIds:
    def test_entry_id_non_empty(self):
        er = _preview()["execution_result"]
        assert isinstance(er["broker_order_id_entry"], str)
        assert er["broker_order_id_entry"].strip() != ""

    def test_exit_id_non_empty(self):
        er = _preview()["execution_result"]
        assert isinstance(er["broker_order_id_exit"], str)
        assert er["broker_order_id_exit"].strip() != ""

    def test_ids_differ(self):
        er = _preview()["execution_result"]
        assert er["broker_order_id_entry"] != er["broker_order_id_exit"]

    def test_ids_deterministic(self):
        r1 = preview_live_execution(dict(_LONG_INTENT), _now_utc=_TS)
        r2 = preview_live_execution(dict(_LONG_INTENT), _now_utc=_TS)
        assert r1["execution_result"]["broker_order_id_entry"] == \
               r2["execution_result"]["broker_order_id_entry"]


# ---------------------------------------------------------------------------
# M. Preview does no file IO (source inspection)
# ---------------------------------------------------------------------------

_FILE_IO_MARKERS = (
    "open(",
    "write_text",
    "read_text",
    "os.path",
    "os.makedirs",
)


class TestNoFileIO:
    def test_preview_source_has_no_file_io(self):
        src = Path(_REPO_ROOT / "ant_colony" / "live" / "live_execution_preview.py")
        text = src.read_text(encoding="utf-8")
        for marker in _FILE_IO_MARKERS:
            assert marker not in text, (
                f"live_execution_preview.py contains file IO marker: {marker!r}"
            )


# ---------------------------------------------------------------------------
# N. Preview does no broker calls (source inspection)
# ---------------------------------------------------------------------------

_BROKER_MARKERS = (
    "broker_adapter",
    "bitvavo",
    "place_order",
    "create_order",
    "broker_adapters",
)


class TestNoBrokerCalls:
    def test_preview_source_has_no_broker_imports(self):
        src = Path(_REPO_ROOT / "ant_colony" / "live" / "live_execution_preview.py")
        text = src.read_text(encoding="utf-8")
        for marker in _BROKER_MARKERS:
            assert marker not in text, (
                f"live_execution_preview.py references broker: {marker!r}"
            )


# ---------------------------------------------------------------------------
# O. Exceptions never leak out
# ---------------------------------------------------------------------------

class TestNoExceptions:
    @pytest.mark.parametrize("bad_input", [
        None, 42, "string", [], True, {k: None for k in _REQUIRED_INTENT_FIELDS},
    ])
    def test_no_exception_on_bad_input(self, bad_input):
        result = preview_live_execution(bad_input, _now_utc=_TS)
        assert isinstance(result, dict)
        assert "ok" in result

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0):
            result = preview_live_execution(v, _now_utc=_TS)
            assert isinstance(result, dict)

    def test_failed_result_has_execution_result_none(self):
        result = preview_live_execution(None, _now_utc=_TS)
        assert result["execution_result"] is None


# ---------------------------------------------------------------------------
# P. PNL calculation correctness
# ---------------------------------------------------------------------------

class TestPnlCalculation:
    def test_long_profit(self):
        # long: (606 - 600) * 0.25 = 1.5
        result = _preview(entry_price=600.0, exit_price=606.0, qty=0.25)
        assert result["execution_result"]["realized_pnl_eur"] == pytest.approx(1.5)

    def test_long_loss(self):
        # long: (594 - 600) * 0.25 = -1.5
        result = _preview(entry_price=600.0, exit_price=594.0, qty=0.25)
        assert result["ok"] is True
        assert result["execution_result"]["realized_pnl_eur"] == pytest.approx(-1.5)

    def test_short_profit(self):
        # short: (600 - 594) * 0.25 = 1.5
        result = preview_live_execution(
            _intent(position_side="short", entry_price=600.0, exit_price=594.0, qty=0.25),
            _now_utc=_TS,
        )
        assert result["execution_result"]["realized_pnl_eur"] == pytest.approx(1.5)

    def test_slippage_is_zero(self):
        result = _preview()
        assert result["execution_result"]["slippage_eur"] == 0.0

    def test_hold_duration_is_zero(self):
        result = _preview()
        assert result["execution_result"]["hold_duration_minutes"] == 0.0

    def test_quality_flag_is_ok(self):
        result = _preview()
        assert result["execution_result"]["execution_quality_flag"] == "OK"


# ---------------------------------------------------------------------------
# Q. Determinism: same input → same output
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_intent_same_result(self):
        r1 = preview_live_execution(dict(_LONG_INTENT), _now_utc=_TS)
        r2 = preview_live_execution(dict(_LONG_INTENT), _now_utc=_TS)
        assert r1 == r2

    def test_different_side_different_pnl(self):
        long_r = preview_live_execution(
            _intent(position_side="long", entry_price=600.0, exit_price=606.0),
            _now_utc=_TS,
        )
        short_r = preview_live_execution(
            _intent(position_side="short", entry_price=600.0, exit_price=606.0),
            _now_utc=_TS,
        )
        assert long_r["execution_result"]["realized_pnl_eur"] != \
               short_r["execution_result"]["realized_pnl_eur"]
