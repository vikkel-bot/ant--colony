"""
AC-158: Tests for Live Exit Intent and Safe Close Path

Verifies:
  EXIT INTENT
  A. OPEN_POSITION + valid exit_reason + operator_approved=True → ok=True
  B. FLAT → blocked
  C. UNKNOWN → blocked
  D. POSITION_MISMATCH → blocked
  E. Invalid exit_reason → blocked
  F. operator_approved=False → blocked
  G. operator_approved non-bool → blocked
  H. Short position → blocked (not supported this phase)
  I. Exit intent output shape and field values

  EXIT EXECUTOR
  J. All gates open + mocked adapter success → ok=True
  K. live_enabled=False → blocked
  L. allow_broker_execution=False → blocked
  M. Manual macro freeze (risk_state=FREEZE) → blocked
  N. Manual freeze (freeze_new_entries=True) → blocked
  O. Auto-freeze active → blocked
  P. Invalid exit intent → blocked
  Q. Duplicate exit attempt → blocked
  R. Malformed adapter response → blocked
  S. Adapter exception does not leak
  T. Output shape (ok, reason, exit_execution_raw)

  EXIT RECONCILER
  U. Valid entry + valid exit → fully closed AC-148 record
  V. PnL calculation correct for long (positive gain)
  W. PnL calculation correct for long (loss)
  X. Partial fill → PARTIAL_FILL quality flag
  Y. Invalid broker_response → blocked
  Z. entry_market mismatch with position_state → blocked
  AA. sentinel broker_order_id_entry → blocked
  AB. validate_live_execution_result() passes on closed record
  AC. hold_duration_minutes >= 0
  AD. Reconciler: missing entry fields → blocked
  AE. Reconciler: position_state not OPEN_POSITION → blocked

  MARKER TESTS
  AF. No paper imports in exit modules
  AG. No direct network/http in exit modules
  AH. No exceptions leak from any module (fail-closed)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_exit_intent import build_live_exit_intent
from ant_colony.live.bitvavo_live_exit_executor import (
    execute_live_exit,
    _reset_exit_dedup_for_testing,
)
from ant_colony.live.live_exit_reconciler import reconcile_live_exit
from ant_colony.live.live_execution_result_schema import validate_live_execution_result

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_NOW = "2026-04-12T12:00:00Z"
_ENTRY_TS = "2025-01-01T10:00:00Z"   # fixed past time → hold_duration always >= 0

_OPEN_POSITION_RECORD = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_state": "OPEN_POSITION",
    "entry_order_id": "BTV-ORDER-001",
    "entry_price": 600.0,
    "qty": 0.08,
    "position_side": "long",
    "ts_observed_utc": _NOW,
    "reason": "broker confirms open position consistent with entry record",
}

_LIVE_LANE_CFG = {
    "lane": "live_test",
    "enabled": True,
    "live_enabled": True,
    "market": "BNB-EUR",
    "strategy": "EDGE3",
    "max_notional_eur": 50,
    "max_positions": 1,
    "allow_broker_execution": True,
    "allow_shared_state": False,
    "allow_paper_inputs": False,
    "base_output_dir": "C:\\Trading\\ANT_LIVE",
}

_MACRO_NORMAL = {
    "risk_state": "NORMAL",
    "reason": "",
    "freeze_new_entries": False,
    "updated_ts_utc": "",
}

_AUTO_FREEZE_CLEAR = {
    "allow": True,
    "reason": "AUTO_FREEZE_CLEAR",
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
}

_ENTRY_EXECUTION_RESULT = {
    "trade_id": "LIVE-BNBEUR-EDGE3-LONG-20250101T100000",
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "qty": 0.08,
    "entry_ts_utc": _ENTRY_TS,
    "exit_ts_utc": _ENTRY_TS,
    "entry_price": 600.0,
    "exit_price": 600.0,
    "realized_pnl_eur": 0.0,
    "slippage_eur": 0.0,
    "hold_duration_minutes": 0.0,
    "exit_reason": "UNKNOWN",
    "execution_quality_flag": "OK",
    "broker_order_id_entry": "BTV-ORDER-001",
    "broker_order_id_exit": "ENTRY_ONLY_PENDING_EXIT",
    "ts_recorded_utc": _ENTRY_TS,
}

_MOCK_EXIT_BROKER_RESPONSE_OK = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-EXIT-002",
        "status": "filled",
        "side": "sell",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-EXIT-002",
            "market": "BNB-EUR",
            "side": "sell",
            "orderType": "market",
            "status": "filled",
            "amount": "0.08",
            "filledAmount": "0.08",
            "price": "620.0",
            "created": 1735727400000,   # 2025-01-01T10:30:00Z → 30 min hold
        },
    },
    "error": None,
    "meta": {"latency_ms": 90, "attempts": 1, "rate_limited": False},
}

_MOCK_EXIT_BROKER_RESPONSE_ERROR = {
    "ok": False,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": None,
    "error": {"type": "BROKER_REJECTED", "code": "205", "message": "insufficient funds", "retryable": False},
    "meta": {"latency_ms": 80, "attempts": 1, "rate_limited": False},
}


class _MockAdapterOk:
    def place_order(self, order_request):
        return dict(_MOCK_EXIT_BROKER_RESPONSE_OK)


class _MockAdapterError:
    def place_order(self, order_request):
        return dict(_MOCK_EXIT_BROKER_RESPONSE_ERROR)


class _MockAdapterRaises:
    def place_order(self, order_request):
        raise RuntimeError("simulated adapter crash")


def _intent(position=None, exit_reason="SIGNAL", operator_approved=True):
    pos = position if position is not None else dict(_OPEN_POSITION_RECORD)
    return build_live_exit_intent(pos, exit_reason, operator_approved)


def _exec_exit(intent=None, lane=None, macro=None, auto_freeze=None, adapter=None):
    if intent is None:
        intent = _intent()["exit_intent"]
    if lane is None:
        lane = dict(_LIVE_LANE_CFG)
    if macro is None:
        macro = dict(_MACRO_NORMAL)
    if auto_freeze is None:
        auto_freeze = dict(_AUTO_FREEZE_CLEAR)
    if adapter is None:
        adapter = _MockAdapterOk()
    return execute_live_exit(intent, lane, macro, auto_freeze, _adapter=adapter)


# ---------------------------------------------------------------------------
# A. OPEN_POSITION + valid exit_reason + operator_approved=True → ok=True
# ---------------------------------------------------------------------------

class TestExitIntentValid:
    def test_ok_true(self):
        assert _intent()["ok"] is True

    def test_reason_exit_intent_ready(self):
        assert _intent()["reason"] == "EXIT_INTENT_READY"

    def test_exit_intent_not_none(self):
        assert _intent()["exit_intent"] is not None

    def test_exit_intent_order_side_sell(self):
        assert _intent()["exit_intent"]["order_side"] == "sell"

    def test_all_valid_reasons(self):
        for reason in ("SL", "TP", "SIGNAL", "OPERATOR_KILL", "MANUAL"):
            r = _intent(exit_reason=reason)
            assert r["ok"] is True, f"failed for exit_reason={reason!r}"

    def test_exit_intent_contains_entry_order_id(self):
        assert _intent()["exit_intent"]["entry_order_id"] == "BTV-ORDER-001"

    def test_exit_intent_contains_entry_price(self):
        assert _intent()["exit_intent"]["entry_price"] == 600.0

    def test_exit_intent_contains_qty(self):
        assert _intent()["exit_intent"]["qty"] == 0.08

    def test_ts_intent_utc_present(self):
        ts = _intent()["exit_intent"]["ts_intent_utc"]
        assert isinstance(ts, str) and len(ts) > 0


# ---------------------------------------------------------------------------
# B–D. Non-OPEN_POSITION states blocked
# ---------------------------------------------------------------------------

class TestExitIntentPositionState:
    def test_flat_blocked(self):
        pos = dict(_OPEN_POSITION_RECORD, position_state="FLAT")
        r = build_live_exit_intent(pos, "SIGNAL", True)
        assert r["ok"] is False

    def test_unknown_blocked(self):
        pos = dict(_OPEN_POSITION_RECORD, position_state="UNKNOWN")
        r = build_live_exit_intent(pos, "SIGNAL", True)
        assert r["ok"] is False

    def test_mismatch_blocked(self):
        pos = dict(_OPEN_POSITION_RECORD, position_state="POSITION_MISMATCH")
        r = build_live_exit_intent(pos, "SIGNAL", True)
        assert r["ok"] is False

    def test_none_position_state_blocked(self):
        pos = dict(_OPEN_POSITION_RECORD, position_state=None)
        r = build_live_exit_intent(pos, "SIGNAL", True)
        assert r["ok"] is False

    def test_reason_mentions_open_position(self):
        pos = dict(_OPEN_POSITION_RECORD, position_state="FLAT")
        r = build_live_exit_intent(pos, "SIGNAL", True)
        assert "OPEN_POSITION" in r["reason"] or "open" in r["reason"].lower()


# ---------------------------------------------------------------------------
# E. Invalid exit_reason → blocked
# ---------------------------------------------------------------------------

class TestExitIntentReason:
    def test_invalid_reason_blocked(self):
        r = _intent(exit_reason="CRASH")
        assert r["ok"] is False

    def test_none_reason_blocked(self):
        r = _intent(exit_reason=None)
        assert r["ok"] is False

    def test_empty_reason_blocked(self):
        r = _intent(exit_reason="")
        assert r["ok"] is False

    def test_reason_mentions_whitelist(self):
        r = _intent(exit_reason="CRASH")
        assert "exit_reason" in r["reason"].lower() or "SL" in r["reason"]


# ---------------------------------------------------------------------------
# F–G. operator_approved failures
# ---------------------------------------------------------------------------

class TestExitIntentOperatorApproved:
    def test_false_blocked(self):
        r = _intent(operator_approved=False)
        assert r["ok"] is False

    def test_none_blocked(self):
        r = _intent(operator_approved=None)
        assert r["ok"] is False

    def test_string_blocked(self):
        r = _intent(operator_approved="yes")
        assert r["ok"] is False

    def test_int_blocked(self):
        r = _intent(operator_approved=1)
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# H. Short position → blocked
# ---------------------------------------------------------------------------

class TestExitIntentShort:
    def test_short_position_blocked(self):
        pos = dict(_OPEN_POSITION_RECORD, position_side="short")
        r = build_live_exit_intent(pos, "SIGNAL", True)
        assert r["ok"] is False

    def test_short_reason_mentions_not_supported(self):
        pos = dict(_OPEN_POSITION_RECORD, position_side="short")
        r = build_live_exit_intent(pos, "SIGNAL", True)
        assert "short" in r["reason"].lower() or "support" in r["reason"].lower()


# ---------------------------------------------------------------------------
# I. Exit intent output shape
# ---------------------------------------------------------------------------

class TestExitIntentShape:
    _REQUIRED_OUTPUT_KEYS = ("ok", "reason", "exit_intent")
    _REQUIRED_INTENT_KEYS = (
        "lane", "market", "strategy_key", "position_side", "order_side",
        "qty", "exit_reason", "operator_approved", "entry_order_id",
        "entry_price", "ts_intent_utc",
    )

    def test_output_has_required_keys(self):
        r = _intent()
        for k in self._REQUIRED_OUTPUT_KEYS:
            assert k in r, f"missing key: {k}"

    def test_intent_has_required_keys(self):
        intent = _intent()["exit_intent"]
        for k in self._REQUIRED_INTENT_KEYS:
            assert k in intent, f"missing intent key: {k}"

    def test_blocked_result_has_required_keys(self):
        r = _intent(exit_reason="BAD")
        for k in self._REQUIRED_OUTPUT_KEYS:
            assert k in r, f"missing key: {k}"

    def test_blocked_intent_is_none(self):
        r = _intent(exit_reason="BAD")
        assert r["exit_intent"] is None


# ---------------------------------------------------------------------------
# J. All gates open + mocked adapter success → ok=True
# ---------------------------------------------------------------------------

class TestExecutorSuccess:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_ok_true(self):
        assert _exec_exit()["ok"] is True

    def test_reason_live_exit_executed(self):
        assert _exec_exit()["reason"] == "LIVE_EXIT_EXECUTED"

    def test_exit_execution_raw_present(self):
        assert _exec_exit()["exit_execution_raw"] is not None

    def test_exit_execution_raw_ok(self):
        assert _exec_exit()["exit_execution_raw"]["ok"] is True


# ---------------------------------------------------------------------------
# K. live_enabled=False → blocked
# ---------------------------------------------------------------------------

class TestExecutorGateLiveEnabled:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_live_disabled_blocked(self):
        r = _exec_exit(lane=dict(_LIVE_LANE_CFG, live_enabled=False))
        assert r["ok"] is False

    def test_reason_mentions_live_gate(self):
        r = _exec_exit(lane=dict(_LIVE_LANE_CFG, live_enabled=False))
        assert "LIVE_GATE_BLOCKED" in r["reason"] or "LIVE_DISABLED" in r["reason"]


# ---------------------------------------------------------------------------
# L. allow_broker_execution=False → blocked
# ---------------------------------------------------------------------------

class TestExecutorGateBrokerExecution:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_broker_execution_false_blocked(self):
        r = _exec_exit(lane=dict(_LIVE_LANE_CFG, allow_broker_execution=False))
        assert r["ok"] is False

    def test_lane_disabled_blocked(self):
        r = _exec_exit(lane=dict(_LIVE_LANE_CFG, enabled=False))
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# M–N. Macro freeze blocks
# ---------------------------------------------------------------------------

class TestExecutorMacroFreeze:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_risk_state_freeze_blocked(self):
        r = _exec_exit(macro=dict(_MACRO_NORMAL, risk_state="FREEZE"))
        assert r["ok"] is False

    def test_freeze_new_entries_blocked(self):
        r = _exec_exit(macro=dict(_MACRO_NORMAL, freeze_new_entries=True))
        assert r["ok"] is False

    def test_none_macro_blocked(self):
        r = execute_live_exit(_intent()["exit_intent"], _LIVE_LANE_CFG, None, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# O. Auto-freeze blocks
# ---------------------------------------------------------------------------

class TestExecutorAutoFreeze:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_auto_freeze_active_blocked(self):
        r = _exec_exit(auto_freeze={"allow": False, "reason": "extreme move", "risk_state": "FREEZE", "freeze_new_entries": True})
        assert r["ok"] is False

    def test_auto_freeze_reason_propagated(self):
        r = _exec_exit(auto_freeze={"allow": False, "reason": "stale data 200s", "risk_state": "FREEZE", "freeze_new_entries": True})
        assert "stale data 200s" in r["reason"]

    def test_none_auto_freeze_blocked(self):
        r = execute_live_exit(_intent()["exit_intent"], _LIVE_LANE_CFG, _MACRO_NORMAL, None, _adapter=_MockAdapterOk())
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# P. Invalid exit intent → blocked
# ---------------------------------------------------------------------------

class TestExecutorInvalidIntent:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_none_intent_blocked(self):
        assert execute_live_exit(None, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())["ok"] is False

    def test_empty_dict_intent_blocked(self):
        assert execute_live_exit({}, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())["ok"] is False

    def test_wrong_market_blocked(self):
        intent = dict(_intent()["exit_intent"], market="BTC-EUR")
        assert execute_live_exit(intent, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())["ok"] is False

    def test_operator_not_approved_blocked(self):
        intent = dict(_intent()["exit_intent"], operator_approved=False)
        assert execute_live_exit(intent, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())["ok"] is False


# ---------------------------------------------------------------------------
# Q. Duplicate exit attempt → blocked
# ---------------------------------------------------------------------------

class TestExecutorDuplicateExit:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_second_close_blocked(self):
        intent = _intent()["exit_intent"]
        first = execute_live_exit(intent, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        second = execute_live_exit(intent, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert first["ok"] is True
        assert second["ok"] is False

    def test_duplicate_reason_mentions_duplicate(self):
        intent = _intent()["exit_intent"]
        execute_live_exit(intent, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        r = execute_live_exit(intent, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert "DUPLICATE" in r["reason"]

    def test_different_order_id_not_blocked(self):
        _reset_exit_dedup_for_testing()
        intent1 = dict(_intent()["exit_intent"], entry_order_id="ORDER-A")
        intent2 = dict(_intent()["exit_intent"], entry_order_id="ORDER-B")
        r1 = execute_live_exit(intent1, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        r2 = execute_live_exit(intent2, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert r1["ok"] is True
        assert r2["ok"] is True


# ---------------------------------------------------------------------------
# R. Malformed adapter response → blocked
# ---------------------------------------------------------------------------

class TestExecutorAdapterError:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_adapter_error_response_blocked(self):
        r = _exec_exit(adapter=_MockAdapterError())
        assert r["ok"] is False

    def test_adapter_error_reason_propagated(self):
        r = _exec_exit(adapter=_MockAdapterError())
        assert "insufficient funds" in r["reason"]


# ---------------------------------------------------------------------------
# S. Adapter exception does not leak
# ---------------------------------------------------------------------------

class TestExecutorNoExceptions:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    def test_adapter_raise_does_not_leak(self):
        r = _exec_exit(adapter=_MockAdapterRaises())
        assert isinstance(r, dict)
        assert r["ok"] is False

    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_no_exception_bad_intent(self, bad):
        r = execute_live_exit(bad, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert isinstance(r, dict)
        assert "ok" in r


# ---------------------------------------------------------------------------
# T. Output shape
# ---------------------------------------------------------------------------

class TestExecutorOutputShape:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    _REQUIRED = ("ok", "reason", "exit_execution_raw")

    def test_ok_result_has_keys(self):
        r = _exec_exit()
        for k in self._REQUIRED:
            assert k in r, f"missing key: {k}"

    def test_blocked_result_has_keys(self):
        r = _exec_exit(lane=dict(_LIVE_LANE_CFG, live_enabled=False))
        for k in self._REQUIRED:
            assert k in r, f"missing key: {k}"

    def test_blocked_exit_execution_raw_is_none(self):
        r = _exec_exit(lane=dict(_LIVE_LANE_CFG, live_enabled=False))
        assert r["exit_execution_raw"] is None


# ---------------------------------------------------------------------------
# U. Valid entry + valid exit → AC-148 closed record
# ---------------------------------------------------------------------------

class TestReconcilerValid:
    def test_ok_true(self):
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["ok"] is True

    def test_reason(self):
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["reason"] == "LIVE_EXIT_RECONCILED"

    def test_closed_trade_result_present(self):
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["closed_trade_result"] is not None

    def test_exit_order_id_correct(self):
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        assert cr["broker_order_id_exit"] == "BTV-EXIT-002"

    def test_broker_order_id_entry_preserved(self):
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        assert cr["broker_order_id_entry"] == "BTV-ORDER-001"


# ---------------------------------------------------------------------------
# V. PnL calculation correct for long (positive gain)
# ---------------------------------------------------------------------------

class TestReconcilerPnL:
    def _cr(self, entry_price=600.0, exit_price_raw="620.0", qty=0.08):
        entry = dict(_ENTRY_EXECUTION_RESULT, entry_price=entry_price, qty=qty)
        resp = dict(_MOCK_EXIT_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["raw"] = dict(resp["data"]["raw"], price=exit_price_raw)
        return reconcile_live_exit(
            entry, _OPEN_POSITION_RECORD, _intent()["exit_intent"], resp
        )["closed_trade_result"]

    def test_long_positive_pnl(self):
        cr = self._cr(entry_price=600.0, exit_price_raw="620.0", qty=0.08)
        # (620 - 600) * 0.08 = 1.6
        assert abs(cr["realized_pnl_eur"] - 1.6) < 1e-6

    def test_long_zero_pnl(self):
        cr = self._cr(entry_price=600.0, exit_price_raw="600.0", qty=0.08)
        assert abs(cr["realized_pnl_eur"]) < 1e-6

    def test_exit_price_from_broker_raw(self):
        cr = self._cr(exit_price_raw="615.5")
        assert cr["exit_price"] == 615.5

    def test_exit_price_fallback_to_entry(self):
        entry = dict(_ENTRY_EXECUTION_RESULT, entry_price=600.0)
        resp = dict(_MOCK_EXIT_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["raw"] = {"orderId": "BTV-EXIT-002", "status": "filled"}
        cr = reconcile_live_exit(
            entry, _OPEN_POSITION_RECORD, _intent()["exit_intent"], resp
        )["closed_trade_result"]
        assert cr["exit_price"] == 600.0


# ---------------------------------------------------------------------------
# W. PnL calculation for long loss
# ---------------------------------------------------------------------------

class TestReconcilerPnLLoss:
    def test_long_negative_pnl(self):
        entry = dict(_ENTRY_EXECUTION_RESULT, entry_price=600.0, qty=0.08)
        resp = dict(_MOCK_EXIT_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["raw"] = dict(resp["data"]["raw"], price="580.0")
        cr = reconcile_live_exit(
            entry, _OPEN_POSITION_RECORD, _intent()["exit_intent"], resp
        )["closed_trade_result"]
        # (580 - 600) * 0.08 = -1.6
        assert abs(cr["realized_pnl_eur"] - (-1.6)) < 1e-6


# ---------------------------------------------------------------------------
# X. Partial fill → PARTIAL_FILL quality flag
# ---------------------------------------------------------------------------

class TestReconcilerPartialFill:
    def test_partial_fill_flag(self):
        resp = dict(_MOCK_EXIT_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["raw"] = dict(resp["data"]["raw"], filledAmount="0.04")
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], resp,
        )["closed_trade_result"]
        assert cr["execution_quality_flag"] == "PARTIAL_FILL"

    def test_full_fill_ok_flag(self):
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        assert cr["execution_quality_flag"] == "OK"


# ---------------------------------------------------------------------------
# Y. Invalid broker_response → blocked
# ---------------------------------------------------------------------------

class TestReconcilerBadBrokerResponse:
    def test_error_response_blocked(self):
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_ERROR,
        )
        assert r["ok"] is False

    def test_none_response_blocked(self):
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], None,
        )
        assert r["ok"] is False

    def test_missing_order_id_blocked(self):
        resp = dict(_MOCK_EXIT_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["order_id"] = ""
        resp["data"]["raw"] = {}
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], resp,
        )
        assert r["ok"] is False

    def test_data_not_dict_blocked(self):
        resp = dict(_MOCK_EXIT_BROKER_RESPONSE_OK, data="bad")
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], resp,
        )
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# Z. entry/position market mismatch → blocked
# ---------------------------------------------------------------------------

class TestReconcilerMarketMismatch:
    def test_position_state_market_mismatch_blocked(self):
        bad_position = dict(_OPEN_POSITION_RECORD, market="BTC-EUR")
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, bad_position,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["ok"] is False

    def test_entry_wrong_market_blocked(self):
        bad_entry = dict(_ENTRY_EXECUTION_RESULT, market="BTC-EUR")
        r = reconcile_live_exit(
            bad_entry, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# AA. Sentinel broker_order_id_entry blocked
# ---------------------------------------------------------------------------

class TestReconcilerSentinelOrderId:
    def test_sentinel_entry_order_id_blocked(self):
        bad_entry = dict(_ENTRY_EXECUTION_RESULT, broker_order_id_entry="ENTRY_ONLY_PENDING_EXIT")
        r = reconcile_live_exit(
            bad_entry, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["ok"] is False

    def test_empty_entry_order_id_blocked(self):
        bad_entry = dict(_ENTRY_EXECUTION_RESULT, broker_order_id_entry="")
        r = reconcile_live_exit(
            bad_entry, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# AB. validate_live_execution_result() passes on closed record
# ---------------------------------------------------------------------------

class TestReconcilerAC148Compatibility:
    _AC148_FIELDS = (
        "trade_id", "lane", "market", "strategy_key", "position_side",
        "qty", "entry_ts_utc", "exit_ts_utc", "entry_price", "exit_price",
        "realized_pnl_eur", "slippage_eur", "hold_duration_minutes",
        "exit_reason", "execution_quality_flag",
        "broker_order_id_entry", "broker_order_id_exit", "ts_recorded_utc",
    )

    def test_schema_validator_passes(self):
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        result = validate_live_execution_result(cr)
        assert result["ok"] is True, f"AC-148 schema failed: {result['reason']}"

    def test_all_ac148_fields_present(self):
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        for f in self._AC148_FIELDS:
            assert f in cr, f"missing AC-148 field: {f}"

    def test_exit_reason_from_intent(self):
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent(exit_reason="SL")["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        assert cr["exit_reason"] == "SL"

    def test_broker_order_id_exit_not_sentinel(self):
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        assert cr["broker_order_id_exit"] != "ENTRY_ONLY_PENDING_EXIT"
        assert cr["broker_order_id_exit"] == "BTV-EXIT-002"


# ---------------------------------------------------------------------------
# AC. hold_duration_minutes >= 0
# ---------------------------------------------------------------------------

class TestReconcilerHoldDuration:
    def test_hold_duration_non_negative(self):
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        assert cr["hold_duration_minutes"] >= 0.0

    def test_hold_duration_is_30_minutes(self):
        # entry = 2025-01-01T10:00:00Z, created = 1735727400000 = 2025-01-01T10:30:00Z
        cr = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )["closed_trade_result"]
        assert abs(cr["hold_duration_minutes"] - 30.0) < 0.1


# ---------------------------------------------------------------------------
# AD. Missing entry fields → blocked
# ---------------------------------------------------------------------------

class TestReconcilerMissingEntryFields:
    @pytest.mark.parametrize("field", [
        "trade_id", "lane", "market", "strategy_key", "position_side",
        "qty", "entry_ts_utc", "entry_price", "broker_order_id_entry",
    ])
    def test_missing_field_blocked(self, field):
        entry = dict(_ENTRY_EXECUTION_RESULT)
        del entry[field]
        r = reconcile_live_exit(
            entry, _OPEN_POSITION_RECORD,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# AE. position_state not OPEN_POSITION → blocked
# ---------------------------------------------------------------------------

class TestReconcilerPositionState:
    def test_flat_state_blocked(self):
        pos = dict(_OPEN_POSITION_RECORD, position_state="FLAT")
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, pos,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["ok"] is False

    def test_unknown_state_blocked(self):
        pos = dict(_OPEN_POSITION_RECORD, position_state="UNKNOWN")
        r = reconcile_live_exit(
            _ENTRY_EXECUTION_RESULT, pos,
            _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK,
        )
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# AF. No paper imports in exit modules
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
)


class TestSourceMarkers:
    def _src(self, filename: str) -> str:
        return (_REPO_ROOT / "ant_colony" / "live" / filename).read_text(encoding="utf-8")

    def test_exit_intent_no_paper(self):
        src = self._src("live_exit_intent.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"live_exit_intent.py contains: {m!r}"

    def test_exit_executor_no_paper(self):
        src = self._src("bitvavo_live_exit_executor.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"bitvavo_live_exit_executor.py contains: {m!r}"

    def test_exit_reconciler_no_paper(self):
        src = self._src("live_exit_reconciler.py")
        for m in _PAPER_MARKERS:
            assert m not in src, f"live_exit_reconciler.py contains: {m!r}"

    def test_exit_intent_no_direct_http(self):
        src = self._src("live_exit_intent.py")
        for m in _NET_MARKERS:
            assert m not in src

    def test_exit_reconciler_no_direct_http(self):
        src = self._src("live_exit_reconciler.py")
        for m in _NET_MARKERS:
            assert m not in src


# ---------------------------------------------------------------------------
# AH. No exceptions leak from any module
# ---------------------------------------------------------------------------

class TestNoExceptions:
    def setup_method(self):
        _reset_exit_dedup_for_testing()

    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_intent_no_exception(self, bad):
        r = build_live_exit_intent(bad, "SIGNAL", True)
        assert isinstance(r, dict)
        assert "ok" in r

    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_executor_no_exception_bad_intent(self, bad):
        r = execute_live_exit(bad, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert isinstance(r, dict)
        assert r["ok"] is False

    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_reconciler_no_exception_bad_entry(self, bad):
        r = reconcile_live_exit(bad, _OPEN_POSITION_RECORD, _intent()["exit_intent"], _MOCK_EXIT_BROKER_RESPONSE_OK)
        assert isinstance(r, dict)
        assert "ok" in r

    def test_reconciler_always_returns_dict(self):
        for v in (None, {}, [], "x", 0, True):
            r = reconcile_live_exit(v, v, v, v)
            assert isinstance(r, dict)
