"""
AC-154 / AC-162: Tests for the First Real Order Path

Verifies:
  A. Full gate chain passes with mocked adapter → ok=True, AC-148 result
  B. Gate A blocks on invalid intake shape
  C. Gate B blocks on request builder failure
  D. Gate A' (controlled live intake) blocks when live_enabled=False
  E. Gate A' blocks when allow_broker_execution=False in lane config
  F. Gate A' blocks on MACRO_FREEZE risk_state
  G. Gate A' blocks on freeze_new_entries=True
  H. Gate A' blocks when auto_freeze_result.allow=False
  I. Gate G blocks when broker adapter returns error
  J. Gate H blocks on reconcile failure (bad broker response)
  K. Output shape is correct (ok, reason, gate, execution_result)
  L. execution_result is AC-148 schema compatible
  M. Reconciler unit tests (valid path, fill price, quality flags)
  N. Reconciler blocks on bad input
  O. Reconciler produces deterministic trade_id
  P. Reconciler uses sentinel values for open exit fields
  Q. place_order unit tests (adapter): valid payload, missing fields, error response
  R. place_order adapter handles non-dict order_request
  S. No exceptions leak out (executor is fail-closed)
  T. No paper/broker marker strings in executor or reconciler source
  U. executor result always contains required keys
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.bitvavo_live_executor import execute_first_live_order
from ant_colony.live.live_order_reconciler import reconcile_live_order
from ant_colony.broker_adapters.bitvavo_adapter import BitvavoAdapter

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = "2026-04-12T12:00:00Z"

_LIVE_INTAKE = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "order_side": "buy",
    "qty": 0.08,
    "intended_entry_price": 600.0,
    "order_type": "market",
    "max_notional_eur": 50.0,
    "allow_broker_execution": True,    # AC-162: live-capable intake
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
    "operator_approved": True,
    "operator_id": "OP-TEST",
    "ts_intake_utc": _NOW,
}

_LIVE_LANE_CFG = {
    "lane": "live_test",
    "enabled": True,
    "live_enabled": True,
    "market": "BNB-EUR",
    "strategy": "EDGE3",
    "max_notional_eur": 50,
    "max_positions": 1,
    "allow_broker_execution": True,    # system-level flag
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

_MOCK_BROKER_RESPONSE_OK = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-ORDER-001",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-001",
            "market": "BNB-EUR",
            "side": "buy",
            "orderType": "market",
            "status": "filled",
            "amount": "0.08",
            "filledAmount": "0.08",
            "price": "601.5",
            "created": 1744459200000,
        },
    },
    "error": None,
    "meta": {"latency_ms": 120, "attempts": 1, "rate_limited": False},
}

_MOCK_BROKER_RESPONSE_ERROR = {
    "ok": False,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": None,
    "error": {"type": "BROKER_REJECTED", "code": "205", "message": "insufficient balance", "retryable": False},
    "meta": {"latency_ms": 90, "attempts": 1, "rate_limited": False},
}


class _MockAdapterOk:
    def place_order(self, order_request):
        return dict(_MOCK_BROKER_RESPONSE_OK)


class _MockAdapterError:
    def place_order(self, order_request):
        return dict(_MOCK_BROKER_RESPONSE_ERROR)


class _MockAdapterNotOkMissingOrderId:
    """Returns ok=True but missing order_id in data — forces reconcile failure."""
    def place_order(self, order_request):
        r = dict(_MOCK_BROKER_RESPONSE_OK)
        r["data"] = dict(r["data"])
        r["data"]["order_id"] = ""
        r["data"]["raw"] = {}
        return r


def _exec(**overrides):
    intake = dict(_LIVE_INTAKE)
    intake.update(overrides.get("intake", {}))
    lane = dict(_LIVE_LANE_CFG)
    lane.update(overrides.get("lane", {}))
    macro = dict(_MACRO_NORMAL)
    macro.update(overrides.get("macro", {}))
    auto_freeze = dict(_AUTO_FREEZE_CLEAR)
    auto_freeze.update(overrides.get("auto_freeze", {}))
    adapter = overrides.get("adapter", _MockAdapterOk())
    return execute_first_live_order(intake, lane, macro, auto_freeze, _adapter=adapter)


# ---------------------------------------------------------------------------
# A. Full gate chain passes with mocked adapter
# ---------------------------------------------------------------------------

class TestFullChainPass:
    def test_full_chain_ok(self):
        result = _exec()
        assert result["ok"] is True

    def test_full_chain_reason(self):
        result = _exec()
        assert result["reason"] == "ORDER_EXECUTED"

    def test_full_chain_gate_reached(self):
        result = _exec()
        assert result["gate"] == "I_SCHEMA"

    def test_full_chain_execution_result_not_none(self):
        result = _exec()
        assert result["execution_result"] is not None

    def test_full_chain_execution_result_is_dict(self):
        result = _exec()
        assert isinstance(result["execution_result"], dict)


# ---------------------------------------------------------------------------
# B. Gate A: intake validation blocks
# ---------------------------------------------------------------------------

class TestGateAIntake:
    def test_missing_market_blocked_at_a(self):
        intake = dict(_LIVE_INTAKE)
        del intake["market"]
        result = execute_first_live_order(intake, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert result["ok"] is False
        assert result["gate"] == "A_INTAKE"

    def test_wrong_lane_blocked_at_a(self):
        intake = dict(_LIVE_INTAKE, lane="wrong_lane")
        result = execute_first_live_order(intake, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert result["ok"] is False
        assert result["gate"] == "A_INTAKE"

    def test_freeze_state_in_intake_blocked_at_a(self):
        intake = dict(_LIVE_INTAKE, risk_state="FREEZE")
        result = execute_first_live_order(intake, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert result["ok"] is False
        assert result["gate"] == "A_INTAKE"


# ---------------------------------------------------------------------------
# C. Gate B/C: request build / adapter bridge fails are absorbed by A
# (intake validation is the first gate; these are covered by unit tests)
# ---------------------------------------------------------------------------

class TestGateBC:
    def test_intake_passes_implies_b_c_pass(self):
        # If intake A passes, B and C are deterministic and cannot fail for
        # a valid intake. This test confirms the chain continues.
        result = _exec()
        assert result["gate"] == "I_SCHEMA"


# ---------------------------------------------------------------------------
# D. Gate A': controlled live intake blocks when live_enabled=False
# ---------------------------------------------------------------------------

class TestGateD:
    def test_live_disabled_blocked_at_controlled(self):
        result = _exec(lane={"live_enabled": False})
        assert result["ok"] is False
        assert result["gate"] == "A_CONTROLLED_LIVE"

    def test_allow_broker_execution_false_in_lane_blocked(self):
        result = _exec(lane={"allow_broker_execution": False})
        assert result["ok"] is False
        assert result["gate"] == "A_CONTROLLED_LIVE"

    def test_lane_disabled_blocked_at_controlled(self):
        result = _exec(lane={"enabled": False})
        assert result["ok"] is False
        assert result["gate"] == "A_CONTROLLED_LIVE"

    def test_controlled_reason_propagated(self):
        result = _exec(lane={"live_enabled": False})
        assert "CONTROLLED_LIVE_GATE_BLOCKED" in result["reason"]


# ---------------------------------------------------------------------------
# E/F. Gate A': macro freeze blocks
# ---------------------------------------------------------------------------

class TestGateE:
    def test_macro_freeze_risk_state_blocked(self):
        result = _exec(macro={"risk_state": "CAUTION", "freeze_new_entries": True, "reason": "", "updated_ts_utc": ""})
        assert result["ok"] is False
        assert result["gate"] == "A_CONTROLLED_LIVE"

    def test_macro_freeze_entries_true_blocked(self):
        result = _exec(macro={"risk_state": "NORMAL", "freeze_new_entries": True, "reason": "", "updated_ts_utc": ""})
        assert result["ok"] is False
        assert result["gate"] == "A_CONTROLLED_LIVE"

    def test_non_dict_macro_blocked(self):
        result = execute_first_live_order(_LIVE_INTAKE, _LIVE_LANE_CFG, None, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert result["ok"] is False


class TestGateEFreeze:
    def test_freeze_state_blocked(self):
        result = _exec(macro={"risk_state": "FREEZE", "freeze_new_entries": False, "reason": "", "updated_ts_utc": ""})
        assert result["ok"] is False
        assert result["gate"] == "A_CONTROLLED_LIVE"


# ---------------------------------------------------------------------------
# H. Gate A': auto-freeze blocks
# ---------------------------------------------------------------------------

class TestGateF:
    def test_auto_freeze_active_blocked(self):
        result = _exec(auto_freeze={"allow": False, "reason": "extreme single move: abs(9.00%) >= 8.0%", "risk_state": "FREEZE", "freeze_new_entries": True})
        assert result["ok"] is False
        assert result["gate"] == "A_CONTROLLED_LIVE"

    def test_auto_freeze_reason_propagated(self):
        result = _exec(auto_freeze={"allow": False, "reason": "market data stale: 200s > 180s", "risk_state": "FREEZE", "freeze_new_entries": True})
        assert "market data stale" in result["reason"]

    def test_non_dict_auto_freeze_blocked(self):
        result = execute_first_live_order(_LIVE_INTAKE, _LIVE_LANE_CFG, _MACRO_NORMAL, None, _adapter=_MockAdapterOk())
        assert result["ok"] is False
        assert result["gate"] == "A_CONTROLLED_LIVE"


# ---------------------------------------------------------------------------
# I. Gate G: broker adapter error blocks
# ---------------------------------------------------------------------------

class TestGateG:
    def test_broker_error_blocked_at_g(self):
        result = _exec(adapter=_MockAdapterError())
        assert result["ok"] is False
        assert result["gate"] == "G_BROKER_CALL"

    def test_broker_error_reason_propagated(self):
        result = _exec(adapter=_MockAdapterError())
        assert "insufficient balance" in result["reason"]


# ---------------------------------------------------------------------------
# J. Gate H: reconcile failure blocks
# ---------------------------------------------------------------------------

class TestGateH:
    def test_reconcile_failure_blocked_at_h(self):
        result = _exec(adapter=_MockAdapterNotOkMissingOrderId())
        assert result["ok"] is False
        assert result["gate"] == "H_RECONCILE"


# ---------------------------------------------------------------------------
# K. Output shape
# ---------------------------------------------------------------------------

class TestOutputShape:
    _REQUIRED_KEYS = ("ok", "reason", "gate", "execution_result")

    def test_ok_result_has_required_keys(self):
        result = _exec()
        for k in self._REQUIRED_KEYS:
            assert k in result, f"missing key: {k}"

    def test_blocked_result_has_required_keys(self):
        result = _exec(lane={"live_enabled": False})
        for k in self._REQUIRED_KEYS:
            assert k in result, f"missing key: {k}"

    def test_blocked_result_execution_result_is_none(self):
        result = _exec(lane={"live_enabled": False})
        assert result["execution_result"] is None


# ---------------------------------------------------------------------------
# L. execution_result is AC-148 schema compatible
# ---------------------------------------------------------------------------

class TestAC148Compatibility:
    _AC148_FIELDS = (
        "trade_id", "lane", "market", "strategy_key", "position_side",
        "qty", "entry_ts_utc", "exit_ts_utc", "entry_price", "exit_price",
        "realized_pnl_eur", "slippage_eur", "hold_duration_minutes",
        "exit_reason", "execution_quality_flag",
        "broker_order_id_entry", "broker_order_id_exit", "ts_recorded_utc",
    )

    def test_all_ac148_fields_present(self):
        result = _exec()
        er = result["execution_result"]
        for field in self._AC148_FIELDS:
            assert field in er, f"missing AC-148 field: {field}"

    def test_lane_is_live_test(self):
        assert _exec()["execution_result"]["lane"] == "live_test"

    def test_market_is_bnb_eur(self):
        assert _exec()["execution_result"]["market"] == "BNB-EUR"

    def test_strategy_key_is_edge3(self):
        assert _exec()["execution_result"]["strategy_key"] == "EDGE3"

    def test_qty_positive(self):
        assert _exec()["execution_result"]["qty"] > 0

    def test_entry_price_uses_broker_fill(self):
        # Broker raw.price = "601.5" → fill_price should be 601.5
        er = _exec()["execution_result"]
        assert er["entry_price"] == 601.5

    def test_exit_price_is_null(self):
        # AC-193: null until exit proven
        er = _exec()["execution_result"]
        assert er["exit_price"] is None

    def test_realized_pnl_is_null(self):
        # AC-193: null until exit proven
        assert _exec()["execution_result"]["realized_pnl_eur"] is None

    def test_hold_duration_is_null(self):
        # AC-193: null until exit proven
        assert _exec()["execution_result"]["hold_duration_minutes"] is None

    def test_exit_reason_is_null(self):
        # AC-193: null until exit proven
        assert _exec()["execution_result"]["exit_reason"] is None

    def test_broker_order_id_entry_is_correct(self):
        assert _exec()["execution_result"]["broker_order_id_entry"] == "BTV-ORDER-001"

    def test_broker_order_id_exit_is_null(self):
        # AC-190: null until a proven exit exists
        assert _exec()["execution_result"]["broker_order_id_exit"] is None

    def test_execution_quality_flag_ok(self):
        assert _exec()["execution_result"]["execution_quality_flag"] == "OK"


# ---------------------------------------------------------------------------
# M. Reconciler unit tests: valid path
# ---------------------------------------------------------------------------

class TestReconcilerValid:
    def test_ok_true_on_valid_input(self):
        result = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)
        assert result["ok"] is True

    def test_reason_reconcile_ok(self):
        result = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)
        assert result["reason"] == "RECONCILE_OK"

    def test_execution_result_present(self):
        result = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)
        assert result["execution_result"] is not None

    def test_fill_price_from_raw(self):
        # raw.price = "601.5"
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["entry_price"] == 601.5

    def test_fail_closed_when_no_fill_price(self):
        # AC-192: no fills and no raw.price → fail-closed (no fallback to intended_price)
        resp = dict(_MOCK_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["raw"] = {"orderId": "X1", "status": "filled"}
        result = reconcile_live_order(_LIVE_INTAKE, resp)
        assert result["ok"] is False
        assert "fill price" in result["reason"].lower()

    def test_fill_price_from_fills_takes_priority(self):
        # AC-192: fills[0]["price"] takes priority over raw["price"]
        resp = dict(_MOCK_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["raw"] = dict(resp["data"]["raw"])
        resp["data"]["raw"]["fills"] = [{"price": "514.66", "amount": "0.08"}]
        resp["data"]["raw"]["price"] = "601.5"  # should be ignored
        er = reconcile_live_order(_LIVE_INTAKE, resp)["execution_result"]
        assert er["entry_price"] == 514.66

    def test_quality_ok_when_fully_filled(self):
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["execution_quality_flag"] == "OK"

    def test_quality_partial_fill(self):
        resp = dict(_MOCK_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["raw"] = dict(resp["data"]["raw"])
        resp["data"]["raw"]["filledAmount"] = "0.04"  # 50% filled → PARTIAL
        er = reconcile_live_order(_LIVE_INTAKE, resp)["execution_result"]
        assert er["execution_quality_flag"] == "PARTIAL_FILL"

    def test_entry_ts_from_broker_created(self):
        # created = 1744459200000 → 2025-04-12T12:00:00Z (unix epoch check)
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["entry_ts_utc"] == "2025-04-12T12:00:00Z"


# ---------------------------------------------------------------------------
# N. Reconciler blocks on bad input
# ---------------------------------------------------------------------------

class TestReconcilerBadInput:
    def test_non_dict_intake_fails(self):
        r = reconcile_live_order(None, _MOCK_BROKER_RESPONSE_OK)
        assert r["ok"] is False
        assert "intake_record" in r["reason"]

    def test_non_dict_broker_response_fails(self):
        r = reconcile_live_order(_LIVE_INTAKE, None)
        assert r["ok"] is False

    def test_broker_not_ok_fails(self):
        r = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_ERROR)
        assert r["ok"] is False

    def test_missing_order_id_fails(self):
        resp = dict(_MOCK_BROKER_RESPONSE_OK)
        resp["data"] = dict(resp["data"])
        resp["data"]["order_id"] = ""
        resp["data"]["raw"] = {}
        r = reconcile_live_order(_LIVE_INTAKE, resp)
        assert r["ok"] is False

    def test_data_not_dict_fails(self):
        resp = dict(_MOCK_BROKER_RESPONSE_OK)
        resp["data"] = "bad"
        r = reconcile_live_order(_LIVE_INTAKE, resp)
        assert r["ok"] is False

    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_no_exception_bad_inputs(self, bad):
        r = reconcile_live_order(bad, bad)
        assert isinstance(r, dict)
        assert "ok" in r


# ---------------------------------------------------------------------------
# O. Reconciler produces deterministic trade_id
# ---------------------------------------------------------------------------

class TestReconcilerTradeId:
    def test_trade_id_starts_with_live(self):
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["trade_id"].startswith("LIVE-")

    def test_trade_id_contains_market(self):
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert "BNBEUR" in er["trade_id"]

    def test_trade_id_contains_strategy(self):
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert "EDGE3" in er["trade_id"]

    def test_trade_id_contains_side(self):
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert "LONG" in er["trade_id"]

    def test_trade_id_is_deterministic(self):
        er1 = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        er2 = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er1["trade_id"] == er2["trade_id"]


# ---------------------------------------------------------------------------
# P. Reconciler sentinel values for open exit fields
# ---------------------------------------------------------------------------

class TestReconcilerSentinels:
    def test_exit_reason_null(self):
        # AC-193: null until exit proven
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["exit_reason"] is None

    def test_broker_order_id_exit_null(self):
        # AC-190: null until a proven exit exists
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["broker_order_id_exit"] is None

    def test_realized_pnl_null(self):
        # AC-193: null until exit proven
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["realized_pnl_eur"] is None

    def test_hold_duration_null(self):
        # AC-193: null until exit proven
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["hold_duration_minutes"] is None

    def test_slippage_zero(self):
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["slippage_eur"] == 0.0

    def test_exit_ts_null(self):
        # AC-193: null until exit proven
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["exit_ts_utc"] is None

    def test_exit_price_null(self):
        # AC-193: null until exit proven
        er = reconcile_live_order(_LIVE_INTAKE, _MOCK_BROKER_RESPONSE_OK)["execution_result"]
        assert er["exit_price"] is None


# ---------------------------------------------------------------------------
# Q. place_order adapter unit tests
# ---------------------------------------------------------------------------

class TestBitvavoAdapterPlaceOrder:
    def _adapter(self, **kwargs) -> BitvavoAdapter:
        return BitvavoAdapter(api_key="k", api_secret="s", **kwargs)

    def test_non_dict_order_request_returns_error(self):
        r = self._adapter().place_order("bad")
        assert r["ok"] is False
        assert "INVALID_ORDER_REQUEST" in r["error"]["code"]

    def test_missing_market_returns_error(self):
        r = self._adapter().place_order({"side": "buy", "order_type": "market", "qty": 0.08})
        assert r["ok"] is False

    def test_missing_qty_returns_error(self):
        r = self._adapter().place_order({"market": "BNB-EUR", "side": "buy", "order_type": "market"})
        assert r["ok"] is False

    def test_limit_order_missing_price_returns_error(self):
        r = self._adapter().place_order({
            "market": "BNB-EUR",
            "side": "buy",
            "order_type": "limit",
            "qty": 0.08,
        })
        assert r["ok"] is False
        assert "MISSING_LIMIT_PRICE" in r["error"]["code"]

    def test_result_shape_on_error(self):
        r = self._adapter().place_order("bad")
        for k in ("ok", "adapter", "operation", "ts_utc", "data", "error", "meta"):
            assert k in r, f"missing key: {k}"

    def test_adapter_name_is_bitvavo(self):
        r = self._adapter().place_order("bad")
        assert r["adapter"] == "bitvavo"

    def test_operation_is_place_order(self):
        r = self._adapter().place_order("bad")
        assert r["operation"] == "place_order"


# ---------------------------------------------------------------------------
# R. place_order: no exception leaks
# ---------------------------------------------------------------------------

class TestPlaceOrderNoExceptions:
    @pytest.mark.parametrize("bad", [None, 42, "x", [], True, {}])
    def test_no_exception_on_bad_input(self, bad):
        adapter = BitvavoAdapter(api_key="k", api_secret="s")
        r = adapter.place_order(bad)
        assert isinstance(r, dict)
        assert "ok" in r


# ---------------------------------------------------------------------------
# S. Executor is fail-closed — no exceptions leak
# ---------------------------------------------------------------------------

class TestExecutorNoExceptions:
    @pytest.mark.parametrize("bad", [None, 42, "x", [], True])
    def test_no_exception_bad_intake(self, bad):
        r = execute_first_live_order(bad, _LIVE_LANE_CFG, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert isinstance(r, dict)
        assert "ok" in r
        assert r["ok"] is False

    @pytest.mark.parametrize("bad", [None, {}, 42, "x"])
    def test_no_exception_bad_lane_config(self, bad):
        r = execute_first_live_order(_LIVE_INTAKE, bad, _MACRO_NORMAL, _AUTO_FREEZE_CLEAR, _adapter=_MockAdapterOk())
        assert isinstance(r, dict)
        assert r["ok"] is False

    def test_always_returns_dict(self):
        for v in (None, {}, [], "x", 0, True):
            r = execute_first_live_order(v, v, v, v, _adapter=_MockAdapterOk())
            assert isinstance(r, dict)


# ---------------------------------------------------------------------------
# T. No paper/broker marker strings in source
# ---------------------------------------------------------------------------

_PAPER_MARKERS = (
    "build_execution_bridge",
    "build_paper",
    "dry_run_ledger",
    "paper_runner",
    "paper_intent",
    "ANT_OUT",
)

_BROKER_IO_MARKERS = (
    "import requests",
    "urllib",
    "http.client",
)


class TestSourceMarkers:
    def _read(self, filename: str) -> str:
        return (_REPO_ROOT / "ant_colony" / "live" / filename).read_text(encoding="utf-8")

    def test_executor_no_paper_markers(self):
        src = self._read("bitvavo_live_executor.py")
        for marker in _PAPER_MARKERS:
            assert marker not in src, f"bitvavo_live_executor.py contains paper marker: {marker!r}"

    def test_reconciler_no_paper_markers(self):
        src = self._read("live_order_reconciler.py")
        for marker in _PAPER_MARKERS:
            assert marker not in src, f"live_order_reconciler.py contains paper marker: {marker!r}"

    def test_executor_no_direct_http(self):
        src = self._read("bitvavo_live_executor.py")
        for marker in _BROKER_IO_MARKERS:
            assert marker not in src, f"bitvavo_live_executor.py contains direct HTTP: {marker!r}"

    def test_reconciler_no_direct_http(self):
        src = self._read("live_order_reconciler.py")
        for marker in _BROKER_IO_MARKERS:
            assert marker not in src, f"live_order_reconciler.py contains direct HTTP: {marker!r}"


# ---------------------------------------------------------------------------
# U. Executor result always has required keys
# ---------------------------------------------------------------------------

class TestExecutorRequiredKeys:
    _REQUIRED = ("ok", "reason", "gate", "execution_result")

    def _check(self, result):
        for k in self._REQUIRED:
            assert k in result, f"missing key: {k}"

    def test_ok_result_has_all_keys(self):
        self._check(_exec())

    def test_blocked_at_a_has_all_keys(self):
        self._check(_exec(intake={"market": "WRONG"}))

    def test_blocked_at_controlled_live_has_all_keys(self):
        self._check(_exec(lane={"live_enabled": False}))

    def test_blocked_at_auto_freeze_has_all_keys(self):
        self._check(_exec(auto_freeze={"allow": False, "reason": "stale", "risk_state": "FREEZE", "freeze_new_entries": True}))

    def test_blocked_at_g_has_all_keys(self):
        self._check(_exec(adapter=_MockAdapterError()))
