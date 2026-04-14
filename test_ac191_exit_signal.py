"""
AC-191: EDGE3 Exit Signal Tests

Verifies:
  A. Price above TP threshold → exit intent with exit_reason="TP"
  B. Price below SL threshold → exit intent with exit_reason="SL"
  C. Price within range → None (no exit)
  D. Price fetch error → runner returns BLOCKED (fail-closed)
  E. exit_intent contains operator_approved=True
  F. exit_intent contains required fields (lane, market, strategy_key, qty, etc.)
  G. TP boundary (exact threshold) → no exit (not strictly >)
  H. SL boundary (exact threshold) → no exit (not strictly <)
  I. evaluate_exit_signal never raises
  J. Runner with open position and TP price → EXIT_EXECUTED (mock adapter)
  K. Runner with open position and SL price → EXIT_EXECUTED (mock adapter)
  L. Runner with open position, price within range → LIVE_GATE_READY blocked
     by open-position guard (no auto-exit)
  M. Runner with no open position → LIVE_GATE_READY (no auto-exit triggered)
  N. exit_intent entry_order_id matches broker_order_id_entry from artifact
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_exit_signal import evaluate_exit_signal
from ant_colony.live.live_lane_runner import run
from ant_colony.live.bitvavo_live_exit_executor import _reset_exit_dedup_for_testing

_NOW = "2026-04-14T12:00:00Z"

_ARTIFACT = {
    "trade_id":              "TRADE-001",
    "lane":                  "live_test",
    "market":                "BNB-EUR",
    "strategy_key":          "EDGE3",
    "position_side":         "long",
    "qty":                   0.08,
    "entry_price":           600.0,
    "broker_order_id_entry": "BTV-ORDER-001",
}

_CONFIG = {
    "lane":                  "live_test",
    "enabled":               True,
    "live_enabled":          True,
    "market":                "BNB-EUR",
    "strategy":              "EDGE3",
    "max_notional_eur":      50,
    "max_positions":         1,
    "allow_broker_execution": True,
    "allow_shared_state":    False,
    "allow_paper_inputs":    False,
    "sl_pct":                0.03,
    "tp_pct":                0.05,
}

_MACRO_NORMAL = {
    "risk_state":        "NORMAL",
    "reason":            "",
    "freeze_new_entries": False,
    "updated_ts_utc":    "",
}

_AUTO_FREEZE_CLEAR = {
    "allow":             True,
    "reason":            "AUTO_FREEZE_CLEAR",
    "risk_state":        "NORMAL",
    "freeze_new_entries": False,
}

# entry_price=600, tp_pct=0.05 → TP at 630; sl_pct=0.03 → SL at 582
_TP_PRICE = 631.0   # strictly above 630
_SL_PRICE = 581.0   # strictly below 582
_IN_RANGE  = 610.0  # within [582, 630]


# ---------------------------------------------------------------------------
# Mock adapters
# ---------------------------------------------------------------------------

class _MockAdapterPrice:
    """Returns a configurable current price from get_market_data."""
    def __init__(self, price: float):
        self._price = price

    def get_market_data(self, market, interval, limit=1):
        return {
            "ok": True,
            "data": {
                "market": market,
                "interval": interval,
                "rows": [{"close": self._price}],
                "count": 1,
            },
        }

    def place_order(self, order_request):
        return {
            "ok": True,
            "adapter": "bitvavo",
            "operation": "place_order",
            "ts_utc": _NOW,
            "data": {
                "market": "BNB-EUR",
                "order_id": "MOCK-EXIT-AC191",
                "status": "filled",
                "side": "sell",
                "order_type": "market",
                "qty": 0.08,
                "raw": {
                    "orderId": "MOCK-EXIT-AC191",
                    "market": "BNB-EUR",
                    "side": "sell",
                    "orderType": "market",
                    "status": "filled",
                    "amount": "0.08",
                    "filledAmount": "0.08",
                    "price": str(self._price),
                    "created": 1744632000000,
                },
            },
            "error": None,
            "meta": {"latency_ms": 90, "attempts": 1, "rate_limited": False},
        }


class _MockAdapterPriceFail:
    """Simulates price fetch failure."""
    def get_market_data(self, market, interval, limit=1):
        return {"ok": False, "error": {"type": "NETWORK_ERROR", "message": "timeout"}}

    def place_order(self, order_request):
        return {"ok": False}


def _cfg_with_output(tmp_path: Path) -> dict:
    return {**_CONFIG, "base_output_dir": str(tmp_path)}


def _write_execution(tmp_path: Path) -> None:
    exec_dir = tmp_path / "live_test" / "execution"
    exec_dir.mkdir(parents=True, exist_ok=True)
    (exec_dir / "TRADE001.json").write_text(json.dumps({
        "trade_id": "TRADE-001",
        "market": "BNB-EUR",
        "strategy_key": "EDGE3",
        "position_side": "long",
        "qty": 0.08,
        "entry_price": 600.0,
        "broker_order_id_entry": "BTV-ORDER-001",
        "broker_order_id_exit": None,
        "lane": "live_test",
    }), encoding="utf-8")


# ---------------------------------------------------------------------------
# A. Price above TP → exit_reason = "TP"
# ---------------------------------------------------------------------------

def test_A_price_above_tp_returns_tp():
    result = evaluate_exit_signal(_ARTIFACT, _CONFIG, _TP_PRICE)
    assert result is not None
    assert result.get("exit_reason") == "TP"


# ---------------------------------------------------------------------------
# B. Price below SL → exit_reason = "SL"
# ---------------------------------------------------------------------------

def test_B_price_below_sl_returns_sl():
    result = evaluate_exit_signal(_ARTIFACT, _CONFIG, _SL_PRICE)
    assert result is not None
    assert result.get("exit_reason") == "SL"


# ---------------------------------------------------------------------------
# C. Price within range → None
# ---------------------------------------------------------------------------

def test_C_price_within_range_returns_none():
    result = evaluate_exit_signal(_ARTIFACT, _CONFIG, _IN_RANGE)
    assert result is None


# ---------------------------------------------------------------------------
# D. Price fetch failure → runner BLOCKED
# ---------------------------------------------------------------------------

def test_D_price_fetch_failure_runner_blocked(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    _write_execution(tmp_path)
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record=None,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterPriceFail(),
    )
    assert result["state"] == "BLOCKED"
    assert "PRICE_FETCH_FAILED" in result["reason"]


# ---------------------------------------------------------------------------
# E. exit_intent has operator_approved=True
# ---------------------------------------------------------------------------

def test_E_operator_approved_true():
    result = evaluate_exit_signal(_ARTIFACT, _CONFIG, _TP_PRICE)
    assert result is not None
    assert result.get("operator_approved") is True


# ---------------------------------------------------------------------------
# F. exit_intent contains all required fields
# ---------------------------------------------------------------------------

def test_F_exit_intent_required_fields():
    result = evaluate_exit_signal(_ARTIFACT, _CONFIG, _TP_PRICE)
    assert result is not None
    for field in ("lane", "market", "strategy_key", "position_side", "order_side",
                  "qty", "exit_reason", "operator_approved", "entry_order_id",
                  "entry_price", "ts_intent_utc"):
        assert field in result, f"missing field: {field}"


# ---------------------------------------------------------------------------
# G. Exact TP boundary → no exit (must be strictly >)
# ---------------------------------------------------------------------------

def test_G_tp_boundary_exact_no_exit():
    tp_exact = 600.0 * 1.05  # exactly 630.0
    result = evaluate_exit_signal(_ARTIFACT, _CONFIG, tp_exact)
    assert result is None


# ---------------------------------------------------------------------------
# H. Exact SL boundary → no exit (must be strictly <)
# ---------------------------------------------------------------------------

def test_H_sl_boundary_exact_no_exit():
    sl_exact = 600.0 * 0.97  # exactly 582.0
    result = evaluate_exit_signal(_ARTIFACT, _CONFIG, sl_exact)
    assert result is None


# ---------------------------------------------------------------------------
# I. evaluate_exit_signal never raises
# ---------------------------------------------------------------------------

def test_I_never_raises():
    for args in [
        ({}, {}, 0),
        (None, None, -1),
        (_ARTIFACT, _CONFIG, "not_a_number"),
        (_ARTIFACT, {}, 610.0),
    ]:
        try:
            evaluate_exit_signal(*args)
        except Exception as exc:
            pytest.fail(f"evaluate_exit_signal raised: {exc}")


# ---------------------------------------------------------------------------
# J. Runner with open position + TP price → EXIT_EXECUTED
# ---------------------------------------------------------------------------

def test_J_runner_tp_price_exit_executed(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    _write_execution(tmp_path)
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record=None,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterPrice(_TP_PRICE),
    )
    assert result["state"] == "EXIT_EXECUTED", f"unexpected: {result}"


# ---------------------------------------------------------------------------
# K. Runner with open position + SL price → EXIT_EXECUTED
# ---------------------------------------------------------------------------

def test_K_runner_sl_price_exit_executed(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    _write_execution(tmp_path)
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record=None,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterPrice(_SL_PRICE),
    )
    assert result["state"] == "EXIT_EXECUTED", f"unexpected: {result}"


# ---------------------------------------------------------------------------
# L. Runner with open position, price in range → open-position guard blocks
# ---------------------------------------------------------------------------

def test_L_runner_in_range_no_auto_exit(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    _write_execution(tmp_path)
    # No adapter needed — no price fetch when within range
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record=None,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterPrice(_IN_RANGE),
    )
    # Within range: no auto-exit → returns LIVE_GATE_READY (position guard
    # only fires on entry, not on gate-check calls without intake)
    assert result["state"] == "LIVE_GATE_READY"
    assert result.get("exit_artifact_dir") is None


# ---------------------------------------------------------------------------
# M. Runner without open position → LIVE_GATE_READY (no auto-exit triggered)
# ---------------------------------------------------------------------------

def test_M_runner_no_open_position_gate_ready(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    # No execution artifacts written
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record=None,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterPrice(_TP_PRICE),
    )
    assert result["state"] == "LIVE_GATE_READY"


# ---------------------------------------------------------------------------
# N. entry_order_id in exit_intent matches broker_order_id_entry
# ---------------------------------------------------------------------------

def test_N_entry_order_id_matches():
    result = evaluate_exit_signal(_ARTIFACT, _CONFIG, _TP_PRICE)
    assert result is not None
    assert result["entry_order_id"] == _ARTIFACT["broker_order_id_entry"]
