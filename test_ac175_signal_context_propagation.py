"""
AC-175: Signal context propagation into feedback and memory artifacts.

Verifies:
  A. signal_key from intake propagates to feedback artifact
  B. signal_strength from intake propagates to feedback artifact
  C. signal_key propagates to memory artifact
  D. signal_strength propagates to memory artifact
  E. Missing signal_key falls back to "UNKNOWN" sentinel (not a failure)
  F. Missing signal_strength falls back to -1.0 sentinel (not a failure)
  G. Invalid signal_strength (out of range) falls back to -1.0
  H. signal_key empty string falls back to "UNKNOWN"
  I. market_regime_at_entry and volatility_at_entry still UNKNOWN (unchanged)
  J. Regression: AC-174 values (latency, slippage) still correct alongside signal context
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.bitvavo_live_executor import execute_and_persist_live_order

_NOW = "2026-04-13T10:00:00Z"

_INTAKE_BASE = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "order_side": "buy",
    "qty": 0.08,
    "intended_entry_price": 600.0,
    "order_type": "market",
    "max_notional_eur": 50.0,
    "allow_broker_execution": True,
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
    "operator_approved": True,
    "operator_id": "OP-TEST",
    "ts_intake_utc": _NOW,
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

_BROKER_RESPONSE_OK = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-ORDER-175",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-175",
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
    "meta": {"latency_ms": 95, "attempts": 1, "rate_limited": False},
}


class _MockAdapter:
    def place_order(self, _req):
        return dict(_BROKER_RESPONSE_OK)


def _lane_cfg(tmp_path):
    return {
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
        "base_output_dir": str(tmp_path),
    }


def _run(tmp_path, intake_overrides=None):
    intake = {**_INTAKE_BASE, **(intake_overrides or {})}
    return execute_and_persist_live_order(
        intake,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapter(),
    )


def _feedback(result):
    return json.loads(Path(result["artifacts"]["feedback"]).read_text(encoding="utf-8"))


def _memory(result):
    return json.loads(Path(result["artifacts"]["memory"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. signal_key propagates to feedback
# ---------------------------------------------------------------------------

def test_A_signal_key_in_feedback(tmp_path):
    result = _run(tmp_path, {"signal_key": "EDGE3_BREAKOUT_V2"})
    assert result["ok"] is True
    fb = _feedback(result)
    assert fb["signal_key"] == "EDGE3_BREAKOUT_V2"


# ---------------------------------------------------------------------------
# B. signal_strength propagates to feedback
# ---------------------------------------------------------------------------

def test_B_signal_strength_in_feedback(tmp_path):
    result = _run(tmp_path, {"signal_strength": 0.82})
    assert result["ok"] is True
    fb = _feedback(result)
    assert abs(fb["signal_strength"] - 0.82) < 1e-9


# ---------------------------------------------------------------------------
# C. signal_key propagates to memory
# ---------------------------------------------------------------------------

def test_C_signal_key_in_memory(tmp_path):
    result = _run(tmp_path, {"signal_key": "EDGE3_BREAKOUT_V2"})
    assert result["ok"] is True
    mem = _memory(result)
    assert mem["signal_key"] == "EDGE3_BREAKOUT_V2"


# ---------------------------------------------------------------------------
# D. signal_strength propagates to memory
# ---------------------------------------------------------------------------

def test_D_signal_strength_in_memory(tmp_path):
    result = _run(tmp_path, {"signal_strength": 0.82})
    assert result["ok"] is True
    mem = _memory(result)
    assert abs(mem["signal_strength"] - 0.82) < 1e-9


# ---------------------------------------------------------------------------
# E. Missing signal_key → "UNKNOWN" sentinel, not a failure
# ---------------------------------------------------------------------------

def test_E_missing_signal_key_sentinel(tmp_path):
    result = _run(tmp_path)  # no signal_key in intake
    assert result["ok"] is True
    fb = _feedback(result)
    assert fb["signal_key"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# F. Missing signal_strength → -1.0 sentinel, not a failure
# ---------------------------------------------------------------------------

def test_F_missing_signal_strength_sentinel(tmp_path):
    result = _run(tmp_path)  # no signal_strength in intake
    assert result["ok"] is True
    fb = _feedback(result)
    assert fb["signal_strength"] == -1.0


# ---------------------------------------------------------------------------
# G. Out-of-range signal_strength falls back to -1.0
# ---------------------------------------------------------------------------

def test_G_out_of_range_signal_strength_fallback(tmp_path):
    result = _run(tmp_path, {"signal_strength": 1.5})
    assert result["ok"] is True
    fb = _feedback(result)
    assert fb["signal_strength"] == -1.0


# ---------------------------------------------------------------------------
# H. Empty signal_key falls back to "UNKNOWN"
# ---------------------------------------------------------------------------

def test_H_empty_signal_key_fallback(tmp_path):
    result = _run(tmp_path, {"signal_key": "   "})
    assert result["ok"] is True
    fb = _feedback(result)
    assert fb["signal_key"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# I. market_regime and volatility still UNKNOWN
# ---------------------------------------------------------------------------

def test_I_regime_and_volatility_still_unknown(tmp_path):
    result = _run(tmp_path, {"signal_key": "EDGE3_BREAKOUT_V2", "signal_strength": 0.7})
    assert result["ok"] is True
    fb = _feedback(result)
    assert fb["market_regime_at_entry"] == "UNKNOWN"
    assert fb["volatility_at_entry"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# J. Regression: AC-174 values still correct alongside signal context
# ---------------------------------------------------------------------------

def test_J_regression_ac174_values_intact(tmp_path):
    result = _run(tmp_path, {"signal_key": "EDGE3_BREAKOUT_V2", "signal_strength": 0.9})
    assert result["ok"] is True
    fb = _feedback(result)
    # latency from meta.latency_ms = 95
    assert fb["entry_latency_ms"] == 95
    # slippage from raw.price: (601.5 - 600.0) * 0.08 = 0.12
    assert abs(fb["slippage_vs_expected_eur"] - 0.12) < 1e-6
