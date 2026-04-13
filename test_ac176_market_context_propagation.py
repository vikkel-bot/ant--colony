"""
AC-176: Market context (regime/volatility) propagation into feedback and memory.

Verifies:
  A. market_regime_at_entry from intake propagates to feedback
  B. volatility_at_entry from intake propagates to feedback
  C. market_regime_at_entry propagates to memory
  D. volatility_at_entry propagates to memory
  E. Missing market_regime falls back to "UNKNOWN" (not a failure)
  F. Missing volatility falls back to "UNKNOWN" (not a failure)
  G. Invalid regime value falls back to "UNKNOWN"
  H. Invalid volatility value falls back to "UNKNOWN"
  I. Lowercase regime value accepted (normalised to uppercase)
  J. Regression: all AC-174/175 causal fields still correct alongside regime context
  K. queen_action_required=False when regime and volatility are known
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
    # AC-175 signal context
    "signal_key": "EDGE3_BREAKOUT_V2",
    "signal_strength": 0.75,
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
        "order_id": "BTV-ORDER-176",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-176",
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
# A. market_regime_at_entry propagates to feedback
# ---------------------------------------------------------------------------

def test_A_regime_in_feedback(tmp_path):
    result = _run(tmp_path, {"market_regime_at_entry": "BULL"})
    assert result["ok"] is True
    assert _feedback(result)["market_regime_at_entry"] == "BULL"


def test_A_bear_regime_in_feedback(tmp_path):
    result = _run(tmp_path, {"market_regime_at_entry": "BEAR"})
    assert result["ok"] is True
    assert _feedback(result)["market_regime_at_entry"] == "BEAR"


def test_A_sideways_regime_in_feedback(tmp_path):
    result = _run(tmp_path, {"market_regime_at_entry": "SIDEWAYS"})
    assert result["ok"] is True
    assert _feedback(result)["market_regime_at_entry"] == "SIDEWAYS"


# ---------------------------------------------------------------------------
# B. volatility_at_entry propagates to feedback
# ---------------------------------------------------------------------------

def test_B_volatility_low_in_feedback(tmp_path):
    result = _run(tmp_path, {"volatility_at_entry": "LOW"})
    assert result["ok"] is True
    assert _feedback(result)["volatility_at_entry"] == "LOW"


def test_B_volatility_mid_in_feedback(tmp_path):
    result = _run(tmp_path, {"volatility_at_entry": "MID"})
    assert result["ok"] is True
    assert _feedback(result)["volatility_at_entry"] == "MID"


def test_B_volatility_high_in_feedback(tmp_path):
    result = _run(tmp_path, {"volatility_at_entry": "HIGH"})
    assert result["ok"] is True
    assert _feedback(result)["volatility_at_entry"] == "HIGH"


# ---------------------------------------------------------------------------
# C. market_regime_at_entry propagates to memory
# ---------------------------------------------------------------------------

def test_C_regime_in_memory(tmp_path):
    result = _run(tmp_path, {"market_regime_at_entry": "BULL"})
    assert result["ok"] is True
    assert _memory(result)["market_regime_at_entry"] == "BULL"


# ---------------------------------------------------------------------------
# D. volatility_at_entry propagates to memory
# ---------------------------------------------------------------------------

def test_D_volatility_in_memory(tmp_path):
    result = _run(tmp_path, {"volatility_at_entry": "MID"})
    assert result["ok"] is True
    assert _memory(result)["volatility_at_entry"] == "MID"


# ---------------------------------------------------------------------------
# E. Missing regime → "UNKNOWN" sentinel, not a failure
# ---------------------------------------------------------------------------

def test_E_missing_regime_sentinel(tmp_path):
    result = _run(tmp_path)  # no market_regime_at_entry
    assert result["ok"] is True
    assert _feedback(result)["market_regime_at_entry"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# F. Missing volatility → "UNKNOWN" sentinel, not a failure
# ---------------------------------------------------------------------------

def test_F_missing_volatility_sentinel(tmp_path):
    result = _run(tmp_path)  # no volatility_at_entry
    assert result["ok"] is True
    assert _feedback(result)["volatility_at_entry"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# G. Invalid regime value falls back to "UNKNOWN"
# ---------------------------------------------------------------------------

def test_G_invalid_regime_fallback(tmp_path):
    result = _run(tmp_path, {"market_regime_at_entry": "TRENDING_UP"})
    assert result["ok"] is True
    assert _feedback(result)["market_regime_at_entry"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# H. Invalid volatility value falls back to "UNKNOWN"
# ---------------------------------------------------------------------------

def test_H_invalid_volatility_fallback(tmp_path):
    result = _run(tmp_path, {"volatility_at_entry": "EXTREME"})
    assert result["ok"] is True
    assert _feedback(result)["volatility_at_entry"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# I. Lowercase values normalised to uppercase
# ---------------------------------------------------------------------------

def test_I_lowercase_regime_accepted(tmp_path):
    result = _run(tmp_path, {"market_regime_at_entry": "bull"})
    assert result["ok"] is True
    assert _feedback(result)["market_regime_at_entry"] == "BULL"


def test_I_lowercase_volatility_accepted(tmp_path):
    result = _run(tmp_path, {"volatility_at_entry": "low"})
    assert result["ok"] is True
    assert _feedback(result)["volatility_at_entry"] == "LOW"


# ---------------------------------------------------------------------------
# J. Regression: AC-174/175 causal fields intact
# ---------------------------------------------------------------------------

def test_J_regression_all_causal_fields(tmp_path):
    result = _run(tmp_path, {
        "market_regime_at_entry": "BULL",
        "volatility_at_entry": "MID",
    })
    assert result["ok"] is True
    fb = _feedback(result)
    assert fb["market_regime_at_entry"] == "BULL"
    assert fb["volatility_at_entry"] == "MID"
    assert fb["signal_key"] == "EDGE3_BREAKOUT_V2"
    assert abs(fb["signal_strength"] - 0.75) < 1e-9
    assert fb["entry_latency_ms"] == 95
    assert abs(fb["slippage_vs_expected_eur"] - 0.12) < 1e-6


# ---------------------------------------------------------------------------
# K. queen_action_required=False when regime and volatility are known
# ---------------------------------------------------------------------------

def test_K_queen_action_not_required_when_context_known(tmp_path):
    result = _run(tmp_path, {
        "market_regime_at_entry": "BULL",
        "volatility_at_entry": "LOW",
    })
    assert result["ok"] is True
    mem = _memory(result)
    # execution_quality_flag=OK, regime known, volatility known → no queen action needed
    assert mem["queen_action_required"] is False
