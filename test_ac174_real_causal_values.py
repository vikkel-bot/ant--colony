"""
AC-174: Real runtime causal values in feedback/memory artifacts.

Verifies:
  A. entry_latency_ms is taken from broker_response["meta"]["latency_ms"]
  B. slippage_vs_expected_eur computed from fills[0].price when fills present
  C. slippage_vs_expected_eur falls back to raw.price when fills absent
  D. slippage_vs_expected_eur falls back to execution_result.entry_price when no raw price
  E. Remaining four causal fields still UNKNOWN/sentinel
  F. Memory artifact contains real entry_latency_ms
  G. Memory artifact contains real slippage_vs_expected_eur
  H. entry_latency_ms=0 when meta absent (safe fallback)
  I. slippage_vs_expected_eur=0.0 when intended_entry_price absent (safe fallback)
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

_INTAKE = {
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

# Broker response with fills (priority path B)
_BROKER_RESPONSE_FILLS = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-ORDER-174A",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-174A",
            "market": "BNB-EUR",
            "side": "buy",
            "orderType": "market",
            "status": "filled",
            "amount": "0.08",
            "filledAmount": "0.08",
            "fills": [{"price": "613.00", "qty": "0.08"}],
        },
    },
    "error": None,
    "meta": {"latency_ms": 120, "attempts": 1, "rate_limited": False},
}

# Broker response with raw.price (fallback path C) but no fills
_BROKER_RESPONSE_RAW_PRICE = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-ORDER-174B",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-174B",
            "market": "BNB-EUR",
            "side": "buy",
            "orderType": "market",
            "status": "filled",
            "amount": "0.08",
            "filledAmount": "0.08",
            "price": "601.5",
        },
    },
    "error": None,
    "meta": {"latency_ms": 95, "attempts": 1, "rate_limited": False},
}

# Broker response with no fill price at all — slippage falls back to entry_price
_BROKER_RESPONSE_NO_PRICE = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-ORDER-174C",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-174C",
            "market": "BNB-EUR",
            "side": "buy",
            "orderType": "market",
            "status": "filled",
            "amount": "0.08",
            "filledAmount": "0.08",
        },
    },
    "error": None,
    "meta": {"latency_ms": 55, "attempts": 1, "rate_limited": False},
}


def _make_adapter(response):
    class _Adapter:
        def place_order(self, _req):
            return dict(response)
    return _Adapter()


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


def _run(tmp_path, broker_response):
    return execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_make_adapter(broker_response),
    )


def _load_memory(result):
    return json.loads(Path(result["artifacts"]["memory"]).read_text(encoding="utf-8"))


def _load_feedback(result):
    return json.loads(Path(result["artifacts"]["feedback"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. entry_latency_ms from meta.latency_ms
# ---------------------------------------------------------------------------

def test_A_entry_latency_ms_from_meta(tmp_path):
    result = _run(tmp_path, _BROKER_RESPONSE_RAW_PRICE)
    assert result["ok"] is True
    fb = _load_feedback(result)
    assert fb["entry_latency_ms"] == 95


# ---------------------------------------------------------------------------
# B. slippage uses fills[0].price when fills present
# ---------------------------------------------------------------------------

def test_B_slippage_from_fills_price(tmp_path):
    result = _run(tmp_path, _BROKER_RESPONSE_FILLS)
    assert result["ok"] is True
    fb = _load_feedback(result)
    # (613.00 - 600.0) * 0.08 = 1.04
    assert abs(fb["slippage_vs_expected_eur"] - 1.04) < 1e-6


# ---------------------------------------------------------------------------
# C. slippage falls back to raw.price when fills absent
# ---------------------------------------------------------------------------

def test_C_slippage_from_raw_price(tmp_path):
    result = _run(tmp_path, _BROKER_RESPONSE_RAW_PRICE)
    assert result["ok"] is True
    fb = _load_feedback(result)
    # (601.5 - 600.0) * 0.08 = 0.12
    assert abs(fb["slippage_vs_expected_eur"] - 0.12) < 1e-6


# ---------------------------------------------------------------------------
# D. slippage falls back to entry_price when no raw price
# ---------------------------------------------------------------------------

def test_D_no_fill_price_fails_closed(tmp_path):
    # AC-192: broker response without fills or raw.price → execution fails closed.
    # No fallback to intended_entry_price.
    result = _run(tmp_path, _BROKER_RESPONSE_NO_PRICE)
    assert result["ok"] is False
    assert "fill price" in result.get("reason", "").lower() or result.get("state") == "BLOCKED"


# ---------------------------------------------------------------------------
# E. Remaining four causal fields still UNKNOWN/sentinel
# ---------------------------------------------------------------------------

def test_E_sentinel_causal_fields_unchanged(tmp_path):
    result = _run(tmp_path, _BROKER_RESPONSE_RAW_PRICE)
    assert result["ok"] is True
    fb = _load_feedback(result)
    assert fb["market_regime_at_entry"] == "UNKNOWN"
    assert fb["volatility_at_entry"] == "UNKNOWN"
    assert fb["signal_strength"] == -1.0
    assert fb["signal_key"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# F. Memory artifact contains real entry_latency_ms
# ---------------------------------------------------------------------------

def test_F_memory_has_real_latency(tmp_path):
    result = _run(tmp_path, _BROKER_RESPONSE_RAW_PRICE)
    assert result["ok"] is True
    mem = _load_memory(result)
    assert mem["entry_latency_ms"] == 95


# ---------------------------------------------------------------------------
# G. Memory artifact contains real slippage
# ---------------------------------------------------------------------------

def test_G_memory_has_real_slippage(tmp_path):
    result = _run(tmp_path, _BROKER_RESPONSE_RAW_PRICE)
    assert result["ok"] is True
    mem = _load_memory(result)
    assert abs(mem["slippage_vs_expected_eur"] - 0.12) < 1e-6


# ---------------------------------------------------------------------------
# H. entry_latency_ms=0 when meta absent
# ---------------------------------------------------------------------------

def test_H_latency_zero_when_meta_absent(tmp_path):
    resp = dict(_BROKER_RESPONSE_RAW_PRICE)
    resp["meta"] = {}
    result = _run(tmp_path, resp)
    assert result["ok"] is True
    fb = _load_feedback(result)
    assert fb["entry_latency_ms"] == 0


# ---------------------------------------------------------------------------
# I. slippage=0.0 when intended_entry_price absent from intake
# ---------------------------------------------------------------------------

def test_I_slippage_zero_when_no_intended_price(tmp_path):
    intake = {k: v for k, v in _INTAKE.items() if k != "intended_entry_price"}
    result = execute_and_persist_live_order(
        intake,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_make_adapter(_BROKER_RESPONSE_RAW_PRICE),
    )
    # intake validation may block on missing intended_entry_price — that's fine
    if not result["ok"]:
        assert "INTAKE_INVALID" in result["reason"] or "REQUEST_BUILD" in result["reason"]
        return
    fb = _load_feedback(result)
    assert fb["slippage_vs_expected_eur"] == 0.0
