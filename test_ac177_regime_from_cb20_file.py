"""
AC-177: cb20 regime snapshot enrichment of live intake via live_lane_runner.

Verifies:
  A. market_regime_at_entry read from cb20_regime.json and propagated to feedback
  B. volatility_at_entry read from cb20_regime.json and propagated to feedback
  C. Missing file → UNKNOWN fallback (not a failure)
  D. Invalid trend_regime value → UNKNOWN fallback
  E. Invalid vol_regime value → UNKNOWN fallback
  F. Caller-supplied market_regime_at_entry takes precedence over file value
  G. Caller-supplied volatility_at_entry takes precedence over file value
  H. Regression: AC-174/175 causal fields intact when regime file present
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live import live_lane_runner

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
    "signal_key": "EDGE3_BREAKOUT_V2",
    "signal_strength": 0.75,
}

_MACRO_NORMAL = {
    "risk_state": "NORMAL",
    "reason": "",
    "freeze_new_entries": False,
    "updated_ts_utc": "",
}

_BROKER_RESPONSE_OK = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-ORDER-177",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-177",
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
    "meta": {"latency_ms": 88, "attempts": 1, "rate_limited": False},
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


def _macro_cfg():
    return dict(_MACRO_NORMAL)


def _write_cb20(workers_dir, market, trend_regime, vol_regime):
    """Write a cb20_regime.json snapshot for the given market under workers_dir."""
    report_dir = workers_dir / market / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    snap = {
        "market": market,
        "trend_regime": trend_regime,
        "vol_regime": vol_regime,
        "ts_utc": _NOW,
    }
    (report_dir / "cb20_regime.json").write_text(
        json.dumps(snap), encoding="utf-8"
    )


def _run(tmp_path, workers_dir, intake_overrides=None):
    """
    Run via live_lane_runner.run() with a patched _WORKERS_DIR so the file
    lookup hits our tmp workers directory.
    """
    import ant_colony.live.live_lane_runner as _runner_mod

    original_workers_dir = _runner_mod._WORKERS_DIR
    _runner_mod._WORKERS_DIR = workers_dir
    try:
        intake = {**_INTAKE_BASE, **(intake_overrides or {})}
        result = live_lane_runner.run(
            config=_lane_cfg(tmp_path),
            macro_config=_macro_cfg(),
            intake_record=intake,
            _adapter=_MockAdapter(),
        )
    finally:
        _runner_mod._WORKERS_DIR = original_workers_dir
    return result


def _feedback(result):
    return json.loads(Path(result["artifacts"]["feedback"]).read_text(encoding="utf-8"))


def _memory(result):
    return json.loads(Path(result["artifacts"]["memory"]).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# A. market_regime_at_entry read from file → propagated to feedback
# ---------------------------------------------------------------------------

def test_A_regime_from_file_in_feedback(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "BULL", "LOW")
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["market_regime_at_entry"] == "BULL"


def test_A_bear_regime_from_file(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "BEAR", "HIGH")
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["market_regime_at_entry"] == "BEAR"


def test_A_sideways_regime_from_file(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "SIDEWAYS", "MID")
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["market_regime_at_entry"] == "SIDEWAYS"


# ---------------------------------------------------------------------------
# B. volatility_at_entry read from file → propagated to feedback
# ---------------------------------------------------------------------------

def test_B_volatility_from_file_in_feedback(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "BULL", "MID")
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["volatility_at_entry"] == "MID"


def test_B_high_volatility_from_file(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "BEAR", "HIGH")
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["volatility_at_entry"] == "HIGH"


# ---------------------------------------------------------------------------
# C. Missing file → UNKNOWN fallback, not a failure
# ---------------------------------------------------------------------------

def test_C_missing_file_fallback_regime(tmp_path):
    workers = tmp_path / "workers"  # no file written
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["market_regime_at_entry"] == "UNKNOWN"


def test_C_missing_file_fallback_volatility(tmp_path):
    workers = tmp_path / "workers"  # no file written
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["volatility_at_entry"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# D. Invalid trend_regime value → UNKNOWN fallback
# ---------------------------------------------------------------------------

def test_D_invalid_trend_regime_fallback(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "TRENDING_UP", "LOW")
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["market_regime_at_entry"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# E. Invalid vol_regime value → UNKNOWN fallback
# ---------------------------------------------------------------------------

def test_E_invalid_vol_regime_fallback(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "BULL", "EXTREME")
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["volatility_at_entry"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# F. Caller-supplied market_regime_at_entry takes precedence over file
# ---------------------------------------------------------------------------

def test_F_caller_regime_takes_precedence(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "BEAR", "HIGH")  # file says BEAR
    result = _run(tmp_path, workers, {"market_regime_at_entry": "SIDEWAYS"})  # caller says SIDEWAYS
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["market_regime_at_entry"] == "SIDEWAYS"


# ---------------------------------------------------------------------------
# G. Caller-supplied volatility_at_entry takes precedence over file
# ---------------------------------------------------------------------------

def test_G_caller_volatility_takes_precedence(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "BULL", "HIGH")  # file says HIGH
    result = _run(tmp_path, workers, {"volatility_at_entry": "LOW"})  # caller says LOW
    assert result["state"] == "EXECUTED"
    assert _feedback(result)["volatility_at_entry"] == "LOW"


# ---------------------------------------------------------------------------
# H. Regression: AC-174/175 causal fields intact when regime file present
# ---------------------------------------------------------------------------

def test_H_regression_all_causal_fields(tmp_path):
    workers = tmp_path / "workers"
    _write_cb20(workers, "BNB-EUR", "BULL", "MID")
    result = _run(tmp_path, workers)
    assert result["state"] == "EXECUTED"
    fb = _feedback(result)
    assert fb["market_regime_at_entry"] == "BULL"
    assert fb["volatility_at_entry"] == "MID"
    assert fb["signal_key"] == "EDGE3_BREAKOUT_V2"
    assert abs(fb["signal_strength"] - 0.75) < 1e-9
    assert fb["entry_latency_ms"] == 88
    assert abs(fb["slippage_vs_expected_eur"] - 0.12) < 1e-6
