"""
AC-173: Tests for feedback and memory artifact wiring in execute_and_persist_live_order.

Verifies:
  A. feedback/ directory created after successful live execution
  B. memory/ directory created after successful live execution
  C. feedback artifact contains expected trade_id
  D. memory artifact contains expected trade_id
  E. artifacts dict contains all four paths: execution, broker, feedback, memory
  F. feedback artifact passes live_feedback_schema validation
  G. memory artifact contains queen_action_required field
  H. Execution failure before feedback: feedback dir not created
  I. Regression: execution and broker artifacts still written (AC-167)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.bitvavo_live_executor import execute_and_persist_live_order
from ant_colony.live.live_feedback_schema import validate_live_feedback_record

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

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

_LANE_CFG_BASE = {
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
        "order_id": "BTV-ORDER-173",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-173",
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


class _MockAdapterOk:
    def place_order(self, order_request):
        return dict(_BROKER_RESPONSE_OK)


class _MockAdapterFail:
    def place_order(self, order_request):
        return {
            "ok": False,
            "error": {"type": "BROKER_REJECTED", "code": "205",
                      "message": "rejected", "retryable": False},
            "meta": {},
        }


def _lane_cfg(tmp_path):
    return {**_LANE_CFG_BASE, "base_output_dir": str(tmp_path)}


def _run(tmp_path, adapter=None):
    return execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=adapter or _MockAdapterOk(),
    )


# ---------------------------------------------------------------------------
# A. feedback/ directory created
# ---------------------------------------------------------------------------

def test_A_feedback_dir_created(tmp_path):
    result = _run(tmp_path)
    assert result["ok"] is True
    assert (tmp_path / "live_test" / "feedback").is_dir()


# ---------------------------------------------------------------------------
# B. memory/ directory created
# ---------------------------------------------------------------------------

def test_B_memory_dir_created(tmp_path):
    result = _run(tmp_path)
    assert result["ok"] is True
    assert (tmp_path / "live_test" / "memory").is_dir()


# ---------------------------------------------------------------------------
# C. feedback artifact contains trade_id
# ---------------------------------------------------------------------------

def test_C_feedback_artifact_contains_trade_id(tmp_path):
    result = _run(tmp_path)
    assert result["ok"] is True
    fb_path = Path(result["artifacts"]["feedback"])
    assert fb_path.exists()
    fb = json.loads(fb_path.read_text(encoding="utf-8"))
    assert fb["trade_id"] == result["execution_result"]["trade_id"]


# ---------------------------------------------------------------------------
# D. memory artifact contains trade_id
# ---------------------------------------------------------------------------

def test_D_memory_artifact_contains_trade_id(tmp_path):
    result = _run(tmp_path)
    assert result["ok"] is True
    mem_path = Path(result["artifacts"]["memory"])
    assert mem_path.exists()
    mem = json.loads(mem_path.read_text(encoding="utf-8"))
    assert mem["trade_id"] == result["execution_result"]["trade_id"]


# ---------------------------------------------------------------------------
# E. artifacts dict has all four keys
# ---------------------------------------------------------------------------

def test_E_artifacts_has_all_four_keys(tmp_path):
    result = _run(tmp_path)
    assert result["ok"] is True
    arts = result["artifacts"]
    assert "execution" in arts
    assert "broker" in arts
    assert "feedback" in arts
    assert "memory" in arts


# ---------------------------------------------------------------------------
# F. feedback artifact passes schema validation
# ---------------------------------------------------------------------------

def test_F_feedback_artifact_passes_schema(tmp_path):
    result = _run(tmp_path)
    fb_path = Path(result["artifacts"]["feedback"])
    fb = json.loads(fb_path.read_text(encoding="utf-8"))
    schema_result = validate_live_feedback_record(fb)
    assert schema_result["ok"] is True, schema_result["reason"]


# ---------------------------------------------------------------------------
# G. memory artifact has queen_action_required
# ---------------------------------------------------------------------------

def test_G_memory_has_queen_action_required(tmp_path):
    result = _run(tmp_path)
    mem_path = Path(result["artifacts"]["memory"])
    mem = json.loads(mem_path.read_text(encoding="utf-8"))
    assert "queen_action_required" in mem
    # UNKNOWN regime/volatility → queen must review
    assert mem["queen_action_required"] is True


# ---------------------------------------------------------------------------
# H. Execution failure: feedback dir not created
# ---------------------------------------------------------------------------

def test_H_execution_failure_no_feedback_dir(tmp_path):
    result = execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterFail(),
    )
    assert result["ok"] is False
    assert not (tmp_path / "live_test" / "feedback").exists()
    assert not (tmp_path / "live_test" / "memory").exists()


# ---------------------------------------------------------------------------
# I. Regression: execution and broker artifacts still written
# ---------------------------------------------------------------------------

def test_I_regression_execution_and_broker_artifacts(tmp_path):
    result = _run(tmp_path)
    assert result["ok"] is True
    assert Path(result["artifacts"]["execution"]).exists()
    assert Path(result["artifacts"]["broker"]).exists()
