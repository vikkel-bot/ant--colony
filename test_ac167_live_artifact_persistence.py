"""
AC-167: Tests for live artifact persistence.

Verifies:
  A. base_output_dir is consumed from live_lane_config
  B. Execution and broker directories are created under {base_output_dir}/{lane}/
  C. Execution artifact file is written with correct content
  D. Broker artifact file is written (secrets stripped)
  E. Execution result survives process boundary (load from disk and compare)
  F. _strip_secrets removes known secret keys recursively
  G. Atomic write: no .tmp file remains after successful write
  H. Persistence failure → ok=False, gate=J_PERSIST, execution_result preserved
  I. feedback artifact written by write_feedback_artifact
  J. memory artifact written by write_memory_artifact
  K. Regression: execute_first_live_order unaffected (no artifacts, no _broker_response key)
  L. execute_and_persist_live_order ok=True includes artifacts key with paths
  M. Missing base_output_dir → execution succeeds without persisting (graceful degradation)
  N. Regression: successful order still returns ok=True
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_artifact_writer import (
    _strip_secrets,
    _write_json_atomic,
    write_entry_artifacts,
    write_feedback_artifact,
    write_memory_artifact,
)
from ant_colony.live.bitvavo_live_executor import (
    execute_first_live_order,
    execute_and_persist_live_order,
)

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

_EXECUTION_RESULT = {
    "trade_id": "LIVE-BNBEUR-EDGE3-LONG-20260413T100000",
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "qty": 0.08,
    "entry_price": 601.5,
    "exit_price": 601.5,
    "entry_ts_utc": _NOW,
    "exit_ts_utc": _NOW,
    "realized_pnl_eur": 0.0,
    "slippage_eur": 0.0,
    "hold_duration_minutes": 0.0,
    "exit_reason": "UNKNOWN",
    "execution_quality_flag": "OK",
    "broker_order_id_entry": "BTV-ORDER-001",
    "broker_order_id_exit": "ENTRY_ONLY_PENDING_EXIT",
    "ts_recorded_utc": _NOW,
}


class _MockAdapterOk:
    """Mock adapter that returns a successful broker response."""

    def place_order(self, order_request):
        return dict(_BROKER_RESPONSE_OK)


def _lane_cfg(tmp_path):
    return {**_LANE_CFG_BASE, "base_output_dir": str(tmp_path)}


# ---------------------------------------------------------------------------
# F. _strip_secrets
# ---------------------------------------------------------------------------

def test_F_strip_secrets_removes_api_key():
    obj = {"api_key": "SECRET", "market": "BNB-EUR", "qty": 0.08}
    result = _strip_secrets(obj)
    assert "api_key" not in result
    assert result["market"] == "BNB-EUR"


def test_F_strip_secrets_removes_nested():
    obj = {"data": {"apiSecret": "S3CR3T", "orderId": "ORD-001"}}
    result = _strip_secrets(obj)
    assert "apiSecret" not in result["data"]
    assert result["data"]["orderId"] == "ORD-001"


def test_F_strip_secrets_traverses_lists():
    obj = {"items": [{"api_key": "K", "v": 1}, {"v": 2}]}
    result = _strip_secrets(obj)
    assert "api_key" not in result["items"][0]
    assert result["items"][0]["v"] == 1
    assert result["items"][1]["v"] == 2


def test_F_strip_secrets_leaves_non_secret_keys():
    obj = {"operatorId": "OP-TEST", "amount": "0.08"}
    result = _strip_secrets(obj)
    assert result == obj


# ---------------------------------------------------------------------------
# G. Atomic write: no .tmp remains after success
# ---------------------------------------------------------------------------

def test_G_atomic_write_no_tmp_remains(tmp_path):
    target = tmp_path / "sub" / "file.json"
    _write_json_atomic(target, {"key": "value"})

    assert target.exists()
    tmp = target.with_suffix(".tmp")
    assert not tmp.exists()


def test_G_atomic_write_creates_parent_dirs(tmp_path):
    target = tmp_path / "a" / "b" / "c" / "file.json"
    _write_json_atomic(target, {"x": 1})
    assert target.exists()


def test_G_atomic_write_content_is_valid_json(tmp_path):
    obj = {"trade_id": "LIVE-TEST", "qty": 0.08}
    target = tmp_path / "artifact.json"
    _write_json_atomic(target, obj)
    loaded = json.loads(target.read_text(encoding="utf-8"))
    assert loaded == obj


# ---------------------------------------------------------------------------
# A, B, C, D. write_entry_artifacts creates dirs and files
# ---------------------------------------------------------------------------

def test_A_base_output_dir_consumed(tmp_path):
    result = write_entry_artifacts(str(tmp_path), "live_test", _EXECUTION_RESULT, _BROKER_RESPONSE_OK)
    assert result["ok"] is True
    assert str(tmp_path) in result["paths"]["execution"]


def test_B_execution_and_broker_dirs_created(tmp_path):
    write_entry_artifacts(str(tmp_path), "live_test", _EXECUTION_RESULT, _BROKER_RESPONSE_OK)
    assert (tmp_path / "live_test" / "execution").is_dir()
    assert (tmp_path / "live_test" / "broker").is_dir()


def test_C_execution_artifact_written_with_correct_content(tmp_path):
    result = write_entry_artifacts(str(tmp_path), "live_test", _EXECUTION_RESULT, _BROKER_RESPONSE_OK)
    exec_path = Path(result["paths"]["execution"])
    assert exec_path.exists()
    loaded = json.loads(exec_path.read_text(encoding="utf-8"))
    assert loaded["trade_id"] == _EXECUTION_RESULT["trade_id"]
    assert loaded["market"] == "BNB-EUR"


def test_D_broker_artifact_secrets_stripped(tmp_path):
    broker_with_secrets = {
        **_BROKER_RESPONSE_OK,
        "api_key": "MY-SECRET-KEY",
        "api_secret": "MY-SECRET-SECRET",
    }
    result = write_entry_artifacts(str(tmp_path), "live_test", _EXECUTION_RESULT, broker_with_secrets)
    broker_path = Path(result["paths"]["broker"])
    content = broker_path.read_text(encoding="utf-8")
    assert "MY-SECRET-KEY" not in content
    assert "MY-SECRET-SECRET" not in content
    # Non-secret data is preserved
    assert "BTV-ORDER-001" in content


# ---------------------------------------------------------------------------
# E. Execution result survives process boundary (file → load → compare)
# ---------------------------------------------------------------------------

def test_E_execution_result_survives_process_boundary(tmp_path):
    """Write execution result to disk and read it back — proves file survives reload."""
    result = write_entry_artifacts(str(tmp_path), "live_test", _EXECUTION_RESULT, _BROKER_RESPONSE_OK)
    assert result["ok"] is True

    exec_path = Path(result["paths"]["execution"])
    loaded = json.loads(exec_path.read_text(encoding="utf-8"))

    assert loaded["trade_id"] == _EXECUTION_RESULT["trade_id"]
    assert loaded["entry_price"] == _EXECUTION_RESULT["entry_price"]
    assert loaded["broker_order_id_entry"] == _EXECUTION_RESULT["broker_order_id_entry"]
    assert loaded["execution_quality_flag"] == "OK"


# ---------------------------------------------------------------------------
# H. Persistence failure → ok=False, gate=J_PERSIST, execution_result preserved
# ---------------------------------------------------------------------------

def test_H_persistence_failure_returns_ok_false(tmp_path):
    """When the writer fails, execute_and_persist_live_order is fail-closed."""

    def _failing_writer(base_output_dir, lane, execution_result, broker_response):
        return {"ok": False, "reason": "disk full simulation"}

    result = execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
        _writer=_failing_writer,
    )

    assert result["ok"] is False
    assert result["gate"] == "J_PERSIST"
    assert "disk full simulation" in result["reason"]
    assert "ORDER_EXECUTED_PERSISTENCE_FAILED" in result["reason"]


def test_H_persistence_failure_preserves_execution_result(tmp_path):
    """execution_result is still returned even when persistence fails."""

    def _failing_writer(base_output_dir, lane, execution_result, broker_response):
        return {"ok": False, "reason": "network share unavailable"}

    result = execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
        _writer=_failing_writer,
    )

    assert result["ok"] is False
    assert result["execution_result"] is not None
    assert result["execution_result"]["trade_id"] is not None


# ---------------------------------------------------------------------------
# I. write_feedback_artifact
# ---------------------------------------------------------------------------

def test_I_feedback_artifact_written(tmp_path):
    feedback = {
        "trade_id": "LIVE-BNBEUR-EDGE3-LONG-20260413T100000",
        "realized_pnl_eur": 1.23,
        "exit_reason": "STRATEGY_EXIT",
    }
    result = write_feedback_artifact(str(tmp_path), "live_test", feedback)
    assert result["ok"] is True
    path = Path(result["paths"]["feedback"])
    assert path.exists()
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["trade_id"] == feedback["trade_id"]
    assert loaded["realized_pnl_eur"] == 1.23


def test_I_feedback_dir_created(tmp_path):
    feedback = {"trade_id": "LIVE-TEST-001"}
    write_feedback_artifact(str(tmp_path), "live_test", feedback)
    assert (tmp_path / "live_test" / "feedback").is_dir()


# ---------------------------------------------------------------------------
# J. write_memory_artifact
# ---------------------------------------------------------------------------

def test_J_memory_artifact_written(tmp_path):
    memory = {
        "entry_id": "MEM-BNBEUR-20260413",
        "strategy_key": "EDGE3",
        "pattern": "impulse_pullback",
    }
    result = write_memory_artifact(str(tmp_path), "live_test", memory)
    assert result["ok"] is True
    path = Path(result["paths"]["memory"])
    assert path.exists()
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["entry_id"] == memory["entry_id"]


def test_J_memory_uses_trade_id_when_no_entry_id(tmp_path):
    memory = {"trade_id": "LIVE-BNBEUR-EDGE3-LONG-20260413T100000", "data": "x"}
    result = write_memory_artifact(str(tmp_path), "live_test", memory)
    assert result["ok"] is True
    path = Path(result["paths"]["memory"])
    assert "LIVE" in path.name


# ---------------------------------------------------------------------------
# K. Regression: execute_first_live_order unaffected
# ---------------------------------------------------------------------------

def test_K_execute_first_live_order_returns_no_broker_response_key(tmp_path):
    """execute_first_live_order must not expose _broker_response in its return value."""
    result = execute_first_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    assert result["ok"] is True
    assert "_broker_response" not in result


def test_K_execute_first_live_order_writes_no_artifacts(tmp_path):
    """execute_first_live_order must not write any files to disk."""
    execute_first_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    # tmp_path should remain empty (or at most have dirs created by the test fixture itself)
    all_files = list(tmp_path.rglob("*.json"))
    assert all_files == [], f"execute_first_live_order must not write files: {all_files}"


# ---------------------------------------------------------------------------
# L. execute_and_persist_live_order ok=True includes artifacts
# ---------------------------------------------------------------------------

def test_L_execute_and_persist_returns_ok_true_with_artifacts(tmp_path):
    result = execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    assert result["ok"] is True
    assert "artifacts" in result
    assert "execution" in result["artifacts"]
    assert "broker" in result["artifacts"]


def test_L_artifacts_paths_point_to_real_files(tmp_path):
    result = execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    assert result["ok"] is True
    exec_path = Path(result["artifacts"]["execution"])
    broker_path = Path(result["artifacts"]["broker"])
    assert exec_path.exists(), f"execution artifact not found: {exec_path}"
    assert broker_path.exists(), f"broker artifact not found: {broker_path}"


def test_L_execution_result_in_return_and_on_disk(tmp_path):
    result = execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    assert result["ok"] is True
    on_disk = json.loads(Path(result["artifacts"]["execution"]).read_text(encoding="utf-8"))
    assert on_disk["trade_id"] == result["execution_result"]["trade_id"]


# ---------------------------------------------------------------------------
# M. Missing base_output_dir → graceful degradation (no error)
# ---------------------------------------------------------------------------

def test_M_missing_base_output_dir_succeeds_without_artifacts():
    """If base_output_dir is absent from live_lane_config, execution still succeeds."""
    cfg_without_dir = {k: v for k, v in _LANE_CFG_BASE.items() if k != "base_output_dir"}

    result = execute_and_persist_live_order(
        _INTAKE,
        cfg_without_dir,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    assert result["ok"] is True
    assert "artifacts" not in result


def test_M_empty_base_output_dir_succeeds_without_artifacts():
    cfg_empty_dir = {**_LANE_CFG_BASE, "base_output_dir": ""}

    result = execute_and_persist_live_order(
        _INTAKE,
        cfg_empty_dir,
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    assert result["ok"] is True
    assert "artifacts" not in result


# ---------------------------------------------------------------------------
# N. Regression: successful order still returns ok=True
# ---------------------------------------------------------------------------

def test_N_regression_successful_order_ok_true(tmp_path):
    result = execute_and_persist_live_order(
        _INTAKE,
        _lane_cfg(tmp_path),
        _MACRO_NORMAL,
        _AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    assert result["ok"] is True
    assert result["gate"] == "I_SCHEMA"
    assert result["execution_result"] is not None
    assert result["execution_result"]["market"] == "BNB-EUR"
