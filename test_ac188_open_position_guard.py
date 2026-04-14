"""
AC-188 Task 1: Open Position Guard in Live Execution Gate

Verifies:
  A. No execution artifacts → gate remains open (allow=True)
  B. Execution artifact present, no exit → OPEN_POSITION_EXISTS blocked
  C. Execution artifact present AND exit artifact present → gate open
  D. Execution for different market → gate open (not blocked)
  E. Execution for different strategy → gate open (not blocked)
  F. Unreadable execution artifact → fail-closed (OPEN_POSITION_EXISTS)
  G. Execution artifact missing broker_order_id_entry → fail-closed
  H. base_output_dir not in config → guard skipped (gate open)
  I. reason field == "OPEN_POSITION_EXISTS" when blocked
  J. Gate still respects all prior gate conditions (e.g. lane disabled)
  K. Multiple executions same pair, all have exits → gate open
  L. Multiple executions same pair, one missing exit → blocked
  M. Execution dir missing entirely → gate open
  N. Exit dir exists but wrong file → blocked (no matching exit)
  O. Gate never raises
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_execution_gate import evaluate_live_execution_gate, _check_open_position

_MACRO_NORMAL = {
    "risk_state": "NORMAL",
    "reason": "",
    "freeze_new_entries": False,
    "updated_ts_utc": "",
}


def _gate_open_cfg(tmp_path: Path) -> dict:
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


def _write_execution(tmp_path: Path, lane: str, trade_id: str,
                     market: str = "BNB-EUR", strategy_key: str = "EDGE3",
                     broker_order_id_entry: str = "BTV-ORDER-001") -> Path:
    exec_dir = tmp_path / lane / "execution"
    exec_dir.mkdir(parents=True, exist_ok=True)
    path = exec_dir / f"{trade_id}.json"
    path.write_text(json.dumps({
        "trade_id": trade_id,
        "market": market,
        "strategy_key": strategy_key,
        "broker_order_id_entry": broker_order_id_entry,
    }), encoding="utf-8")
    return path


def _write_exit(tmp_path: Path, lane: str, broker_order_id: str) -> Path:
    from re import compile, sub
    safe = sub(r"[^a-zA-Z0-9_\-]", "_", broker_order_id)
    exit_dir = tmp_path / lane / "exit"
    exit_dir.mkdir(parents=True, exist_ok=True)
    path = exit_dir / f"{safe}.json"
    path.write_text(json.dumps({"entry_order_id": broker_order_id, "ok": True}),
                    encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# A. No execution artifacts → gate open
# ---------------------------------------------------------------------------

def test_A_no_executions_gate_open(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
    assert result["allow"] is True


# ---------------------------------------------------------------------------
# B. Execution present, no exit → OPEN_POSITION_EXISTS
# ---------------------------------------------------------------------------

def test_B_open_position_blocks(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    _write_execution(tmp_path, "live_test", "TRADE-001")
    result = _check_open_position(cfg, "NORMAL")
    assert result is not None
    assert result["allow"] is False
    assert result["reason"] == "OPEN_POSITION_EXISTS"


# ---------------------------------------------------------------------------
# C. Execution AND matching exit → gate open
# ---------------------------------------------------------------------------

def test_C_execution_with_exit_gate_open(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    _write_execution(tmp_path, "live_test", "TRADE-001",
                     broker_order_id_entry="BTV-ORDER-001")
    _write_exit(tmp_path, "live_test", "BTV-ORDER-001")
    result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
    assert result["allow"] is True


# ---------------------------------------------------------------------------
# D. Execution for different market → gate open
# ---------------------------------------------------------------------------

def test_D_different_market_not_blocked(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    _write_execution(tmp_path, "live_test", "TRADE-OTHER",
                     market="ETH-EUR", strategy_key="EDGE3",
                     broker_order_id_entry="BTV-ORDER-ETH-001")
    result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
    assert result["allow"] is True


# ---------------------------------------------------------------------------
# E. Execution for different strategy → gate open
# ---------------------------------------------------------------------------

def test_E_different_strategy_not_blocked(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    _write_execution(tmp_path, "live_test", "TRADE-OTHER2",
                     market="BNB-EUR", strategy_key="OTHER_STRATEGY",
                     broker_order_id_entry="BTV-ORDER-002")
    result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
    assert result["allow"] is True


# ---------------------------------------------------------------------------
# F. Unreadable execution artifact → fail-closed
# ---------------------------------------------------------------------------

def test_F_unreadable_artifact_fail_closed(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    exec_dir = tmp_path / "live_test" / "execution"
    exec_dir.mkdir(parents=True, exist_ok=True)
    (exec_dir / "bad.json").write_text("{not valid json", encoding="utf-8")
    result = _check_open_position(cfg, "NORMAL")
    assert result is not None
    assert result["allow"] is False
    assert "OPEN_POSITION_EXISTS" in result["reason"]


# ---------------------------------------------------------------------------
# G. Execution artifact missing broker_order_id_entry → fail-closed
# ---------------------------------------------------------------------------

def test_G_missing_broker_order_id_fail_closed(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    exec_dir = tmp_path / "live_test" / "execution"
    exec_dir.mkdir(parents=True, exist_ok=True)
    (exec_dir / "t.json").write_text(json.dumps({
        "trade_id": "T1",
        "market": "BNB-EUR",
        "strategy_key": "EDGE3",
        # broker_order_id_entry intentionally missing
    }), encoding="utf-8")
    result = _check_open_position(cfg, "NORMAL")
    assert result is not None
    assert result["allow"] is False
    assert "OPEN_POSITION_EXISTS" in result["reason"]


# ---------------------------------------------------------------------------
# H. No base_output_dir → guard skipped, gate open
# ---------------------------------------------------------------------------

def test_H_no_base_output_dir_guard_skipped(tmp_path):
    cfg = {
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
        # base_output_dir intentionally absent
    }
    result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
    assert result["allow"] is True


# ---------------------------------------------------------------------------
# I. reason field is exactly "OPEN_POSITION_EXISTS"
# ---------------------------------------------------------------------------

def test_I_reason_exact_string(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    _write_execution(tmp_path, "live_test", "TRADE-001")
    result = _check_open_position(cfg, "NORMAL")
    assert result is not None
    assert result["reason"] == "OPEN_POSITION_EXISTS"


# ---------------------------------------------------------------------------
# J. Prior gate conditions still respected (lane disabled blocks before guard)
# ---------------------------------------------------------------------------

def test_J_prior_gates_respected(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    cfg["enabled"] = False
    _write_execution(tmp_path, "live_test", "TRADE-001")
    result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
    assert result["allow"] is False
    assert result["reason"] == "LANE_DISABLED"


# ---------------------------------------------------------------------------
# K. Multiple executions same pair, all have exits → gate open
# ---------------------------------------------------------------------------

def test_K_multiple_executions_all_closed(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    for i in range(3):
        _write_execution(tmp_path, "live_test", f"TRADE-{i:03d}",
                         broker_order_id_entry=f"BTV-ORDER-{i:03d}")
        _write_exit(tmp_path, "live_test", f"BTV-ORDER-{i:03d}")
    result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
    assert result["allow"] is True


# ---------------------------------------------------------------------------
# L. Multiple executions, one missing exit → blocked
# ---------------------------------------------------------------------------

def test_L_multiple_executions_one_open(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    # Two closed, one open
    for i in range(2):
        _write_execution(tmp_path, "live_test", f"TRADE-{i:03d}",
                         broker_order_id_entry=f"BTV-ORDER-{i:03d}")
        _write_exit(tmp_path, "live_test", f"BTV-ORDER-{i:03d}")
    _write_execution(tmp_path, "live_test", "TRADE-999",
                     broker_order_id_entry="BTV-ORDER-999")  # no exit
    result = _check_open_position(cfg, "NORMAL")
    assert result is not None
    assert result["allow"] is False
    assert result["reason"] == "OPEN_POSITION_EXISTS"


# ---------------------------------------------------------------------------
# M. Execution dir missing → gate open
# ---------------------------------------------------------------------------

def test_M_exec_dir_missing_gate_open(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    # Don't create any dirs
    result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
    assert result["allow"] is True


# ---------------------------------------------------------------------------
# N. Exit dir exists but no file for this execution → blocked
# ---------------------------------------------------------------------------

def test_N_exit_dir_exists_wrong_file(tmp_path):
    cfg = _gate_open_cfg(tmp_path)
    _write_execution(tmp_path, "live_test", "TRADE-001",
                     broker_order_id_entry="BTV-ORDER-001")
    # Write exit for a DIFFERENT order id
    _write_exit(tmp_path, "live_test", "BTV-ORDER-999")
    result = _check_open_position(cfg, "NORMAL")
    assert result is not None
    assert result["allow"] is False
    assert result["reason"] == "OPEN_POSITION_EXISTS"


# ---------------------------------------------------------------------------
# O. Gate never raises
# ---------------------------------------------------------------------------

def test_O_never_raises(tmp_path):
    for cfg in [
        {},
        None,
        {"base_output_dir": str(tmp_path)},
        _gate_open_cfg(tmp_path),
    ]:
        try:
            result = evaluate_live_execution_gate(cfg, _MACRO_NORMAL)
            assert isinstance(result, dict)
        except Exception as exc:
            pytest.fail(f"gate raised unexpectedly: {exc}")
