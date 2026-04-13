"""
AC-168: Tests for live_lane_runner wired into persistent execution path.

Verifies:
  A. runner calls execute_and_persist_live_order when gates are open + intake provided
  B. base_output_dir from live_lane_config is consumed through runner path
  C. artifacts are written through the real runner flow (files exist on disk)
  D. blocked gate (LANE_DISABLED) does not reach execution
  E. blocked gate (MACRO_FREEZE_ACTIVE) does not reach execution
  F. blocked gate (LIVE_DISABLED) does not reach execution
  G. blocked gate (BROKER_EXECUTION_DISABLED) does not reach execution
  H. execution failure → state=BLOCKED, reason contains failure detail
  I. no intake_record → state=LIVE_GATE_READY (legacy behavior preserved)
  J. state=EXECUTED on successful run-through
  K. execution_result in runner return when state=EXECUTED
  L. artifacts key present in runner return when state=EXECUTED
  M. auto_freeze_result=FREEZE blocks before broker call
  N. runner return always has component=live_lane_runner
  O. Regression: existing AC-153 tests still pass (gate-only, no intake)
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

from ant_colony.live.live_lane_runner import run

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_NOW = "2026-04-13T10:00:00Z"

_LANE_CFG_OPEN = {
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
    "base_output_dir": "C:\\Trading\\ANT_LIVE",  # required by live_lane_guard; only consumed when intake_record is given
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

_AUTO_FREEZE_ACTIVE = {
    "allow": False,
    "reason": "extreme absolute move: 12.00% >= 10%",
    "risk_state": "FREEZE",
    "freeze_new_entries": True,
}

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

_BROKER_RESPONSE_OK = {
    "ok": True,
    "adapter": "bitvavo",
    "operation": "place_order",
    "ts_utc": _NOW,
    "data": {
        "market": "BNB-EUR",
        "order_id": "BTV-ORDER-168",
        "status": "filled",
        "side": "buy",
        "order_type": "market",
        "qty": 0.08,
        "raw": {
            "orderId": "BTV-ORDER-168",
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
            "error": {"type": "BROKER_REJECTED", "code": "205", "message": "operatorId parameter is required.", "retryable": False},
            "meta": {},
        }


def _lane_cfg(tmp_path):
    return {**_LANE_CFG_OPEN, "base_output_dir": str(tmp_path)}


def _lane_cfg_no_dir():
    return dict(_LANE_CFG_OPEN)


# ---------------------------------------------------------------------------
# A. runner calls persistent execution path when gates open + intake provided
# ---------------------------------------------------------------------------

def test_A_runner_calls_execution_when_gates_open_and_intake_provided(tmp_path):
    """When all gates are open and intake_record is supplied, execution is called."""
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "EXECUTED"


# ---------------------------------------------------------------------------
# B. base_output_dir consumed through runner path
# ---------------------------------------------------------------------------

def test_B_base_output_dir_consumed_through_runner(tmp_path):
    """base_output_dir from the lane config reaches the artifact writer."""
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "EXECUTED"
    # artifacts paths must start with tmp_path (proves config was consumed)
    exec_path = result["artifacts"]["execution"]
    assert str(tmp_path) in exec_path


# ---------------------------------------------------------------------------
# C. artifacts are written through the real runner flow
# ---------------------------------------------------------------------------

def test_C_artifacts_written_to_disk_through_runner(tmp_path):
    """Both execution and broker artifact files exist on disk after runner call."""
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    assert result["state"] == "EXECUTED"
    exec_path = Path(result["artifacts"]["execution"])
    broker_path = Path(result["artifacts"]["broker"])
    assert exec_path.exists(), f"execution artifact missing: {exec_path}"
    assert broker_path.exists(), f"broker artifact missing: {broker_path}"


def test_C_execution_artifact_readable_json(tmp_path):
    """Execution artifact is valid JSON with expected trade fields."""
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )

    loaded = json.loads(Path(result["artifacts"]["execution"]).read_text(encoding="utf-8"))
    assert loaded["market"] == "BNB-EUR"
    assert loaded["trade_id"].startswith("LIVE-")


# ---------------------------------------------------------------------------
# D. LANE_DISABLED blocks before execution
# ---------------------------------------------------------------------------

def test_D_lane_disabled_does_not_execute(tmp_path):
    adapter = _MockAdapterOk()
    result = run(
        config={**_lane_cfg(tmp_path), "enabled": False},
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=adapter,
    )
    assert result["state"] == "BLOCKED"
    assert result["reason"] == "LANE_DISABLED"
    # no artifacts created
    assert not list(tmp_path.rglob("*.json"))


# ---------------------------------------------------------------------------
# E. MACRO_FREEZE_ACTIVE blocks before execution
# ---------------------------------------------------------------------------

def test_E_macro_freeze_blocks_before_execution(tmp_path):
    frozen_macro = {
        "risk_state": "FREEZE",
        "reason": "manual freeze",
        "freeze_new_entries": True,
        "updated_ts_utc": "",
    }
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=frozen_macro,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "BLOCKED"
    assert result["reason"] == "MACRO_FREEZE_ACTIVE"
    assert not list(tmp_path.rglob("*.json"))


# ---------------------------------------------------------------------------
# F. LIVE_DISABLED blocks before execution
# ---------------------------------------------------------------------------

def test_F_live_disabled_blocks_before_execution(tmp_path):
    result = run(
        config={**_lane_cfg(tmp_path), "live_enabled": False},
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "BLOCKED"
    assert "LIVE_DISABLED" in result["reason"]
    assert not list(tmp_path.rglob("*.json"))


# ---------------------------------------------------------------------------
# G. BROKER_EXECUTION_DISABLED blocks before execution
# ---------------------------------------------------------------------------

def test_G_broker_execution_disabled_blocks(tmp_path):
    result = run(
        config={**_lane_cfg(tmp_path), "allow_broker_execution": False},
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "BLOCKED"
    assert not list(tmp_path.rglob("*.json"))


# ---------------------------------------------------------------------------
# H. Execution failure → state=BLOCKED with detail
# ---------------------------------------------------------------------------

def test_H_execution_failure_returns_blocked_state(tmp_path):
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterFail(),
    )
    assert result["state"] == "BLOCKED"
    assert "reason" in result
    assert len(result["reason"]) > 0


def test_H_execution_failure_preserves_gate_info(tmp_path):
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterFail(),
    )
    assert result["state"] == "BLOCKED"
    assert "gate" in result


# ---------------------------------------------------------------------------
# I. No intake_record → LIVE_GATE_READY (legacy behavior preserved)
# ---------------------------------------------------------------------------

def test_I_no_intake_record_returns_gate_ready():
    result = run(
        config=_LANE_CFG_OPEN,
        macro_config=_MACRO_NORMAL,
        # intake_record not provided
    )
    assert result["state"] == "LIVE_GATE_READY"


def test_I_gate_ready_has_legacy_note():
    result = run(
        config=_LANE_CFG_OPEN,
        macro_config=_MACRO_NORMAL,
    )
    assert "no execution" in result.get("note", "").lower()


def test_I_gate_ready_no_artifacts_key():
    result = run(
        config=_LANE_CFG_OPEN,
        macro_config=_MACRO_NORMAL,
    )
    assert "artifacts" not in result
    assert "execution_result" not in result


# ---------------------------------------------------------------------------
# J. state=EXECUTED on success
# ---------------------------------------------------------------------------

def test_J_state_executed_on_success(tmp_path):
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "EXECUTED"


# ---------------------------------------------------------------------------
# K. execution_result present when state=EXECUTED
# ---------------------------------------------------------------------------

def test_K_execution_result_in_return(tmp_path):
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "EXECUTED"
    er = result["execution_result"]
    assert er is not None
    assert er["market"] == "BNB-EUR"
    assert er["trade_id"].startswith("LIVE-")


# ---------------------------------------------------------------------------
# L. artifacts key present when state=EXECUTED
# ---------------------------------------------------------------------------

def test_L_artifacts_key_present_on_executed(tmp_path):
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert "artifacts" in result
    assert "execution" in result["artifacts"]
    assert "broker" in result["artifacts"]


# ---------------------------------------------------------------------------
# M. auto_freeze_result FREEZE blocks before broker call
# ---------------------------------------------------------------------------

def test_M_auto_freeze_active_blocks_execution(tmp_path):
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_ACTIVE,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "BLOCKED"
    assert not list(tmp_path.rglob("*.json"))


# ---------------------------------------------------------------------------
# N. component=live_lane_runner always present
# ---------------------------------------------------------------------------

def test_N_component_always_live_lane_runner_on_blocked(tmp_path):
    result = run(
        config={**_lane_cfg(tmp_path), "enabled": False},
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
    )
    assert result["component"] == "live_lane_runner"


def test_N_component_always_live_lane_runner_on_executed(tmp_path):
    result = run(
        config=_lane_cfg(tmp_path),
        macro_config=_MACRO_NORMAL,
        intake_record=_INTAKE,
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["component"] == "live_lane_runner"


def test_N_component_always_live_lane_runner_on_gate_ready():
    result = run(
        config=_LANE_CFG_OPEN,
        macro_config=_MACRO_NORMAL,
    )
    assert result["component"] == "live_lane_runner"


# ---------------------------------------------------------------------------
# O. Regression: existing gate-only tests (no intake) still behave correctly
# ---------------------------------------------------------------------------

def test_O_regression_blocked_lane_has_correct_shape():
    """Matches the contract tested in AC-153 TestRunnerBlockedOutput."""
    # Use enabled=False so the runner blocks at Gate 2 (allow_broker_execution=False guaranteed)
    cfg = {**_LANE_CFG_OPEN, "enabled": False}
    result = run(config=cfg, macro_config=_MACRO_NORMAL)
    assert result["state"] == "BLOCKED"
    assert result["component"] == "live_lane_runner"
    assert result["allow_broker_execution"] is False
    assert result["market"] == "BNB-EUR"
    assert result["strategy"] == "EDGE3"


def test_O_regression_gate_ready_full_shape():
    """Matches the contract tested in AC-153 TestRunnerGateReadyOutput."""
    result = run(config=_LANE_CFG_OPEN, macro_config=_MACRO_NORMAL)
    assert result["state"] == "LIVE_GATE_READY"
    assert result["live_enabled"] is True
    assert result["allow_broker_execution"] is True
    assert result["risk_state"] == "NORMAL"
    assert result["component"] == "live_lane_runner"
    assert "note" in result
