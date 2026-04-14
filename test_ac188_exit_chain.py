"""
AC-188 Task 2: Exit Chain Connected to Live Lane Runner

Verifies:
  A. Runner with exit_intent in intake → state=EXIT_EXECUTED (mock adapter)
  B. exit_artifact written under {base_output_dir}/{lane}/exit/
  C. Exit artifact contains entry_order_id (for open-position guard cross-ref)
  D. Exit artifact contains ts_recorded_utc, ok=True
  E. Runner returns exit_artifact_dir field pointing to exit/ dir
  F. Failed exit (broker error) → state=BLOCKED, reason propagated
  G. Exit gate still respects lane-level gates (lane disabled → BLOCKED)
  H. write_exit_artifact creates correct directory and file
  I. write_exit_artifact returns ok=True and correct path
  J. write_exit_artifact keyed on entry_order_id
  K. write_exit_artifact never raises
  L. After exit artifact written, open-position guard allows new entry
  M. Runner without exit_intent takes entry path (existing behavior preserved)
  N. Runner fail-closed on malformed exit_intent
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_lane_runner import run
from ant_colony.live.live_artifact_writer import write_exit_artifact
from ant_colony.live.live_execution_gate import evaluate_live_execution_gate, _check_open_position
from ant_colony.live.bitvavo_live_exit_executor import _reset_exit_dedup_for_testing

_NOW = "2026-04-14T12:00:00Z"

_LIVE_CFG_OPEN = {
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

_VALID_EXIT_INTENT = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "order_side": "sell",
    "qty": 0.08,
    "exit_reason": "TP",
    "operator_approved": True,
    "entry_order_id": "BTV-ORDER-001",
    "entry_price": 600.0,
    "ts_intent_utc": _NOW,
}


class _MockAdapterOk:
    def place_order(self, order_request):
        return {
            "ok": True,
            "adapter": "bitvavo",
            "operation": "place_order",
            "ts_utc": _NOW,
            "data": {
                "market": "BNB-EUR",
                "order_id": "BTV-EXIT-002",
                "status": "filled",
                "side": "sell",
                "order_type": "market",
                "qty": 0.08,
                "raw": {
                    "orderId": "BTV-EXIT-002",
                    "market": "BNB-EUR",
                    "side": "sell",
                    "orderType": "market",
                    "status": "filled",
                    "amount": "0.08",
                    "filledAmount": "0.08",
                    "price": "620.0",
                    "created": 1735727400000,
                },
            },
            "error": None,
            "meta": {"latency_ms": 90, "attempts": 1, "rate_limited": False},
        }


class _MockAdapterError:
    def place_order(self, order_request):
        return {
            "ok": False,
            "adapter": "bitvavo",
            "operation": "place_order",
            "ts_utc": _NOW,
            "data": None,
            "error": {"type": "BROKER_REJECTED", "code": "205",
                      "message": "insufficient funds", "retryable": False},
            "meta": {"latency_ms": 80, "attempts": 1, "rate_limited": False},
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg_with_output(tmp_path: Path) -> dict:
    return {**_LIVE_CFG_OPEN, "base_output_dir": str(tmp_path)}


def _write_execution(tmp_path: Path, lane: str = "live_test",
                     broker_order_id: str = "BTV-ORDER-001") -> None:
    exec_dir = tmp_path / lane / "execution"
    exec_dir.mkdir(parents=True, exist_ok=True)
    (exec_dir / "TRADE001.json").write_text(json.dumps({
        "trade_id": "TRADE001",
        "market": "BNB-EUR",
        "strategy_key": "EDGE3",
        "broker_order_id_entry": broker_order_id,
    }), encoding="utf-8")


# ---------------------------------------------------------------------------
# A. Runner with exit_intent → EXIT_EXECUTED
# ---------------------------------------------------------------------------

def test_A_exit_intent_in_intake_produces_exit(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record={"exit_intent": _VALID_EXIT_INTENT},
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "EXIT_EXECUTED", f"unexpected state: {result}"


# ---------------------------------------------------------------------------
# B. Exit artifact written under exit/
# ---------------------------------------------------------------------------

def test_B_exit_artifact_written(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record={"exit_intent": _VALID_EXIT_INTENT},
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    exit_dir = tmp_path / "live_test" / "exit"
    assert exit_dir.exists()
    files = list(exit_dir.glob("*.json"))
    assert len(files) == 1


# ---------------------------------------------------------------------------
# C. Exit artifact contains entry_order_id
# ---------------------------------------------------------------------------

def test_C_exit_artifact_contains_entry_order_id(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record={"exit_intent": _VALID_EXIT_INTENT},
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    exit_dir = tmp_path / "live_test" / "exit"
    artifact = json.loads(list(exit_dir.glob("*.json"))[0].read_text(encoding="utf-8"))
    assert artifact["entry_order_id"] == "BTV-ORDER-001"


# ---------------------------------------------------------------------------
# D. Exit artifact contains ts_recorded_utc and ok=True
# ---------------------------------------------------------------------------

def test_D_exit_artifact_fields(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record={"exit_intent": _VALID_EXIT_INTENT},
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    exit_dir = tmp_path / "live_test" / "exit"
    artifact = json.loads(list(exit_dir.glob("*.json"))[0].read_text(encoding="utf-8"))
    assert "ts_recorded_utc" in artifact
    assert artifact["ok"] is True


# ---------------------------------------------------------------------------
# E. Runner returns exit_artifact_dir
# ---------------------------------------------------------------------------

def test_E_runner_returns_exit_artifact_dir(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record={"exit_intent": _VALID_EXIT_INTENT},
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert "exit_artifact_dir" in result
    assert "exit" in result["exit_artifact_dir"]


# ---------------------------------------------------------------------------
# F. Failed exit (broker error) → state=BLOCKED
# ---------------------------------------------------------------------------

def test_F_failed_exit_blocked(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record={"exit_intent": _VALID_EXIT_INTENT},
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterError(),
    )
    assert result["state"] == "BLOCKED"
    assert "BROKER_CALL_FAILED" in result.get("reason", "") or result.get("reason")


# ---------------------------------------------------------------------------
# G. Lane disabled → BLOCKED before exit attempt
# ---------------------------------------------------------------------------

def test_G_lane_disabled_blocked_before_exit(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = {**_LIVE_CFG_OPEN, "enabled": False, "base_output_dir": str(tmp_path)}
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record={"exit_intent": _VALID_EXIT_INTENT},
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "BLOCKED"
    # No exit artifact should be written
    exit_dir = tmp_path / "live_test" / "exit"
    assert not exit_dir.exists() or len(list(exit_dir.glob("*.json"))) == 0


# ---------------------------------------------------------------------------
# H. write_exit_artifact creates correct directory and file
# ---------------------------------------------------------------------------

def test_H_write_exit_artifact_creates_file(tmp_path):
    artifact = {
        "entry_order_id": "BTV-ORDER-001",
        "market": "BNB-EUR",
        "ok": True,
    }
    result = write_exit_artifact(str(tmp_path), "live_test", artifact)
    assert result["ok"] is True
    assert (tmp_path / "live_test" / "exit" / "BTV-ORDER-001.json").exists()


# ---------------------------------------------------------------------------
# I. write_exit_artifact returns ok=True and correct path
# ---------------------------------------------------------------------------

def test_I_write_exit_artifact_path(tmp_path):
    artifact = {"entry_order_id": "BTV-ORDER-002", "ok": True}
    result = write_exit_artifact(str(tmp_path), "live_test", artifact)
    assert result["ok"] is True
    assert "exit" in result["paths"]["exit"]
    assert "BTV-ORDER-002" in result["paths"]["exit"]


# ---------------------------------------------------------------------------
# J. write_exit_artifact keyed on entry_order_id
# ---------------------------------------------------------------------------

def test_J_write_exit_artifact_keyed_on_entry_order_id(tmp_path):
    artifact = {"entry_order_id": "MY-ORDER-XYZ", "ok": True}
    write_exit_artifact(str(tmp_path), "live_test", artifact)
    assert (tmp_path / "live_test" / "exit" / "MY-ORDER-XYZ.json").exists()


# ---------------------------------------------------------------------------
# K. write_exit_artifact never raises
# ---------------------------------------------------------------------------

def test_K_write_exit_artifact_no_raise():
    result = write_exit_artifact(r"Z:\nonexistent_ac188", "live_test", {})
    assert isinstance(result, dict)
    assert "ok" in result


# ---------------------------------------------------------------------------
# L. After exit written, open-position guard allows new entry
# ---------------------------------------------------------------------------

def test_L_guard_allows_after_exit_written(tmp_path):
    cfg = {**_LIVE_CFG_OPEN, "base_output_dir": str(tmp_path)}
    # Write an execution artifact
    _write_execution(tmp_path, broker_order_id="BTV-ORDER-001")
    # Guard should block (open position exists)
    result = _check_open_position(cfg, "NORMAL")
    assert result is not None
    assert result["allow"] is False

    # Now write the exit artifact
    write_exit_artifact(str(tmp_path), "live_test", {
        "entry_order_id": "BTV-ORDER-001",
        "ok": True,
    })
    # Guard should now allow (position closed)
    result = _check_open_position(cfg, "NORMAL")
    assert result is None


# ---------------------------------------------------------------------------
# M. Runner without exit_intent takes entry path (existing behavior)
# ---------------------------------------------------------------------------

def test_M_no_exit_intent_uses_entry_path(tmp_path):
    """Without exit_intent, runner should not produce EXIT_EXECUTED state."""
    cfg = _cfg_with_output(tmp_path)
    # No intake_record → LIVE_GATE_READY (no execution attempted)
    result = run(config=cfg, macro_config=_MACRO_NORMAL)
    assert result["state"] == "LIVE_GATE_READY"
    assert result.get("exit_artifact_dir") is None


# ---------------------------------------------------------------------------
# N. Malformed exit_intent → BLOCKED
# ---------------------------------------------------------------------------

def test_N_malformed_exit_intent_blocked(tmp_path):
    _reset_exit_dedup_for_testing()
    cfg = _cfg_with_output(tmp_path)
    result = run(
        config=cfg,
        macro_config=_MACRO_NORMAL,
        intake_record={"exit_intent": {"bad": "data"}},
        auto_freeze_result=_AUTO_FREEZE_CLEAR,
        _adapter=_MockAdapterOk(),
    )
    assert result["state"] == "BLOCKED"
