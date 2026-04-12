"""
AC-153: Tests for Live Execution Gate and explicit live_enabled control

Verifies:
  A. Default config (live_enabled=false) → blocked
  B. live_enabled missing → blocked
  C. live_enabled wrong type → blocked
  D. enabled=false → blocked
  E. live_enabled=false → blocked
  F. allow_broker_execution=false with live_enabled=true → blocked
  G. live_enabled=true + allow_broker_execution=true + NORMAL → allow
  H. FREEZE risk_state → blocked
  I. freeze_new_entries=true → blocked
  J. Invalid market → blocked
  K. Invalid strategy → blocked
  L. max_notional_eur > 50 → blocked
  M. max_positions != 1 → blocked
  N. allow_shared_state=true → blocked
  O. allow_paper_inputs=true → blocked
  P. Intake with operator_approved=false → blocked
  Q. Intake with operator_approved=true + valid → allow
  R. Runner output correct when blocked (LIVE_DISABLED)
  S. Runner output correct when gate ready (LIVE_GATE_READY)
  T. Gate never raises exceptions
  U. live_enabled=true without allow_broker_execution=true → blocked
  V. Repo default config is blocked (double-check)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_execution_gate import evaluate_live_execution_gate
from ant_colony.live.live_lane_runner import run, load_config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LANE_CFG_GATE_OPEN = {
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
    "base_output_dir": "C:\\Trading\\ANT_LIVE",
}

_MACRO_NORMAL = {
    "risk_state": "NORMAL",
    "reason": "",
    "freeze_new_entries": False,
    "updated_ts_utc": "",
}

_VALID_INTAKE = {
    "lane": "live_test",
    "market": "BNB-EUR",
    "strategy_key": "EDGE3",
    "position_side": "long",
    "order_side": "buy",
    "qty": 0.08,
    "intended_entry_price": 600.0,
    "order_type": "market",
    "max_notional_eur": 50,
    "allow_broker_execution": False,
    "risk_state": "NORMAL",
    "freeze_new_entries": False,
    "operator_approved": True,
    "ts_intake_utc": "2026-04-01T10:00:00Z",
}


def _cfg(**overrides) -> dict:
    r = dict(_LANE_CFG_GATE_OPEN)
    r.update(overrides)
    return r


def _macro(**overrides) -> dict:
    r = dict(_MACRO_NORMAL)
    r.update(overrides)
    return r


def _intake(**overrides) -> dict:
    r = dict(_VALID_INTAKE)
    r.update(overrides)
    return r


def _gate(cfg=None, macro=None, intake=None) -> dict:
    return evaluate_live_execution_gate(
        cfg if cfg is not None else _cfg(),
        macro if macro is not None else _macro(),
        intake,
    )


# ---------------------------------------------------------------------------
# A. Default config → blocked
# ---------------------------------------------------------------------------

class TestDefaultConfigBlocked:
    def test_default_config_blocked(self):
        cfg = load_config()
        result = evaluate_live_execution_gate(cfg, _macro())
        assert result["allow"] is False

    def test_default_config_live_enabled_false(self):
        cfg = load_config()
        assert cfg.get("live_enabled") is False

    def test_default_config_allow_broker_false(self):
        cfg = load_config()
        assert cfg.get("allow_broker_execution") is False


# ---------------------------------------------------------------------------
# B. live_enabled missing → blocked
# ---------------------------------------------------------------------------

class TestLiveEnabledMissing:
    def test_missing_live_enabled_blocked(self):
        c = dict(_LANE_CFG_GATE_OPEN)
        del c["live_enabled"]
        result = _gate(cfg=c)
        assert result["allow"] is False
        assert "live_enabled" in result["reason"].lower()


# ---------------------------------------------------------------------------
# C. live_enabled wrong type → blocked
# ---------------------------------------------------------------------------

class TestLiveEnabledWrongType:
    def test_string_blocked(self):
        assert _gate(cfg=_cfg(live_enabled="true"))["allow"] is False

    def test_int_blocked(self):
        assert _gate(cfg=_cfg(live_enabled=1))["allow"] is False

    def test_none_blocked(self):
        assert _gate(cfg=_cfg(live_enabled=None))["allow"] is False

    def test_wrong_type_reason_mentions_bool(self):
        result = _gate(cfg=_cfg(live_enabled="yes"))
        assert "bool" in result["reason"].lower()


# ---------------------------------------------------------------------------
# D. enabled=false → blocked
# ---------------------------------------------------------------------------

class TestEnabledFalse:
    def test_enabled_false_blocked(self):
        assert _gate(cfg=_cfg(enabled=False))["allow"] is False

    def test_enabled_false_reason(self):
        result = _gate(cfg=_cfg(enabled=False))
        assert "LANE_DISABLED" in result["reason"] or "disabled" in result["reason"].lower()


# ---------------------------------------------------------------------------
# E. live_enabled=false → blocked
# ---------------------------------------------------------------------------

class TestLiveEnabledFalse:
    def test_live_disabled_blocked(self):
        result = _gate(cfg=_cfg(live_enabled=False))
        assert result["allow"] is False

    def test_live_disabled_reason(self):
        result = _gate(cfg=_cfg(live_enabled=False))
        assert "LIVE_DISABLED" in result["reason"]

    def test_live_disabled_live_enabled_field_false(self):
        result = _gate(cfg=_cfg(live_enabled=False))
        assert result["live_enabled"] is False


# ---------------------------------------------------------------------------
# F. live_enabled=true + allow_broker_execution=false → blocked
# ---------------------------------------------------------------------------

class TestBrokerExecutionFalse:
    def test_live_true_broker_false_blocked(self):
        result = _gate(cfg=_cfg(live_enabled=True, allow_broker_execution=False))
        assert result["allow"] is False
        assert "BROKER_EXECUTION_DISABLED" in result["reason"]

    def test_live_enabled_true_in_result(self):
        result = _gate(cfg=_cfg(live_enabled=True, allow_broker_execution=False))
        assert result["live_enabled"] is True


# ---------------------------------------------------------------------------
# G. Full open gate → allow
# ---------------------------------------------------------------------------

class TestGateOpen:
    def test_gate_open_allow(self):
        assert _gate()["allow"] is True

    def test_gate_open_reason(self):
        assert _gate()["reason"] == "LIVE_EXECUTION_GATE_OPEN"

    def test_gate_open_live_enabled_true(self):
        assert _gate()["live_enabled"] is True

    def test_gate_open_allow_broker_true(self):
        assert _gate()["allow_broker_execution"] is True

    def test_gate_open_risk_state_normal(self):
        assert _gate()["risk_state"] == "NORMAL"

    def test_caution_state_gate_open(self):
        result = _gate(macro=_macro(risk_state="CAUTION"))
        assert result["allow"] is True
        assert result["risk_state"] == "CAUTION"


# ---------------------------------------------------------------------------
# H. FREEZE → blocked
# ---------------------------------------------------------------------------

class TestFreezeBlocked:
    def test_freeze_risk_state_blocked(self):
        result = _gate(macro=_macro(risk_state="FREEZE"))
        assert result["allow"] is False

    def test_freeze_reason_mentions_freeze(self):
        result = _gate(macro=_macro(risk_state="FREEZE"))
        assert "FREEZE" in result["reason"]

    def test_freeze_risk_state_in_result(self):
        result = _gate(macro=_macro(risk_state="FREEZE"))
        assert result["risk_state"] == "FREEZE"


# ---------------------------------------------------------------------------
# I. freeze_new_entries=true → blocked
# ---------------------------------------------------------------------------

class TestFreezeNewEntries:
    def test_freeze_flag_blocked(self):
        result = _gate(macro=_macro(freeze_new_entries=True))
        assert result["allow"] is False
        assert "freeze" in result["reason"].lower()


# ---------------------------------------------------------------------------
# J. Invalid market → blocked
# ---------------------------------------------------------------------------

class TestInvalidMarket:
    def test_wrong_market_blocked(self):
        assert _gate(cfg=_cfg(market="BTC-EUR"))["allow"] is False

    def test_market_reason(self):
        result = _gate(cfg=_cfg(market="BTC-EUR"))
        assert "market" in result["reason"].lower()


# ---------------------------------------------------------------------------
# K. Invalid strategy → blocked
# ---------------------------------------------------------------------------

class TestInvalidStrategy:
    def test_wrong_strategy_blocked(self):
        assert _gate(cfg=_cfg(strategy="RSI_SIMPLE"))["allow"] is False

    def test_strategy_reason(self):
        result = _gate(cfg=_cfg(strategy="RSI_SIMPLE"))
        assert "strategy" in result["reason"].lower()


# ---------------------------------------------------------------------------
# L. max_notional_eur > 50 → blocked
# ---------------------------------------------------------------------------

class TestNotionalBounds:
    def test_above_50_blocked(self):
        assert _gate(cfg=_cfg(max_notional_eur=51))["allow"] is False

    def test_exactly_50_allowed(self):
        assert _gate(cfg=_cfg(max_notional_eur=50))["allow"] is True

    def test_zero_blocked(self):
        assert _gate(cfg=_cfg(max_notional_eur=0))["allow"] is False


# ---------------------------------------------------------------------------
# M. max_positions != 1 → blocked
# ---------------------------------------------------------------------------

class TestMaxPositions:
    def test_two_positions_blocked(self):
        assert _gate(cfg=_cfg(max_positions=2))["allow"] is False

    def test_one_position_allowed(self):
        assert _gate(cfg=_cfg(max_positions=1))["allow"] is True


# ---------------------------------------------------------------------------
# N. allow_shared_state=true → blocked
# ---------------------------------------------------------------------------

class TestSharedState:
    def test_shared_state_true_blocked(self):
        assert _gate(cfg=_cfg(allow_shared_state=True))["allow"] is False


# ---------------------------------------------------------------------------
# O. allow_paper_inputs=true → blocked
# ---------------------------------------------------------------------------

class TestPaperInputs:
    def test_paper_inputs_true_blocked(self):
        assert _gate(cfg=_cfg(allow_paper_inputs=True))["allow"] is False


# ---------------------------------------------------------------------------
# P. Intake operator_approved=false → blocked
# ---------------------------------------------------------------------------

class TestIntakeOperatorNotApproved:
    def test_operator_not_approved_blocked(self):
        result = _gate(intake=_intake(operator_approved=False))
        assert result["allow"] is False
        assert "OPERATOR_NOT_APPROVED" in result["reason"]

    def test_operator_non_bool_blocked(self):
        result = _gate(intake=_intake(operator_approved="yes"))
        assert result["allow"] is False


# ---------------------------------------------------------------------------
# Q. Valid intake + operator_approved=true → allow
# ---------------------------------------------------------------------------

class TestIntakeApproved:
    def test_valid_intake_allow(self):
        result = _gate(intake=_intake(operator_approved=True))
        assert result["allow"] is True

    def test_no_intake_also_allow(self):
        # intake is optional — gate open without it too
        assert _gate(intake=None)["allow"] is True


# ---------------------------------------------------------------------------
# R. Runner blocked output (LIVE_DISABLED)
# ---------------------------------------------------------------------------

class TestRunnerBlockedOutput:
    def test_runner_blocked_default_config(self):
        cfg = load_config()
        result = run(config=cfg, macro_config=_macro())
        assert result["state"] == "BLOCKED"
        assert result["component"] == "live_lane_runner"

    def test_runner_blocked_has_live_enabled(self):
        result = run(config=load_config(), macro_config=_macro())
        assert "live_enabled" in result
        assert result["live_enabled"] is False

    def test_runner_blocked_has_allow_broker_execution(self):
        result = run(config=load_config(), macro_config=_macro())
        assert "allow_broker_execution" in result
        assert result["allow_broker_execution"] is False

    def test_runner_blocked_has_market_and_strategy(self):
        result = run(config=load_config(), macro_config=_macro())
        assert result["market"] == "BNB-EUR"
        assert result["strategy"] == "EDGE3"


# ---------------------------------------------------------------------------
# S. Runner LIVE_GATE_READY output
# ---------------------------------------------------------------------------

class TestRunnerGateReadyOutput:
    def test_runner_gate_ready(self):
        result = run(config=_cfg(), macro_config=_macro())
        assert result["state"] == "LIVE_GATE_READY"

    def test_runner_gate_ready_live_enabled(self):
        result = run(config=_cfg(), macro_config=_macro())
        assert result["live_enabled"] is True

    def test_runner_gate_ready_allow_broker(self):
        result = run(config=_cfg(), macro_config=_macro())
        assert result["allow_broker_execution"] is True

    def test_runner_gate_ready_risk_state(self):
        result = run(config=_cfg(), macro_config=_macro())
        assert result["risk_state"] == "NORMAL"

    def test_runner_gate_ready_note(self):
        result = run(config=_cfg(), macro_config=_macro())
        assert "no execution" in result["note"].lower()

    def test_runner_gate_ready_component(self):
        result = run(config=_cfg(), macro_config=_macro())
        assert result["component"] == "live_lane_runner"


# ---------------------------------------------------------------------------
# T. Gate never raises exceptions
# ---------------------------------------------------------------------------

class TestNoExceptions:
    @pytest.mark.parametrize("bad_cfg", [None, {}, 42, "x", []])
    def test_no_exception_bad_cfg(self, bad_cfg):
        result = evaluate_live_execution_gate(bad_cfg, _macro())
        assert isinstance(result, dict)
        assert result["allow"] is False

    @pytest.mark.parametrize("bad_macro", [None, {}, 42])
    def test_no_exception_bad_macro(self, bad_macro):
        result = evaluate_live_execution_gate(_cfg(), bad_macro)
        assert isinstance(result, dict)
        assert result["allow"] is False


# ---------------------------------------------------------------------------
# U. live_enabled=true without allow_broker_execution=true → blocked
# ---------------------------------------------------------------------------

class TestLiveEnabledWithoutBroker:
    def test_live_true_broker_not_true_blocked(self):
        result = _gate(cfg=_cfg(live_enabled=True, allow_broker_execution=False))
        assert result["allow"] is False

    def test_reason_broker_execution_disabled(self):
        result = _gate(cfg=_cfg(live_enabled=True, allow_broker_execution=False))
        assert "BROKER_EXECUTION_DISABLED" in result["reason"]


# ---------------------------------------------------------------------------
# V. Repo default: double-check both sentinel values are false
# ---------------------------------------------------------------------------

class TestRepoDefaultSentinels:
    def test_repo_default_live_enabled_false(self):
        assert load_config()["live_enabled"] is False

    def test_repo_default_allow_broker_false(self):
        assert load_config()["allow_broker_execution"] is False

    def test_repo_default_enabled_false(self):
        assert load_config()["enabled"] is False
