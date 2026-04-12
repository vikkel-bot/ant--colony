"""
AC-147: Tests for Macro Freeze Guard and runner integration

Verifies:
  A. NORMAL + freeze_new_entries=false → allow
  B. FREEZE → block
  C. CAUTION + freeze_new_entries=false → allow
  D. NORMAL + freeze_new_entries=true → block
  E. CAUTION + freeze_new_entries=true → block
  F. Invalid risk_state → fail-closed
  G. freeze_new_entries non-bool → fail-closed
  H. Missing risk_state → fail-closed
  I. Missing freeze_new_entries → fail-closed
  J. Empty config → fail-closed
  K. Guard return shape is correct
  L. Runner blocks when macro frozen
  M. Runner is READY at NORMAL with no freeze
  N. Runner stays BLOCKED by lane guard regardless of macro state
  O. Default config on disk is NORMAL + freeze=false → allow
  P. load_macro_config returns empty dict on missing file
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.macro_freeze_guard import check, load_macro_config
from ant_colony.live.live_lane_runner import run

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LANE_CFG_ENABLED = {
    "lane": "live_test",
    "enabled": True,
    "live_enabled": False,
    "market": "BNB-EUR",
    "strategy": "EDGE3",
    "max_notional_eur": 50,
    "max_positions": 1,
    "allow_broker_execution": False,
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


def _macro(**overrides) -> dict:
    c = dict(_MACRO_NORMAL)
    c.update(overrides)
    return c


# ---------------------------------------------------------------------------
# A. NORMAL + freeze_new_entries=false → allow
# ---------------------------------------------------------------------------

class TestNormalAllow:
    def test_normal_no_freeze_allow(self):
        g = check(_macro(risk_state="NORMAL", freeze_new_entries=False))
        assert g["allow"] is True

    def test_normal_allow_reason(self):
        g = check(_macro())
        assert "no freeze" in g["reason"].lower()

    def test_normal_allow_component(self):
        g = check(_macro())
        assert g["component"] == "macro_freeze_guard"

    def test_normal_allow_risk_state(self):
        g = check(_macro())
        assert g["risk_state"] == "NORMAL"


# ---------------------------------------------------------------------------
# B. FREEZE → block
# ---------------------------------------------------------------------------

class TestFreezeBlocks:
    def test_freeze_state_blocked(self):
        g = check(_macro(risk_state="FREEZE"))
        assert g["allow"] is False

    def test_freeze_state_risk_state_in_result(self):
        g = check(_macro(risk_state="FREEZE"))
        assert g["risk_state"] == "FREEZE"

    def test_freeze_state_reason_mentions_freeze(self):
        g = check(_macro(risk_state="FREEZE"))
        assert "freeze" in g["reason"].lower()

    def test_freeze_with_custom_reason(self):
        g = check(_macro(risk_state="FREEZE", reason="weekend risk event"))
        assert "weekend risk event" in g["reason"]


# ---------------------------------------------------------------------------
# C. CAUTION + freeze_new_entries=false → allow
# ---------------------------------------------------------------------------

class TestCautionAllow:
    def test_caution_no_freeze_allow(self):
        g = check(_macro(risk_state="CAUTION", freeze_new_entries=False))
        assert g["allow"] is True

    def test_caution_risk_state_in_result(self):
        g = check(_macro(risk_state="CAUTION", freeze_new_entries=False))
        assert g["risk_state"] == "CAUTION"


# ---------------------------------------------------------------------------
# D–E. freeze_new_entries=true → block (any risk_state)
# ---------------------------------------------------------------------------

class TestFreezeFlag:
    def test_normal_freeze_flag_true_blocked(self):
        g = check(_macro(risk_state="NORMAL", freeze_new_entries=True))
        assert g["allow"] is False

    def test_caution_freeze_flag_true_blocked(self):
        g = check(_macro(risk_state="CAUTION", freeze_new_entries=True))
        assert g["allow"] is False

    def test_freeze_flag_reason_mentions_freeze(self):
        g = check(_macro(risk_state="NORMAL", freeze_new_entries=True))
        assert "freeze" in g["reason"].lower()

    def test_freeze_flag_with_custom_reason(self):
        g = check(_macro(risk_state="NORMAL", freeze_new_entries=True, reason="ops halt"))
        assert "ops halt" in g["reason"]


# ---------------------------------------------------------------------------
# F. Invalid risk_state → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidRiskState:
    def test_unknown_string_blocked(self):
        g = check(_macro(risk_state="PANIC"))
        assert g["allow"] is False

    def test_none_risk_state_blocked(self):
        g = check(_macro(risk_state=None))
        assert g["allow"] is False

    def test_int_risk_state_blocked(self):
        g = check(_macro(risk_state=0))
        assert g["allow"] is False

    def test_invalid_reason_mentions_risk_state(self):
        g = check(_macro(risk_state="PANIC"))
        assert "risk_state" in g["reason"]


# ---------------------------------------------------------------------------
# G. freeze_new_entries non-bool → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidFreezeFlag:
    def test_string_freeze_blocked(self):
        g = check(_macro(freeze_new_entries="true"))
        assert g["allow"] is False

    def test_int_freeze_blocked(self):
        g = check(_macro(freeze_new_entries=1))
        assert g["allow"] is False

    def test_none_freeze_blocked(self):
        g = check(_macro(freeze_new_entries=None))
        assert g["allow"] is False

    def test_invalid_freeze_reason_mentions_bool(self):
        g = check(_macro(freeze_new_entries="yes"))
        assert "bool" in g["reason"].lower()


# ---------------------------------------------------------------------------
# H–J. Missing fields → fail-closed
# ---------------------------------------------------------------------------

class TestMissingFields:
    def test_missing_risk_state_blocked(self):
        c = dict(_MACRO_NORMAL)
        del c["risk_state"]
        g = check(c)
        assert g["allow"] is False

    def test_missing_freeze_new_entries_blocked(self):
        c = dict(_MACRO_NORMAL)
        del c["freeze_new_entries"]
        g = check(c)
        assert g["allow"] is False

    def test_empty_config_blocked(self):
        g = check({})
        assert g["allow"] is False


# ---------------------------------------------------------------------------
# K. Guard return shape is correct
# ---------------------------------------------------------------------------

class TestGuardShape:
    def test_allow_shape(self):
        g = check(_macro())
        for key in ("allow", "reason", "risk_state", "component"):
            assert key in g

    def test_block_shape(self):
        g = check(_macro(risk_state="FREEZE"))
        for key in ("allow", "reason", "risk_state", "component"):
            assert key in g

    def test_component_always_correct(self):
        assert check(_macro())["component"] == "macro_freeze_guard"
        assert check(_macro(risk_state="FREEZE"))["component"] == "macro_freeze_guard"


# ---------------------------------------------------------------------------
# L. Runner blocks when macro frozen
# ---------------------------------------------------------------------------

class TestRunnerMacroFreezeBlocks:
    def test_runner_blocked_by_freeze_state(self):
        result = run(
            config=dict(_LANE_CFG_ENABLED),
            macro_config=_macro(risk_state="FREEZE"),
        )
        assert result["state"] == "BLOCKED"
        assert result["reason"] == "MACRO_FREEZE_ACTIVE"

    def test_runner_blocked_reason_has_risk_state(self):
        result = run(
            config=dict(_LANE_CFG_ENABLED),
            macro_config=_macro(risk_state="FREEZE"),
        )
        assert result["risk_state"] == "FREEZE"

    def test_runner_blocked_by_freeze_flag(self):
        result = run(
            config=dict(_LANE_CFG_ENABLED),
            macro_config=_macro(risk_state="NORMAL", freeze_new_entries=True),
        )
        assert result["state"] == "BLOCKED"
        assert result["reason"] == "MACRO_FREEZE_ACTIVE"

    def test_runner_blocked_has_required_keys(self):
        result = run(
            config=dict(_LANE_CFG_ENABLED),
            macro_config=_macro(risk_state="FREEZE"),
        )
        for key in ("component", "lane", "state", "reason", "risk_state",
                    "market", "strategy"):
            assert key in result


# ---------------------------------------------------------------------------
# M. Runner is READY at NORMAL with no freeze
# ---------------------------------------------------------------------------

class TestRunnerReady:
    # AC-153: READY replaced by LIVE_GATE_READY. With live_enabled=False (safe
    # default in _LANE_CFG_ENABLED), runner correctly blocks at gate 4 with
    # LIVE_DISABLED. Macro guard is still exercised before gate 4.
    def test_runner_live_disabled_after_macro_ok(self):
        result = run(
            config=dict(_LANE_CFG_ENABLED),
            macro_config=_macro(),
        )
        assert result["state"] == "BLOCKED"
        assert "LIVE_DISABLED" in result["reason"]

    def test_runner_live_disabled_has_risk_state(self):
        result = run(
            config=dict(_LANE_CFG_ENABLED),
            macro_config=_macro(),
        )
        assert result["risk_state"] == "NORMAL"

    def test_runner_broker_execution_false(self):
        result = run(
            config=dict(_LANE_CFG_ENABLED),
            macro_config=_macro(),
        )
        assert result["allow_broker_execution"] is False

    def test_runner_caution_no_freeze_live_disabled(self):
        result = run(
            config=dict(_LANE_CFG_ENABLED),
            macro_config=_macro(risk_state="CAUTION"),
        )
        assert result["state"] == "BLOCKED"
        assert result["risk_state"] == "CAUTION"


# ---------------------------------------------------------------------------
# N. Lane guard still blocks regardless of macro state
# ---------------------------------------------------------------------------

class TestLaneGuardTakesPrecedence:
    def test_lane_guard_blocks_before_macro(self):
        bad_lane = dict(_LANE_CFG_ENABLED)
        bad_lane["allow_shared_state"] = True
        result = run(
            config=bad_lane,
            macro_config=_macro(),  # macro is fine
        )
        assert result["state"] == "BLOCKED"
        assert "allow_shared_state" in result["reason"]

    def test_disabled_lane_blocks_before_macro(self):
        disabled = dict(_LANE_CFG_ENABLED)
        disabled["enabled"] = False
        result = run(
            config=disabled,
            macro_config=_macro(),
        )
        assert result["state"] == "BLOCKED"
        assert "LANE_DISABLED" in result["reason"] or "disabled" in result["reason"].lower()


# ---------------------------------------------------------------------------
# O. Default config on disk → allow
# ---------------------------------------------------------------------------

class TestDefaultConfigOnDisk:
    def test_default_macro_config_allow(self):
        cfg = load_macro_config()
        g = check(cfg)
        assert g["allow"] is True

    def test_default_macro_config_is_normal(self):
        cfg = load_macro_config()
        assert cfg.get("risk_state") == "NORMAL"

    def test_default_macro_config_freeze_false(self):
        cfg = load_macro_config()
        assert cfg.get("freeze_new_entries") is False


# ---------------------------------------------------------------------------
# P. load_macro_config returns empty dict on missing file
# ---------------------------------------------------------------------------

class TestLoadMacroConfig:
    def test_missing_file_returns_empty_dict(self, tmp_path):
        cfg = load_macro_config(tmp_path / "nonexistent.json")
        assert isinstance(cfg, dict)
        assert cfg == {}

    def test_bad_json_returns_empty_dict(self, tmp_path):
        p = tmp_path / "bad.json"
        p.write_text("not json", encoding="utf-8")
        cfg = load_macro_config(p)
        assert cfg == {}
