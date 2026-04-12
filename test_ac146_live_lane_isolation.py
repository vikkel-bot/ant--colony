"""
AC-146: Tests for Live/Test Lane Isolation

Verifies:
  A. Config valid + enabled=false  → BLOCKED (disabled)
  B. Config valid + enabled=true   → READY
  C. Guard blocks allow_shared_state=true
  D. Guard blocks allow_paper_inputs=true
  E. Guard blocks allow_broker_execution=true
  F. Guard blocks max_notional_eur > 50
  G. Guard blocks max_notional_eur <= 0
  H. Guard blocks max_positions != 1
  I. Runner output is valid JSON with required keys
  J. Runner does not read from paper/simulation paths
  K. Runner does not import paper execution modules
  L. Guard blocks empty base_output_dir
  M. Guard blocks wrong market
  N. Guard blocks wrong strategy
  O. Missing enabled field → blocked
"""
from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ant_colony.live.live_lane_guard import validate
from ant_colony.live.live_lane_runner import load_config, run

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_CONFIG = {
    "lane": "live_test",
    "enabled": False,
    "market": "BNB-EUR",
    "strategy": "EDGE3",
    "max_notional_eur": 50,
    "max_positions": 1,
    "allow_broker_execution": False,
    "allow_shared_state": False,
    "allow_paper_inputs": False,
    "base_output_dir": "C:\\Trading\\ANT_LIVE",
}


def _cfg(**overrides) -> dict:
    c = dict(_VALID_CONFIG)
    c.update(overrides)
    return c


# ---------------------------------------------------------------------------
# A. Valid config, disabled → runner returns BLOCKED
# ---------------------------------------------------------------------------

class TestDisabledLane:
    def test_disabled_returns_blocked(self):
        result = run(_cfg(enabled=False))
        assert result["state"] == "BLOCKED"
        assert result["component"] == "live_lane_runner"
        assert result["lane"] == "live_test"

    def test_disabled_reason_mentions_disabled(self):
        result = run(_cfg(enabled=False))
        assert "disabled" in result["reason"].lower()


# ---------------------------------------------------------------------------
# B. Valid config, enabled=true → READY
# ---------------------------------------------------------------------------

class TestEnabledLane:
    def test_enabled_returns_ready(self):
        result = run(_cfg(enabled=True))
        assert result["state"] == "READY"
        assert result["allow_broker_execution"] is False
        assert result["note"] == "isolated lane only; no execution"

    def test_ready_has_required_keys(self):
        result = run(_cfg(enabled=True))
        for key in ("component", "lane", "state", "market", "strategy",
                    "allow_broker_execution", "note"):
            assert key in result, f"missing key: {key}"


# ---------------------------------------------------------------------------
# C. allow_shared_state=true → blocked
# ---------------------------------------------------------------------------

class TestSharedStateBlocked:
    def test_shared_state_true_blocked(self):
        g = validate(_cfg(allow_shared_state=True))
        assert g["allow"] is False
        assert "allow_shared_state" in g["reason"]


# ---------------------------------------------------------------------------
# D. allow_paper_inputs=true → blocked
# ---------------------------------------------------------------------------

class TestPaperInputsBlocked:
    def test_paper_inputs_true_blocked(self):
        g = validate(_cfg(allow_paper_inputs=True))
        assert g["allow"] is False
        assert "allow_paper_inputs" in g["reason"]


# ---------------------------------------------------------------------------
# E. allow_broker_execution=true → blocked
# ---------------------------------------------------------------------------

class TestBrokerExecutionBlocked:
    def test_broker_execution_true_blocked(self):
        g = validate(_cfg(allow_broker_execution=True))
        assert g["allow"] is False
        assert "allow_broker_execution" in g["reason"]


# ---------------------------------------------------------------------------
# F–G. max_notional_eur out of range → blocked
# ---------------------------------------------------------------------------

class TestNotionalBounds:
    def test_notional_above_50_blocked(self):
        g = validate(_cfg(max_notional_eur=51))
        assert g["allow"] is False

    def test_notional_exactly_50_allowed(self):
        g = validate(_cfg(max_notional_eur=50))
        assert g["allow"] is True

    def test_notional_zero_blocked(self):
        g = validate(_cfg(max_notional_eur=0))
        assert g["allow"] is False

    def test_notional_negative_blocked(self):
        g = validate(_cfg(max_notional_eur=-10))
        assert g["allow"] is False


# ---------------------------------------------------------------------------
# H. max_positions != 1 → blocked
# ---------------------------------------------------------------------------

class TestMaxPositions:
    def test_positions_two_blocked(self):
        g = validate(_cfg(max_positions=2))
        assert g["allow"] is False

    def test_positions_zero_blocked(self):
        g = validate(_cfg(max_positions=0))
        assert g["allow"] is False

    def test_positions_one_allowed(self):
        g = validate(_cfg(max_positions=1))
        assert g["allow"] is True


# ---------------------------------------------------------------------------
# I. Runner output is valid JSON with required keys
# ---------------------------------------------------------------------------

class TestRunnerOutput:
    def test_blocked_output_json_serialisable(self):
        result = run(_cfg(enabled=False))
        dumped = json.dumps(result)  # must not raise
        loaded = json.loads(dumped)
        assert loaded["state"] == "BLOCKED"

    def test_ready_output_json_serialisable(self):
        result = run(_cfg(enabled=True))
        dumped = json.dumps(result)
        loaded = json.loads(dumped)
        assert loaded["state"] == "READY"

    def test_blocked_required_keys(self):
        result = run(_cfg(enabled=False))
        for key in ("component", "lane", "state", "reason", "market", "strategy"):
            assert key in result


# ---------------------------------------------------------------------------
# J. Runner does not read paper/simulation paths
# ---------------------------------------------------------------------------

PAPER_PATH_MARKERS = (
    "ANT_OUT",
    "build_execution_bridge",
    "build_paper",
    "dry_run_ledger",
    "paper_runner",
    "paper_intent",
)


class TestNoPaperReads:
    def test_runner_source_has_no_paper_path_references(self):
        runner_src = Path(_REPO_ROOT / "ant_colony" / "live" / "live_lane_runner.py")
        text = runner_src.read_text(encoding="utf-8")
        for marker in PAPER_PATH_MARKERS:
            assert marker not in text, (
                f"live_lane_runner.py references paper artefact path: '{marker}'"
            )

    def test_guard_source_has_no_paper_path_references(self):
        guard_src = Path(_REPO_ROOT / "ant_colony" / "live" / "live_lane_guard.py")
        text = guard_src.read_text(encoding="utf-8")
        for marker in PAPER_PATH_MARKERS:
            assert marker not in text, (
                f"live_lane_guard.py references paper artefact path: '{marker}'"
            )


# ---------------------------------------------------------------------------
# K. Runner does not import paper execution modules
# ---------------------------------------------------------------------------

PAPER_MODULES = (
    "build_execution_bridge_paper_lite",
    "build_paper_runner_intake_lite",
    "build_paper_intent_pack_lite",
    "build_dry_run_ledger_lite",
    "build_promotion_gate_lite",
)


class TestNoPaperImports:
    def test_runner_imports_no_paper_modules(self):
        runner_src = Path(_REPO_ROOT / "ant_colony" / "live" / "live_lane_runner.py")
        text = runner_src.read_text(encoding="utf-8")
        for mod in PAPER_MODULES:
            assert mod not in text, (
                f"live_lane_runner.py imports paper module: '{mod}'"
            )

    def test_guard_imports_no_paper_modules(self):
        guard_src = Path(_REPO_ROOT / "ant_colony" / "live" / "live_lane_guard.py")
        text = guard_src.read_text(encoding="utf-8")
        for mod in PAPER_MODULES:
            assert mod not in text, (
                f"live_lane_guard.py imports paper module: '{mod}'"
            )


# ---------------------------------------------------------------------------
# L. Empty base_output_dir → blocked
# ---------------------------------------------------------------------------

class TestBaseOutputDir:
    def test_empty_string_blocked(self):
        g = validate(_cfg(base_output_dir=""))
        assert g["allow"] is False

    def test_whitespace_only_blocked(self):
        g = validate(_cfg(base_output_dir="   "))
        assert g["allow"] is False

    def test_valid_path_allowed(self):
        g = validate(_cfg(base_output_dir="C:\\Trading\\ANT_LIVE"))
        assert g["allow"] is True


# ---------------------------------------------------------------------------
# M. Wrong market → blocked
# ---------------------------------------------------------------------------

class TestMarketConstraint:
    def test_wrong_market_blocked(self):
        g = validate(_cfg(market="BTC-EUR"))
        assert g["allow"] is False
        assert "market" in g["reason"]

    def test_correct_market_allowed(self):
        g = validate(_cfg(market="BNB-EUR"))
        assert g["allow"] is True


# ---------------------------------------------------------------------------
# N. Wrong strategy → blocked
# ---------------------------------------------------------------------------

class TestStrategyConstraint:
    def test_wrong_strategy_blocked(self):
        g = validate(_cfg(strategy="RSI_SIMPLE"))
        assert g["allow"] is False
        assert "strategy" in g["reason"]

    def test_correct_strategy_allowed(self):
        g = validate(_cfg(strategy="EDGE3"))
        assert g["allow"] is True


# ---------------------------------------------------------------------------
# O. Missing enabled field → blocked
# ---------------------------------------------------------------------------

class TestMissingEnabled:
    def test_missing_enabled_blocked(self):
        c = dict(_VALID_CONFIG)
        del c["enabled"]
        g = validate(c)
        assert g["allow"] is False
        assert "enabled" in g["reason"]

    def test_non_bool_enabled_blocked(self):
        g = validate(_cfg(enabled="yes"))
        assert g["allow"] is False


# ---------------------------------------------------------------------------
# Load config from disk (smoke test)
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_default_config_loads(self):
        cfg = load_config()
        assert cfg.get("lane") == "live_test"
        assert cfg.get("enabled") is False

    def test_default_config_is_disabled(self):
        cfg = load_config()
        assert cfg["enabled"] is False

    def test_missing_config_returns_empty_dict(self, tmp_path):
        cfg = load_config(tmp_path / "nonexistent.json")
        assert isinstance(cfg, dict)
        assert cfg == {}
