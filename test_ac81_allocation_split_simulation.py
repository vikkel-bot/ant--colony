"""
AC-81: Multi-Strategy Allocation Splits (Simulation-Only) — tests

Covers:
  A. Explicit weights — EXPLICIT mode, correct per-strategy splits
  B. No weights provided — EQUAL mode
  C. Weights sum > 1.0 — NORMALIZED mode
  D. Empty / no strategies — BASELINE mode (fail-closed)
  E. Invalid market_specs input — fail-closed baseline result
  F. simulation_only and non_binding always True
  G. Total weights consistent per market and across markets
  H. simulated_notional correct when equity provided
  I. All required per-split fields present
  J. All required split_summary fields present
  K. No mutation of inputs
  L. Mixed valid/invalid markets in one call
  M. split_mode summary labels correct
"""
import copy
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load module
# ---------------------------------------------------------------------------
_MOD_PATH = (
    Path(__file__).parent
    / "ant_colony"
    / "build_allocation_split_simulation_lite.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("_splits", _MOD_PATH)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_m = _load()

build_allocation_splits = _m.build_allocation_splits
SPLIT_REASON_CODES      = _m.SPLIT_REASON_CODES
SPLIT_MODE_EXPLICIT     = _m.SPLIT_MODE_EXPLICIT
SPLIT_MODE_NORMALIZED   = _m.SPLIT_MODE_NORMALIZED
SPLIT_MODE_EQUAL        = _m.SPLIT_MODE_EQUAL
SPLIT_MODE_BASELINE     = _m.SPLIT_MODE_BASELINE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spec(market, strategies):
    return {"market": market, "strategies": strategies}


def _strat(strategy_id, family="WORKER", weight=None):
    s = {"strategy_id": strategy_id, "strategy_family": family}
    if weight is not None:
        s["weight_fraction"] = weight
    return s


_REQUIRED_SPLIT_FIELDS = {
    "strategy_id", "strategy_family",
    "simulated_weight", "simulated_notional",
    "split_reason", "split_reason_code", "split_confidence",
}

_REQUIRED_SUMMARY_FIELDS = {
    "total_markets", "total_markets_split",
    "total_strategies_active", "total_weight_assigned", "split_mode",
}

_REQUIRED_MARKET_FIELDS = {
    "market", "splits",
    "market_total_weight", "market_split_mode", "market_split_valid",
}

_REQUIRED_TOP_FIELDS = {
    "split_summary", "market_splits", "simulation_only", "non_binding",
}


# ---------------------------------------------------------------------------
# A. Explicit weights
# ---------------------------------------------------------------------------

class TestExplicitWeights:
    def _result(self):
        specs = [_spec("BTC-EUR", [
            _strat("EDGE3", weight=0.6),
            _strat("EDGE4", weight=0.4),
        ])]
        return build_allocation_splits(specs)

    def test_split_mode_explicit(self):
        ms = self._result()["market_splits"][0]
        assert ms["market_split_mode"] == SPLIT_MODE_EXPLICIT

    def test_two_splits_produced(self):
        ms = self._result()["market_splits"][0]
        assert len(ms["splits"]) == 2

    def test_weights_match_input(self):
        splits = self._result()["market_splits"][0]["splits"]
        by_id = {s["strategy_id"]: s for s in splits}
        assert abs(by_id["EDGE3"]["simulated_weight"] - 0.6) < 1e-6
        assert abs(by_id["EDGE4"]["simulated_weight"] - 0.4) < 1e-6

    def test_total_weight_is_one(self):
        ms = self._result()["market_splits"][0]
        assert abs(ms["market_total_weight"] - 1.0) < 1e-6

    def test_market_split_valid_true(self):
        assert self._result()["market_splits"][0]["market_split_valid"] is True

    def test_reason_code_explicit(self):
        splits = self._result()["market_splits"][0]["splits"]
        for s in splits:
            assert s["split_reason_code"] == "SPLIT_EXPLICIT_WEIGHT"

    def test_confidence_one(self):
        splits = self._result()["market_splits"][0]["splits"]
        for s in splits:
            assert s["split_confidence"] == pytest.approx(1.0)

    def test_all_required_split_fields(self):
        for s in self._result()["market_splits"][0]["splits"]:
            for f in _REQUIRED_SPLIT_FIELDS:
                assert f in s, f"Missing split field: {f}"


# ---------------------------------------------------------------------------
# B. No weights → EQUAL mode
# ---------------------------------------------------------------------------

class TestEqualSplit:
    def _result(self):
        specs = [_spec("ETH-EUR", [
            _strat("EDGE3"),
            _strat("EDGE4"),
            _strat("EDGE5"),
        ])]
        return build_allocation_splits(specs)

    def test_split_mode_equal(self):
        assert self._result()["market_splits"][0]["market_split_mode"] == SPLIT_MODE_EQUAL

    def test_three_splits(self):
        assert len(self._result()["market_splits"][0]["splits"]) == 3

    def test_equal_weights(self):
        splits = self._result()["market_splits"][0]["splits"]
        for s in splits:
            assert abs(s["simulated_weight"] - 1/3) < 1e-6

    def test_total_weight_one(self):
        ms = self._result()["market_splits"][0]
        assert abs(ms["market_total_weight"] - 1.0) < 1e-6

    def test_reason_code_equal(self):
        splits = self._result()["market_splits"][0]["splits"]
        for s in splits:
            assert s["split_reason_code"] == "SPLIT_EQUAL_WEIGHT"

    def test_confidence_lower_than_explicit(self):
        splits = self._result()["market_splits"][0]["splits"]
        for s in splits:
            assert s["split_confidence"] < 1.0


# ---------------------------------------------------------------------------
# C. Weights sum > 1.0 → NORMALIZED mode
# ---------------------------------------------------------------------------

class TestNormalizedWeights:
    def _result(self):
        specs = [_spec("SOL-EUR", [
            _strat("EDGE3", weight=1.5),
            _strat("EDGE4", weight=0.5),
        ])]
        return build_allocation_splits(specs)

    def test_split_mode_normalized(self):
        ms = self._result()["market_splits"][0]
        assert ms["market_split_mode"] == SPLIT_MODE_NORMALIZED

    def test_weights_normalized_to_one(self):
        ms = self._result()["market_splits"][0]
        assert abs(ms["market_total_weight"] - 1.0) < 1e-5

    def test_weights_proportional(self):
        splits = self._result()["market_splits"][0]["splits"]
        by_id = {s["strategy_id"]: s for s in splits}
        # 1.5/(1.5+0.5)=0.75, 0.5/2.0=0.25
        assert abs(by_id["EDGE3"]["simulated_weight"] - 0.75) < 1e-6
        assert abs(by_id["EDGE4"]["simulated_weight"] - 0.25) < 1e-6

    def test_reason_code_normalized(self):
        splits = self._result()["market_splits"][0]["splits"]
        for s in splits:
            assert s["split_reason_code"] == "SPLIT_NORMALIZED_WEIGHT"

    def test_confidence_between_equal_and_explicit(self):
        splits = self._result()["market_splits"][0]["splits"]
        for s in splits:
            # NORMALIZED confidence is 0.8 (between EQUAL=0.6 and EXPLICIT=1.0)
            assert s["split_confidence"] == pytest.approx(0.8)


# ---------------------------------------------------------------------------
# D. Empty / no strategies → BASELINE mode
# ---------------------------------------------------------------------------

class TestBaselineFallback:
    def test_empty_strategies_list(self):
        result = build_allocation_splits([_spec("XRP-EUR", [])])
        ms = result["market_splits"][0]
        assert ms["market_split_mode"] == SPLIT_MODE_BASELINE
        assert ms["market_split_valid"] is False
        assert ms["market_total_weight"] == pytest.approx(0.0)
        assert len(ms["splits"]) == 1
        assert ms["splits"][0]["strategy_id"] == "BASELINE"

    def test_baseline_split_confidence_zero(self):
        result = build_allocation_splits([_spec("ADA-EUR", [])])
        assert result["market_splits"][0]["splits"][0]["split_confidence"] == pytest.approx(0.0)

    def test_baseline_reason_code(self):
        result = build_allocation_splits([_spec("ADA-EUR", [])])
        assert result["market_splits"][0]["splits"][0]["split_reason_code"] == "SPLIT_BASELINE_HOLD"

    def test_null_strategies(self):
        result = build_allocation_splits([{"market": "BNB-EUR", "strategies": None}])
        ms = result["market_splits"][0]
        assert ms["market_split_mode"] == SPLIT_MODE_BASELINE

    def test_no_valid_strategy_ids(self):
        specs = [_spec("BTC-EUR", [{"strategy_family": "WORKER"}])]  # no strategy_id
        result = build_allocation_splits(specs)
        ms = result["market_splits"][0]
        assert ms["market_split_mode"] == SPLIT_MODE_BASELINE


# ---------------------------------------------------------------------------
# E. Invalid market_specs input
# ---------------------------------------------------------------------------

class TestInvalidInput:
    @pytest.mark.parametrize("bad", [None, "string", 42, {}])
    def test_non_list_gives_baseline_result(self, bad):
        result = build_allocation_splits(bad)
        assert result["simulation_only"] is True
        assert result["non_binding"] is True
        assert result["split_summary"]["total_markets"] == 0
        assert result["split_summary"]["split_mode"] == "BASELINE_HOLD"

    def test_non_dict_spec_in_list(self):
        result = build_allocation_splits(["not_a_dict"])
        ms = result["market_splits"][0]
        assert ms["market_split_mode"] == SPLIT_MODE_BASELINE

    def test_empty_list_is_valid(self):
        result = build_allocation_splits([])
        assert result["split_summary"]["total_markets"] == 0
        assert result["simulation_only"] is True


# ---------------------------------------------------------------------------
# F. simulation_only and non_binding always True
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    _inputs = [
        lambda: build_allocation_splits([]),
        lambda: build_allocation_splits(None),
        lambda: build_allocation_splits([_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]),
        lambda: build_allocation_splits([_spec("BTC-EUR", [])]),
    ]

    def test_simulation_only_always_true(self):
        for fn in self._inputs:
            assert fn()["simulation_only"] is True

    def test_non_binding_always_true(self):
        for fn in self._inputs:
            assert fn()["non_binding"] is True


# ---------------------------------------------------------------------------
# G. Weight consistency
# ---------------------------------------------------------------------------

class TestWeightConsistency:
    def test_single_strategy_explicit_weight_one(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        ms = build_allocation_splits(specs)["market_splits"][0]
        assert abs(ms["market_total_weight"] - 1.0) < 1e-6

    def test_single_strategy_no_weight_equal_one(self):
        specs = [_spec("ETH-EUR", [_strat("EDGE3")])]
        ms = build_allocation_splits(specs)["market_splits"][0]
        assert abs(ms["market_total_weight"] - 1.0) < 1e-6

    def test_partial_weight_less_than_one(self):
        # Only 0.7 total explicit — not > 1.0, so EXPLICIT mode, total=0.7
        specs = [_spec("BTC-EUR", [
            _strat("EDGE3", weight=0.4),
            _strat("EDGE4", weight=0.3),
        ])]
        ms = build_allocation_splits(specs)["market_splits"][0]
        assert ms["market_split_mode"] == SPLIT_MODE_EXPLICIT
        assert abs(ms["market_total_weight"] - 0.7) < 1e-6

    def test_total_weight_assigned_sums_across_markets(self):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)]),
            _spec("ETH-EUR", [_strat("EDGE3", weight=1.0)]),
        ]
        result = build_allocation_splits(specs)
        expected = 1.0 + 1.0
        assert abs(result["split_summary"]["total_weight_assigned"] - expected) < 1e-5

    def test_normalized_total_weight_is_one(self):
        specs = [_spec("BTC-EUR", [
            _strat("EDGE3", weight=3.0),
            _strat("EDGE4", weight=2.0),
        ])]
        ms = build_allocation_splits(specs)["market_splits"][0]
        assert abs(ms["market_total_weight"] - 1.0) < 1e-5


# ---------------------------------------------------------------------------
# H. simulated_notional
# ---------------------------------------------------------------------------

class TestSimulatedNotional:
    def test_notional_correct_with_equity(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])]
        result = build_allocation_splits(specs, total_equity_eur=10_000.0)
        splits = result["market_splits"][0]["splits"]
        by_id = {s["strategy_id"]: s for s in splits}
        assert abs(by_id["EDGE3"]["simulated_notional"] - 6000.0) < 0.01
        assert abs(by_id["EDGE4"]["simulated_notional"] - 4000.0) < 0.01

    def test_notional_zero_when_no_equity(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        result = build_allocation_splits(specs, total_equity_eur=0.0)
        assert result["market_splits"][0]["splits"][0]["simulated_notional"] == pytest.approx(0.0)

    def test_notional_equal_split_with_equity(self):
        specs = [_spec("ETH-EUR", [_strat("EDGE3"), _strat("EDGE4")])]
        result = build_allocation_splits(specs, total_equity_eur=1000.0)
        splits = result["market_splits"][0]["splits"]
        for s in splits:
            assert abs(s["simulated_notional"] - 500.0) < 0.01


# ---------------------------------------------------------------------------
# I. All required per-split fields
# ---------------------------------------------------------------------------

class TestRequiredSplitFields:
    def _splits(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        return build_allocation_splits(specs)["market_splits"][0]["splits"]

    def test_all_fields_present(self):
        for s in self._splits():
            for f in _REQUIRED_SPLIT_FIELDS:
                assert f in s, f"Missing: {f}"

    def test_baseline_split_has_all_fields(self):
        result = build_allocation_splits([_spec("BTC-EUR", [])])
        for s in result["market_splits"][0]["splits"]:
            for f in _REQUIRED_SPLIT_FIELDS:
                assert f in s, f"Missing in baseline: {f}"


# ---------------------------------------------------------------------------
# J. All required summary and top-level fields
# ---------------------------------------------------------------------------

class TestRequiredSummaryFields:
    def _result(self):
        return build_allocation_splits([_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])])

    def test_top_level_fields(self):
        r = self._result()
        for f in _REQUIRED_TOP_FIELDS:
            assert f in r, f"Missing top-level field: {f}"

    def test_summary_fields(self):
        r = self._result()
        for f in _REQUIRED_SUMMARY_FIELDS:
            assert f in r["split_summary"], f"Missing summary field: {f}"

    def test_market_fields(self):
        r = self._result()
        for ms in r["market_splits"]:
            for f in _REQUIRED_MARKET_FIELDS:
                assert f in ms, f"Missing market field: {f}"


# ---------------------------------------------------------------------------
# K. No mutation of inputs
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_specs_not_mutated(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])]
        original = copy.deepcopy(specs)
        build_allocation_splits(specs)
        assert specs == original

    def test_strategy_dict_not_mutated(self):
        strat = _strat("EDGE3", weight=0.7)
        specs = [_spec("BTC-EUR", [strat])]
        original_strat = copy.deepcopy(strat)
        build_allocation_splits(specs)
        assert strat == original_strat


# ---------------------------------------------------------------------------
# L. Mixed valid/invalid markets
# ---------------------------------------------------------------------------

class TestMixedMarkets:
    def _result(self):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)]),
            _spec("ETH-EUR", []),
            _spec("SOL-EUR", [_strat("EDGE3")]),
        ]
        return build_allocation_splits(specs)

    def test_three_market_entries(self):
        assert len(self._result()["market_splits"]) == 3

    def test_btc_valid_eth_invalid(self):
        mss = {ms["market"]: ms for ms in self._result()["market_splits"]}
        assert mss["BTC-EUR"]["market_split_valid"] is True
        assert mss["ETH-EUR"]["market_split_valid"] is False
        assert mss["SOL-EUR"]["market_split_valid"] is True

    def test_summary_counts_only_valid(self):
        r = self._result()
        assert r["split_summary"]["total_markets_split"] == 2  # BTC and SOL
        assert r["split_summary"]["total_markets"] == 3

    def test_top_split_mode_mixed(self):
        r = self._result()
        assert r["split_summary"]["split_mode"] == "MIXED"

    def test_strategies_active_excludes_baseline(self):
        r = self._result()
        # BTC: 2 strategies, SOL: 1 strategy
        assert r["split_summary"]["total_strategies_active"] == 3


# ---------------------------------------------------------------------------
# M. split_mode summary labels
# ---------------------------------------------------------------------------

class TestSplitModeLabels:
    def test_all_valid_gives_multi_strategy(self):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=1.0)]),
            _spec("ETH-EUR", [_strat("EDGE3", weight=1.0)]),
        ]
        r = build_allocation_splits(specs)
        assert r["split_summary"]["split_mode"] == "MULTI_STRATEGY"

    def test_all_invalid_gives_baseline_hold(self):
        specs = [_spec("BTC-EUR", []), _spec("ETH-EUR", [])]
        r = build_allocation_splits(specs)
        assert r["split_summary"]["split_mode"] == "BASELINE_HOLD"

    def test_empty_list_gives_baseline_hold(self):
        r = build_allocation_splits([])
        assert r["split_summary"]["split_mode"] == "BASELINE_HOLD"

    def test_mixed_gives_mixed(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)]), _spec("ETH-EUR", [])]
        r = build_allocation_splits(specs)
        assert r["split_summary"]["split_mode"] == "MIXED"
