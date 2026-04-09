"""
AC-82: Queen Capital Allocator (Simulation-Only) — tests

Covers:
  A. Capital consistency: allocated + unallocated == total_equity
  B. Multi-strategy allocation → correct EUR amounts
  C. Baseline-hold → zero capital allocated, all unallocated
  D. Equal market fractions when not provided
  E. Custom market_capital_fractions (normalized if needed)
  F. Invalid splits_result → fail-closed zero result
  G. Invalid/negative equity → fail-closed
  H. simulation_only and non_binding always True
  I. allocated_capital_by_strategy sums across markets
  J. All required output fields present
  K. No mutation of inputs
  L. build_capital_allocation_from_specs convenience wrapper
  M. Empty market list → EMPTY mode
"""
import copy
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load modules
# ---------------------------------------------------------------------------
_ALLOC_PATH = (
    Path(__file__).parent / "ant_colony" / "build_queen_capital_allocator_lite.py"
)
_SPLITS_PATH = (
    Path(__file__).parent / "ant_colony" / "build_allocation_split_simulation_lite.py"
)


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_a  = _load(_ALLOC_PATH,  "_alloc")
_sp = _load(_SPLITS_PATH, "_splits")

build_capital_allocation           = _a.build_capital_allocation
build_capital_allocation_from_specs = _a.build_capital_allocation_from_specs
build_allocation_splits            = _sp.build_allocation_splits
ALLOCATION_REASON_CODES            = _a.ALLOCATION_REASON_CODES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spec(market, strategies):
    return {"market": market, "strategies": strategies}


def _strat(sid, family="WORKER", weight=None):
    s = {"strategy_id": sid, "strategy_family": family}
    if weight is not None:
        s["weight_fraction"] = weight
    return s


def _splits(specs, equity=0.0):
    return build_allocation_splits(specs, total_equity_eur=equity)


_REQUIRED_SUMMARY_FIELDS = {
    "allocated_capital_total", "unallocated_capital", "total_equity_eur",
    "allocation_mode", "allocation_reason", "allocation_reason_code",
}

_REQUIRED_TOP_FIELDS = {
    "allocation_summary", "allocated_capital_by_market",
    "allocated_capital_by_strategy", "market_allocations",
    "simulation_only", "non_binding",
}

_REQUIRED_MARKET_ALLOC_FIELDS = {
    "market", "market_capital_eur", "market_capital_fraction",
    "market_split_mode", "market_split_valid", "strategy_allocations",
}

_REQUIRED_STRATEGY_ALLOC_FIELDS = {
    "strategy_id", "strategy_family", "capital_eur",
    "simulated_weight", "split_reason_code",
}


# ---------------------------------------------------------------------------
# A. Capital consistency: allocated + unallocated == total_equity
# ---------------------------------------------------------------------------

class TestCapitalConsistency:
    def _result(self, equity=10_000.0, mcf=None):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)]),
            _spec("ETH-EUR", [_strat("EDGE3")]),
        ]
        sr = _splits(specs)
        return build_capital_allocation(sr, equity, mcf)

    def _check_consistency(self, r, equity):
        s = r["allocation_summary"]
        total = s["allocated_capital_total"] + s["unallocated_capital"]
        assert abs(total - equity) < 0.01, f"consistency failed: {total} != {equity}"

    def test_two_valid_markets_consistent(self):
        self._check_consistency(self._result(), 10_000.0)

    def test_zero_equity_consistent(self):
        self._check_consistency(self._result(equity=0.0), 0.0)

    def test_large_equity_consistent(self):
        self._check_consistency(self._result(equity=1_000_000.0), 1_000_000.0)

    def test_custom_fractions_consistent(self):
        r = self._result(mcf={"BTC-EUR": 0.7, "ETH-EUR": 0.3})
        self._check_consistency(r, 10_000.0)

    def test_sum_by_market_equals_total(self):
        r = self._result(equity=10_000.0)
        s = r["allocation_summary"]
        market_sum = sum(r["allocated_capital_by_market"].values())
        assert abs(market_sum - s["allocated_capital_total"]) < 0.01


# ---------------------------------------------------------------------------
# B. Multi-strategy allocation amounts
# ---------------------------------------------------------------------------

class TestMultiStrategyAmounts:
    def _result(self, mcf=None):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])]
        sr = _splits(specs)
        return build_capital_allocation(sr, 10_000.0, mcf)

    def test_mode_multi_strategy(self):
        r = self._result()
        assert r["allocation_summary"]["allocation_mode"] == "MULTI_STRATEGY"

    def test_btc_gets_all_capital_with_one_market(self):
        r = self._result()
        # Single valid market → gets 100% of equity
        assert abs(r["allocated_capital_by_market"]["BTC-EUR"] - 10_000.0) < 0.01

    def test_edge3_gets_60pct(self):
        r = self._result()
        assert abs(r["allocated_capital_by_strategy"]["EDGE3"] - 6_000.0) < 0.01

    def test_edge4_gets_40pct(self):
        r = self._result()
        assert abs(r["allocated_capital_by_strategy"]["EDGE4"] - 4_000.0) < 0.01

    def test_unallocated_is_zero_full_weight(self):
        r = self._result()
        assert abs(r["allocation_summary"]["unallocated_capital"]) < 0.01

    def test_strategy_allocations_populated(self):
        r = self._result()
        ma = r["market_allocations"][0]
        assert len(ma["strategy_allocations"]) == 2

    def test_all_strategy_alloc_fields(self):
        r = self._result()
        for sa in r["market_allocations"][0]["strategy_allocations"]:
            for f in _REQUIRED_STRATEGY_ALLOC_FIELDS:
                assert f in sa, f"Missing strategy alloc field: {f}"


# ---------------------------------------------------------------------------
# C. Baseline-hold → zero capital
# ---------------------------------------------------------------------------

class TestBaselineHoldZeroCapital:
    def _result(self):
        specs = [_spec("BTC-EUR", []), _spec("ETH-EUR", [])]
        sr = _splits(specs)
        return build_capital_allocation(sr, 10_000.0)

    def test_mode_baseline_hold(self):
        r = self._result()
        assert r["allocation_summary"]["allocation_mode"] == "BASELINE_HOLD"

    def test_allocated_total_zero(self):
        r = self._result()
        assert r["allocation_summary"]["allocated_capital_total"] == pytest.approx(0.0)

    def test_unallocated_equals_equity(self):
        r = self._result()
        assert abs(r["allocation_summary"]["unallocated_capital"] - 10_000.0) < 0.01

    def test_by_market_all_zero(self):
        r = self._result()
        for v in r["allocated_capital_by_market"].values():
            assert v == pytest.approx(0.0)

    def test_by_strategy_empty(self):
        r = self._result()
        assert r["allocated_capital_by_strategy"] == {}


# ---------------------------------------------------------------------------
# D. Equal market fractions when not provided
# ---------------------------------------------------------------------------

class TestEqualMarketFractions:
    def _result(self):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=1.0)]),
            _spec("ETH-EUR", [_strat("EDGE4", weight=1.0)]),
        ]
        sr = _splits(specs)
        return build_capital_allocation(sr, 10_000.0)  # no mcf

    def test_equal_split_50_50(self):
        r = self._result()
        btc = r["allocated_capital_by_market"]["BTC-EUR"]
        eth = r["allocated_capital_by_market"]["ETH-EUR"]
        assert abs(btc - 5_000.0) < 0.01
        assert abs(eth - 5_000.0) < 0.01

    def test_total_allocated_equals_equity(self):
        r = self._result()
        assert abs(r["allocation_summary"]["allocated_capital_total"] - 10_000.0) < 0.01


# ---------------------------------------------------------------------------
# E. Custom market_capital_fractions
# ---------------------------------------------------------------------------

class TestCustomMarketFractions:
    def _specs(self):
        return [
            _spec("BTC-EUR", [_strat("EDGE3", weight=1.0)]),
            _spec("ETH-EUR", [_strat("EDGE4", weight=1.0)]),
        ]

    def test_custom_70_30(self):
        sr = _splits(self._specs())
        r = build_capital_allocation(sr, 10_000.0, {"BTC-EUR": 0.7, "ETH-EUR": 0.3})
        assert abs(r["allocated_capital_by_market"]["BTC-EUR"] - 7_000.0) < 0.01
        assert abs(r["allocated_capital_by_market"]["ETH-EUR"] - 3_000.0) < 0.01

    def test_unnormalized_fractions_normalized(self):
        # Provide fractions summing to 2.0 → normalized to 0.5/0.5
        sr = _splits(self._specs())
        r = build_capital_allocation(sr, 10_000.0, {"BTC-EUR": 1.0, "ETH-EUR": 1.0})
        btc = r["allocated_capital_by_market"]["BTC-EUR"]
        eth = r["allocated_capital_by_market"]["ETH-EUR"]
        assert abs(btc - eth) < 0.01  # equal after normalization

    def test_missing_market_in_fractions_gets_zero(self):
        # Only BTC provided → ETH gets 0
        sr = _splits(self._specs())
        r = build_capital_allocation(sr, 10_000.0, {"BTC-EUR": 1.0})
        assert r["allocated_capital_by_market"].get("ETH-EUR", 0.0) == pytest.approx(0.0)

    def test_all_zero_fractions_falls_back_to_equal(self):
        sr = _splits(self._specs())
        r = build_capital_allocation(sr, 10_000.0, {"BTC-EUR": 0.0, "ETH-EUR": 0.0})
        btc = r["allocated_capital_by_market"]["BTC-EUR"]
        eth = r["allocated_capital_by_market"]["ETH-EUR"]
        assert abs(btc - eth) < 0.01


# ---------------------------------------------------------------------------
# F. Invalid splits_result → fail-closed
# ---------------------------------------------------------------------------

class TestInvalidSplitsResult:
    @pytest.mark.parametrize("bad", [None, "string", 42, [], {}])
    def test_invalid_gives_zero_result(self, bad):
        r = build_capital_allocation(bad, 10_000.0)
        assert r["simulation_only"] is True
        assert r["non_binding"]     is True
        assert r["allocation_summary"]["allocated_capital_total"] == pytest.approx(0.0)
        assert r["allocation_summary"]["allocation_mode"] == "BASELINE_HOLD"

    def test_missing_market_splits_key(self):
        r = build_capital_allocation({"other_key": []}, 10_000.0)
        assert r["allocation_summary"]["allocated_capital_total"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# G. Invalid equity
# ---------------------------------------------------------------------------

class TestInvalidEquity:
    def _sr(self):
        return _splits([_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])])

    def test_negative_equity(self):
        r = build_capital_allocation(self._sr(), -500.0)
        assert r["allocation_summary"]["allocation_reason_code"] == "CAPITAL_INVALID_EQUITY"
        assert r["simulation_only"] is True

    def test_none_equity(self):
        r = build_capital_allocation(self._sr(), None)
        assert r["simulation_only"] is True

    def test_string_equity(self):
        r = build_capital_allocation(self._sr(), "ten_thousand")
        assert r["simulation_only"] is True

    def test_zero_equity_valid(self):
        r = build_capital_allocation(self._sr(), 0.0)
        # Zero equity is valid — everything is unallocated
        assert r["allocation_summary"]["total_equity_eur"] == pytest.approx(0.0)
        assert r["simulation_only"] is True


# ---------------------------------------------------------------------------
# H. simulation_only and non_binding always True
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    _cases = [
        lambda: build_capital_allocation(
            build_allocation_splits([_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]),
            10_000.0
        ),
        lambda: build_capital_allocation(
            build_allocation_splits([_spec("BTC-EUR", [])]),
            10_000.0
        ),
        lambda: build_capital_allocation(None, 10_000.0),
        lambda: build_capital_allocation({}, 10_000.0),
    ]

    def test_simulation_only_always_true(self):
        for fn in self._cases:
            assert fn()["simulation_only"] is True

    def test_non_binding_always_true(self):
        for fn in self._cases:
            assert fn()["non_binding"] is True


# ---------------------------------------------------------------------------
# I. allocated_capital_by_strategy sums across markets
# ---------------------------------------------------------------------------

class TestStrategyCapitalAggregation:
    def test_same_strategy_two_markets_summed(self):
        # EDGE3 in both BTC and ETH — capital should sum
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=1.0)]),
            _spec("ETH-EUR", [_strat("EDGE3", weight=1.0)]),
        ]
        sr = _splits(specs)
        r  = build_capital_allocation(sr, 10_000.0)  # equal split → 5k each
        # EDGE3 should get 5000 + 5000 = 10000
        assert abs(r["allocated_capital_by_strategy"]["EDGE3"] - 10_000.0) < 0.01

    def test_different_strategies_separate_entries(self):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)]),
        ]
        sr = _splits(specs)
        r  = build_capital_allocation(sr, 10_000.0)
        assert "EDGE3" in r["allocated_capital_by_strategy"]
        assert "EDGE4" in r["allocated_capital_by_strategy"]
        assert r["allocated_capital_by_strategy"]["EDGE3"] != r["allocated_capital_by_strategy"]["EDGE4"]


# ---------------------------------------------------------------------------
# J. All required fields
# ---------------------------------------------------------------------------

class TestRequiredFields:
    def _result(self):
        sr = _splits([_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])])
        return build_capital_allocation(sr, 1000.0)

    def test_top_level_fields(self):
        r = self._result()
        for f in _REQUIRED_TOP_FIELDS:
            assert f in r, f"Missing top field: {f}"

    def test_summary_fields(self):
        r = self._result()
        for f in _REQUIRED_SUMMARY_FIELDS:
            assert f in r["allocation_summary"], f"Missing summary field: {f}"

    def test_market_alloc_fields(self):
        r = self._result()
        for ma in r["market_allocations"]:
            for f in _REQUIRED_MARKET_ALLOC_FIELDS:
                assert f in ma, f"Missing market alloc field: {f}"

    def test_zero_result_has_required_fields(self):
        r = build_capital_allocation(None, 1000.0)
        for f in _REQUIRED_TOP_FIELDS:
            assert f in r

    def test_empty_result_has_required_fields(self):
        r = build_capital_allocation({"market_splits": []}, 1000.0)
        for f in _REQUIRED_TOP_FIELDS:
            assert f in r


# ---------------------------------------------------------------------------
# K. No mutation of inputs
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_splits_result_not_mutated(self):
        sr = _splits([_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])])
        original = copy.deepcopy(sr)
        build_capital_allocation(sr, 10_000.0)
        assert sr == original

    def test_market_fractions_not_mutated(self):
        sr  = _splits([_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])])
        mcf = {"BTC-EUR": 1.0}
        original = copy.deepcopy(mcf)
        build_capital_allocation(sr, 10_000.0, mcf)
        assert mcf == original


# ---------------------------------------------------------------------------
# L. build_capital_allocation_from_specs convenience wrapper
# ---------------------------------------------------------------------------

class TestConvenienceWrapper:
    def test_result_has_both_keys(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        r = build_capital_allocation_from_specs(specs, 10_000.0)
        assert "splits_result"      in r
        assert "capital_allocation" in r

    def test_simulation_only_in_both_layers(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        r = build_capital_allocation_from_specs(specs, 10_000.0)
        assert r["splits_result"]["simulation_only"]      is True
        assert r["capital_allocation"]["simulation_only"] is True

    def test_capital_consistency_via_wrapper(self):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)]),
            _spec("ETH-EUR", [_strat("EDGE3")]),
        ]
        r    = build_capital_allocation_from_specs(specs, 10_000.0)
        ca   = r["capital_allocation"]["allocation_summary"]
        total = ca["allocated_capital_total"] + ca["unallocated_capital"]
        assert abs(total - 10_000.0) < 0.01

    def test_invalid_specs_returns_baseline(self):
        r = build_capital_allocation_from_specs(None, 10_000.0)
        assert r["capital_allocation"]["allocation_summary"]["allocated_capital_total"] == pytest.approx(0.0)

    def test_custom_fractions_via_wrapper(self):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=1.0)]),
            _spec("ETH-EUR", [_strat("EDGE4", weight=1.0)]),
        ]
        r = build_capital_allocation_from_specs(
            specs, 10_000.0,
            market_capital_fractions={"BTC-EUR": 0.8, "ETH-EUR": 0.2}
        )
        ca = r["capital_allocation"]
        assert abs(ca["allocated_capital_by_market"]["BTC-EUR"] - 8_000.0) < 0.01
        assert abs(ca["allocated_capital_by_market"]["ETH-EUR"] - 2_000.0) < 0.01


# ---------------------------------------------------------------------------
# M. Empty market list → EMPTY mode
# ---------------------------------------------------------------------------

class TestEmptyMode:
    def test_empty_splits_gives_empty_mode(self):
        sr = _splits([])
        r  = build_capital_allocation(sr, 10_000.0)
        assert r["allocation_summary"]["allocation_mode"] == "EMPTY"
        assert r["allocation_summary"]["allocated_capital_total"] == pytest.approx(0.0)
        assert abs(r["allocation_summary"]["unallocated_capital"] - 10_000.0) < 0.01
