"""
AC-83: Execution Bridge (Paper-Only, Live-Ready Boundary) — tests

Covers:
  A. Valid allocation → BRIDGE_ACTIVE with ALLOWED intents
  B. Baseline-hold allocation → BRIDGE_BASELINE, no intents
  C. Zero-capital strategy → BLOCKED intent (ZERO_CAPITAL)
  D. BASELINE placeholder strategy → BLOCKED (BASELINE_PLACEHOLDER)
  E. market_split_valid=False → BLOCKED (MARKET_INVALID)
  F. Invalid allocation input → BRIDGE_BLOCKED / PAPER_REJECTED
  G. paper_only always True
  H. live_activation_allowed always False
  I. blocked_reasons correctly populated
  J. intents_by_market and intents_by_strategy populated
  K. intent_count == allowed_count + blocked_count
  L. All required output fields present
  M. No mutation of inputs
  N. build_bridge_from_specs full pipeline
  O. Backward compat: AC-81/82 output unchanged
"""
import copy
import importlib.util
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load modules
# ---------------------------------------------------------------------------
_BRIDGE_PATH  = Path(__file__).parent / "ant_colony" / "build_execution_bridge_paper_lite.py"
_ALLOC_PATH   = Path(__file__).parent / "ant_colony" / "build_queen_capital_allocator_lite.py"
_SPLITS_PATH  = Path(__file__).parent / "ant_colony" / "build_allocation_split_simulation_lite.py"


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_br  = _load(_BRIDGE_PATH,  "_bridge")
_al  = _load(_ALLOC_PATH,   "_alloc")
_sp  = _load(_SPLITS_PATH,  "_splits")

build_execution_bridge      = _br.build_execution_bridge
build_bridge_from_specs     = _br.build_bridge_from_specs

build_capital_allocation    = _al.build_capital_allocation
build_allocation_splits     = _sp.build_allocation_splits

BRIDGE_ACTIVE   = _br.BRIDGE_ACTIVE
BRIDGE_BLOCKED  = _br.BRIDGE_BLOCKED
BRIDGE_BASELINE = _br.BRIDGE_BASELINE

INTENT_ALLOWED  = _br.INTENT_ALLOWED
INTENT_BLOCKED  = _br.INTENT_BLOCKED


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


def _alloc(specs, equity=10_000.0, mcf=None):
    sr = build_allocation_splits(specs, total_equity_eur=0.0)
    return build_capital_allocation(sr, equity, mcf)


_REQUIRED_BRIDGE_FIELDS = {
    "execution_bridge_status", "bridge_mode",
    "intent_count", "allowed_count", "blocked_count",
    "blocked_reasons", "intents_by_market", "intents_by_strategy",
    "paper_only", "live_activation_allowed",
}

_REQUIRED_INTENT_FIELDS = {
    "market", "strategy_id", "strategy_family",
    "intent_action", "intent_notional_eur", "intent_weight",
    "intent_status", "block_reason",
    "paper_only", "live_activation_allowed",
}


# ---------------------------------------------------------------------------
# A. Valid allocation → BRIDGE_ACTIVE
# ---------------------------------------------------------------------------

class TestBridgeActive:
    def _bridge(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])]
        return build_execution_bridge(_alloc(specs))

    def test_bridge_status_active(self):
        assert self._bridge()["execution_bridge_status"] == BRIDGE_ACTIVE

    def test_bridge_mode_paper_multi(self):
        assert self._bridge()["bridge_mode"] == "PAPER_MULTI_STRATEGY"

    def test_intent_count_two(self):
        assert self._bridge()["intent_count"] == 2

    def test_allowed_count_two(self):
        assert self._bridge()["allowed_count"] == 2

    def test_blocked_count_zero(self):
        assert self._bridge()["blocked_count"] == 0

    def test_blocked_reasons_empty(self):
        assert self._bridge()["blocked_reasons"] == []

    def test_btc_in_intents_by_market(self):
        assert "BTC-EUR" in self._bridge()["intents_by_market"]

    def test_edge3_in_intents_by_strategy(self):
        assert "EDGE3" in self._bridge()["intents_by_strategy"]

    def test_intent_action_paper_allocate(self):
        bridge = self._bridge()
        for intent in bridge["intents_by_market"]["BTC-EUR"]:
            assert intent["intent_action"] == "PAPER_ALLOCATE"

    def test_intent_notional_positive(self):
        bridge = self._bridge()
        for intent in bridge["intents_by_market"]["BTC-EUR"]:
            if intent["intent_status"] == INTENT_ALLOWED:
                assert intent["intent_notional_eur"] > 0.0

    def test_all_required_fields(self):
        b = self._bridge()
        for f in _REQUIRED_BRIDGE_FIELDS:
            assert f in b, f"Missing bridge field: {f}"


# ---------------------------------------------------------------------------
# B. Baseline-hold allocation → BRIDGE_BASELINE
# ---------------------------------------------------------------------------

class TestBridgeBaseline:
    def _bridge(self):
        specs = [_spec("BTC-EUR", []), _spec("ETH-EUR", [])]
        return build_execution_bridge(_alloc(specs))

    def test_bridge_status_baseline(self):
        assert self._bridge()["execution_bridge_status"] == BRIDGE_BASELINE

    def test_bridge_mode_paper_baseline(self):
        assert self._bridge()["bridge_mode"] == "PAPER_BASELINE_HOLD"

    def test_intent_count_zero(self):
        assert self._bridge()["intent_count"] == 0

    def test_intents_by_market_empty(self):
        assert self._bridge()["intents_by_market"] == {}

    def test_intents_by_strategy_empty(self):
        assert self._bridge()["intents_by_strategy"] == {}

    def test_paper_only_true(self):
        assert self._bridge()["paper_only"] is True

    def test_live_activation_false(self):
        assert self._bridge()["live_activation_allowed"] is False

    def test_all_required_fields(self):
        b = self._bridge()
        for f in _REQUIRED_BRIDGE_FIELDS:
            assert f in b


# ---------------------------------------------------------------------------
# C. Zero-capital strategy → BLOCKED (ZERO_CAPITAL)
# ---------------------------------------------------------------------------

class TestZeroCapitalBlocked:
    """
    ZERO_CAPITAL blocking is tested via crafted capital allocation dicts.
    When equity=0 the AC-82 allocator omits strategy entries entirely
    (market_capital=0 skips the strategy loop), so the bridge produces
    BRIDGE_BASELINE with no intents. To test ZERO_CAPITAL specifically
    we inject strategy_allocations with capital_eur=0 directly.
    """

    def _crafted_ca_zero_capital(self):
        return {
            "allocation_summary": {"allocation_mode": "MULTI_STRATEGY"},
            "market_allocations": [{
                "market": "BTC-EUR",
                "market_split_valid": True,
                "market_split_mode": "EXPLICIT",
                "strategy_allocations": [
                    {
                        "strategy_id":       "EDGE3",
                        "strategy_family":   "MEAN_REVERSION",
                        "capital_eur":       0.0,
                        "simulated_weight":  0.6,
                        "split_reason_code": "SPLIT_EXPLICIT_WEIGHT",
                    },
                    {
                        "strategy_id":       "EDGE4",
                        "strategy_family":   "BREAKOUT",
                        "capital_eur":       0.0,
                        "simulated_weight":  0.4,
                        "split_reason_code": "SPLIT_EXPLICIT_WEIGHT",
                    },
                ],
            }],
        }

    def test_intents_blocked_with_zero_capital(self):
        b = build_execution_bridge(self._crafted_ca_zero_capital())
        for intents in b["intents_by_market"].values():
            for i in intents:
                assert i["intent_status"] == INTENT_BLOCKED

    def test_block_reason_zero_capital(self):
        b = build_execution_bridge(self._crafted_ca_zero_capital())
        for intents in b["intents_by_market"].values():
            for i in intents:
                assert i["block_reason"] == "ZERO_CAPITAL"

    def test_bridge_status_blocked(self):
        b = build_execution_bridge(self._crafted_ca_zero_capital())
        assert b["execution_bridge_status"] == BRIDGE_BLOCKED

    def test_blocked_reasons_contains_zero_capital(self):
        b = build_execution_bridge(self._crafted_ca_zero_capital())
        assert "ZERO_CAPITAL" in b["blocked_reasons"]

    def test_zero_equity_via_pipeline_gives_baseline(self):
        # When equity=0 the AC-82 allocator produces no strategy entries →
        # bridge correctly returns BRIDGE_BASELINE (nothing to execute)
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])]
        ca = _alloc(specs, equity=0.0)
        b  = build_execution_bridge(ca)
        assert b["execution_bridge_status"] == BRIDGE_BASELINE
        assert b["paper_only"]              is True
        assert b["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# D. BASELINE placeholder strategy → BLOCKED (BASELINE_PLACEHOLDER)
# ---------------------------------------------------------------------------

class TestBaselinePlaceholderBlocked:
    def _bridge(self):
        # baseline strategy gets injected by AC-81 for empty strategy lists
        specs = [_spec("BTC-EUR", [])]
        ca = _alloc(specs, equity=10_000.0)
        return build_execution_bridge(ca)

    def test_bridge_status_baseline(self):
        # No strategies → baseline bridge (no intents at all from AC-82)
        b = self._bridge()
        assert b["execution_bridge_status"] == BRIDGE_BASELINE

    def test_direct_baseline_strategy_blocked(self):
        # Inject a BASELINE strategy directly via crafted allocation
        ca = {
            "allocation_summary": {"allocation_mode": "MULTI_STRATEGY"},
            "market_allocations": [{
                "market": "BTC-EUR",
                "market_split_valid": True,
                "market_split_mode": "BASELINE",
                "strategy_allocations": [{
                    "strategy_id":       "BASELINE",
                    "strategy_family":   "BASELINE",
                    "capital_eur":       5000.0,
                    "simulated_weight":  1.0,
                    "split_reason_code": "SPLIT_BASELINE_HOLD",
                }],
            }],
        }
        b = build_execution_bridge(ca)
        intents = b["intents_by_market"]["BTC-EUR"]
        assert len(intents) == 1
        assert intents[0]["intent_status"] == INTENT_BLOCKED
        assert intents[0]["block_reason"] == "BASELINE_PLACEHOLDER"


# ---------------------------------------------------------------------------
# E. market_split_valid=False → BLOCKED (MARKET_INVALID)
# ---------------------------------------------------------------------------

class TestMarketInvalidBlocked:
    def _bridge(self):
        ca = {
            "allocation_summary": {"allocation_mode": "MULTI_STRATEGY"},
            "market_allocations": [{
                "market": "BTC-EUR",
                "market_split_valid": False,
                "market_split_mode": "EQUAL",
                "strategy_allocations": [{
                    "strategy_id":       "EDGE3",
                    "strategy_family":   "MEAN_REVERSION",
                    "capital_eur":       5000.0,
                    "simulated_weight":  1.0,
                    "split_reason_code": "SPLIT_EQUAL_WEIGHT",
                }],
            }],
        }
        return build_execution_bridge(ca)

    def test_intent_blocked_market_invalid(self):
        b = self._bridge()
        intents = b["intents_by_market"]["BTC-EUR"]
        assert intents[0]["intent_status"] == INTENT_BLOCKED
        assert intents[0]["block_reason"] == "MARKET_INVALID"

    def test_bridge_status_blocked(self):
        assert self._bridge()["execution_bridge_status"] == BRIDGE_BLOCKED


# ---------------------------------------------------------------------------
# F. Invalid allocation input → BRIDGE_BLOCKED / PAPER_REJECTED
# ---------------------------------------------------------------------------

class TestInvalidAllocationInput:
    @pytest.mark.parametrize("bad", [None, "string", 42, [], {}])
    def test_non_dict_gives_rejected(self, bad):
        b = build_execution_bridge(bad)
        assert b["execution_bridge_status"] == BRIDGE_BLOCKED
        assert b["bridge_mode"] == "PAPER_REJECTED"
        assert b["paper_only"] is True
        assert b["live_activation_allowed"] is False

    def test_missing_allocation_summary(self):
        b = build_execution_bridge({"market_allocations": []})
        assert b["execution_bridge_status"] == BRIDGE_BLOCKED

    def test_missing_market_allocations(self):
        b = build_execution_bridge({"allocation_summary": {"allocation_mode": "MULTI_STRATEGY"}})
        assert b["execution_bridge_status"] == BRIDGE_BLOCKED


# ---------------------------------------------------------------------------
# G+H. paper_only always True, live_activation_allowed always False
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    _cases = [
        lambda: build_execution_bridge(
            build_capital_allocation(
                build_allocation_splits([_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]),
                10_000.0,
            )
        ),
        lambda: build_execution_bridge(
            build_capital_allocation(
                build_allocation_splits([_spec("BTC-EUR", [])]),
                10_000.0,
            )
        ),
        lambda: build_execution_bridge(None),
        lambda: build_execution_bridge({}),
    ]

    def test_paper_only_always_true(self):
        for fn in self._cases:
            assert fn()["paper_only"] is True

    def test_live_activation_always_false(self):
        for fn in self._cases:
            assert fn()["live_activation_allowed"] is False

    def test_intent_paper_only_true(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        b = build_execution_bridge(_alloc(specs))
        for intents in b["intents_by_market"].values():
            for i in intents:
                assert i["paper_only"] is True

    def test_intent_live_activation_false(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        b = build_execution_bridge(_alloc(specs))
        for intents in b["intents_by_market"].values():
            for i in intents:
                assert i["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# I. blocked_reasons correctly populated
# ---------------------------------------------------------------------------

class TestBlockedReasons:
    def test_zero_equity_pipeline_gives_baseline_no_reasons(self):
        # Zero equity → AC-82 produces no strategy entries → BRIDGE_BASELINE, no blocked reasons
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        b = build_execution_bridge(_alloc(specs, equity=0.0))
        assert b["execution_bridge_status"] == BRIDGE_BASELINE
        assert b["blocked_reasons"] == []

    def test_no_block_reasons_when_all_allowed(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        b = build_execution_bridge(_alloc(specs, equity=10_000.0))
        assert b["blocked_reasons"] == []

    def test_blocked_reasons_is_sorted_list(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        b = build_execution_bridge(_alloc(specs, equity=0.0))
        assert isinstance(b["blocked_reasons"], list)
        assert b["blocked_reasons"] == sorted(b["blocked_reasons"])


# ---------------------------------------------------------------------------
# J. intents_by_market and intents_by_strategy populated
# ---------------------------------------------------------------------------

class TestIntentsByMarketAndStrategy:
    def _bridge(self):
        specs = [
            _spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)]),
            _spec("ETH-EUR", [_strat("EDGE3", weight=1.0)]),
        ]
        return build_execution_bridge(_alloc(specs))

    def test_both_markets_present(self):
        b = self._bridge()
        assert "BTC-EUR" in b["intents_by_market"]
        assert "ETH-EUR" in b["intents_by_market"]

    def test_btc_has_two_intents(self):
        b = self._bridge()
        assert len(b["intents_by_market"]["BTC-EUR"]) == 2

    def test_eth_has_one_intent(self):
        b = self._bridge()
        assert len(b["intents_by_market"]["ETH-EUR"]) == 1

    def test_edge3_aggregated_across_markets(self):
        b = self._bridge()
        # EDGE3 appears in both BTC and ETH
        assert len(b["intents_by_strategy"]["EDGE3"]) == 2

    def test_edge4_only_btc(self):
        b = self._bridge()
        assert len(b["intents_by_strategy"]["EDGE4"]) == 1


# ---------------------------------------------------------------------------
# K. intent_count == allowed_count + blocked_count
# ---------------------------------------------------------------------------

class TestIntentCountConsistency:
    @pytest.mark.parametrize("equity", [0.0, 10_000.0])
    def test_count_consistency(self, equity):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])]
        b = build_execution_bridge(_alloc(specs, equity=equity))
        assert b["intent_count"] == b["allowed_count"] + b["blocked_count"]


# ---------------------------------------------------------------------------
# L. All required output fields
# ---------------------------------------------------------------------------

class TestRequiredFields:
    def _check(self, b):
        for f in _REQUIRED_BRIDGE_FIELDS:
            assert f in b, f"Missing: {f}"

    def test_active_path(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        self._check(build_execution_bridge(_alloc(specs)))

    def test_baseline_path(self):
        specs = [_spec("BTC-EUR", [])]
        self._check(build_execution_bridge(_alloc(specs)))

    def test_rejected_path(self):
        self._check(build_execution_bridge(None))

    def test_intent_required_fields(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        b = build_execution_bridge(_alloc(specs))
        for intents in b["intents_by_market"].values():
            for i in intents:
                for f in _REQUIRED_INTENT_FIELDS:
                    assert f in i, f"Missing intent field: {f}"


# ---------------------------------------------------------------------------
# M. No mutation of inputs
# ---------------------------------------------------------------------------

class TestNoMutation:
    def test_capital_allocation_not_mutated(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])]
        ca = _alloc(specs)
        original = copy.deepcopy(ca)
        build_execution_bridge(ca)
        assert ca == original


# ---------------------------------------------------------------------------
# N. build_bridge_from_specs full pipeline
# ---------------------------------------------------------------------------

class TestFullPipeline:
    def test_three_keys_present(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        r = build_bridge_from_specs(specs, 10_000.0)
        assert "splits_result"      in r
        assert "capital_allocation" in r
        assert "execution_bridge"   in r

    def test_active_path_end_to_end(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=0.6), _strat("EDGE4", weight=0.4)])]
        r = build_bridge_from_specs(specs, 10_000.0)
        b = r["execution_bridge"]
        assert b["execution_bridge_status"] == BRIDGE_ACTIVE
        assert b["paper_only"]              is True
        assert b["live_activation_allowed"] is False

    def test_baseline_path_end_to_end(self):
        specs = [_spec("BTC-EUR", [])]
        r = build_bridge_from_specs(specs, 10_000.0)
        assert r["execution_bridge"]["execution_bridge_status"] == BRIDGE_BASELINE

    def test_invalid_specs_end_to_end(self):
        r = build_bridge_from_specs(None, 10_000.0)
        b = r["execution_bridge"]
        assert b["paper_only"]              is True
        assert b["live_activation_allowed"] is False

    def test_all_layers_simulation_only(self):
        specs = [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])]
        r = build_bridge_from_specs(specs, 10_000.0)
        assert r["splits_result"]["simulation_only"]           is True
        assert r["capital_allocation"]["simulation_only"]      is True
        assert r["execution_bridge"]["paper_only"]             is True
        assert r["execution_bridge"]["live_activation_allowed"] is False


# ---------------------------------------------------------------------------
# O. Backward compat: AC-81/82 output fields unchanged
# ---------------------------------------------------------------------------

class TestBackwardCompat:
    _AC81_FIELDS = {"split_summary", "market_splits", "simulation_only", "non_binding"}
    _AC82_FIELDS = {"allocation_summary", "market_allocations", "simulation_only", "non_binding"}

    def test_splits_fields_preserved(self):
        r = build_bridge_from_specs(
            [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])], 10_000.0
        )
        for f in self._AC81_FIELDS:
            assert f in r["splits_result"], f"AC-81 field missing: {f}"

    def test_capital_alloc_fields_preserved(self):
        r = build_bridge_from_specs(
            [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])], 10_000.0
        )
        for f in self._AC82_FIELDS:
            assert f in r["capital_allocation"], f"AC-82 field missing: {f}"

    def test_bridge_is_separate_dict(self):
        r = build_bridge_from_specs(
            [_spec("BTC-EUR", [_strat("EDGE3", weight=1.0)])], 10_000.0
        )
        assert r["execution_bridge"] is not r["capital_allocation"]
        assert r["execution_bridge"] is not r["splits_result"]
