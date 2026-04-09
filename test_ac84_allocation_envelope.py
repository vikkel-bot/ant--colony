"""
AC-84: Cross-Market / Cross-Asset Allocation Envelope — Test Suite

Tests the build_allocation_envelope() function and build_envelope_from_specs()
full-chain convenience wrapper.

Coverage:
  - Always-True flags (envelope_non_binding, envelope_simulation_only)
  - Fail-closed: invalid input → ENVELOPE_REJECTED
  - Baseline input → ENVELOPE_BASELINE
  - Active multi-strategy input → ENVELOPE_ACTIVE
  - Asset class classification (known markets → crypto, unknown → crypto default)
  - Capital consistency: total_allocated_capital + unallocated_capital == total_equity_eur
  - asset_class_allocations grouping and weight accumulation
  - market_allocations structure
  - Full chain via build_envelope_from_specs
"""
import sys
import importlib.util
from pathlib import Path
import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

def _load(name: str, rel: str):
    path = Path(__file__).parent / rel
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_env_mod   = _load("_env",   "ant_colony/build_allocation_envelope_lite.py")
_alloc_mod = _load("_alloc", "ant_colony/build_queen_capital_allocator_lite.py")

build_allocation_envelope = _env_mod.build_allocation_envelope
build_envelope_from_specs = _env_mod.build_envelope_from_specs

ENVELOPE_ACTIVE   = _env_mod.ENVELOPE_ACTIVE
ENVELOPE_BASELINE = _env_mod.ENVELOPE_BASELINE
ENVELOPE_REJECTED = _env_mod.ENVELOPE_REJECTED


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_capital_allocation(
    allocation_mode: str = "MULTI_STRATEGY",
    total_equity: float = 10_000.0,
    market_allocations: list = None,
    allocated_total: float = None,
):
    """Craft a minimal AC-82-shaped capital_allocation dict."""
    if market_allocations is None:
        market_allocations = []
    if allocated_total is None:
        allocated_total = sum(m.get("market_capital_eur", 0.0) for m in market_allocations)
    unallocated = round(total_equity - allocated_total, 4)
    return {
        "allocation_summary": {
            "allocated_capital_total": allocated_total,
            "unallocated_capital":     unallocated,
            "total_equity_eur":        total_equity,
            "allocation_mode":         allocation_mode,
            "allocation_reason":       f"{allocation_mode}|allocated={allocated_total:.2f}",
            "allocation_reason_code":  "CAPITAL_ALLOCATED_MULTI_STRATEGY",
        },
        "allocated_capital_by_market":   {},
        "allocated_capital_by_strategy": {},
        "market_allocations":            market_allocations,
        "simulation_only": True,
        "non_binding":     True,
    }


def _market_entry(
    market: str = "BTC-EUR",
    capital_eur: float = 5000.0,
    fraction: float = 0.5,
    split_valid: bool = True,
    strategies: list = None,
):
    if strategies is None:
        strategies = [
            {"strategy_id": "EDGE3", "strategy_family": "MR", "capital_eur": capital_eur, "simulated_weight": 1.0},
        ]
    return {
        "market":                    market,
        "market_capital_eur":        capital_eur,
        "market_capital_fraction":   fraction,
        "market_split_mode":         "MULTI_STRATEGY" if split_valid else "BASELINE",
        "market_split_valid":        split_valid,
        "strategy_allocations":      strategies,
    }


# ---------------------------------------------------------------------------
# 1. Always-True contract flags
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    def test_active_envelope_non_binding(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            market_allocations=[_market_entry()],
        )
        env = build_allocation_envelope(ca)
        assert env["envelope_non_binding"] is True

    def test_active_envelope_simulation_only(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            market_allocations=[_market_entry()],
        )
        env = build_allocation_envelope(ca)
        assert env["envelope_simulation_only"] is True

    def test_baseline_envelope_non_binding(self):
        ca = _make_capital_allocation(allocation_mode="BASELINE_HOLD")
        env = build_allocation_envelope(ca)
        assert env["envelope_non_binding"] is True

    def test_baseline_envelope_simulation_only(self):
        ca = _make_capital_allocation(allocation_mode="BASELINE_HOLD")
        env = build_allocation_envelope(ca)
        assert env["envelope_simulation_only"] is True

    def test_rejected_envelope_non_binding(self):
        env = build_allocation_envelope(None)
        assert env["envelope_non_binding"] is True

    def test_rejected_envelope_simulation_only(self):
        env = build_allocation_envelope(None)
        assert env["envelope_simulation_only"] is True


# ---------------------------------------------------------------------------
# 2. Fail-closed: invalid input → ENVELOPE_REJECTED
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_none_input(self):
        env = build_allocation_envelope(None)
        assert env["allocation_envelope_status"] == ENVELOPE_REJECTED

    def test_string_input(self):
        env = build_allocation_envelope("bad")
        assert env["allocation_envelope_status"] == ENVELOPE_REJECTED

    def test_list_input(self):
        env = build_allocation_envelope([])
        assert env["allocation_envelope_status"] == ENVELOPE_REJECTED

    def test_missing_allocation_summary(self):
        env = build_allocation_envelope({"market_allocations": []})
        assert env["allocation_envelope_status"] == ENVELOPE_REJECTED

    def test_missing_market_allocations(self):
        env = build_allocation_envelope({"allocation_summary": {}})
        assert env["allocation_envelope_status"] == ENVELOPE_REJECTED

    def test_rejected_reason_code(self):
        env = build_allocation_envelope(None)
        assert env["allocation_reason_code"] == "ENVELOPE_INVALID_INPUT"

    def test_rejected_zero_capital(self):
        env = build_allocation_envelope(None)
        assert env["total_allocated_capital"] == 0.0

    def test_rejected_zero_equity(self):
        env = build_allocation_envelope(None)
        assert env["total_equity_eur"] == 0.0

    def test_rejected_empty_asset_classes(self):
        env = build_allocation_envelope(None)
        assert env["asset_class_allocations"] == {}

    def test_rejected_empty_market_allocations(self):
        env = build_allocation_envelope(None)
        assert env["market_allocations"] == []


# ---------------------------------------------------------------------------
# 3. Baseline input → ENVELOPE_BASELINE
# ---------------------------------------------------------------------------

class TestBaselineEnvelope:
    def test_baseline_hold_mode(self):
        ca = _make_capital_allocation(allocation_mode="BASELINE_HOLD")
        env = build_allocation_envelope(ca)
        assert env["allocation_envelope_status"] == ENVELOPE_BASELINE

    def test_empty_mode(self):
        ca = _make_capital_allocation(allocation_mode="EMPTY")
        env = build_allocation_envelope(ca)
        assert env["allocation_envelope_status"] == ENVELOPE_BASELINE

    def test_baseline_reason_code(self):
        ca = _make_capital_allocation(allocation_mode="BASELINE_HOLD")
        env = build_allocation_envelope(ca)
        assert env["allocation_reason_code"] == "ENVELOPE_BASELINE_HOLD"

    def test_baseline_zero_allocated(self):
        ca = _make_capital_allocation(allocation_mode="BASELINE_HOLD", total_equity=10_000.0)
        env = build_allocation_envelope(ca)
        assert env["total_allocated_capital"] == 0.0

    def test_baseline_full_unallocated(self):
        ca = _make_capital_allocation(allocation_mode="BASELINE_HOLD", total_equity=10_000.0)
        env = build_allocation_envelope(ca)
        assert env["unallocated_capital"] == 10_000.0

    def test_baseline_allocation_mode_preserved(self):
        ca = _make_capital_allocation(allocation_mode="BASELINE_HOLD")
        env = build_allocation_envelope(ca)
        assert env["allocation_mode"] == "BASELINE_HOLD"

    def test_multi_strategy_zero_capital_gives_baseline(self):
        # MULTI_STRATEGY mode but 0 allocated → ENVELOPE_BASELINE
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            allocated_total=0.0,
            market_allocations=[],
        )
        env = build_allocation_envelope(ca)
        assert env["allocation_envelope_status"] == ENVELOPE_BASELINE


# ---------------------------------------------------------------------------
# 4. Active envelope
# ---------------------------------------------------------------------------

class TestActiveEnvelope:
    def _active_ca(self):
        return _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[
                _market_entry("BTC-EUR", capital_eur=5000.0, fraction=0.5),
                _market_entry("ETH-EUR", capital_eur=5000.0, fraction=0.5),
            ],
            allocated_total=10_000.0,
        )

    def test_active_status(self):
        env = build_allocation_envelope(self._active_ca())
        assert env["allocation_envelope_status"] == ENVELOPE_ACTIVE

    def test_active_reason_code(self):
        env = build_allocation_envelope(self._active_ca())
        assert env["allocation_reason_code"] == "ENVELOPE_CAPITAL_ALLOCATED"

    def test_allocation_mode_preserved(self):
        env = build_allocation_envelope(self._active_ca())
        assert env["allocation_mode"] == "MULTI_STRATEGY"

    def test_total_allocated_capital(self):
        env = build_allocation_envelope(self._active_ca())
        assert env["total_allocated_capital"] == 10_000.0

    def test_unallocated_zero(self):
        env = build_allocation_envelope(self._active_ca())
        assert env["unallocated_capital"] == 0.0

    def test_total_equity(self):
        env = build_allocation_envelope(self._active_ca())
        assert env["total_equity_eur"] == 10_000.0

    def test_capital_consistency(self):
        env = build_allocation_envelope(self._active_ca())
        assert abs(
            env["total_allocated_capital"] + env["unallocated_capital"]
            - env["total_equity_eur"]
        ) < 1e-6

    def test_total_weight_sum(self):
        env = build_allocation_envelope(self._active_ca())
        # 0.5 + 0.5 = 1.0
        assert abs(env["total_allocated_weight"] - 1.0) < 1e-6

    def test_mixed_mode_active(self):
        ca = _make_capital_allocation(
            allocation_mode="MIXED",
            total_equity=10_000.0,
            market_allocations=[
                _market_entry("BTC-EUR", capital_eur=6000.0, fraction=0.6),
                _market_entry("ETH-EUR", capital_eur=0.0,    fraction=0.0, split_valid=False),
            ],
            allocated_total=6000.0,
        )
        env = build_allocation_envelope(ca)
        assert env["allocation_envelope_status"] == ENVELOPE_ACTIVE


# ---------------------------------------------------------------------------
# 5. Asset class classification
# ---------------------------------------------------------------------------

class TestAssetClassClassification:
    def test_known_btc_classified_crypto(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[_market_entry("BTC-EUR", 10_000.0, 1.0)],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        assert "crypto" in env["asset_class_allocations"]

    def test_known_eth_classified_crypto(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[_market_entry("ETH-EUR", 10_000.0, 1.0)],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        market_entry = env["market_allocations"][0]
        assert market_entry["asset_class"] == "crypto"

    def test_unknown_market_defaults_to_crypto(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[_market_entry("NEWCOIN-EUR", 10_000.0, 1.0)],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        assert "crypto" in env["asset_class_allocations"]

    def test_all_crypto_markets_in_one_bucket(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[
                _market_entry("BTC-EUR", 5000.0, 0.5),
                _market_entry("ETH-EUR", 3000.0, 0.3),
                _market_entry("SOL-EUR", 2000.0, 0.2),
            ],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        ac = env["asset_class_allocations"]
        assert len(ac) == 1
        assert "crypto" in ac

    def test_crypto_bucket_capital_sum(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[
                _market_entry("BTC-EUR", 5000.0, 0.5),
                _market_entry("ETH-EUR", 5000.0, 0.5),
            ],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        assert env["asset_class_allocations"]["crypto"]["capital_eur"] == 10_000.0

    def test_crypto_bucket_markets_list(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[
                _market_entry("BTC-EUR", 5000.0, 0.5),
                _market_entry("ETH-EUR", 5000.0, 0.5),
            ],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        markets = env["asset_class_allocations"]["crypto"]["markets"]
        assert sorted(markets) == ["BTC-EUR", "ETH-EUR"]

    def test_crypto_bucket_weight_fraction(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[
                _market_entry("BTC-EUR", 5000.0, 0.5),
                _market_entry("ETH-EUR", 5000.0, 0.5),
            ],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        wf = env["asset_class_allocations"]["crypto"]["weight_fraction"]
        assert abs(wf - 1.0) < 1e-6


# ---------------------------------------------------------------------------
# 6. market_allocations structure
# ---------------------------------------------------------------------------

class TestMarketAllocationsStructure:
    def _two_market_ca(self):
        return _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[
                _market_entry("BTC-EUR", 6000.0, 0.6),
                _market_entry("ETH-EUR", 4000.0, 0.4),
            ],
            allocated_total=10_000.0,
        )

    def test_market_count(self):
        env = build_allocation_envelope(self._two_market_ca())
        assert len(env["market_allocations"]) == 2

    def test_market_entry_has_market_field(self):
        env = build_allocation_envelope(self._two_market_ca())
        markets = {m["market"] for m in env["market_allocations"]}
        assert markets == {"BTC-EUR", "ETH-EUR"}

    def test_market_entry_has_asset_class(self):
        env = build_allocation_envelope(self._two_market_ca())
        for m in env["market_allocations"]:
            assert "asset_class" in m

    def test_market_entry_has_capital(self):
        env = build_allocation_envelope(self._two_market_ca())
        caps = {m["market"]: m["market_capital_eur"] for m in env["market_allocations"]}
        assert caps["BTC-EUR"] == 6000.0
        assert caps["ETH-EUR"] == 4000.0

    def test_market_entry_has_fraction(self):
        env = build_allocation_envelope(self._two_market_ca())
        for m in env["market_allocations"]:
            assert "market_capital_fraction" in m

    def test_market_entry_has_strategy_count(self):
        env = build_allocation_envelope(self._two_market_ca())
        for m in env["market_allocations"]:
            assert m["strategy_count"] >= 0

    def test_market_entry_strategy_allocations_present(self):
        env = build_allocation_envelope(self._two_market_ca())
        for m in env["market_allocations"]:
            assert "strategy_allocations" in m
            assert isinstance(m["strategy_allocations"], list)

    def test_market_entry_split_valid_preserved(self):
        env = build_allocation_envelope(self._two_market_ca())
        for m in env["market_allocations"]:
            assert "market_split_valid" in m

    def test_baseline_market_zero_capital(self):
        ca = _make_capital_allocation(
            allocation_mode="MIXED",
            total_equity=10_000.0,
            market_allocations=[
                _market_entry("BTC-EUR", 10_000.0, 1.0),
                _market_entry("SOL-EUR", 0.0, 0.0, split_valid=False, strategies=[]),
            ],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        sol = next(m for m in env["market_allocations"] if m["market"] == "SOL-EUR")
        assert sol["market_capital_eur"] == 0.0


# ---------------------------------------------------------------------------
# 7. Capital consistency
# ---------------------------------------------------------------------------

class TestCapitalConsistency:
    def test_consistency_full_allocation(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=10_000.0,
            market_allocations=[_market_entry("BTC-EUR", 10_000.0, 1.0)],
            allocated_total=10_000.0,
        )
        env = build_allocation_envelope(ca)
        assert abs(env["total_allocated_capital"] + env["unallocated_capital"]
                   - env["total_equity_eur"]) < 1e-6

    def test_consistency_partial_allocation(self):
        ca = _make_capital_allocation(
            allocation_mode="MIXED",
            total_equity=10_000.0,
            market_allocations=[_market_entry("BTC-EUR", 6000.0, 0.6)],
            allocated_total=6000.0,
        )
        env = build_allocation_envelope(ca)
        assert abs(env["total_allocated_capital"] + env["unallocated_capital"]
                   - env["total_equity_eur"]) < 1e-6
        assert env["unallocated_capital"] == pytest.approx(4000.0, abs=1e-4)

    def test_consistency_zero_equity(self):
        ca = _make_capital_allocation(
            allocation_mode="BASELINE_HOLD",
            total_equity=0.0,
            allocated_total=0.0,
        )
        env = build_allocation_envelope(ca)
        assert abs(env["total_allocated_capital"] + env["unallocated_capital"]
                   - env["total_equity_eur"]) < 1e-6


# ---------------------------------------------------------------------------
# 8. Full chain via build_envelope_from_specs
# ---------------------------------------------------------------------------

class TestBuildEnvelopeFromSpecs:
    def _two_market_specs(self):
        return [
            {
                "market": "BTC-EUR",
                "strategies": [
                    {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION", "weight_fraction": 0.6},
                    {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT",        "weight_fraction": 0.4},
                ],
            },
            {
                "market": "ETH-EUR",
                "strategies": [
                    {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION"},
                ],
            },
        ]

    def test_returns_three_keys(self):
        result = build_envelope_from_specs(self._two_market_specs(), 10_000.0)
        assert "splits_result"       in result
        assert "capital_allocation"  in result
        assert "allocation_envelope" in result

    def test_envelope_active_on_valid_specs(self):
        result = build_envelope_from_specs(self._two_market_specs(), 10_000.0)
        env = result["allocation_envelope"]
        assert env["allocation_envelope_status"] == ENVELOPE_ACTIVE

    def test_envelope_non_binding_pipeline(self):
        result = build_envelope_from_specs(self._two_market_specs(), 10_000.0)
        assert result["allocation_envelope"]["envelope_non_binding"] is True

    def test_envelope_simulation_only_pipeline(self):
        result = build_envelope_from_specs(self._two_market_specs(), 10_000.0)
        assert result["allocation_envelope"]["envelope_simulation_only"] is True

    def test_capital_consistency_pipeline(self):
        result = build_envelope_from_specs(self._two_market_specs(), 10_000.0)
        env = result["allocation_envelope"]
        assert abs(env["total_allocated_capital"] + env["unallocated_capital"]
                   - env["total_equity_eur"]) < 1e-4

    def test_empty_specs_baseline(self):
        result = build_envelope_from_specs([], 10_000.0)
        env = result["allocation_envelope"]
        assert env["allocation_envelope_status"] == ENVELOPE_BASELINE

    def test_baseline_market_only_gives_baseline(self):
        specs = [{"market": "BTC-EUR", "strategies": []}]
        result = build_envelope_from_specs(specs, 10_000.0)
        env = result["allocation_envelope"]
        assert env["allocation_envelope_status"] == ENVELOPE_BASELINE

    def test_custom_fractions_respected(self):
        result = build_envelope_from_specs(
            self._two_market_specs(),
            10_000.0,
            market_capital_fractions={"BTC-EUR": 0.7, "ETH-EUR": 0.3},
        )
        env = result["allocation_envelope"]
        btc = next(m for m in env["market_allocations"] if m["market"] == "BTC-EUR")
        eth = next(m for m in env["market_allocations"] if m["market"] == "ETH-EUR")
        assert btc["market_capital_eur"] > eth["market_capital_eur"]

    def test_total_equity_preserved(self):
        result = build_envelope_from_specs(self._two_market_specs(), 12_345.0)
        assert result["allocation_envelope"]["total_equity_eur"] == pytest.approx(12_345.0, abs=0.01)

    def test_all_markets_in_envelope(self):
        result = build_envelope_from_specs(self._two_market_specs(), 10_000.0)
        markets = {m["market"] for m in result["allocation_envelope"]["market_allocations"]}
        assert "BTC-EUR" in markets
        assert "ETH-EUR" in markets

    def test_sol_with_no_strategies_baseline(self):
        specs = [
            {"market": "BTC-EUR", "strategies": [
                {"strategy_id": "EDGE3", "strategy_family": "MR", "weight_fraction": 1.0}
            ]},
            {"market": "SOL-EUR", "strategies": []},
        ]
        result = build_envelope_from_specs(specs, 10_000.0)
        sol = next(
            m for m in result["allocation_envelope"]["market_allocations"]
            if m["market"] == "SOL-EUR"
        )
        assert sol["market_capital_eur"] == 0.0


# ---------------------------------------------------------------------------
# 9. Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_non_dict_market_allocation_entry_skipped(self):
        ca = {
            "allocation_summary": {
                "allocated_capital_total": 0.0,
                "unallocated_capital":     10_000.0,
                "total_equity_eur":        10_000.0,
                "allocation_mode":         "BASELINE_HOLD",
                "allocation_reason":       "test",
                "allocation_reason_code":  "X",
            },
            "market_allocations": ["bad_entry", None, 42],
        }
        env = build_allocation_envelope(ca)
        # Should not crash, just produce baseline with no market entries
        assert env["allocation_envelope_status"] == ENVELOPE_BASELINE
        assert env["market_allocations"] == []

    def test_none_market_allocations_list(self):
        ca = {
            "allocation_summary": {
                "allocated_capital_total": 0.0,
                "unallocated_capital":     0.0,
                "total_equity_eur":        0.0,
                "allocation_mode":         "BASELINE_HOLD",
                "allocation_reason":       "",
                "allocation_reason_code":  "",
            },
            "market_allocations": None,
        }
        env = build_allocation_envelope(ca)
        assert env["allocation_envelope_status"] == ENVELOPE_BASELINE

    def test_single_market_single_strategy(self):
        ca = _make_capital_allocation(
            allocation_mode="MULTI_STRATEGY",
            total_equity=1000.0,
            market_allocations=[
                _market_entry("BTC-EUR", 1000.0, 1.0, strategies=[
                    {"strategy_id": "S1", "strategy_family": "MR",
                     "capital_eur": 1000.0, "simulated_weight": 1.0}
                ])
            ],
            allocated_total=1000.0,
        )
        env = build_allocation_envelope(ca)
        assert env["allocation_envelope_status"] == ENVELOPE_ACTIVE
        assert env["market_allocations"][0]["strategy_count"] == 1
