"""
AC-86: Regime-Adjusted Allocation Proposal — Test Suite

Tests build_allocation_proposal() and build_proposal_from_specs() wrapper.

Coverage:
  - Always-True flags (proposal_non_binding, proposal_simulation_only)
  - Fail-closed: invalid overlay or envelope → PROPOSAL_REJECTED
  - Baseline overlay → PROPOSAL_BASELINE
  - Direction logic: UPWEIGHT / DOWNWEIGHT / HOLD
  - Proposed capital = current × (1 + bias), clamped ≥ 0
  - Proposed delta = proposed − current
  - Asset-class proposal aggregation
  - Counts: upweight / downweight / hold
  - total_proposed_capital consistency
  - No execution / allocation side effects
  - Full chain via build_proposal_from_specs
  - Backward compatibility: prior layer outputs unchanged
"""
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

_prop_mod = _load("_prop", "ant_colony/build_allocation_proposal_lite.py")
_ov_mod   = _load("_ov",   "ant_colony/build_regime_overlay_lite.py")
_env_mod  = _load("_env",  "ant_colony/build_allocation_envelope_lite.py")

build_allocation_proposal  = _prop_mod.build_allocation_proposal
build_proposal_from_specs  = _prop_mod.build_proposal_from_specs

PROPOSAL_ACTIVE   = _prop_mod.PROPOSAL_ACTIVE
PROPOSAL_BASELINE = _prop_mod.PROPOSAL_BASELINE
PROPOSAL_REJECTED = _prop_mod.PROPOSAL_REJECTED
DIR_UPWEIGHT      = _prop_mod.DIR_UPWEIGHT
DIR_DOWNWEIGHT    = _prop_mod.DIR_DOWNWEIGHT
DIR_HOLD          = _prop_mod.DIR_HOLD


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _active_overlay(
    bias_by_market: dict = None,
    bias_by_ac: dict = None,
):
    """Craft a minimal OVERLAY_ACTIVE dict (AC-85 shape)."""
    return {
        "regime_overlay_status":          "OVERLAY_ACTIVE",
        "regime_mode":                    "REGIME_AWARE",
        "asset_class_regimes":            {},
        "market_regimes_summary":         {},
        "allocation_bias_by_asset_class": bias_by_ac or {},
        "allocation_bias_by_market":      bias_by_market or {},
        "bias_reason":                    "test",
        "bias_reason_code":               "POSITIVE_BIAS",
        "overlay_non_binding":            True,
        "overlay_simulation_only":        True,
    }


def _baseline_overlay():
    return {
        "regime_overlay_status":          "OVERLAY_BASELINE",
        "regime_mode":                    "REGIME_BASELINE",
        "asset_class_regimes":            {},
        "market_regimes_summary":         {},
        "allocation_bias_by_asset_class": {},
        "allocation_bias_by_market":      {},
        "bias_reason":                    "baseline",
        "bias_reason_code":               "OVERLAY_BASELINE_HOLD",
        "overlay_non_binding":            True,
        "overlay_simulation_only":        True,
    }


def _active_envelope(markets=None):
    """Craft a minimal ENVELOPE_ACTIVE dict (AC-84 shape)."""
    if markets is None:
        markets = [("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)]
    mlist = []
    for market, cap, frac in markets:
        mlist.append({
            "market":                  market,
            "asset_class":             "crypto",
            "market_capital_eur":      cap,
            "market_capital_fraction": frac,
            "market_split_mode":       "MULTI_STRATEGY",
            "market_split_valid":      True,
            "strategy_count":          1,
            "strategy_allocations":    [],
        })
    total = sum(c for _, c, _ in markets)
    return {
        "allocation_envelope_status": "ENVELOPE_ACTIVE",
        "allocation_mode":            "MULTI_STRATEGY",
        "asset_class_allocations":    {},
        "market_allocations":         mlist,
        "total_allocated_weight":     1.0,
        "total_allocated_capital":    total,
        "unallocated_capital":        0.0,
        "total_equity_eur":           total,
        "allocation_reason":          "test",
        "allocation_reason_code":     "ENVELOPE_CAPITAL_ALLOCATED",
        "envelope_non_binding":       True,
        "envelope_simulation_only":   True,
    }


def _bias(scalar, code="BULL_LOW_VOL"):
    return {"bias_scalar": scalar, "bias_reason_code": code}


# ---------------------------------------------------------------------------
# 1. Always-True contract flags
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    def test_active_non_binding(self):
        prop = build_allocation_proposal(_active_overlay(), _active_envelope())
        assert prop["proposal_non_binding"] is True

    def test_active_simulation_only(self):
        prop = build_allocation_proposal(_active_overlay(), _active_envelope())
        assert prop["proposal_simulation_only"] is True

    def test_baseline_non_binding(self):
        prop = build_allocation_proposal(_baseline_overlay(), _active_envelope())
        assert prop["proposal_non_binding"] is True

    def test_baseline_simulation_only(self):
        prop = build_allocation_proposal(_baseline_overlay(), _active_envelope())
        assert prop["proposal_simulation_only"] is True

    def test_rejected_non_binding(self):
        assert build_allocation_proposal(None, None)["proposal_non_binding"] is True

    def test_rejected_simulation_only(self):
        assert build_allocation_proposal(None, None)["proposal_simulation_only"] is True


# ---------------------------------------------------------------------------
# 2. Fail-closed
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_none_overlay(self):
        prop = build_allocation_proposal(None, _active_envelope())
        assert prop["proposal_status"] == PROPOSAL_REJECTED

    def test_none_envelope(self):
        prop = build_allocation_proposal(_active_overlay(), None)
        assert prop["proposal_status"] == PROPOSAL_REJECTED

    def test_both_none(self):
        prop = build_allocation_proposal(None, None)
        assert prop["proposal_status"] == PROPOSAL_REJECTED

    def test_string_overlay(self):
        assert build_allocation_proposal("bad", _active_envelope())["proposal_status"] == PROPOSAL_REJECTED

    def test_missing_overlay_status_key(self):
        assert build_allocation_proposal({}, _active_envelope())["proposal_status"] == PROPOSAL_REJECTED

    def test_missing_envelope_status_key(self):
        assert build_allocation_proposal(_active_overlay(), {})["proposal_status"] == PROPOSAL_REJECTED

    def test_rejected_reason_code(self):
        prop = build_allocation_proposal(None, None)
        assert prop["proposal_reason_code"] == "PROPOSAL_INVALID_INPUT"

    def test_rejected_zero_totals(self):
        prop = build_allocation_proposal(None, None)
        assert prop["total_proposed_capital"] == 0.0
        assert prop["proposed_upweight_count"] == 0
        assert prop["proposed_downweight_count"] == 0
        assert prop["proposed_hold_count"] == 0


# ---------------------------------------------------------------------------
# 3. Baseline overlay → PROPOSAL_BASELINE
# ---------------------------------------------------------------------------

class TestBaselineProposal:
    def test_baseline_overlay_status(self):
        prop = build_allocation_proposal(_baseline_overlay(), _active_envelope())
        assert prop["proposal_status"] == PROPOSAL_BASELINE

    def test_rejected_overlay_gives_baseline(self):
        ov = _active_overlay()
        ov["regime_overlay_status"] = "OVERLAY_REJECTED"
        prop = build_allocation_proposal(ov, _active_envelope())
        assert prop["proposal_status"] == PROPOSAL_BASELINE

    def test_baseline_mode(self):
        prop = build_allocation_proposal(_baseline_overlay(), _active_envelope())
        assert prop["proposal_mode"] == "PROPOSAL_BASELINE"

    def test_baseline_reason_code(self):
        prop = build_allocation_proposal(_baseline_overlay(), _active_envelope())
        assert prop["proposal_reason_code"] == "PROPOSAL_BASELINE_HOLD"

    def test_baseline_empty_proposals(self):
        prop = build_allocation_proposal(_baseline_overlay(), _active_envelope())
        assert prop["market_proposals"] == {}
        assert prop["asset_class_proposals"] == {}

    def test_baseline_zero_counts(self):
        prop = build_allocation_proposal(_baseline_overlay(), _active_envelope())
        assert prop["proposed_upweight_count"] == 0
        assert prop["proposed_downweight_count"] == 0
        assert prop["proposed_hold_count"] == 0


# ---------------------------------------------------------------------------
# 4. Direction logic
# ---------------------------------------------------------------------------

class TestDirectionLogic:
    def _proposal_for(self, bias_scalar, bias_code="BULL_LOW_VOL", capital=5000.0):
        env = _active_envelope([("BTC-EUR", capital, 1.0)])
        ov = _active_overlay(
            bias_by_market={"BTC-EUR": _bias(bias_scalar, bias_code)},
        )
        prop = build_allocation_proposal(ov, env)
        return prop["market_proposals"]["BTC-EUR"]

    def test_positive_bias_upweight(self):
        mp = self._proposal_for(+0.10)
        assert mp["proposed_direction"] == DIR_UPWEIGHT

    def test_negative_bias_downweight(self):
        mp = self._proposal_for(-0.10)
        assert mp["proposed_direction"] == DIR_DOWNWEIGHT

    def test_zero_bias_hold(self):
        mp = self._proposal_for(0.0, "SIDEWAYS_LOW_VOL")
        assert mp["proposed_direction"] == DIR_HOLD

    def test_no_regime_data_hold(self):
        mp = self._proposal_for(0.0, "NO_REGIME_DATA")
        assert mp["proposed_direction"] == DIR_HOLD
        assert mp["proposal_reason_code"] == "HOLD_NO_REGIME_DATA"

    def test_gate_blocked_hold(self):
        mp = self._proposal_for(0.0, "GATE_BLOCKED")
        assert mp["proposed_direction"] == DIR_HOLD
        assert mp["proposal_reason_code"] == "HOLD_GATE_BLOCKED"

    def test_zero_capital_hold(self):
        mp = self._proposal_for(+0.10, "BULL_LOW_VOL", capital=0.0)
        assert mp["proposed_direction"] == DIR_HOLD
        assert mp["proposal_reason_code"] == "HOLD_NO_CAPITAL"

    def test_tiny_positive_bias_upweight(self):
        mp = self._proposal_for(1e-8)
        assert mp["proposed_direction"] == DIR_UPWEIGHT

    def test_tiny_negative_bias_downweight(self):
        mp = self._proposal_for(-1e-8)
        assert mp["proposed_direction"] == DIR_DOWNWEIGHT


# ---------------------------------------------------------------------------
# 5. Proposed capital calculation
# ---------------------------------------------------------------------------

class TestProposedCapital:
    def _proposal(self, capital, bias_scalar, bias_code="BULL_LOW_VOL"):
        env = _active_envelope([("BTC-EUR", capital, 1.0)])
        ov = _active_overlay(
            bias_by_market={"BTC-EUR": _bias(bias_scalar, bias_code)},
        )
        return build_allocation_proposal(ov, env)["market_proposals"]["BTC-EUR"]

    def test_upweight_capital(self):
        mp = self._proposal(10_000.0, +0.10)
        assert mp["proposed_capital_eur"] == pytest.approx(11_000.0, abs=0.01)

    def test_downweight_capital(self):
        mp = self._proposal(10_000.0, -0.10)
        assert mp["proposed_capital_eur"] == pytest.approx(9_000.0, abs=0.01)

    def test_hold_capital_unchanged(self):
        mp = self._proposal(10_000.0, 0.0, "SIDEWAYS_LOW_VOL")
        assert mp["proposed_capital_eur"] == pytest.approx(10_000.0, abs=0.01)

    def test_large_negative_bias_clamped_to_zero(self):
        mp = self._proposal(10_000.0, -2.0, "BEAR_EXTREME_VOL")
        assert mp["proposed_capital_eur"] == 0.0

    def test_delta_positive_on_upweight(self):
        mp = self._proposal(10_000.0, +0.10)
        assert mp["proposed_delta_eur"] > 0

    def test_delta_negative_on_downweight(self):
        mp = self._proposal(10_000.0, -0.10)
        assert mp["proposed_delta_eur"] < 0

    def test_delta_zero_on_hold(self):
        mp = self._proposal(10_000.0, 0.0, "SIDEWAYS_LOW_VOL")
        assert mp["proposed_delta_eur"] == pytest.approx(0.0, abs=1e-6)

    def test_delta_equals_proposed_minus_current(self):
        mp = self._proposal(8_000.0, +0.05)
        assert mp["proposed_delta_eur"] == pytest.approx(
            mp["proposed_capital_eur"] - mp["current_capital_eur"], abs=1e-4
        )

    def test_no_regime_data_capital_unchanged(self):
        mp = self._proposal(5_000.0, 0.0, "NO_REGIME_DATA")
        assert mp["proposed_capital_eur"] == pytest.approx(5_000.0, abs=0.01)

    def test_gate_blocked_capital_unchanged(self):
        mp = self._proposal(5_000.0, 0.0, "GATE_BLOCKED")
        assert mp["proposed_capital_eur"] == pytest.approx(5_000.0, abs=0.01)


# ---------------------------------------------------------------------------
# 6. Counts and total_proposed_capital
# ---------------------------------------------------------------------------

class TestCountsAndTotals:
    def test_upweight_count(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(+0.10),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["proposed_upweight_count"] == 2
        assert prop["proposed_downweight_count"] == 0
        assert prop["proposed_hold_count"] == 0

    def test_downweight_count(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
            "ETH-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["proposed_downweight_count"] == 2
        assert prop["proposed_upweight_count"] == 0

    def test_mixed_counts(self):
        env = _active_envelope([
            ("BTC-EUR", 4000.0, 0.4),
            ("ETH-EUR", 3000.0, 0.3),
            ("SOL-EUR", 3000.0, 0.3),
        ])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
            "SOL-EUR": _bias(0.0, "NO_REGIME_DATA"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["proposed_upweight_count"]   == 1
        assert prop["proposed_downweight_count"] == 1
        assert prop["proposed_hold_count"]       == 1

    def test_total_proposed_capital_all_hold(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(0.0, "NO_REGIME_DATA"),
            "ETH-EUR": _bias(0.0, "NO_REGIME_DATA"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["total_proposed_capital"] == pytest.approx(10_000.0, abs=0.01)

    def test_total_proposed_capital_upweight(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(+0.10),
        })
        prop = build_allocation_proposal(ov, env)
        # 5000*1.1 + 5000*1.1 = 11000
        assert prop["total_proposed_capital"] == pytest.approx(11_000.0, abs=0.01)

    def test_total_proposed_capital_downweight(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
            "ETH-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["total_proposed_capital"] == pytest.approx(9_000.0, abs=0.01)


# ---------------------------------------------------------------------------
# 7. Per-market proposal fields
# ---------------------------------------------------------------------------

class TestMarketProposalFields:
    def _proposal(self, capital=5000.0, bias_scalar=+0.10, bias_code="BULL_LOW_VOL"):
        env = _active_envelope([("BTC-EUR", capital, 1.0)])
        ov = _active_overlay(bias_by_market={"BTC-EUR": _bias(bias_scalar, bias_code)})
        return build_allocation_proposal(ov, env)["market_proposals"]["BTC-EUR"]

    def test_has_market(self):
        assert self._proposal()["market"] == "BTC-EUR"

    def test_has_asset_class(self):
        assert self._proposal()["asset_class"] == "crypto"

    def test_has_current_capital(self):
        assert self._proposal(capital=7000.0)["current_capital_eur"] == pytest.approx(7000.0, abs=0.01)

    def test_has_bias_scalar(self):
        assert self._proposal(bias_scalar=+0.10)["bias_scalar"] == pytest.approx(+0.10, abs=1e-6)

    def test_has_bias_reason_code(self):
        mp = self._proposal(bias_code="BEAR_LOW_VOL")
        assert mp["bias_reason_code"] == "BEAR_LOW_VOL"

    def test_has_proposed_direction(self):
        assert "proposed_direction" in self._proposal()

    def test_has_proposed_capital(self):
        assert "proposed_capital_eur" in self._proposal()

    def test_has_proposed_delta(self):
        assert "proposed_delta_eur" in self._proposal()

    def test_has_proposal_reason_code(self):
        assert "proposal_reason_code" in self._proposal()


# ---------------------------------------------------------------------------
# 8. Asset class proposal aggregation
# ---------------------------------------------------------------------------

class TestAssetClassProposals:
    def test_asset_class_present(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(+0.10),
        })
        prop = build_allocation_proposal(ov, env)
        assert "crypto" in prop["asset_class_proposals"]

    def test_asset_class_current_capital_sum(self):
        env = _active_envelope([("BTC-EUR", 6000.0, 0.6), ("ETH-EUR", 4000.0, 0.4)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(0.0, "NO_REGIME_DATA"),
            "ETH-EUR": _bias(0.0, "NO_REGIME_DATA"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["asset_class_proposals"]["crypto"]["current_capital_eur"] == pytest.approx(10_000.0, abs=0.01)

    def test_asset_class_proposed_capital_sum(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(+0.10),
        })
        prop = build_allocation_proposal(ov, env)
        # 5000*1.1 + 5000*1.1 = 11000
        assert prop["asset_class_proposals"]["crypto"]["proposed_capital_eur"] == pytest.approx(11_000.0, abs=0.01)

    def test_asset_class_market_count(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={})
        prop = build_allocation_proposal(ov, env)
        assert prop["asset_class_proposals"]["crypto"]["market_count"] == 2

    def test_asset_class_upweight(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(+0.10),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["asset_class_proposals"]["crypto"]["proposed_direction"] == DIR_UPWEIGHT

    def test_asset_class_downweight(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
            "ETH-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["asset_class_proposals"]["crypto"]["proposed_direction"] == DIR_DOWNWEIGHT

    def test_asset_class_hold_on_neutral(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(0.0, "NO_REGIME_DATA"),
            "ETH-EUR": _bias(0.0, "NO_REGIME_DATA"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["asset_class_proposals"]["crypto"]["proposed_direction"] == DIR_HOLD

    def test_asset_class_delta_sum(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(+0.10),
        })
        prop = build_allocation_proposal(ov, env)
        # each: 5000*0.10 = 500 delta
        assert prop["asset_class_proposals"]["crypto"]["proposed_delta_eur"] == pytest.approx(1000.0, abs=0.01)


# ---------------------------------------------------------------------------
# 9. proposal_reason_code overall
# ---------------------------------------------------------------------------

class TestOverallReasonCode:
    def test_all_upweight(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(+0.10),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["proposal_reason_code"] == "ALL_UPWEIGHT_OR_HOLD"

    def test_all_downweight(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
            "ETH-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["proposal_reason_code"] == "ALL_DOWNWEIGHT_OR_HOLD"

    def test_mixed_directions(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(+0.10),
            "ETH-EUR": _bias(-0.10, "BEAR_LOW_VOL"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["proposal_reason_code"] == "MIXED_DIRECTIONS"

    def test_all_hold(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        ov = _active_overlay(bias_by_market={
            "BTC-EUR": _bias(0.0, "NO_REGIME_DATA"),
            "ETH-EUR": _bias(0.0, "NO_REGIME_DATA"),
        })
        prop = build_allocation_proposal(ov, env)
        assert prop["proposal_reason_code"] == "ALL_HOLD"


# ---------------------------------------------------------------------------
# 10. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_overlay_unchanged(self):
        import copy
        env = _active_envelope()
        ov = _active_overlay(bias_by_market={"BTC-EUR": _bias(+0.10), "ETH-EUR": _bias(-0.10, "BEAR_LOW_VOL")})
        ov_copy = copy.deepcopy(ov)
        build_allocation_proposal(ov, env)
        assert ov == ov_copy

    def test_envelope_unchanged(self):
        import copy
        env = _active_envelope()
        ov = _active_overlay()
        env_copy = copy.deepcopy(env)
        build_allocation_proposal(ov, env)
        assert env == env_copy

    def test_no_live_activation_field(self):
        prop = build_allocation_proposal(_active_overlay(), _active_envelope())
        assert "live_activation_allowed" not in prop

    def test_no_execution_fields(self):
        prop = build_allocation_proposal(_active_overlay(), _active_envelope())
        forbidden = {"order", "trade", "execution", "broker", "live", "position"}
        for key in prop:
            assert key.lower() not in forbidden, f"unexpected field: {key}"


# ---------------------------------------------------------------------------
# 11. Full chain via build_proposal_from_specs
# ---------------------------------------------------------------------------

class TestBuildProposalFromSpecs:
    def _specs(self):
        return [
            {
                "market": "BTC-EUR",
                "strategies": [
                    {"strategy_id": "EDGE3", "strategy_family": "MR", "weight_fraction": 0.6},
                    {"strategy_id": "EDGE4", "strategy_family": "BR", "weight_fraction": 0.4},
                ],
            },
            {
                "market": "ETH-EUR",
                "strategies": [
                    {"strategy_id": "EDGE3", "strategy_family": "MR"},
                ],
            },
        ]

    def _regimes(self):
        return {
            "BTC-EUR": {"trend_regime": "BULL",  "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
            "ETH-EUR": {"trend_regime": "BEAR",  "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
        }

    def test_returns_five_keys(self):
        result = build_proposal_from_specs(self._specs(), 10_000.0)
        for key in ("splits_result", "capital_allocation", "allocation_envelope",
                    "regime_overlay", "allocation_proposal"):
            assert key in result

    def test_proposal_active_on_valid_specs(self):
        result = build_proposal_from_specs(self._specs(), 10_000.0, market_regimes=self._regimes())
        assert result["allocation_proposal"]["proposal_status"] == PROPOSAL_ACTIVE

    def test_non_binding_pipeline(self):
        result = build_proposal_from_specs(self._specs(), 10_000.0)
        assert result["allocation_proposal"]["proposal_non_binding"] is True

    def test_simulation_only_pipeline(self):
        result = build_proposal_from_specs(self._specs(), 10_000.0)
        assert result["allocation_proposal"]["proposal_simulation_only"] is True

    def test_empty_specs_baseline(self):
        result = build_proposal_from_specs([], 10_000.0)
        assert result["allocation_proposal"]["proposal_status"] == PROPOSAL_BASELINE

    def test_no_regime_all_hold(self):
        result = build_proposal_from_specs(self._specs(), 10_000.0)
        prop = result["allocation_proposal"]
        assert prop["proposed_upweight_count"] == 0
        assert prop["proposed_downweight_count"] == 0

    def test_bull_btc_upweight(self):
        regimes = {"BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0}}
        result = build_proposal_from_specs(self._specs(), 10_000.0, market_regimes=regimes)
        btc = result["allocation_proposal"]["market_proposals"].get("BTC-EUR")
        assert btc is not None
        assert btc["proposed_direction"] == DIR_UPWEIGHT

    def test_bear_eth_downweight(self):
        regimes = {"ETH-EUR": {"trend_regime": "BEAR", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0}}
        result = build_proposal_from_specs(self._specs(), 10_000.0, market_regimes=regimes)
        eth = result["allocation_proposal"]["market_proposals"].get("ETH-EUR")
        assert eth is not None
        assert eth["proposed_direction"] == DIR_DOWNWEIGHT

    def test_ac85_overlay_still_valid_in_pipeline(self):
        result = build_proposal_from_specs(self._specs(), 10_000.0)
        ov = result["regime_overlay"]
        assert "regime_overlay_status" in ov
        assert ov["overlay_non_binding"] is True

    def test_ac84_envelope_still_valid_in_pipeline(self):
        result = build_proposal_from_specs(self._specs(), 10_000.0)
        env = result["allocation_envelope"]
        assert "allocation_envelope_status" in env
        assert env["envelope_non_binding"] is True

    def test_total_proposed_capital_is_float(self):
        result = build_proposal_from_specs(self._specs(), 10_000.0)
        tc = result["allocation_proposal"]["total_proposed_capital"]
        assert isinstance(tc, float)
