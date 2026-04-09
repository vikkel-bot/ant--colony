"""
AC-85: Cross-Asset Regime Overlay + Allocation Bias — Test Suite

Tests build_regime_overlay() and build_overlay_from_specs() full-chain wrapper.

Coverage:
  - Always-True flags (overlay_non_binding, overlay_simulation_only)
  - Fail-closed: invalid envelope → OVERLAY_REJECTED
  - Baseline envelope → OVERLAY_BASELINE
  - Active envelope, no regime data → OVERLAY_ACTIVE with NO_REGIME_DATA bias
  - Active envelope, full regime data → market_regimes and asset_class_regimes correct
  - Bias table: BULL/LOW, BEAR/LOW, BEAR/HIGH, BEAR/EXTREME, SIDEWAYS, GATE_BLOCKED
  - Asset class bias = mean of market biases within class
  - Missing market regime → neutral bias (NO_REGIME_DATA)
  - Overall bias_reason_code: POSITIVE_BIAS / NEGATIVE_BIAS / NEUTRAL_BIAS / NO_REGIME_DATA
  - Full chain via build_overlay_from_specs
  - Backward compatibility: AC-84 envelope output unchanged
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

_ov_mod  = _load("_ov",   "ant_colony/build_regime_overlay_lite.py")
_env_mod = _load("_env",  "ant_colony/build_allocation_envelope_lite.py")

build_regime_overlay    = _ov_mod.build_regime_overlay
build_overlay_from_specs = _ov_mod.build_overlay_from_specs

OVERLAY_ACTIVE   = _ov_mod.OVERLAY_ACTIVE
OVERLAY_BASELINE = _ov_mod.OVERLAY_BASELINE
OVERLAY_REJECTED = _ov_mod.OVERLAY_REJECTED


# ---------------------------------------------------------------------------
# Shared helpers / fixtures
# ---------------------------------------------------------------------------

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
    return {
        "allocation_envelope_status": "ENVELOPE_ACTIVE",
        "allocation_mode":            "MULTI_STRATEGY",
        "asset_class_allocations":    {"crypto": {"capital_eur": sum(c for _, c, _ in markets),
                                                   "weight_fraction": sum(f for _, _, f in markets),
                                                   "markets": [m for m, _, _ in markets]}},
        "market_allocations":         mlist,
        "total_allocated_weight":     1.0,
        "total_allocated_capital":    sum(c for _, c, _ in markets),
        "unallocated_capital":        0.0,
        "total_equity_eur":           sum(c for _, c, _ in markets),
        "allocation_reason":          "test",
        "allocation_reason_code":     "ENVELOPE_CAPITAL_ALLOCATED",
        "envelope_non_binding":       True,
        "envelope_simulation_only":   True,
    }


def _baseline_envelope():
    return {
        "allocation_envelope_status": "ENVELOPE_BASELINE",
        "allocation_mode":            "BASELINE_HOLD",
        "asset_class_allocations":    {},
        "market_allocations":         [],
        "total_allocated_weight":     0.0,
        "total_allocated_capital":    0.0,
        "unallocated_capital":        10_000.0,
        "total_equity_eur":           10_000.0,
        "allocation_reason":          "baseline",
        "allocation_reason_code":     "ENVELOPE_BASELINE_HOLD",
        "envelope_non_binding":       True,
        "envelope_simulation_only":   True,
    }


def _regime(trend="BULL", vol="LOW", gate="ALLOW", size=1.0):
    return {"trend_regime": trend, "vol_regime": vol, "gate": gate, "size_mult": size}


# ---------------------------------------------------------------------------
# 1. Always-True contract flags
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    def test_active_non_binding(self):
        ov = build_regime_overlay(_active_envelope(), {})
        assert ov["overlay_non_binding"] is True

    def test_active_simulation_only(self):
        ov = build_regime_overlay(_active_envelope(), {})
        assert ov["overlay_simulation_only"] is True

    def test_baseline_non_binding(self):
        ov = build_regime_overlay(_baseline_envelope())
        assert ov["overlay_non_binding"] is True

    def test_baseline_simulation_only(self):
        ov = build_regime_overlay(_baseline_envelope())
        assert ov["overlay_simulation_only"] is True

    def test_rejected_non_binding(self):
        ov = build_regime_overlay(None)
        assert ov["overlay_non_binding"] is True

    def test_rejected_simulation_only(self):
        ov = build_regime_overlay(None)
        assert ov["overlay_simulation_only"] is True


# ---------------------------------------------------------------------------
# 2. Fail-closed: invalid envelope → OVERLAY_REJECTED
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_none_input(self):
        assert build_regime_overlay(None)["regime_overlay_status"] == OVERLAY_REJECTED

    def test_string_input(self):
        assert build_regime_overlay("bad")["regime_overlay_status"] == OVERLAY_REJECTED

    def test_list_input(self):
        assert build_regime_overlay([])["regime_overlay_status"] == OVERLAY_REJECTED

    def test_missing_status_key(self):
        assert build_regime_overlay({"allocation_mode": "x"})["regime_overlay_status"] == OVERLAY_REJECTED

    def test_rejected_reason_code(self):
        ov = build_regime_overlay(None)
        assert ov["bias_reason_code"] == "OVERLAY_INVALID_INPUT"

    def test_rejected_empty_market_regimes(self):
        ov = build_regime_overlay(None)
        assert ov["market_regimes_summary"] == {}
        assert ov["asset_class_regimes"] == {}

    def test_rejected_empty_bias_dicts(self):
        ov = build_regime_overlay(None)
        assert ov["allocation_bias_by_market"] == {}
        assert ov["allocation_bias_by_asset_class"] == {}


# ---------------------------------------------------------------------------
# 3. Baseline envelope → OVERLAY_BASELINE
# ---------------------------------------------------------------------------

class TestBaselineOverlay:
    def test_baseline_envelope_gives_baseline_overlay(self):
        ov = build_regime_overlay(_baseline_envelope())
        assert ov["regime_overlay_status"] == OVERLAY_BASELINE

    def test_rejected_envelope_gives_baseline_overlay(self):
        env = _baseline_envelope()
        env["allocation_envelope_status"] = "ENVELOPE_REJECTED"
        ov = build_regime_overlay(env)
        assert ov["regime_overlay_status"] == OVERLAY_BASELINE

    def test_baseline_mode(self):
        ov = build_regime_overlay(_baseline_envelope())
        assert ov["regime_mode"] == "REGIME_BASELINE"

    def test_baseline_reason_code(self):
        ov = build_regime_overlay(_baseline_envelope())
        assert ov["bias_reason_code"] == "OVERLAY_BASELINE_HOLD"

    def test_baseline_empty_market_regimes(self):
        ov = build_regime_overlay(_baseline_envelope())
        assert ov["market_regimes_summary"] == {}

    def test_baseline_empty_asset_class_regimes(self):
        ov = build_regime_overlay(_baseline_envelope())
        assert ov["asset_class_regimes"] == {}


# ---------------------------------------------------------------------------
# 4. Active envelope, no regime data provided
# ---------------------------------------------------------------------------

class TestActiveNoRegimeData:
    def test_status_active(self):
        ov = build_regime_overlay(_active_envelope())
        assert ov["regime_overlay_status"] == OVERLAY_ACTIVE

    def test_mode_regime_aware(self):
        ov = build_regime_overlay(_active_envelope())
        assert ov["regime_mode"] == "REGIME_AWARE"

    def test_no_regime_data_bias_code_per_market(self):
        ov = build_regime_overlay(_active_envelope())
        for market, bias in ov["allocation_bias_by_market"].items():
            assert bias["bias_reason_code"] == "NO_REGIME_DATA", market

    def test_no_regime_data_bias_scalar_zero(self):
        ov = build_regime_overlay(_active_envelope())
        for market, bias in ov["allocation_bias_by_market"].items():
            assert bias["bias_scalar"] == 0.0, market

    def test_no_regime_data_overall_code(self):
        ov = build_regime_overlay(_active_envelope())
        assert ov["bias_reason_code"] == "NO_REGIME_DATA"

    def test_market_regime_available_false(self):
        ov = build_regime_overlay(_active_envelope())
        for m, r in ov["market_regimes_summary"].items():
            assert r["regime_available"] is False

    def test_none_regime_input_treated_as_empty(self):
        ov = build_regime_overlay(_active_envelope(), None)
        assert ov["regime_overlay_status"] == OVERLAY_ACTIVE


# ---------------------------------------------------------------------------
# 5. Bias table correctness
# ---------------------------------------------------------------------------

class TestBiasTable:
    def _single_market_bias(self, trend, vol, gate="ALLOW"):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        regimes = {"BTC-EUR": _regime(trend, vol, gate)}
        ov = build_regime_overlay(env, regimes)
        return ov["allocation_bias_by_market"]["BTC-EUR"]

    def test_bull_low_vol_positive(self):
        b = self._single_market_bias("BULL", "LOW")
        assert b["bias_scalar"] == pytest.approx(+0.10, abs=1e-6)
        assert b["bias_reason_code"] == "BULL_LOW_VOL"

    def test_bull_high_vol_positive_small(self):
        b = self._single_market_bias("BULL", "HIGH")
        assert b["bias_scalar"] == pytest.approx(+0.05, abs=1e-6)
        assert b["bias_reason_code"] == "BULL_HIGH_VOL"

    def test_bull_extreme_vol_neutral(self):
        b = self._single_market_bias("BULL", "EXTREME")
        assert b["bias_scalar"] == pytest.approx(0.0, abs=1e-6)
        assert b["bias_reason_code"] == "BULL_EXTREME_VOL"

    def test_sideways_low_vol_neutral(self):
        b = self._single_market_bias("SIDEWAYS", "LOW")
        assert b["bias_scalar"] == pytest.approx(0.0, abs=1e-6)
        assert b["bias_reason_code"] == "SIDEWAYS_LOW_VOL"

    def test_sideways_high_vol_negative(self):
        b = self._single_market_bias("SIDEWAYS", "HIGH")
        assert b["bias_scalar"] == pytest.approx(-0.05, abs=1e-6)
        assert b["bias_reason_code"] == "SIDEWAYS_HIGH_VOL"

    def test_sideways_extreme_vol_negative(self):
        b = self._single_market_bias("SIDEWAYS", "EXTREME")
        assert b["bias_scalar"] == pytest.approx(-0.10, abs=1e-6)
        assert b["bias_reason_code"] == "SIDEWAYS_EXTREME_VOL"

    def test_bear_low_vol_negative(self):
        b = self._single_market_bias("BEAR", "LOW")
        assert b["bias_scalar"] == pytest.approx(-0.10, abs=1e-6)
        assert b["bias_reason_code"] == "BEAR_LOW_VOL"

    def test_bear_high_vol_more_negative(self):
        b = self._single_market_bias("BEAR", "HIGH")
        assert b["bias_scalar"] == pytest.approx(-0.20, abs=1e-6)
        assert b["bias_reason_code"] == "BEAR_HIGH_VOL"

    def test_bear_extreme_vol_most_negative(self):
        b = self._single_market_bias("BEAR", "EXTREME")
        assert b["bias_scalar"] == pytest.approx(-0.30, abs=1e-6)
        assert b["bias_reason_code"] == "BEAR_EXTREME_VOL"

    def test_gate_blocked_zero_bias(self):
        b = self._single_market_bias("BULL", "LOW", gate="BLOCK")
        assert b["bias_scalar"] == pytest.approx(0.0, abs=1e-6)
        assert b["bias_reason_code"] == "GATE_BLOCKED"

    def test_unknown_trend_vol_neutral(self):
        b = self._single_market_bias("UNKNOWN", "UNKNOWN")
        assert b["bias_scalar"] == pytest.approx(0.0, abs=1e-6)
        assert b["bias_reason_code"] == "NEUTRAL"


# ---------------------------------------------------------------------------
# 6. Market regime aggregation
# ---------------------------------------------------------------------------

class TestMarketRegimeAggregation:
    def test_market_regimes_summary_keys(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW"),
            "ETH-EUR": _regime("BEAR", "HIGH"),
        }
        ov = build_regime_overlay(env, regimes)
        assert "BTC-EUR" in ov["market_regimes_summary"]
        assert "ETH-EUR" in ov["market_regimes_summary"]

    def test_market_regime_fields(self):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        regimes = {"BTC-EUR": _regime("BULL", "LOW", "ALLOW", 1.0)}
        ov = build_regime_overlay(env, regimes)
        r = ov["market_regimes_summary"]["BTC-EUR"]
        assert r["trend_regime"] == "BULL"
        assert r["vol_regime"]   == "LOW"
        assert r["gate"]         == "ALLOW"
        assert r["size_mult"]    == 1.0
        assert r["regime_available"] is True

    def test_missing_market_regime_available_false(self):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        ov = build_regime_overlay(env, {})
        r = ov["market_regimes_summary"]["BTC-EUR"]
        assert r["regime_available"] is False
        assert r["trend_regime"] is None

    def test_partial_regime_data(self):
        """One market has regime, one does not."""
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {"BTC-EUR": _regime("BULL", "LOW")}
        ov = build_regime_overlay(env, regimes)
        assert ov["market_regimes_summary"]["BTC-EUR"]["regime_available"] is True
        assert ov["market_regimes_summary"]["ETH-EUR"]["regime_available"] is False


# ---------------------------------------------------------------------------
# 7. Asset class regime aggregation
# ---------------------------------------------------------------------------

class TestAssetClassRegimeAggregation:
    def test_asset_class_present(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW"),
            "ETH-EUR": _regime("BEAR", "LOW"),
        }
        ov = build_regime_overlay(env, regimes)
        assert "crypto" in ov["asset_class_regimes"]

    def test_asset_class_market_count(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW"),
            "ETH-EUR": _regime("BULL", "LOW"),
        }
        ov = build_regime_overlay(env, regimes)
        assert ov["asset_class_regimes"]["crypto"]["market_count"] == 2

    def test_asset_class_trend_majority_bull(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW"),
            "ETH-EUR": _regime("BULL", "HIGH"),
        }
        ov = build_regime_overlay(env, regimes)
        assert ov["asset_class_regimes"]["crypto"]["trend_majority"] == "BULL"

    def test_asset_class_trend_majority_bear(self):
        env = _active_envelope([
            ("BTC-EUR", 3000.0, 0.3),
            ("ETH-EUR", 3000.0, 0.3),
            ("SOL-EUR", 4000.0, 0.4),
        ])
        regimes = {
            "BTC-EUR": _regime("BEAR", "LOW"),
            "ETH-EUR": _regime("BEAR", "LOW"),
            "SOL-EUR": _regime("BULL", "LOW"),
        }
        ov = build_regime_overlay(env, regimes)
        assert ov["asset_class_regimes"]["crypto"]["trend_majority"] == "BEAR"

    def test_asset_class_gate_blocked_count(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW", gate="BLOCK"),
            "ETH-EUR": _regime("BULL", "LOW", gate="ALLOW"),
        }
        ov = build_regime_overlay(env, regimes)
        assert ov["asset_class_regimes"]["crypto"]["gate_blocked_count"] == 1

    def test_asset_class_avg_size_mult(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW", size=1.0),
            "ETH-EUR": _regime("BEAR", "LOW", size=0.5),
        }
        ov = build_regime_overlay(env, regimes)
        avg = ov["asset_class_regimes"]["crypto"]["avg_size_mult"]
        assert avg == pytest.approx(0.75, abs=1e-4)

    def test_no_regime_asset_class_empty_trend(self):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        ov = build_regime_overlay(env, {})
        ac = ov["asset_class_regimes"]["crypto"]
        assert ac["trend_majority"] == ""
        assert ac["vol_majority"]   == ""


# ---------------------------------------------------------------------------
# 8. Allocation bias by asset class
# ---------------------------------------------------------------------------

class TestAllocationBiasByAssetClass:
    def test_bull_low_asset_class_positive_bias(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW"),
            "ETH-EUR": _regime("BULL", "LOW"),
        }
        ov = build_regime_overlay(env, regimes)
        assert ov["allocation_bias_by_asset_class"]["crypto"]["bias_scalar"] == pytest.approx(+0.10, abs=1e-6)

    def test_bear_low_asset_class_negative_bias(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BEAR", "LOW"),
            "ETH-EUR": _regime("BEAR", "LOW"),
        }
        ov = build_regime_overlay(env, regimes)
        assert ov["allocation_bias_by_asset_class"]["crypto"]["bias_scalar"] == pytest.approx(-0.10, abs=1e-6)

    def test_mixed_regimes_asset_class_avg_bias(self):
        """BULL+LOW (+0.10) and BEAR+LOW (-0.10) → avg = 0.0"""
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW"),
            "ETH-EUR": _regime("BEAR", "LOW"),
        }
        ov = build_regime_overlay(env, regimes)
        assert ov["allocation_bias_by_asset_class"]["crypto"]["bias_scalar"] == pytest.approx(0.0, abs=1e-6)

    def test_gate_blocked_all_markets_zero_asset_class_bias(self):
        env = _active_envelope([("BTC-EUR", 5000.0, 0.5), ("ETH-EUR", 5000.0, 0.5)])
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW", gate="BLOCK"),
            "ETH-EUR": _regime("BULL", "LOW", gate="BLOCK"),
        }
        ov = build_regime_overlay(env, regimes)
        assert ov["allocation_bias_by_asset_class"]["crypto"]["bias_scalar"] == pytest.approx(0.0, abs=1e-6)
        assert ov["allocation_bias_by_asset_class"]["crypto"]["bias_reason_code"] == "GATE_BLOCKED"

    def test_no_regime_data_asset_class_bias_code(self):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        ov = build_regime_overlay(env, {})
        assert ov["allocation_bias_by_asset_class"]["crypto"]["bias_reason_code"] == "NO_REGIME_DATA"


# ---------------------------------------------------------------------------
# 9. Overall bias_reason_code
# ---------------------------------------------------------------------------

class TestOverallBiasReasonCode:
    def test_positive_overall_bias_code(self):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        ov = build_regime_overlay(env, {"BTC-EUR": _regime("BULL", "LOW")})
        assert ov["bias_reason_code"] == "POSITIVE_BIAS"

    def test_negative_overall_bias_code(self):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        ov = build_regime_overlay(env, {"BTC-EUR": _regime("BEAR", "HIGH")})
        assert ov["bias_reason_code"] == "NEGATIVE_BIAS"

    def test_neutral_overall_bias_code_sideways_low(self):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        ov = build_regime_overlay(env, {"BTC-EUR": _regime("SIDEWAYS", "LOW")})
        assert ov["bias_reason_code"] == "NEUTRAL_BIAS"

    def test_no_regime_overall_code(self):
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        ov = build_regime_overlay(env, {})
        assert ov["bias_reason_code"] == "NO_REGIME_DATA"


# ---------------------------------------------------------------------------
# 10. No execution / allocation side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_envelope_unchanged_after_overlay(self):
        """AC-84 envelope must not be mutated."""
        env = _active_envelope()
        import copy
        env_copy = copy.deepcopy(env)
        build_regime_overlay(env, {"BTC-EUR": _regime("BULL", "LOW")})
        assert env == env_copy

    def test_market_regimes_dict_unchanged(self):
        """market_regimes input must not be mutated."""
        regimes = {"BTC-EUR": _regime("BULL", "LOW")}
        import copy
        regimes_copy = copy.deepcopy(regimes)
        env = _active_envelope([("BTC-EUR", 10_000.0, 1.0)])
        build_regime_overlay(env, regimes)
        assert regimes == regimes_copy

    def test_no_live_activation_field(self):
        """Overlay output must never have a live_activation_allowed field."""
        env = _active_envelope()
        ov = build_regime_overlay(env, {})
        assert "live_activation_allowed" not in ov

    def test_bias_is_observational_only(self):
        """Overlay must not contain any execution fields."""
        env = _active_envelope()
        ov = build_regime_overlay(env, {"BTC-EUR": _regime("BEAR", "HIGH")})
        forbidden = {"order", "trade", "execution", "live", "position", "broker"}
        for key in ov:
            assert key.lower() not in forbidden, f"unexpected execution field: {key}"


# ---------------------------------------------------------------------------
# 11. Full chain via build_overlay_from_specs
# ---------------------------------------------------------------------------

class TestBuildOverlayFromSpecs:
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

    def test_returns_four_keys(self):
        result = build_overlay_from_specs(self._two_market_specs(), 10_000.0)
        assert "splits_result"       in result
        assert "capital_allocation"  in result
        assert "allocation_envelope" in result
        assert "regime_overlay"      in result

    def test_overlay_active_on_valid_specs(self):
        result = build_overlay_from_specs(
            self._two_market_specs(), 10_000.0,
            market_regimes={"BTC-EUR": _regime("BULL", "LOW"), "ETH-EUR": _regime("BEAR", "LOW")}
        )
        assert result["regime_overlay"]["regime_overlay_status"] == OVERLAY_ACTIVE

    def test_overlay_non_binding_pipeline(self):
        result = build_overlay_from_specs(self._two_market_specs(), 10_000.0)
        assert result["regime_overlay"]["overlay_non_binding"] is True

    def test_overlay_simulation_only_pipeline(self):
        result = build_overlay_from_specs(self._two_market_specs(), 10_000.0)
        assert result["regime_overlay"]["overlay_simulation_only"] is True

    def test_empty_specs_baseline_overlay(self):
        result = build_overlay_from_specs([], 10_000.0)
        assert result["regime_overlay"]["regime_overlay_status"] == OVERLAY_BASELINE

    def test_baseline_market_only_gives_baseline_overlay(self):
        specs = [{"market": "BTC-EUR", "strategies": []}]
        result = build_overlay_from_specs(specs, 10_000.0)
        assert result["regime_overlay"]["regime_overlay_status"] == OVERLAY_BASELINE

    def test_no_regime_data_pipeline(self):
        result = build_overlay_from_specs(self._two_market_specs(), 10_000.0)
        ov = result["regime_overlay"]
        assert ov["bias_reason_code"] == "NO_REGIME_DATA"

    def test_with_regime_data_pipeline(self):
        regimes = {
            "BTC-EUR": _regime("SIDEWAYS", "LOW"),
            "ETH-EUR": _regime("BEAR",     "LOW"),
        }
        result = build_overlay_from_specs(self._two_market_specs(), 10_000.0, market_regimes=regimes)
        ov = result["regime_overlay"]
        assert "BTC-EUR" in ov["market_regimes_summary"]
        assert "ETH-EUR" in ov["market_regimes_summary"]

    def test_ac84_envelope_still_valid_in_pipeline(self):
        """AC-84 envelope output must remain well-formed."""
        result = build_overlay_from_specs(self._two_market_specs(), 10_000.0)
        env = result["allocation_envelope"]
        assert "allocation_envelope_status" in env
        assert env["envelope_non_binding"] is True
        assert env["envelope_simulation_only"] is True

    def test_positive_bias_pipeline(self):
        regimes = {
            "BTC-EUR": _regime("BULL", "LOW"),
            "ETH-EUR": _regime("BULL", "LOW"),
        }
        result = build_overlay_from_specs(self._two_market_specs(), 10_000.0, market_regimes=regimes)
        assert result["regime_overlay"]["bias_reason_code"] == "POSITIVE_BIAS"

    def test_negative_bias_pipeline(self):
        regimes = {
            "BTC-EUR": _regime("BEAR", "HIGH"),
            "ETH-EUR": _regime("BEAR", "HIGH"),
        }
        result = build_overlay_from_specs(self._two_market_specs(), 10_000.0, market_regimes=regimes)
        assert result["regime_overlay"]["bias_reason_code"] == "NEGATIVE_BIAS"
