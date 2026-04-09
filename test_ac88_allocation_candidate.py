"""
AC-88: Selected Allocation Candidate + Paper Transition Preview — Test Suite

Tests build_allocation_candidate(), build_paper_transition_preview(),
build_candidate_and_transition(), and build_transition_from_specs().

Coverage:
  - Always-True flags for both candidate and transition
  - Fail-closed: invalid input → CANDIDATE_REJECTED / TRANSITION_REJECTED
  - Baseline selection/candidate → CANDIDATE_BASELINE / TRANSITION_BASELINE
  - Candidate builds from AC-87 selected_proposals correctly
  - Transition direction: INCREASE / DECREASE / HOLD
  - Transition delta = selected − current
  - transition_summary totals consistent
  - estimated_reallocation_count and estimated_hold_count
  - No allocation or execution side effects
  - Full chain via build_transition_from_specs
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

_cand_mod = _load("_cand", "ant_colony/build_allocation_candidate_lite.py")

build_allocation_candidate    = _cand_mod.build_allocation_candidate
build_paper_transition_preview = _cand_mod.build_paper_transition_preview
build_candidate_and_transition = _cand_mod.build_candidate_and_transition
build_transition_from_specs   = _cand_mod.build_transition_from_specs

CANDIDATE_ACTIVE   = _cand_mod.CANDIDATE_ACTIVE
CANDIDATE_BASELINE = _cand_mod.CANDIDATE_BASELINE
CANDIDATE_REJECTED = _cand_mod.CANDIDATE_REJECTED
TRANSITION_ACTIVE   = _cand_mod.TRANSITION_ACTIVE
TRANSITION_BASELINE = _cand_mod.TRANSITION_BASELINE
TRANSITION_REJECTED = _cand_mod.TRANSITION_REJECTED
TRANS_INCREASE = _cand_mod.TRANS_INCREASE
TRANS_DECREASE = _cand_mod.TRANS_DECREASE
TRANS_HOLD     = _cand_mod.TRANS_HOLD


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _selected_proposal(
    market: str,
    direction: str = "HOLD",
    capital: float = 5000.0,
    delta: float = 0.0,
    asset_class: str = "crypto",
    adjusted: bool = False,
):
    return {
        "market":               market,
        "asset_class":          asset_class,
        "original_direction":   direction,
        "selected_direction":   direction,
        "proposed_capital_eur": capital + delta,
        "proposed_delta_eur":   delta,
        "conflict_adjusted":    adjusted,
        "selection_reason_code": "SELECTED_PASS_THROUGH",
    }


def _active_selection(selected_proposals: dict = None):
    return {
        "selection_status":          "SELECTION_ACTIVE",
        "selection_mode":            "CLEAN_SELECTION",
        "conflict_count":            0,
        "conflicts":                 [],
        "selected_proposals":        selected_proposals or {},
        "rejected_proposals":        {},
        "selection_reason":          "test",
        "selection_reason_code":     "SELECTION_CLEAN",
        "selection_non_binding":     True,
        "selection_simulation_only": True,
    }


def _baseline_selection():
    return {
        "selection_status":          "SELECTION_BASELINE",
        "selection_mode":            "SELECTION_BASELINE",
        "conflict_count":            0,
        "conflicts":                 [],
        "selected_proposals":        {},
        "rejected_proposals":        {},
        "selection_reason":          "baseline",
        "selection_reason_code":     "SELECTION_BASELINE_HOLD",
        "selection_non_binding":     True,
        "selection_simulation_only": True,
    }


def _active_envelope(markets=None):
    """Minimal ENVELOPE_ACTIVE dict with per-market capital data."""
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


def _active_candidate(markets=None):
    """Minimal CANDIDATE_ACTIVE dict."""
    if markets is None:
        markets = [("BTC-EUR", 5500.0, "UPWEIGHT"), ("ETH-EUR", 4500.0, "DOWNWEIGHT")]
    sc = {}
    for market, capital, direction in markets:
        sc[market] = {
            "market":               market,
            "asset_class":          "crypto",
            "selected_direction":   direction,
            "proposed_capital_eur": capital,
            "proposed_delta_eur":   0.0,
            "conflict_adjusted":    False,
        }
    total = sum(c for _, c, _ in markets)
    return {
        "candidate_status":         "CANDIDATE_ACTIVE",
        "candidate_mode":           "CANDIDATE_SELECTED",
        "candidate_market_count":   len(markets),
        "candidate_total_capital":  total,
        "candidate_reason":         "test",
        "candidate_reason_code":    "CANDIDATE_BUILT_FROM_SELECTION",
        "selected_candidate":       sc,
        "candidate_non_binding":    True,
        "candidate_simulation_only": True,
    }


# ---------------------------------------------------------------------------
# 1. Always-True contract flags — candidate
# ---------------------------------------------------------------------------

class TestCandidateAlwaysTrueFlags:
    def test_active_non_binding(self):
        cand = build_allocation_candidate(_active_selection())
        assert cand["candidate_non_binding"] is True

    def test_active_simulation_only(self):
        cand = build_allocation_candidate(_active_selection())
        assert cand["candidate_simulation_only"] is True

    def test_baseline_non_binding(self):
        cand = build_allocation_candidate(_baseline_selection())
        assert cand["candidate_non_binding"] is True

    def test_baseline_simulation_only(self):
        cand = build_allocation_candidate(_baseline_selection())
        assert cand["candidate_simulation_only"] is True

    def test_rejected_non_binding(self):
        assert build_allocation_candidate(None)["candidate_non_binding"] is True

    def test_rejected_simulation_only(self):
        assert build_allocation_candidate(None)["candidate_simulation_only"] is True


# ---------------------------------------------------------------------------
# 2. Always-True contract flags — transition
# ---------------------------------------------------------------------------

class TestTransitionAlwaysTrueFlags:
    def test_active_non_binding(self):
        trans = build_paper_transition_preview(_active_candidate(), _active_envelope())
        assert trans["transition_non_binding"] is True

    def test_active_simulation_only(self):
        trans = build_paper_transition_preview(_active_candidate(), _active_envelope())
        assert trans["transition_simulation_only"] is True

    def test_baseline_non_binding(self):
        cand = build_allocation_candidate(_baseline_selection())
        trans = build_paper_transition_preview(cand, _active_envelope())
        assert trans["transition_non_binding"] is True

    def test_baseline_simulation_only(self):
        cand = build_allocation_candidate(_baseline_selection())
        trans = build_paper_transition_preview(cand, _active_envelope())
        assert trans["transition_simulation_only"] is True

    def test_rejected_non_binding(self):
        assert build_paper_transition_preview(None, None)["transition_non_binding"] is True

    def test_rejected_simulation_only(self):
        assert build_paper_transition_preview(None, None)["transition_simulation_only"] is True


# ---------------------------------------------------------------------------
# 3. Fail-closed — candidate
# ---------------------------------------------------------------------------

class TestCandidateFailClosed:
    def test_none_input(self):
        assert build_allocation_candidate(None)["candidate_status"] == CANDIDATE_REJECTED

    def test_string_input(self):
        assert build_allocation_candidate("bad")["candidate_status"] == CANDIDATE_REJECTED

    def test_missing_status_key(self):
        assert build_allocation_candidate({})["candidate_status"] == CANDIDATE_REJECTED

    def test_rejected_reason_code(self):
        assert build_allocation_candidate(None)["candidate_reason_code"] == "CANDIDATE_INVALID_INPUT"

    def test_rejected_empty_candidate(self):
        cand = build_allocation_candidate(None)
        assert cand["selected_candidate"] == {}
        assert cand["candidate_market_count"] == 0
        assert cand["candidate_total_capital"] == 0.0


# ---------------------------------------------------------------------------
# 4. Fail-closed — transition
# ---------------------------------------------------------------------------

class TestTransitionFailClosed:
    def test_none_candidate(self):
        trans = build_paper_transition_preview(None, _active_envelope())
        assert trans["transition_status"] == TRANSITION_REJECTED

    def test_none_envelope(self):
        trans = build_paper_transition_preview(_active_candidate(), None)
        assert trans["transition_status"] == TRANSITION_REJECTED

    def test_both_none(self):
        assert build_paper_transition_preview(None, None)["transition_status"] == TRANSITION_REJECTED

    def test_missing_candidate_status_key(self):
        assert build_paper_transition_preview({}, _active_envelope())["transition_status"] == TRANSITION_REJECTED

    def test_missing_envelope_status_key(self):
        cand = _active_candidate()
        assert build_paper_transition_preview(cand, {})["transition_status"] == TRANSITION_REJECTED

    def test_rejected_reason_code(self):
        assert build_paper_transition_preview(None, None)["transition_reason_code"] == "TRANSITION_INVALID_INPUT"


# ---------------------------------------------------------------------------
# 5. Baseline selection/candidate paths
# ---------------------------------------------------------------------------

class TestBaselinePaths:
    def test_baseline_selection_gives_baseline_candidate(self):
        cand = build_allocation_candidate(_baseline_selection())
        assert cand["candidate_status"] == CANDIDATE_BASELINE

    def test_rejected_selection_gives_baseline_candidate(self):
        sel = _active_selection()
        sel["selection_status"] = "SELECTION_REJECTED"
        cand = build_allocation_candidate(sel)
        assert cand["candidate_status"] == CANDIDATE_BASELINE

    def test_baseline_candidate_gives_baseline_transition(self):
        cand = build_allocation_candidate(_baseline_selection())
        trans = build_paper_transition_preview(cand, _active_envelope())
        assert trans["transition_status"] == TRANSITION_BASELINE

    def test_baseline_candidate_reason_code(self):
        cand = build_allocation_candidate(_baseline_selection())
        assert cand["candidate_reason_code"] == "CANDIDATE_BASELINE_HOLD"

    def test_baseline_transition_reason_code(self):
        cand = build_allocation_candidate(_baseline_selection())
        trans = build_paper_transition_preview(cand, _active_envelope())
        assert trans["transition_reason_code"] == "TRANSITION_BASELINE_HOLD"

    def test_baseline_transition_empty_steps(self):
        cand = build_allocation_candidate(_baseline_selection())
        trans = build_paper_transition_preview(cand, _active_envelope())
        assert trans["transition_steps"] == []

    def test_baseline_candidate_zero_market_count(self):
        cand = build_allocation_candidate(_baseline_selection())
        assert cand["candidate_market_count"] == 0


# ---------------------------------------------------------------------------
# 6. Candidate building from AC-87 selection
# ---------------------------------------------------------------------------

class TestCandidateBuilding:
    def test_candidate_active_status(self):
        sp = {"BTC-EUR": _selected_proposal("BTC-EUR", "UPWEIGHT", 5000.0, 500.0)}
        cand = build_allocation_candidate(_active_selection(sp))
        assert cand["candidate_status"] == CANDIDATE_ACTIVE

    def test_candidate_market_count(self):
        sp = {
            "BTC-EUR": _selected_proposal("BTC-EUR", "UPWEIGHT",   5000.0, 500.0),
            "ETH-EUR": _selected_proposal("ETH-EUR", "DOWNWEIGHT",  5000.0, -500.0),
        }
        cand = build_allocation_candidate(_active_selection(sp))
        assert cand["candidate_market_count"] == 2

    def test_candidate_total_capital(self):
        sp = {
            "BTC-EUR": _selected_proposal("BTC-EUR", "UPWEIGHT", 5000.0, 500.0),   # → 5500
            "ETH-EUR": _selected_proposal("ETH-EUR", "HOLD",     4000.0, 0.0),     # → 4000
        }
        cand = build_allocation_candidate(_active_selection(sp))
        assert cand["candidate_total_capital"] == pytest.approx(9500.0, abs=0.01)

    def test_selected_candidate_keys(self):
        sp = {
            "BTC-EUR": _selected_proposal("BTC-EUR"),
            "ETH-EUR": _selected_proposal("ETH-EUR"),
        }
        cand = build_allocation_candidate(_active_selection(sp))
        assert set(cand["selected_candidate"].keys()) == {"BTC-EUR", "ETH-EUR"}

    def test_selected_candidate_direction_preserved(self):
        sp = {"BTC-EUR": _selected_proposal("BTC-EUR", "DOWNWEIGHT")}
        cand = build_allocation_candidate(_active_selection(sp))
        assert cand["selected_candidate"]["BTC-EUR"]["selected_direction"] == "DOWNWEIGHT"

    def test_selected_candidate_capital_preserved(self):
        sp = {"BTC-EUR": _selected_proposal("BTC-EUR", "UPWEIGHT", 5000.0, 500.0)}
        cand = build_allocation_candidate(_active_selection(sp))
        assert cand["selected_candidate"]["BTC-EUR"]["proposed_capital_eur"] == pytest.approx(5500.0, abs=0.01)

    def test_conflict_adjusted_preserved(self):
        sp = {"BTC-EUR": _selected_proposal("BTC-EUR", "HOLD", adjusted=True)}
        cand = build_allocation_candidate(_active_selection(sp))
        assert cand["selected_candidate"]["BTC-EUR"]["conflict_adjusted"] is True

    def test_empty_selection_candidate_active_zero_markets(self):
        cand = build_allocation_candidate(_active_selection({}))
        assert cand["candidate_status"] == CANDIDATE_ACTIVE
        assert cand["candidate_market_count"] == 0


# ---------------------------------------------------------------------------
# 7. Transition direction logic
# ---------------------------------------------------------------------------

class TestTransitionDirection:
    def _transition_step(self, sel_capital, cur_capital, market="BTC-EUR"):
        cand = _active_candidate([(market, sel_capital, "HOLD")])
        env  = _active_envelope([(market, cur_capital, 1.0)])
        trans = build_paper_transition_preview(cand, env)
        return trans["transition_steps"][0]

    def test_increase_when_selected_higher(self):
        step = self._transition_step(6000.0, 5000.0)
        assert step["transition_direction"] == TRANS_INCREASE

    def test_decrease_when_selected_lower(self):
        step = self._transition_step(4000.0, 5000.0)
        assert step["transition_direction"] == TRANS_DECREASE

    def test_hold_when_equal(self):
        step = self._transition_step(5000.0, 5000.0)
        assert step["transition_direction"] == TRANS_HOLD

    def test_hold_when_tiny_delta(self):
        step = self._transition_step(5000.0 + 1e-10, 5000.0)
        assert step["transition_direction"] == TRANS_HOLD

    def test_delta_value_correct(self):
        step = self._transition_step(6000.0, 5000.0)
        assert step["delta_eur"] == pytest.approx(1000.0, abs=0.01)

    def test_delta_negative_on_decrease(self):
        step = self._transition_step(4000.0, 5000.0)
        assert step["delta_eur"] == pytest.approx(-1000.0, abs=0.01)

    def test_current_from_envelope(self):
        step = self._transition_step(5500.0, 3333.0)
        assert step["current_capital_eur"] == pytest.approx(3333.0, abs=0.01)

    def test_selected_from_candidate(self):
        step = self._transition_step(7777.0, 5000.0)
        assert step["selected_capital_eur"] == pytest.approx(7777.0, abs=0.01)

    def test_market_not_in_envelope_defaults_zero_current(self):
        """Market in candidate but not in envelope → current = 0."""
        cand = _active_candidate([("NEW-EUR", 5000.0, "HOLD")])
        env  = _active_envelope([("BTC-EUR", 5000.0, 1.0)])  # no NEW-EUR
        trans = build_paper_transition_preview(cand, env)
        step = next(s for s in trans["transition_steps"] if s["market"] == "NEW-EUR")
        assert step["current_capital_eur"] == 0.0
        assert step["transition_direction"] == TRANS_INCREASE


# ---------------------------------------------------------------------------
# 8. Transition summary totals
# ---------------------------------------------------------------------------

class TestTransitionSummary:
    def _trans(self, markets):
        """markets = [(market, sel_cap, cur_cap)]"""
        cand_markets = [(m, sc, "HOLD") for m, sc, _ in markets]
        env_markets  = [(m, cc, 1.0 / len(markets)) for m, _, cc in markets]
        cand = _active_candidate(cand_markets)
        env  = _active_envelope(env_markets)
        return build_paper_transition_preview(cand, env)

    def test_total_increase_correct(self):
        trans = self._trans([("BTC-EUR", 6000.0, 5000.0), ("ETH-EUR", 4000.0, 4000.0)])
        assert trans["transition_summary"]["total_increase_eur"] == pytest.approx(1000.0, abs=0.01)

    def test_total_decrease_correct(self):
        trans = self._trans([("BTC-EUR", 4000.0, 5000.0), ("ETH-EUR", 5000.0, 5000.0)])
        assert trans["transition_summary"]["total_decrease_eur"] == pytest.approx(1000.0, abs=0.01)

    def test_net_change_positive(self):
        trans = self._trans([("BTC-EUR", 6000.0, 5000.0)])
        assert trans["transition_summary"]["net_change_eur"] == pytest.approx(1000.0, abs=0.01)

    def test_net_change_negative(self):
        trans = self._trans([("BTC-EUR", 4000.0, 5000.0)])
        assert trans["transition_summary"]["net_change_eur"] == pytest.approx(-1000.0, abs=0.01)

    def test_net_change_zero_on_hold(self):
        trans = self._trans([("BTC-EUR", 5000.0, 5000.0)])
        assert trans["transition_summary"]["net_change_eur"] == pytest.approx(0.0, abs=0.01)

    def test_reallocation_count(self):
        trans = self._trans([
            ("BTC-EUR", 6000.0, 5000.0),  # INCREASE
            ("ETH-EUR", 4000.0, 5000.0),  # DECREASE
            ("SOL-EUR", 3000.0, 3000.0),  # HOLD
        ])
        assert trans["estimated_reallocation_count"] == 2
        assert trans["estimated_hold_count"] == 1

    def test_all_hold_zero_realloc(self):
        trans = self._trans([
            ("BTC-EUR", 5000.0, 5000.0),
            ("ETH-EUR", 3000.0, 3000.0),
        ])
        assert trans["estimated_reallocation_count"] == 0
        assert trans["estimated_hold_count"] == 2

    def test_reason_code_all_hold(self):
        trans = self._trans([("BTC-EUR", 5000.0, 5000.0)])
        assert trans["transition_reason_code"] == "TRANSITION_ALL_HOLD"

    def test_reason_code_preview_active(self):
        trans = self._trans([("BTC-EUR", 6000.0, 5000.0)])
        assert trans["transition_reason_code"] == "TRANSITION_PREVIEW_ACTIVE"


# ---------------------------------------------------------------------------
# 9. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_selection_unchanged_by_candidate_build(self):
        import copy
        sel = _active_selection({"BTC-EUR": _selected_proposal("BTC-EUR", "UPWEIGHT")})
        sel_copy = copy.deepcopy(sel)
        build_allocation_candidate(sel)
        assert sel == sel_copy

    def test_candidate_unchanged_by_transition(self):
        import copy
        cand = _active_candidate()
        env  = _active_envelope()
        cand_copy = copy.deepcopy(cand)
        build_paper_transition_preview(cand, env)
        assert cand == cand_copy

    def test_envelope_unchanged_by_transition(self):
        import copy
        cand = _active_candidate()
        env  = _active_envelope()
        env_copy = copy.deepcopy(env)
        build_paper_transition_preview(cand, env)
        assert env == env_copy

    def test_no_live_activation_field_candidate(self):
        cand = build_allocation_candidate(_active_selection())
        assert "live_activation_allowed" not in cand

    def test_no_live_activation_field_transition(self):
        trans = build_paper_transition_preview(_active_candidate(), _active_envelope())
        assert "live_activation_allowed" not in trans

    def test_no_execution_fields_candidate(self):
        cand = build_allocation_candidate(_active_selection())
        forbidden = {"order", "trade", "execution", "broker", "live", "position"}
        for key in cand:
            assert key.lower() not in forbidden

    def test_no_execution_fields_transition(self):
        trans = build_paper_transition_preview(_active_candidate(), _active_envelope())
        forbidden = {"order", "trade", "execution", "broker", "live", "position"}
        for key in trans:
            assert key.lower() not in forbidden


# ---------------------------------------------------------------------------
# 10. build_candidate_and_transition combined
# ---------------------------------------------------------------------------

class TestCombinedBuilder:
    def test_returns_two_keys(self):
        result = build_candidate_and_transition(_active_selection(), _active_envelope())
        assert "allocation_candidate"     in result
        assert "paper_transition_preview" in result

    def test_candidate_non_binding(self):
        result = build_candidate_and_transition(_active_selection(), _active_envelope())
        assert result["allocation_candidate"]["candidate_non_binding"] is True

    def test_transition_non_binding(self):
        result = build_candidate_and_transition(_active_selection(), _active_envelope())
        assert result["paper_transition_preview"]["transition_non_binding"] is True

    def test_baseline_input_gives_baseline_both(self):
        result = build_candidate_and_transition(_baseline_selection(), _active_envelope())
        assert result["allocation_candidate"]["candidate_status"]  == CANDIDATE_BASELINE
        assert result["paper_transition_preview"]["transition_status"] == TRANSITION_BASELINE


# ---------------------------------------------------------------------------
# 11. Full chain via build_transition_from_specs
# ---------------------------------------------------------------------------

class TestBuildTransitionFromSpecs:
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

    def test_returns_eight_keys(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        for key in ("splits_result", "capital_allocation", "allocation_envelope",
                    "regime_overlay", "allocation_proposal", "conflict_selection",
                    "allocation_candidate", "paper_transition_preview"):
            assert key in result

    def test_candidate_non_binding_pipeline(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        assert result["allocation_candidate"]["candidate_non_binding"] is True

    def test_candidate_simulation_only_pipeline(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        assert result["allocation_candidate"]["candidate_simulation_only"] is True

    def test_transition_non_binding_pipeline(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        assert result["paper_transition_preview"]["transition_non_binding"] is True

    def test_transition_simulation_only_pipeline(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        assert result["paper_transition_preview"]["transition_simulation_only"] is True

    def test_empty_specs_baseline(self):
        result = build_transition_from_specs([], 10_000.0)
        assert result["allocation_candidate"]["candidate_status"]       == CANDIDATE_BASELINE
        assert result["paper_transition_preview"]["transition_status"]  == TRANSITION_BASELINE

    def test_no_regime_candidate_active(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        assert result["allocation_candidate"]["candidate_status"] == CANDIDATE_ACTIVE

    def test_no_regime_all_hold_transition(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        trans = result["paper_transition_preview"]
        # No regime data → all HOLD → no reallocation
        assert trans["estimated_reallocation_count"] == 0

    def test_upweight_gives_increase_transition(self):
        regimes = {
            "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
            "ETH-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
        }
        result = build_transition_from_specs(self._specs(), 10_000.0, market_regimes=regimes)
        trans  = result["paper_transition_preview"]
        steps  = {s["market"]: s for s in trans["transition_steps"]}
        # Both UPWEIGHT → proposed_capital > current → INCREASE
        if "BTC-EUR" in steps:
            assert steps["BTC-EUR"]["transition_direction"] == TRANS_INCREASE

    def test_prior_layer_outputs_intact(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        assert result["conflict_selection"]["selection_non_binding"] is True
        assert result["allocation_proposal"]["proposal_non_binding"] is True
        assert result["regime_overlay"]["overlay_non_binding"] is True
        assert result["allocation_envelope"]["envelope_non_binding"] is True

    def test_transition_steps_have_required_fields(self):
        result = build_transition_from_specs(self._specs(), 10_000.0)
        for step in result["paper_transition_preview"]["transition_steps"]:
            for field in ("market", "asset_class", "current_capital_eur",
                          "selected_capital_eur", "delta_eur", "transition_direction"):
                assert field in step, f"missing field {field!r} in step"
