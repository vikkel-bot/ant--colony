"""
AC-87: Proposal Conflict Resolver + Safe Selection — Test Suite

Tests resolve_proposal_conflicts() and build_selection_from_specs() wrapper.

Coverage:
  - Always-True flags (selection_non_binding, selection_simulation_only)
  - Fail-closed: invalid proposal → SELECTION_REJECTED
  - Baseline proposal → SELECTION_BASELINE
  - Clean selection: no conflicts → CLEAN_SELECTION, all passed through
  - OPPOSITE_DIRECTIONS_IN_ASSET_CLASS conflict detection + resolution
  - MARKET_AC_DIRECTION_MISMATCH conflict detection + resolution
  - INVALID_PROPOSAL_FIELDS detection + rejection
  - Multiple conflict types simultaneously
  - DOWNWEIGHT not downgraded during opposite-direction conflict (caution rule)
  - conflict_count, conflicts list
  - selected_proposals / rejected_proposals structure
  - No allocation or execution side effects
  - Full chain via build_selection_from_specs
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

_sel_mod = _load("_sel", "ant_colony/build_proposal_conflict_resolver_lite.py")

resolve_proposal_conflicts = _sel_mod.resolve_proposal_conflicts
build_selection_from_specs = _sel_mod.build_selection_from_specs

SELECTION_ACTIVE   = _sel_mod.SELECTION_ACTIVE
SELECTION_BASELINE = _sel_mod.SELECTION_BASELINE
SELECTION_REJECTED = _sel_mod.SELECTION_REJECTED

CONFLICT_OPPOSITE_DIRS  = _sel_mod.CONFLICT_OPPOSITE_DIRS
CONFLICT_AC_MISMATCH    = _sel_mod.CONFLICT_AC_MISMATCH
CONFLICT_INVALID_FIELDS = _sel_mod.CONFLICT_INVALID_FIELDS

DIR_UPWEIGHT   = _sel_mod.DIR_UPWEIGHT
DIR_DOWNWEIGHT = _sel_mod.DIR_DOWNWEIGHT
DIR_HOLD       = _sel_mod.DIR_HOLD


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _market_proposal(
    market: str,
    direction: str,
    asset_class: str = "crypto",
    capital: float = 5000.0,
    delta: float = 0.0,
    bias_scalar: float = 0.0,
    bias_code: str = "SIDEWAYS_LOW_VOL",
    prop_code: str = "HOLD_NEUTRAL_BIAS",
):
    return {
        "market":               market,
        "asset_class":          asset_class,
        "current_capital_eur":  capital,
        "bias_scalar":          bias_scalar,
        "bias_reason_code":     bias_code,
        "proposed_direction":   direction,
        "proposed_capital_eur": capital + delta,
        "proposed_delta_eur":   delta,
        "proposal_reason_code": prop_code,
    }


def _ac_proposal(
    asset_class: str,
    direction: str,
    capital: float = 10_000.0,
):
    return {
        "asset_class":          asset_class,
        "current_capital_eur":  capital,
        "bias_scalar":          0.0,
        "proposed_direction":   direction,
        "proposed_capital_eur": capital,
        "proposed_delta_eur":   0.0,
        "market_count":         1,
        "proposal_reason_code": "HOLD_NEUTRAL_BIAS",
    }


def _active_proposal(
    market_proposals: dict = None,
    ac_proposals: dict = None,
):
    """Craft a minimal PROPOSAL_ACTIVE dict (AC-86 shape)."""
    return {
        "proposal_status":           "PROPOSAL_ACTIVE",
        "proposal_mode":             "REGIME_ADJUSTED",
        "asset_class_proposals":     ac_proposals or {},
        "market_proposals":          market_proposals or {},
        "total_proposed_capital":    10_000.0,
        "proposed_upweight_count":   0,
        "proposed_downweight_count": 0,
        "proposed_hold_count":       0,
        "proposal_reason":           "test",
        "proposal_reason_code":      "ALL_HOLD",
        "proposal_non_binding":      True,
        "proposal_simulation_only":  True,
    }


def _baseline_proposal():
    return {
        "proposal_status":           "PROPOSAL_BASELINE",
        "proposal_mode":             "PROPOSAL_BASELINE",
        "asset_class_proposals":     {},
        "market_proposals":          {},
        "total_proposed_capital":    0.0,
        "proposed_upweight_count":   0,
        "proposed_downweight_count": 0,
        "proposed_hold_count":       0,
        "proposal_reason":           "baseline",
        "proposal_reason_code":      "PROPOSAL_BASELINE_HOLD",
        "proposal_non_binding":      True,
        "proposal_simulation_only":  True,
    }


# ---------------------------------------------------------------------------
# 1. Always-True contract flags
# ---------------------------------------------------------------------------

class TestAlwaysTrueFlags:
    def test_active_non_binding(self):
        sel = resolve_proposal_conflicts(_active_proposal())
        assert sel["selection_non_binding"] is True

    def test_active_simulation_only(self):
        sel = resolve_proposal_conflicts(_active_proposal())
        assert sel["selection_simulation_only"] is True

    def test_baseline_non_binding(self):
        sel = resolve_proposal_conflicts(_baseline_proposal())
        assert sel["selection_non_binding"] is True

    def test_baseline_simulation_only(self):
        sel = resolve_proposal_conflicts(_baseline_proposal())
        assert sel["selection_simulation_only"] is True

    def test_rejected_non_binding(self):
        sel = resolve_proposal_conflicts(None)
        assert sel["selection_non_binding"] is True

    def test_rejected_simulation_only(self):
        sel = resolve_proposal_conflicts(None)
        assert sel["selection_simulation_only"] is True


# ---------------------------------------------------------------------------
# 2. Fail-closed
# ---------------------------------------------------------------------------

class TestFailClosed:
    def test_none_input(self):
        assert resolve_proposal_conflicts(None)["selection_status"] == SELECTION_REJECTED

    def test_string_input(self):
        assert resolve_proposal_conflicts("bad")["selection_status"] == SELECTION_REJECTED

    def test_list_input(self):
        assert resolve_proposal_conflicts([])["selection_status"] == SELECTION_REJECTED

    def test_missing_proposal_status_key(self):
        assert resolve_proposal_conflicts({})["selection_status"] == SELECTION_REJECTED

    def test_rejected_reason_code(self):
        assert resolve_proposal_conflicts(None)["selection_reason_code"] == "SELECTION_INVALID_INPUT"

    def test_rejected_empty_dicts(self):
        sel = resolve_proposal_conflicts(None)
        assert sel["selected_proposals"] == {}
        assert sel["rejected_proposals"] == {}
        assert sel["conflicts"] == []
        assert sel["conflict_count"] == 0


# ---------------------------------------------------------------------------
# 3. Baseline proposal → SELECTION_BASELINE
# ---------------------------------------------------------------------------

class TestBaselineSelection:
    def test_baseline_proposal_gives_baseline(self):
        sel = resolve_proposal_conflicts(_baseline_proposal())
        assert sel["selection_status"] == SELECTION_BASELINE

    def test_rejected_proposal_gives_baseline(self):
        p = _active_proposal()
        p["proposal_status"] = "PROPOSAL_REJECTED"
        sel = resolve_proposal_conflicts(p)
        assert sel["selection_status"] == SELECTION_BASELINE

    def test_baseline_mode(self):
        sel = resolve_proposal_conflicts(_baseline_proposal())
        assert sel["selection_mode"] == "SELECTION_BASELINE"

    def test_baseline_reason_code(self):
        sel = resolve_proposal_conflicts(_baseline_proposal())
        assert sel["selection_reason_code"] == "SELECTION_BASELINE_HOLD"

    def test_baseline_zero_conflicts(self):
        sel = resolve_proposal_conflicts(_baseline_proposal())
        assert sel["conflict_count"] == 0
        assert sel["conflicts"] == []


# ---------------------------------------------------------------------------
# 4. Clean selection (no conflicts)
# ---------------------------------------------------------------------------

class TestCleanSelection:
    def test_all_hold_clean(self):
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_HOLD),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_HOLD),
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert sel["selection_status"] == SELECTION_ACTIVE
        assert sel["selection_mode"]   == "CLEAN_SELECTION"
        assert sel["conflict_count"]   == 0

    def test_all_upweight_same_class_clean(self):
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_UPWEIGHT),
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert sel["selection_mode"]  == "CLEAN_SELECTION"
        assert sel["conflict_count"]  == 0

    def test_all_downweight_same_class_clean(self):
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_DOWNWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_DOWNWEIGHT),
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert sel["conflict_count"] == 0

    def test_clean_pass_through_direction(self):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT)}
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        s = sel["selected_proposals"]["BTC-EUR"]
        assert s["selected_direction"] == DIR_UPWEIGHT
        assert s["conflict_adjusted"]  is False

    def test_clean_reason_code(self):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", DIR_HOLD)}
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert sel["selection_reason_code"] == "SELECTION_CLEAN"

    def test_markets_in_selected(self):
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_HOLD),
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert set(sel["selected_proposals"].keys()) == {"BTC-EUR", "ETH-EUR"}

    def test_no_rejected_on_clean(self):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", DIR_HOLD)}
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert sel["rejected_proposals"] == {}


# ---------------------------------------------------------------------------
# 5. OPPOSITE_DIRECTIONS_IN_ASSET_CLASS conflict
# ---------------------------------------------------------------------------

class TestOppositeDirsConflict:
    def _opposite_proposal(self):
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_DOWNWEIGHT),
        }
        return _active_proposal(mp)

    def test_conflict_detected(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        assert sel["conflict_count"] >= 1

    def test_conflict_type_correct(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        types = [c["conflict_type"] for c in sel["conflicts"]]
        assert CONFLICT_OPPOSITE_DIRS in types

    def test_upweight_downgraded_to_hold(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        btc = sel["selected_proposals"]["BTC-EUR"]
        assert btc["selected_direction"] == DIR_HOLD
        assert btc["conflict_adjusted"]  is True

    def test_downweight_not_downgraded(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        eth = sel["selected_proposals"]["ETH-EUR"]
        assert eth["selected_direction"] == DIR_DOWNWEIGHT
        assert eth["conflict_adjusted"]  is False

    def test_original_direction_preserved(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        btc = sel["selected_proposals"]["BTC-EUR"]
        assert btc["original_direction"] == DIR_UPWEIGHT

    def test_conflict_mode_resolved(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        assert sel["selection_mode"] == "CONFLICT_RESOLVED"

    def test_conflict_reason_code(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        assert sel["selection_reason_code"] == "SELECTION_CONFLICT_RESOLVED"

    def test_conflict_asset_class_present(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        opp = [c for c in sel["conflicts"] if c["conflict_type"] == CONFLICT_OPPOSITE_DIRS]
        assert len(opp) >= 1
        assert opp[0]["asset_class"] == "crypto"

    def test_conflict_markets_listed(self):
        sel = resolve_proposal_conflicts(self._opposite_proposal())
        opp = [c for c in sel["conflicts"] if c["conflict_type"] == CONFLICT_OPPOSITE_DIRS]
        markets = opp[0]["markets"]
        assert "BTC-EUR" in markets
        assert "ETH-EUR" in markets

    def test_three_markets_two_up_one_down(self):
        """Both UPWEIGHT markets should be downgraded to HOLD."""
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT),
            "SOL-EUR": _market_proposal("SOL-EUR", DIR_UPWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_DOWNWEIGHT),
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert sel["selected_proposals"]["BTC-EUR"]["selected_direction"] == DIR_HOLD
        assert sel["selected_proposals"]["SOL-EUR"]["selected_direction"] == DIR_HOLD
        assert sel["selected_proposals"]["ETH-EUR"]["selected_direction"] == DIR_DOWNWEIGHT


# ---------------------------------------------------------------------------
# 6. MARKET_AC_DIRECTION_MISMATCH conflict
# ---------------------------------------------------------------------------

class TestMarketAcMismatch:
    def _mismatch_proposal(self):
        # Market is UPWEIGHT but asset class is DOWNWEIGHT
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT)}
        ac = {"crypto": _ac_proposal("crypto", DIR_DOWNWEIGHT)}
        return _active_proposal(mp, ac)

    def test_conflict_detected(self):
        sel = resolve_proposal_conflicts(self._mismatch_proposal())
        assert sel["conflict_count"] >= 1

    def test_conflict_type_correct(self):
        sel = resolve_proposal_conflicts(self._mismatch_proposal())
        types = [c["conflict_type"] for c in sel["conflicts"]]
        assert CONFLICT_AC_MISMATCH in types

    def test_market_downgraded_to_hold(self):
        sel = resolve_proposal_conflicts(self._mismatch_proposal())
        btc = sel["selected_proposals"]["BTC-EUR"]
        assert btc["selected_direction"] == DIR_HOLD
        assert btc["conflict_adjusted"]  is True

    def test_downmarket_with_upward_ac_mismatch(self):
        # Market is DOWNWEIGHT but asset class is UPWEIGHT → mismatch
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", DIR_DOWNWEIGHT)}
        ac = {"crypto": _ac_proposal("crypto", DIR_UPWEIGHT)}
        sel = resolve_proposal_conflicts(_active_proposal(mp, ac))
        btc = sel["selected_proposals"]["BTC-EUR"]
        assert btc["selected_direction"] == DIR_HOLD

    def test_hold_ac_never_causes_mismatch(self):
        # AC is HOLD → no mismatch regardless of market direction
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT)}
        ac = {"crypto": _ac_proposal("crypto", DIR_HOLD)}
        sel = resolve_proposal_conflicts(_active_proposal(mp, ac))
        assert sel["conflict_count"] == 0
        assert sel["selected_proposals"]["BTC-EUR"]["selected_direction"] == DIR_UPWEIGHT

    def test_matching_directions_no_conflict(self):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT)}
        ac = {"crypto": _ac_proposal("crypto", DIR_UPWEIGHT)}
        sel = resolve_proposal_conflicts(_active_proposal(mp, ac))
        types = [c["conflict_type"] for c in sel["conflicts"]]
        assert CONFLICT_AC_MISMATCH not in types

    def test_conflict_reason_code_resolved(self):
        sel = resolve_proposal_conflicts(self._mismatch_proposal())
        assert sel["selection_reason_code"] == "SELECTION_CONFLICT_RESOLVED"


# ---------------------------------------------------------------------------
# 7. INVALID_PROPOSAL_FIELDS
# ---------------------------------------------------------------------------

class TestInvalidFields:
    def test_missing_direction_field(self):
        mp = {
            "BTC-EUR": {
                "market": "BTC-EUR", "asset_class": "crypto",
                "proposed_capital_eur": 5000.0, "proposed_delta_eur": 0.0,
                # proposed_direction missing
            }
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert "BTC-EUR" in sel["rejected_proposals"]

    def test_invalid_direction_value(self):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", "SIDEWAYS")}
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert "BTC-EUR" in sel["rejected_proposals"]

    def test_non_numeric_capital(self):
        mp = {
            "BTC-EUR": {
                "market": "BTC-EUR", "asset_class": "crypto",
                "proposed_direction": DIR_HOLD,
                "proposed_capital_eur": "not-a-number",
                "proposed_delta_eur": 0.0,
            }
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert "BTC-EUR" in sel["rejected_proposals"]

    def test_non_dict_entry(self):
        mp = {"BTC-EUR": "not-a-dict"}
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert "BTC-EUR" in sel["rejected_proposals"]

    def test_invalid_market_not_in_selected(self):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", "INVALID_DIR")}
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert "BTC-EUR" not in sel["selected_proposals"]

    def test_invalid_conflict_type_recorded(self):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", "INVALID_DIR")}
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        types = [c["conflict_type"] for c in sel["conflicts"]]
        assert CONFLICT_INVALID_FIELDS in types

    def test_valid_market_still_selected_when_one_invalid(self):
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", "INVALID"),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_HOLD),
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert "ETH-EUR" in sel["selected_proposals"]

    def test_rejection_entry_has_code(self):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", "INVALID")}
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        rej = sel["rejected_proposals"]["BTC-EUR"]
        assert rej["rejection_code"] == "REJECTED_INVALID_FIELDS"


# ---------------------------------------------------------------------------
# 8. Multiple conflict types simultaneously
# ---------------------------------------------------------------------------

class TestMultipleConflicts:
    def test_opposite_plus_invalid(self):
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_DOWNWEIGHT),
            "SOL-EUR": _market_proposal("SOL-EUR", "INVALID_DIR"),
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        types = {c["conflict_type"] for c in sel["conflicts"]}
        assert CONFLICT_OPPOSITE_DIRS  in types
        assert CONFLICT_INVALID_FIELDS in types
        assert sel["conflict_count"] >= 2

    def test_opposite_plus_mismatch(self):
        # BTC UPWEIGHT, ETH DOWNWEIGHT → opposite conflict within crypto
        # BTC also mismatches if AC is DOWNWEIGHT
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_DOWNWEIGHT),
        }
        ac = {"crypto": _ac_proposal("crypto", DIR_DOWNWEIGHT)}
        sel = resolve_proposal_conflicts(_active_proposal(mp, ac))
        # At least opposite dirs conflict; BTC should be downgraded
        btc = sel["selected_proposals"]["BTC-EUR"]
        assert btc["selected_direction"] == DIR_HOLD

    def test_conflict_count_equals_len_conflicts(self):
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_DOWNWEIGHT),
        }
        sel = resolve_proposal_conflicts(_active_proposal(mp))
        assert sel["conflict_count"] == len(sel["conflicts"])


# ---------------------------------------------------------------------------
# 9. Selected proposal fields
# ---------------------------------------------------------------------------

class TestSelectedProposalFields:
    def _sel(self, direction=DIR_HOLD):
        mp = {"BTC-EUR": _market_proposal("BTC-EUR", direction, capital=7000.0, delta=500.0)}
        return resolve_proposal_conflicts(_active_proposal(mp))["selected_proposals"]["BTC-EUR"]

    def test_has_market(self):
        assert self._sel()["market"] == "BTC-EUR"

    def test_has_asset_class(self):
        assert self._sel()["asset_class"] == "crypto"

    def test_has_original_direction(self):
        assert "original_direction" in self._sel()

    def test_has_selected_direction(self):
        assert "selected_direction" in self._sel()

    def test_has_proposed_capital(self):
        s = self._sel()
        assert "proposed_capital_eur" in s
        assert isinstance(s["proposed_capital_eur"], float)

    def test_has_proposed_delta(self):
        assert "proposed_delta_eur" in self._sel()

    def test_has_conflict_adjusted(self):
        s = self._sel()
        assert isinstance(s["conflict_adjusted"], bool)

    def test_has_selection_reason_code(self):
        assert "selection_reason_code" in self._sel()

    def test_capital_preserved_from_proposal(self):
        s = self._sel(DIR_UPWEIGHT)
        assert s["proposed_capital_eur"] == pytest.approx(7500.0, abs=0.01)


# ---------------------------------------------------------------------------
# 10. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_proposal_unchanged(self):
        import copy
        mp = {
            "BTC-EUR": _market_proposal("BTC-EUR", DIR_UPWEIGHT),
            "ETH-EUR": _market_proposal("ETH-EUR", DIR_DOWNWEIGHT),
        }
        prop = _active_proposal(mp)
        prop_copy = copy.deepcopy(prop)
        resolve_proposal_conflicts(prop)
        assert prop == prop_copy

    def test_no_live_activation_field(self):
        sel = resolve_proposal_conflicts(_active_proposal())
        assert "live_activation_allowed" not in sel

    def test_no_execution_fields(self):
        sel = resolve_proposal_conflicts(_active_proposal())
        forbidden = {"order", "trade", "execution", "broker", "live", "position"}
        for key in sel:
            assert key.lower() not in forbidden


# ---------------------------------------------------------------------------
# 11. Full chain via build_selection_from_specs
# ---------------------------------------------------------------------------

class TestBuildSelectionFromSpecs:
    def _specs(self):
        return [
            {
                "market": "BTC-EUR",
                "strategies": [
                    {"strategy_id": "EDGE3", "strategy_family": "MR", "weight_fraction": 0.5},
                    {"strategy_id": "EDGE4", "strategy_family": "BR", "weight_fraction": 0.5},
                ],
            },
            {
                "market": "ETH-EUR",
                "strategies": [
                    {"strategy_id": "EDGE3", "strategy_family": "MR"},
                ],
            },
        ]

    def test_returns_six_keys(self):
        result = build_selection_from_specs(self._specs(), 10_000.0)
        for key in ("splits_result", "capital_allocation", "allocation_envelope",
                    "regime_overlay", "allocation_proposal", "conflict_selection"):
            assert key in result

    def test_selection_non_binding_pipeline(self):
        result = build_selection_from_specs(self._specs(), 10_000.0)
        assert result["conflict_selection"]["selection_non_binding"] is True

    def test_selection_simulation_only_pipeline(self):
        result = build_selection_from_specs(self._specs(), 10_000.0)
        assert result["conflict_selection"]["selection_simulation_only"] is True

    def test_empty_specs_baseline_selection(self):
        result = build_selection_from_specs([], 10_000.0)
        assert result["conflict_selection"]["selection_status"] == SELECTION_BASELINE

    def test_no_regime_clean_hold(self):
        result = build_selection_from_specs(self._specs(), 10_000.0)
        sel = result["conflict_selection"]
        # No regime data → all HOLD → no conflicts
        assert sel["conflict_count"] == 0

    def test_conflicting_regimes_detected(self):
        regimes = {
            "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
            "ETH-EUR": {"trend_regime": "BEAR", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        }
        result = build_selection_from_specs(self._specs(), 10_000.0, market_regimes=regimes)
        sel = result["conflict_selection"]
        assert sel["conflict_count"] >= 1
        types = {c["conflict_type"] for c in sel["conflicts"]}
        assert CONFLICT_OPPOSITE_DIRS in types

    def test_conflicting_upweight_downgraded(self):
        regimes = {
            "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
            "ETH-EUR": {"trend_regime": "BEAR", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
        }
        result = build_selection_from_specs(self._specs(), 10_000.0, market_regimes=regimes)
        btc = result["conflict_selection"]["selected_proposals"].get("BTC-EUR")
        assert btc is not None
        assert btc["selected_direction"] == DIR_HOLD

    def test_downweight_stays_after_conflict(self):
        regimes = {
            "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
            "ETH-EUR": {"trend_regime": "BEAR", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
        }
        result = build_selection_from_specs(self._specs(), 10_000.0, market_regimes=regimes)
        eth = result["conflict_selection"]["selected_proposals"].get("ETH-EUR")
        assert eth is not None
        assert eth["selected_direction"] == DIR_DOWNWEIGHT

    def test_ac86_proposal_still_valid(self):
        result = build_selection_from_specs(self._specs(), 10_000.0)
        prop = result["allocation_proposal"]
        assert "proposal_status" in prop
        assert prop["proposal_non_binding"] is True

    def test_consistent_regimes_clean_selection(self):
        regimes = {
            "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
            "ETH-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
        }
        result = build_selection_from_specs(self._specs(), 10_000.0, market_regimes=regimes)
        sel = result["conflict_selection"]
        assert sel["selection_mode"] == "CLEAN_SELECTION"
        assert sel["conflict_count"] == 0
