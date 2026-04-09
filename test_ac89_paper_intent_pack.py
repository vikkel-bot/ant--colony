"""
AC-89: Paper Intent Pack + Transition Audit Summary — Test Suite

Tests build_paper_intent_pack(), build_transition_audit_summary(),
build_intent_pack_and_audit(), and build_intent_pack_from_specs().

Coverage:
  - Always-True flags: intent_pack_non_binding, intent_pack_simulation_only,
    paper_only, audit_non_binding, audit_simulation_only
  - Fail-closed: invalid input → PACK_REJECTED / AUDIT_REJECTED
  - Baseline transition → PACK_BASELINE → AUDIT_BASELINE
  - INCREASE/DECREASE/HOLD steps → correct intent actions
  - intent_count / allowed_count / blocked_count consistency
  - blocked_reasons populated from blocked intents
  - Audit totals: total_increase_eur, total_decrease_eur, net_change_eur
  - total_hold_count, total_markets_reviewed
  - Invalid step fields → BLOCKED intent
  - No allocation or execution side effects
  - Full chain via build_intent_pack_from_specs
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

_pack_mod = _load("_pack", "ant_colony/build_paper_intent_pack_lite.py")

build_paper_intent_pack        = _pack_mod.build_paper_intent_pack
build_transition_audit_summary = _pack_mod.build_transition_audit_summary
build_intent_pack_and_audit    = _pack_mod.build_intent_pack_and_audit
build_intent_pack_from_specs   = _pack_mod.build_intent_pack_from_specs

PACK_ACTIVE    = _pack_mod.PACK_ACTIVE
PACK_BASELINE  = _pack_mod.PACK_BASELINE
PACK_REJECTED  = _pack_mod.PACK_REJECTED
AUDIT_COMPLETE = _pack_mod.AUDIT_COMPLETE
AUDIT_BASELINE = _pack_mod.AUDIT_BASELINE
AUDIT_REJECTED = _pack_mod.AUDIT_REJECTED
ACTION_INCREASE = _pack_mod.ACTION_INCREASE
ACTION_DECREASE = _pack_mod.ACTION_DECREASE
ACTION_HOLD     = _pack_mod.ACTION_HOLD
ACTION_BLOCKED  = _pack_mod.ACTION_BLOCKED
INTENT_ALLOWED  = _pack_mod.INTENT_ALLOWED
INTENT_BLOCKED  = _pack_mod.INTENT_BLOCKED


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _step(
    market: str = "BTC-EUR",
    direction: str = "HOLD",
    current: float = 5000.0,
    selected: float = 5000.0,
    delta: float = 0.0,
    asset_class: str = "crypto",
):
    return {
        "market":               market,
        "asset_class":          asset_class,
        "current_capital_eur":  current,
        "selected_capital_eur": selected,
        "delta_eur":            delta,
        "transition_direction": direction,
        "transition_reason_code": "PAPER_HOLD",
    }


def _active_transition(steps=None):
    if steps is None:
        steps = [_step()]
    total_inc = sum(s.get("delta_eur", 0.0) for s in steps if isinstance(s, dict) and s.get("delta_eur", 0.0) > 0)
    total_dec = sum(abs(s.get("delta_eur", 0.0)) for s in steps if isinstance(s, dict) and s.get("delta_eur", 0.0) < 0)
    return {
        "transition_status":            "TRANSITION_ACTIVE",
        "transition_mode":              "PAPER_TRANSITION_PREVIEW",
        "transition_steps":             steps,
        "transition_summary": {
            "total_increase_eur": total_inc,
            "total_decrease_eur": total_dec,
            "net_change_eur":     round(total_inc - total_dec, 4),
        },
        "estimated_reallocation_count": sum(1 for s in steps if isinstance(s, dict) and s.get("delta_eur", 0.0) != 0),
        "estimated_hold_count":         sum(1 for s in steps if isinstance(s, dict) and s.get("delta_eur", 0.0) == 0),
        "transition_reason":            "test",
        "transition_reason_code":       "TRANSITION_ALL_HOLD",
        "transition_non_binding":       True,
        "transition_simulation_only":   True,
    }


def _baseline_transition():
    return {
        "transition_status":            "TRANSITION_BASELINE",
        "transition_mode":              "TRANSITION_BASELINE",
        "transition_steps":             [],
        "transition_summary":           {"total_increase_eur": 0.0, "total_decrease_eur": 0.0, "net_change_eur": 0.0},
        "estimated_reallocation_count": 0,
        "estimated_hold_count":         0,
        "transition_reason":            "baseline",
        "transition_reason_code":       "TRANSITION_BASELINE_HOLD",
        "transition_non_binding":       True,
        "transition_simulation_only":   True,
    }


def _active_pack(intents=None):
    if intents is None:
        intents = []
    allowed  = [i for i in intents if i.get("intent_status") == INTENT_ALLOWED]
    blocked  = [i for i in intents if i.get("intent_status") == INTENT_BLOCKED]
    reasons  = sorted({i.get("block_reason","") for i in blocked if i.get("block_reason")})
    return {
        "intent_pack_status":          PACK_ACTIVE,
        "intent_pack_mode":            "PAPER_INTENT_PACK",
        "intents":                     intents,
        "intent_count":                len(intents),
        "allowed_count":               len(allowed),
        "blocked_count":               len(blocked),
        "blocked_reasons":             reasons,
        "intent_pack_non_binding":     True,
        "intent_pack_simulation_only": True,
        "paper_only":                  True,
    }


# ---------------------------------------------------------------------------
# 1. Always-True flags — intent pack
# ---------------------------------------------------------------------------

class TestPackAlwaysTrueFlags:
    def test_active_non_binding(self):
        pack = build_paper_intent_pack(_active_transition())
        assert pack["intent_pack_non_binding"] is True

    def test_active_simulation_only(self):
        pack = build_paper_intent_pack(_active_transition())
        assert pack["intent_pack_simulation_only"] is True

    def test_active_paper_only(self):
        pack = build_paper_intent_pack(_active_transition())
        assert pack["paper_only"] is True

    def test_baseline_non_binding(self):
        pack = build_paper_intent_pack(_baseline_transition())
        assert pack["intent_pack_non_binding"] is True

    def test_baseline_paper_only(self):
        pack = build_paper_intent_pack(_baseline_transition())
        assert pack["paper_only"] is True

    def test_rejected_non_binding(self):
        assert build_paper_intent_pack(None)["intent_pack_non_binding"] is True

    def test_rejected_paper_only(self):
        assert build_paper_intent_pack(None)["paper_only"] is True

    def test_per_intent_paper_only(self):
        steps = [_step("BTC-EUR", "HOLD")]
        pack = build_paper_intent_pack(_active_transition(steps))
        for intent in pack["intents"]:
            assert intent["paper_only"] is True


# ---------------------------------------------------------------------------
# 2. Always-True flags — audit
# ---------------------------------------------------------------------------

class TestAuditAlwaysTrueFlags:
    def test_active_non_binding(self):
        pack  = build_paper_intent_pack(_active_transition())
        audit = build_transition_audit_summary(pack)
        assert audit["audit_non_binding"] is True

    def test_active_simulation_only(self):
        pack  = build_paper_intent_pack(_active_transition())
        audit = build_transition_audit_summary(pack)
        assert audit["audit_simulation_only"] is True

    def test_baseline_non_binding(self):
        pack  = build_paper_intent_pack(_baseline_transition())
        audit = build_transition_audit_summary(pack)
        assert audit["audit_non_binding"] is True

    def test_rejected_non_binding(self):
        assert build_transition_audit_summary(None)["audit_non_binding"] is True


# ---------------------------------------------------------------------------
# 3. Fail-closed — intent pack
# ---------------------------------------------------------------------------

class TestPackFailClosed:
    def test_none_input(self):
        assert build_paper_intent_pack(None)["intent_pack_status"] == PACK_REJECTED

    def test_string_input(self):
        assert build_paper_intent_pack("bad")["intent_pack_status"] == PACK_REJECTED

    def test_missing_status_key(self):
        assert build_paper_intent_pack({})["intent_pack_status"] == PACK_REJECTED

    def test_rejected_reason_in_blocked_reasons(self):
        pack = build_paper_intent_pack(None)
        assert len(pack["blocked_reasons"]) >= 1

    def test_rejected_zero_intents(self):
        pack = build_paper_intent_pack(None)
        assert pack["intent_count"]   == 0
        assert pack["allowed_count"]  == 0
        assert pack["blocked_count"]  == 0
        assert pack["intents"]        == []


# ---------------------------------------------------------------------------
# 4. Fail-closed — audit
# ---------------------------------------------------------------------------

class TestAuditFailClosed:
    def test_none_input(self):
        assert build_transition_audit_summary(None)["audit_status"] == AUDIT_REJECTED

    def test_string_input(self):
        assert build_transition_audit_summary("bad")["audit_status"] == AUDIT_REJECTED

    def test_missing_status_key(self):
        assert build_transition_audit_summary({})["audit_status"] == AUDIT_REJECTED

    def test_rejected_reason_code(self):
        assert build_transition_audit_summary(None)["audit_reason_code"] == "AUDIT_INVALID_INPUT"

    def test_rejected_zero_totals(self):
        audit = build_transition_audit_summary(None)
        assert audit["total_markets_reviewed"] == 0
        assert audit["total_increase_eur"]     == 0.0
        assert audit["total_decrease_eur"]     == 0.0
        assert audit["net_change_eur"]         == 0.0


# ---------------------------------------------------------------------------
# 5. Baseline paths
# ---------------------------------------------------------------------------

class TestBaselinePaths:
    def test_baseline_transition_gives_baseline_pack(self):
        pack = build_paper_intent_pack(_baseline_transition())
        assert pack["intent_pack_status"] == PACK_BASELINE

    def test_rejected_transition_gives_baseline_pack(self):
        t = _active_transition()
        t["transition_status"] = "TRANSITION_REJECTED"
        pack = build_paper_intent_pack(t)
        assert pack["intent_pack_status"] == PACK_BASELINE

    def test_baseline_pack_gives_baseline_audit(self):
        pack  = build_paper_intent_pack(_baseline_transition())
        audit = build_transition_audit_summary(pack)
        assert audit["audit_status"] == AUDIT_BASELINE

    def test_baseline_audit_reason_code(self):
        pack  = build_paper_intent_pack(_baseline_transition())
        audit = build_transition_audit_summary(pack)
        assert audit["audit_reason_code"] == "AUDIT_BASELINE_HOLD"

    def test_baseline_pack_empty_intents(self):
        pack = build_paper_intent_pack(_baseline_transition())
        assert pack["intents"] == []
        assert pack["intent_count"] == 0


# ---------------------------------------------------------------------------
# 6. Intent action mapping
# ---------------------------------------------------------------------------

class TestIntentActionMapping:
    def _intent_for(self, direction, delta=0.0):
        step = _step("BTC-EUR", direction, 5000.0, 5000.0 + delta, delta)
        pack = build_paper_intent_pack(_active_transition([step]))
        return pack["intents"][0]

    def test_increase_gives_increase_intent(self):
        i = self._intent_for("INCREASE", delta=+500.0)
        assert i["intent_action"] == ACTION_INCREASE
        assert i["intent_status"] == INTENT_ALLOWED

    def test_decrease_gives_decrease_intent(self):
        i = self._intent_for("DECREASE", delta=-500.0)
        assert i["intent_action"] == ACTION_DECREASE
        assert i["intent_status"] == INTENT_ALLOWED

    def test_hold_gives_hold_intent(self):
        i = self._intent_for("HOLD")
        assert i["intent_action"] == ACTION_HOLD
        assert i["intent_status"] == INTENT_ALLOWED

    def test_unknown_direction_gives_blocked(self):
        step = _step("BTC-EUR", "SIDEWAYS")
        pack = build_paper_intent_pack(_active_transition([step]))
        i = pack["intents"][0]
        assert i["intent_action"]  == ACTION_BLOCKED
        assert i["intent_status"]  == INTENT_BLOCKED
        assert i["block_reason"] != ""

    def test_invalid_step_not_dict_gives_blocked(self):
        trans = _active_transition()
        trans["transition_steps"] = ["not-a-dict"]
        pack = build_paper_intent_pack(trans)
        assert pack["intents"][0]["intent_status"] == INTENT_BLOCKED

    def test_step_missing_required_field_gives_blocked(self):
        step = {"market": "BTC-EUR", "asset_class": "crypto"}  # many fields missing
        pack = build_paper_intent_pack(_active_transition([step]))
        assert pack["intents"][0]["intent_status"] == INTENT_BLOCKED


# ---------------------------------------------------------------------------
# 7. Intent fields preserved from step
# ---------------------------------------------------------------------------

class TestIntentFieldsPreserved:
    def _intent(self, market="BTC-EUR", direction="INCREASE", cur=5000.0, sel=5500.0):
        step = _step(market, direction, cur, sel, sel - cur)
        pack = build_paper_intent_pack(_active_transition([step]))
        return pack["intents"][0]

    def test_market_field(self):
        assert self._intent(market="ETH-EUR")["market"] == "ETH-EUR"

    def test_asset_class_field(self):
        step = _step("BTC-EUR", "HOLD", asset_class="etf")
        pack = build_paper_intent_pack(_active_transition([step]))
        assert pack["intents"][0]["asset_class"] == "etf"

    def test_current_capital_field(self):
        assert self._intent(cur=3333.0)["current_capital_eur"] == pytest.approx(3333.0, abs=0.01)

    def test_selected_capital_field(self):
        assert self._intent(sel=7777.0)["selected_capital_eur"] == pytest.approx(7777.0, abs=0.01)

    def test_delta_field(self):
        i = self._intent(cur=5000.0, sel=5500.0)
        assert i["delta_eur"] == pytest.approx(500.0, abs=0.01)

    def test_transition_direction_field(self):
        i = self._intent(direction="DECREASE")
        assert i["transition_direction"] == "DECREASE"

    def test_block_reason_empty_on_allowed(self):
        i = self._intent()
        assert i["block_reason"] == ""


# ---------------------------------------------------------------------------
# 8. Counts
# ---------------------------------------------------------------------------

class TestCounts:
    def test_intent_count_equals_steps(self):
        steps = [_step("BTC-EUR", "INCREASE", delta=500.0),
                 _step("ETH-EUR", "DECREASE", delta=-300.0),
                 _step("SOL-EUR", "HOLD")]
        pack = build_paper_intent_pack(_active_transition(steps))
        assert pack["intent_count"] == 3

    def test_allowed_count(self):
        steps = [_step("BTC-EUR", "INCREASE", delta=500.0),
                 _step("ETH-EUR", "HOLD")]
        pack = build_paper_intent_pack(_active_transition(steps))
        assert pack["allowed_count"] == 2
        assert pack["blocked_count"] == 0

    def test_blocked_count(self):
        steps = [_step("BTC-EUR", "INVALID_DIR")]
        pack = build_paper_intent_pack(_active_transition(steps))
        assert pack["blocked_count"] == 1
        assert pack["allowed_count"] == 0

    def test_blocked_reasons_populated(self):
        steps = [_step("BTC-EUR", "INVALID_DIR")]
        pack = build_paper_intent_pack(_active_transition(steps))
        assert len(pack["blocked_reasons"]) >= 1

    def test_blocked_reasons_distinct(self):
        steps = [_step("BTC-EUR", "BAD"), _step("ETH-EUR", "BAD")]
        pack = build_paper_intent_pack(_active_transition(steps))
        # Same reason → appears only once
        assert len(pack["blocked_reasons"]) == len(set(pack["blocked_reasons"]))

    def test_counts_sum_to_intent_count(self):
        steps = [_step("BTC-EUR", "INCREASE", delta=500.0),
                 _step("ETH-EUR", "INVALID")]
        pack = build_paper_intent_pack(_active_transition(steps))
        assert pack["allowed_count"] + pack["blocked_count"] == pack["intent_count"]


# ---------------------------------------------------------------------------
# 9. Audit totals
# ---------------------------------------------------------------------------

class TestAuditTotals:
    def _audit_for(self, steps):
        pack  = build_paper_intent_pack(_active_transition(steps))
        return build_transition_audit_summary(pack)

    def test_total_increase(self):
        steps = [_step("BTC-EUR", "INCREASE", 5000.0, 5500.0, delta=500.0),
                 _step("ETH-EUR", "HOLD")]
        audit = self._audit_for(steps)
        assert audit["total_increase_eur"] == pytest.approx(500.0, abs=0.01)

    def test_total_decrease(self):
        steps = [_step("BTC-EUR", "DECREASE", 5000.0, 4500.0, delta=-500.0)]
        audit = self._audit_for(steps)
        assert audit["total_decrease_eur"] == pytest.approx(500.0, abs=0.01)

    def test_net_change_positive(self):
        steps = [_step("BTC-EUR", "INCREASE", 5000.0, 5500.0, delta=500.0)]
        audit = self._audit_for(steps)
        assert audit["net_change_eur"] == pytest.approx(500.0, abs=0.01)

    def test_net_change_negative(self):
        steps = [_step("BTC-EUR", "DECREASE", 5000.0, 4500.0, delta=-500.0)]
        audit = self._audit_for(steps)
        assert audit["net_change_eur"] == pytest.approx(-500.0, abs=0.01)

    def test_net_change_zero_all_hold(self):
        steps = [_step("BTC-EUR", "HOLD"), _step("ETH-EUR", "HOLD")]
        audit = self._audit_for(steps)
        assert audit["net_change_eur"] == pytest.approx(0.0, abs=0.01)

    def test_total_hold_count(self):
        steps = [_step("BTC-EUR", "HOLD"),
                 _step("ETH-EUR", "HOLD"),
                 _step("SOL-EUR", "INCREASE", delta=200.0)]
        audit = self._audit_for(steps)
        assert audit["total_hold_count"] == 2

    def test_total_markets_reviewed(self):
        steps = [_step("BTC-EUR", "INCREASE", delta=100.0),
                 _step("ETH-EUR", "HOLD")]
        audit = self._audit_for(steps)
        assert audit["total_markets_reviewed"] == 2

    def test_audit_complete_status(self):
        pack  = build_paper_intent_pack(_active_transition([_step()]))
        audit = build_transition_audit_summary(pack)
        assert audit["audit_status"] == AUDIT_COMPLETE

    def test_audit_all_allowed_code(self):
        pack  = build_paper_intent_pack(_active_transition([_step("BTC-EUR", "HOLD")]))
        audit = build_transition_audit_summary(pack)
        assert audit["audit_reason_code"] == "AUDIT_ALL_ALLOWED"

    def test_audit_with_blocked_code(self):
        pack  = _active_pack([
            {"intent_action": ACTION_HOLD, "intent_status": INTENT_BLOCKED,
             "delta_eur": 0.0, "block_reason": "INVALID_STEP", "paper_only": True}
        ])
        audit = build_transition_audit_summary(pack)
        assert audit["audit_reason_code"] == "AUDIT_WITH_BLOCKED"

    def test_mixed_increase_decrease(self):
        steps = [
            _step("BTC-EUR", "INCREASE", 5000.0, 5600.0, delta=600.0),
            _step("ETH-EUR", "DECREASE", 4000.0, 3600.0, delta=-400.0),
        ]
        audit = self._audit_for(steps)
        assert audit["total_increase_eur"] == pytest.approx(600.0, abs=0.01)
        assert audit["total_decrease_eur"] == pytest.approx(400.0, abs=0.01)
        assert audit["net_change_eur"]     == pytest.approx(200.0, abs=0.01)


# ---------------------------------------------------------------------------
# 10. No side effects
# ---------------------------------------------------------------------------

class TestNoSideEffects:
    def test_transition_unchanged(self):
        import copy
        trans = _active_transition([_step()])
        trans_copy = copy.deepcopy(trans)
        build_paper_intent_pack(trans)
        assert trans == trans_copy

    def test_pack_unchanged_by_audit(self):
        import copy
        pack = build_paper_intent_pack(_active_transition([_step()]))
        pack_copy = copy.deepcopy(pack)
        build_transition_audit_summary(pack)
        assert pack == pack_copy

    def test_no_live_activation_field_pack(self):
        pack = build_paper_intent_pack(_active_transition())
        assert "live_activation_allowed" not in pack

    def test_no_live_activation_field_audit(self):
        pack  = build_paper_intent_pack(_active_transition())
        audit = build_transition_audit_summary(pack)
        assert "live_activation_allowed" not in audit

    def test_no_execution_fields_pack(self):
        pack = build_paper_intent_pack(_active_transition())
        forbidden = {"order", "trade", "execution", "broker", "live", "position"}
        for key in pack:
            assert key.lower() not in forbidden

    def test_no_execution_fields_audit(self):
        pack  = build_paper_intent_pack(_active_transition())
        audit = build_transition_audit_summary(pack)
        forbidden = {"order", "trade", "execution", "broker", "live", "position"}
        for key in audit:
            assert key.lower() not in forbidden


# ---------------------------------------------------------------------------
# 11. Combined builder
# ---------------------------------------------------------------------------

class TestCombinedBuilder:
    def test_returns_two_keys(self):
        result = build_intent_pack_and_audit(_active_transition())
        assert "intent_pack"      in result
        assert "transition_audit" in result

    def test_pack_non_binding(self):
        result = build_intent_pack_and_audit(_active_transition())
        assert result["intent_pack"]["intent_pack_non_binding"] is True

    def test_audit_non_binding(self):
        result = build_intent_pack_and_audit(_active_transition())
        assert result["transition_audit"]["audit_non_binding"] is True

    def test_baseline_gives_both_baseline(self):
        result = build_intent_pack_and_audit(_baseline_transition())
        assert result["intent_pack"]["intent_pack_status"]      == PACK_BASELINE
        assert result["transition_audit"]["audit_status"]        == AUDIT_BASELINE


# ---------------------------------------------------------------------------
# 12. Full chain via build_intent_pack_from_specs
# ---------------------------------------------------------------------------

class TestBuildIntentPackFromSpecs:
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

    def test_returns_ten_keys(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        for key in (
            "splits_result", "capital_allocation", "allocation_envelope",
            "regime_overlay", "allocation_proposal", "conflict_selection",
            "allocation_candidate", "paper_transition_preview",
            "intent_pack", "transition_audit",
        ):
            assert key in result

    def test_pack_non_binding_pipeline(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        assert result["intent_pack"]["intent_pack_non_binding"] is True

    def test_pack_simulation_only_pipeline(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        assert result["intent_pack"]["intent_pack_simulation_only"] is True

    def test_pack_paper_only_pipeline(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        assert result["intent_pack"]["paper_only"] is True

    def test_audit_non_binding_pipeline(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        assert result["transition_audit"]["audit_non_binding"] is True

    def test_audit_simulation_only_pipeline(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        assert result["transition_audit"]["audit_simulation_only"] is True

    def test_empty_specs_baseline(self):
        result = build_intent_pack_from_specs([], 10_000.0)
        assert result["intent_pack"]["intent_pack_status"]    == PACK_BASELINE
        assert result["transition_audit"]["audit_status"]      == AUDIT_BASELINE

    def test_no_regime_hold_intents(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        pack = result["intent_pack"]
        # No regime data → all HOLD → all PAPER_HOLD_INTENT
        for intent in pack["intents"]:
            assert intent["intent_action"] == ACTION_HOLD

    def test_intent_count_matches_markets(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        pack = result["intent_pack"]
        assert pack["intent_count"] == pack["allowed_count"] + pack["blocked_count"]

    def test_upweight_regime_gives_increase_intents(self):
        regimes = {
            "BTC-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
            "ETH-EUR": {"trend_regime": "BULL", "vol_regime": "LOW", "gate": "ALLOW", "size_mult": 1.0},
        }
        result = build_intent_pack_from_specs(self._specs(), 10_000.0, market_regimes=regimes)
        pack = result["intent_pack"]
        intent_map = {i["market"]: i for i in pack["intents"]}
        if "BTC-EUR" in intent_map:
            assert intent_map["BTC-EUR"]["intent_action"] == ACTION_INCREASE

    def test_prior_layer_outputs_intact(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        assert result["paper_transition_preview"]["transition_non_binding"] is True
        assert result["allocation_candidate"]["candidate_non_binding"]      is True
        assert result["conflict_selection"]["selection_non_binding"]        is True

    def test_audit_total_markets_reviewed(self):
        result = build_intent_pack_from_specs(self._specs(), 10_000.0)
        audit  = result["transition_audit"]
        pack   = result["intent_pack"]
        assert audit["total_markets_reviewed"] == pack["intent_count"]
