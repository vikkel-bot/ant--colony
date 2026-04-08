"""
AC49 Synthetic regime-cap validation
Validates AC48 guardrail logic in combination with AC45 smoothing and AC47 audit trail.
Imports production helpers directly — no file I/O, no pipeline modification.

Usage: python test_ac49_regime_caps.py
"""
import importlib.util
from pathlib import Path

# --- Load production module ---
_spec = importlib.util.spec_from_file_location(
    "build_execution_intents_lite",
    Path(__file__).parent / "ant_colony" / "build_execution_intents_lite.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

derive_market_regime_cap     = _mod.derive_market_regime_cap
apply_allocation_guardrails  = _mod.apply_allocation_guardrails
smooth_market_allocations    = _mod.smooth_market_allocations
build_audit_decision_reason  = _mod.build_audit_decision_reason
build_market_decision_reason = _mod.build_market_decision_reason
SMOOTHING_RETAIN = _mod.SMOOTHING_RETAIN
SMOOTHING_ALPHA  = _mod.SMOOTHING_ALPHA
REGIME_CAP_TREND    = _mod.REGIME_CAP_TREND
REGIME_CAP_SIDEWAYS = _mod.REGIME_CAP_SIDEWAYS
REGIME_CAP_BEAR     = _mod.REGIME_CAP_BEAR

# ---------------------------------------------------------------------------
MARKET     = "BTC-EUR"
STRATEGIES = ["EDGE3", "EDGE4"]
EPS        = 1e-6


def make_alloc_map(edge3_pct: float, edge4_pct: float) -> dict:
    """Minimal alloc_map for guardrail input."""
    return {
        "EDGE3": {"allocation_pct": edge3_pct},
        "EDGE4": {"allocation_pct": edge4_pct},
    }


def run_guardrail(label: str, cb20_trend: str, edge3_pct: float, edge4_pct: float,
                  description: str = "") -> tuple:
    """Run regime derivation + guardrail; print compact result. Returns (result, regime_type, regime_cap)."""
    regime_type, regime_cap = derive_market_regime_cap({"cb20_trend": cb20_trend})
    alloc_map = make_alloc_map(edge3_pct, edge4_pct)
    result = apply_allocation_guardrails(MARKET, alloc_map, STRATEGIES, regime_type, regime_cap)

    total = round(sum(result[s]["allocation_pct"] for s in STRATEGIES), 6)
    guardrail_applied = any(result[s]["guardrail_adjusted"] for s in STRATEGIES)

    print(f"\n{'='*64}")
    print(f"SCENARIO {label}" + (f"  —  {description}" if description else ""))
    print(f"{'='*64}")
    print(f"  regime={regime_type}  cap={regime_cap}")
    print(f"  {'Strategy':<8} {'pre':>7} → {'final':>7}  {'adj':>5}  reason")
    print(f"  {'-'*60}")
    for s in STRATEGIES:
        r = result[s]
        print(
            f"  {s:<8} {r['allocation_pre_guardrail_pct']:>7.4f} → "
            f"{r['allocation_pct']:>7.4f}  {str(r['guardrail_adjusted']):>5}  "
            f"{r['guardrail_reason']}"
        )
    print(f"  sum={total:.6f} {'✓' if abs(total - 1.0) < EPS else '✗ ERROR'}"
          f"   market_guardrail_applied={guardrail_applied}")

    return result, regime_type, regime_cap


def run_smooth_then_guardrail(
    label: str, description: str,
    cb20_trend: str,
    biased: dict,          # {"EDGE3": float, "EDGE4": float}
    prev_memory: dict,     # {"EDGE3": float, "EDGE4": float}
) -> tuple:
    """Run smoothing → guardrail chain. Returns (result, regime_type, regime_cap, post_smooth_pcts)."""
    regime_type, regime_cap = derive_market_regime_cap({"cb20_trend": cb20_trend})

    # Build alloc_map with biased targets as allocation_pct (confidence=1.0 → no gating change)
    alloc_map = {s: {"allocation_pct": biased[s]} for s in STRATEGIES}

    # Build memory_state
    memory_state = {
        f"{MARKET}__{s}": {"previous_allocation_pct": prev_memory[s]}
        for s in STRATEGIES
    }

    # AC45 smooth
    alloc_map = smooth_market_allocations(MARKET, alloc_map, memory_state, STRATEGIES)
    post_smooth = {s: alloc_map[s]["allocation_pct"] for s in STRATEGIES}

    # AC48 guardrails
    result = apply_allocation_guardrails(MARKET, alloc_map, STRATEGIES, regime_type, regime_cap)

    total = round(sum(result[s]["allocation_pct"] for s in STRATEGIES), 6)
    guardrail_applied = any(result[s]["guardrail_adjusted"] for s in STRATEGIES)

    print(f"\n{'='*64}")
    print(f"SCENARIO {label}  —  {description}")
    print(f"{'='*64}")
    print(f"  regime={regime_type}  cap={regime_cap}")
    print(f"  {'Strategy':<8} {'target':>7}  {'prev':>7} → {'smoothed':>9} → {'final':>7}  adj  reason")
    print(f"  {'-'*64}")
    for s in STRATEGIES:
        r = result[s]
        print(
            f"  {s:<8} {biased[s]:>7.4f}  {prev_memory[s]:>7.4f} → "
            f"{post_smooth[s]:>9.4f} → {r['allocation_pct']:>7.4f}"
            f"  {str(r['guardrail_adjusted']):>4}  {r['guardrail_reason']}"
        )
    print(f"  sum={total:.6f} {'✓' if abs(total - 1.0) < EPS else '✗ ERROR'}"
          f"   market_guardrail_applied={guardrail_applied}")

    return result, regime_type, regime_cap, post_smooth


# ===========================================================================
# SCENARIOS
# ===========================================================================
print("\n" + "#" * 64)
print("  AC49 SYNTHETIC REGIME-CAP VALIDATION")
print("#" * 64)

# --- A: BULL, winner below cap ---
res_a, rt_a, rc_a = run_guardrail(
    "A", "BULL", 0.62, 0.38,
    "BULL regime — winner 0.62 below cap 0.65, no trigger expected"
)

# --- B: BULL, winner above cap ---
res_b, rt_b, rc_b = run_guardrail(
    "B", "BULL", 0.80, 0.20,
    "BULL regime — winner 0.80 above cap 0.65, cap expected"
)

# --- C: BEAR, same input as B ---
res_c, rt_c, rc_c = run_guardrail(
    "C", "BEAR", 0.80, 0.20,
    "BEAR regime — same input as B, harder cap 0.55"
)

# --- D: SIDEWAYS ---
res_d, rt_d, rc_d = run_guardrail(
    "D", "SIDEWAYS", 0.70, 0.30,
    "SIDEWAYS regime — cap 0.55, 0.70 must be capped"
)

# --- E1: BULL + smoothing, stays below cap ---
res_e1, rt_e1, rc_e1, ps_e1 = run_smooth_then_guardrail(
    "E1",
    "BULL + smoothing — smoothed result stays below cap 0.65",
    cb20_trend="BULL",
    biased={"EDGE3": 0.75, "EDGE4": 0.25},
    prev_memory={"EDGE3": 0.52, "EDGE4": 0.48},
)

# --- E2: BEAR + smoothing, guardrail fires post-smooth ---
res_e2, rt_e2, rc_e2, ps_e2 = run_smooth_then_guardrail(
    "E2",
    "BEAR + smoothing — smoothed pushes past cap 0.55, guardrail fires",
    cb20_trend="BEAR",
    biased={"EDGE3": 0.80, "EDGE4": 0.20},
    prev_memory={"EDGE3": 0.70, "EDGE4": 0.30},
)

# --- F: Audit trail explainability ---
print(f"\n{'='*64}")
print("SCENARIO F  —  Audit trail explainability (2 cases)")
print(f"{'='*64}")

for label, res, rt in [("B BULL cap", res_b, rt_b), ("C BEAR cap", res_c, rt_c)]:
    print(f"\n  [{label}]")
    for s in STRATEGIES:
        r = res[s]
        # Build minimal sr dict as strategy_results would look
        sr = {
            "allowed": True,
            "effective_action": "ENTER_LONG",
            "allocation_pct": r["allocation_pct"],
            "confidence_gate_reason": "HIGH_CONFIDENCE",
            "allocation_bias_reason": "POSITIVE_SCORE",
            "smoothing_applied": False,
            "guardrail_adjusted": r["guardrail_adjusted"],
            "guardrail_reason": r["guardrail_reason"],
        }
        reason_str = build_audit_decision_reason(sr)
        print(f"    {s}: audit_decision_reason =")
        print(f"      {reason_str}")
        print(f"    allocation_pre_guardrail_pct = {r.get('allocation_pre_guardrail_pct')}"
              f"  →  allocation_pct = {r['allocation_pct']}"
              f"  |  regime_type = {r['regime_type']}  cap = {r['regime_cap_pct']}")

    # Market decision
    sr_map = {s: {"allocation_pct": res[s]["allocation_pct"], "smoothing_applied": False}
              for s in STRATEGIES}
    mdr = build_market_decision_reason(STRATEGIES, sr_map)
    print(f"  market_decision_reason = {mdr}")

# ===========================================================================
# PASS / FAIL CHECKS
# ===========================================================================
print(f"\n{'#'*64}")
print("  PASS / FAIL CHECKS")
print(f"{'#'*64}")

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


# === Scenario A ===
check("A: EDGE3 unchanged (no guardrail)",
      abs(res_a["EDGE3"]["allocation_pct"] - 0.62) < EPS,
      f"pct={res_a['EDGE3']['allocation_pct']}")
check("A: EDGE4 unchanged (no guardrail)",
      abs(res_a["EDGE4"]["allocation_pct"] - 0.38) < EPS,
      f"pct={res_a['EDGE4']['allocation_pct']}")
check("A: market_guardrail_applied = False",
      not any(res_a[s]["guardrail_adjusted"] for s in STRATEGIES))
check("A: sum == 1.0",
      abs(sum(res_a[s]["allocation_pct"] for s in STRATEGIES) - 1.0) < EPS)

# === Scenario B ===
check("B: EDGE3 <= BULL cap 0.65",
      res_b["EDGE3"]["allocation_pct"] <= REGIME_CAP_TREND + EPS,
      f"pct={res_b['EDGE3']['allocation_pct']}")
check("B: market_guardrail_applied = True",
      any(res_b[s]["guardrail_adjusted"] for s in STRATEGIES))
check("B: guardrail_reason contains WINNER_CAPPED_BY_REGIME",
      "WINNER_CAPPED_BY_REGIME" in res_b["EDGE3"]["guardrail_reason"])
check("B: sum == 1.0",
      abs(sum(res_b[s]["allocation_pct"] for s in STRATEGIES) - 1.0) < EPS)

# === Scenario C ===
check("C: EDGE3 <= BEAR cap 0.55",
      res_c["EDGE3"]["allocation_pct"] <= REGIME_CAP_BEAR + EPS,
      f"pct={res_c['EDGE3']['allocation_pct']}")
check("C: BEAR caps harder than BULL (EDGE3_C < EDGE3_B)",
      res_c["EDGE3"]["allocation_pct"] < res_b["EDGE3"]["allocation_pct"],
      f"C={res_c['EDGE3']['allocation_pct']:.4f} < B={res_b['EDGE3']['allocation_pct']:.4f}")
check("C: EDGE4 compensated (> 0.38)",
      res_c["EDGE4"]["allocation_pct"] > 0.38,
      f"EDGE4={res_c['EDGE4']['allocation_pct']:.4f}")
check("C: sum == 1.0",
      abs(sum(res_c[s]["allocation_pct"] for s in STRATEGIES) - 1.0) < EPS)

# === Scenario D ===
check("D: EDGE3 <= SIDEWAYS cap 0.55",
      res_d["EDGE3"]["allocation_pct"] <= REGIME_CAP_SIDEWAYS + EPS,
      f"pct={res_d['EDGE3']['allocation_pct']}")
check("D: sum == 1.0",
      abs(sum(res_d[s]["allocation_pct"] for s in STRATEGIES) - 1.0) < EPS)

# === Scenario E1 ===
expected_e1_edge3 = round(0.52 * SMOOTHING_RETAIN + 0.75 * SMOOTHING_ALPHA, 6)
check(f"E1: BULL — post-smooth EDGE3 ≈ {expected_e1_edge3:.4f}",
      abs(ps_e1["EDGE3"] - expected_e1_edge3) < 0.0001,
      f"smoothed={ps_e1['EDGE3']:.4f}")
check("E1: BULL — no guardrail trigger (smoothed < cap 0.65)",
      not any(res_e1[s]["guardrail_adjusted"] for s in STRATEGIES),
      f"EDGE3_smoothed={ps_e1['EDGE3']:.4f} cap=0.65")

# === Scenario E2 ===
expected_e2_edge3 = round(0.70 * SMOOTHING_RETAIN + 0.80 * SMOOTHING_ALPHA, 6)
check(f"E2: BEAR — post-smooth EDGE3 ≈ {expected_e2_edge3:.4f} > cap 0.55",
      abs(ps_e2["EDGE3"] - expected_e2_edge3) < 0.0001 and ps_e2["EDGE3"] > REGIME_CAP_BEAR,
      f"smoothed={ps_e2['EDGE3']:.4f}")
check("E2: BEAR — guardrail fires after smoothing",
      any(res_e2[s]["guardrail_adjusted"] for s in STRATEGIES))
check("E2: BEAR — final EDGE3 <= 0.55",
      res_e2["EDGE3"]["allocation_pct"] <= REGIME_CAP_BEAR + EPS,
      f"final={res_e2['EDGE3']['allocation_pct']:.4f}")
check("E2: sum == 1.0 after guardrail",
      abs(sum(res_e2[s]["allocation_pct"] for s in STRATEGIES) - 1.0) < EPS)

# === Scenario F ===
# Check audit fields are populated and regime info is present
check("F: guardrail_reason populated in alloc result",
      bool(res_b["EDGE3"].get("guardrail_reason")))
check("F: allocation_pre_guardrail_pct set",
      res_b["EDGE3"].get("allocation_pre_guardrail_pct") is not None)
check("F: regime_type set on result entries",
      bool(res_b["EDGE3"].get("regime_type")))
check("F: regime_cap_pct set on result entries",
      res_b["EDGE3"].get("regime_cap_pct") is not None)
sr_check = {
    "allowed": True, "effective_action": "ENTER_LONG",
    "allocation_pct": res_c["EDGE3"]["allocation_pct"],
    "confidence_gate_reason": "HIGH_CONFIDENCE",
    "allocation_bias_reason": "POSITIVE_SCORE",
    "smoothing_applied": False,
    "guardrail_adjusted": res_c["EDGE3"]["guardrail_adjusted"],
    "guardrail_reason": res_c["EDGE3"]["guardrail_reason"],
}
audit_str = build_audit_decision_reason(sr_check)
check("F: audit_decision_reason contains GUARDRAIL on capped strategy",
      "GUARDRAIL" in audit_str,
      f"→ {audit_str}")

# ===========================================================================
# TOTAAL + INTERPRETATIE
# ===========================================================================
passed = sum(1 for _, ok in results if ok)
total  = len(results)
print(f"\n  {'='*48}")
print(f"  TOTAL: {passed}/{total} PASSED {'✓ ALL OK' if passed == total else '✗ FAILURES'}")

print(f"\n{'#'*64}")
print("  INTERPRETATIE")
print(f"{'#'*64}")

regime_logic_ok   = all(ok for _, ok in results[:16])
smoothing_gate_ok = all(ok for _, ok in results[16:22])
audit_ok          = all(ok for _, ok in results[22:])

print(f"  Regime caps werken correct:       {'JA' if regime_logic_ok else 'NEE'}")
print(f"    → BULL 0.62 ongewijzigd (A): {abs(res_a['EDGE3']['allocation_pct'] - 0.62) < EPS}")
print(f"    → BULL caps 0.80→0.65 (B):  {res_b['EDGE3']['allocation_pct']:.4f}")
print(f"    → BEAR caps 0.80→0.55 (C):  {res_c['EDGE3']['allocation_pct']:.4f}  (harder dan BULL)")
print(f"    → SIDEWAYS caps 0.70→0.55 (D): {res_d['EDGE3']['allocation_pct']:.4f}")
print()
print(f"  Guardrail werkt post-smoothing:   {'JA' if smoothing_gate_ok else 'NEE'}")
print(f"    → E1 BULL smoothed={ps_e1['EDGE3']:.4f} < 0.65 → no cap")
print(f"    → E2 BEAR smoothed={ps_e2['EDGE3']:.4f} > 0.55 → cap naar {res_e2['EDGE3']['allocation_pct']:.4f}")
print()
print(f"  Audit trail correct:              {'JA' if audit_ok else 'NEE'}")
print(f"    → guardrail_reason, pre/post pct, regime_type, regime_cap_pct aanwezig")
print(f"    → audit_decision_reason bevat GUARDRAIL bij ingreep")
print()
