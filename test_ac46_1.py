"""
AC46.1 Synthetic feedback validation
Validates allocation logic (normalize → confidence gate → smooth) with controlled synthetic input.

Approach: imports production functions directly — no pipeline modification, no file I/O,
fully reproducible regardless of live data state.

Usage: python test_ac46_1.py
"""
import importlib.util
import sys
from pathlib import Path

# --- Load production module without package import ---
_mod_path = Path(__file__).parent / "ant_colony" / "build_execution_intents_lite.py"
_spec = importlib.util.spec_from_file_location("build_execution_intents_lite", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

normalize_market_allocations = _mod.normalize_market_allocations
apply_confidence_gating       = _mod.apply_confidence_gating
smooth_market_allocations     = _mod.smooth_market_allocations
derive_feedback_confidence    = _mod.derive_feedback_confidence
SMOOTHING_ALPHA               = _mod.SMOOTHING_ALPHA
SMOOTHING_RETAIN              = _mod.SMOOTHING_RETAIN

# ---------------------------------------------------------------------------
MARKET = "BTC-EUR"
STRATEGIES = ["EDGE3", "EDGE4"]


def make_strategy_eval_both_active():
    """Synthetic strategy_eval: both EDGE3 and EDGE4 have ENTER_LONG signal."""
    return {s: {"effective_action": "ENTER_LONG"} for s in STRATEGIES}


def make_feedback(edge3: dict, edge4: dict) -> dict:
    return {
        f"{MARKET}__EDGE3": edge3,
        f"{MARKET}__EDGE4": edge4,
    }


def run_allocation(label: str, feedback_keys: dict, memory_state: dict = None,
                   description: str = "") -> dict:
    """Run the full allocation chain and pretty-print results. Returns alloc_map."""
    if memory_state is None:
        memory_state = {}

    strategy_eval = make_strategy_eval_both_active()

    # AC44 normalize
    alloc_map, active = normalize_market_allocations(MARKET, strategy_eval, feedback_keys)
    # AC46 confidence gate
    alloc_map = apply_confidence_gating(MARKET, alloc_map, active, feedback_keys)
    # AC45 smooth
    alloc_map = smooth_market_allocations(MARKET, alloc_map, memory_state, active)

    total = round(sum(alloc_map[s]["allocation_pct"] for s in STRATEGIES), 6)
    sum_ok = abs(total - 1.0) < 0.0001

    print(f"\n{'='*68}")
    print(f"SCENARIO {label}" + (f" — {description}" if description else ""))
    print(f"{'='*68}")
    hdr = f"  {'Strategy':<16} {'conf':>6} {'neutral':>8} {'biased':>8} {'conf_adj':>10} {'smoothed':>10} {'final':>8}"
    print(hdr)
    print(f"  {'-'*66}")
    for s in STRATEGIES:
        a = alloc_map[s]
        print(
            f"  {MARKET}__{s:<4}"
            f"  {a.get('feedback_confidence', 0.0):>6.4f}"
            f"  {a.get('neutral_target_pct', 0.0):>8.4f}"
            f"  {a.get('biased_target_pct', 0.0):>8.4f}"
            f"  {a.get('confidence_adjusted_target_pct', 0.0):>10.4f}"
            f"  {a.get('allocation_smoothed_pct', 0.0):>10.4f}"
            f"  {a.get('allocation_pct', 0.0):>8.4f}"
        )
        if a.get("smoothing_applied"):
            prev = a.get("allocation_previous_pct", 0.0)
            smo  = a.get("allocation_smoothed_pct", 0.0)
            tgt  = a.get("allocation_target_pct", 0.0)
            print(f"    → smoothing: prev={prev:.4f}  ×{SMOOTHING_RETAIN}  +  target={tgt:.4f}  ×{SMOOTHING_ALPHA}  =  {smo:.4f}")
    print(f"\n  allocation_pct sum = {total:.6f}  {'✓' if sum_ok else '✗ ERROR'}")

    # Confidence reasons
    for s in STRATEGIES:
        a = alloc_map[s]
        print(f"  {MARKET}__{s} conf_gate_reason: {a.get('confidence_gate_reason')}  |  smoothing: {a.get('smoothing_reason')}")

    return alloc_map


# ---------------------------------------------------------------------------
# Synthetic feedback definitions
# ---------------------------------------------------------------------------

FB_A = make_feedback(
    edge3={"score": 1.0, "trade_count": 1, "closed_trade_count": 1, "win_count": 1, "loss_count": 0},
    edge4={"score": 0.0, "trade_count": 1, "closed_trade_count": 1, "win_count": 0, "loss_count": 1},
)

FB_B = make_feedback(
    edge3={"score":  3.0, "trade_count": 6, "closed_trade_count": 5, "win_count": 4, "loss_count": 1},
    edge4={"score": -2.0, "trade_count": 6, "closed_trade_count": 5, "win_count": 1, "loss_count": 4},
)

FB_C = make_feedback(
    edge3={"score": -3.0, "trade_count": 6, "closed_trade_count": 5, "win_count": 1, "loss_count": 4},
    edge4={"score":  2.5, "trade_count": 6, "closed_trade_count": 5, "win_count": 4, "loss_count": 1},
)

# ---------------------------------------------------------------------------
print("\n" + "#" * 68)
print("  AC46.1 SYNTHETIC FEEDBACK VALIDATION")
print("  market_allocation_mode: FEEDBACK_BIASED_CONFIDENCE_GATED_SMOOTHED")
print("#" * 68)

# -------------------------
# MAIN SCENARIOS (no memory)
# -------------------------
map_a = run_allocation("A", FB_A, description="LOW CONFIDENCE POSITIVE — EDGE3 slight edge, low sample")
map_b = run_allocation("B", FB_B, description="HIGH CONFIDENCE POSITIVE — EDGE3 dominant")
map_c = run_allocation("C", FB_C, description="HIGH CONFIDENCE NEGATIVE — EDGE4 dominant")

# -------------------------
# SMOOTHING TEST: A → B1 → B2
# Show convergence from A's allocation toward B's target over two B-cycles
# -------------------------
print(f"\n{'#'*68}")
print("  SMOOTHING CONVERGENCE TEST: A → B1 → B2")
print(f"  (memory carries over between runs, shows EMA approach to target)")
print(f"{'#'*68}")

# A: establish baseline memory
mem = {}
sm_a  = run_allocation("SMOOTH-A", FB_A, memory_state=mem, description="baseline — no prior memory")

# Build memory from A result
mem = {
    f"{MARKET}__{s}": {"previous_allocation_pct": sm_a[s]["allocation_pct"]}
    for s in STRATEGIES
}

sm_b1 = run_allocation("SMOOTH-B1", FB_B, memory_state=mem, description="B with A's memory — first EMA step")
mem = {
    f"{MARKET}__{s}": {"previous_allocation_pct": sm_b1[s]["allocation_pct"]}
    for s in STRATEGIES
}

sm_b2 = run_allocation("SMOOTH-B2", FB_B, memory_state=mem, description="B with B1's memory — second EMA step")

# ---------------------------------------------------------------------------
# PASS / FAIL CHECKS
# ---------------------------------------------------------------------------
print(f"\n{'#'*68}")
print("  PASS / FAIL CHECKS")
print(f"{'#'*68}")

results = []

def check(label: str, condition: bool, detail: str = ""):
    status = "PASS" if condition else "FAIL"
    results.append((label, condition))
    suffix = f"  [{detail}]" if detail else ""
    print(f"  [{status}]  {label}{suffix}")


edge3_a_pct  = map_a["EDGE3"]["allocation_pct"]
edge4_a_pct  = map_a["EDGE4"]["allocation_pct"]
edge3_b_pct  = map_b["EDGE3"]["allocation_pct"]
edge4_c_pct  = map_c["EDGE4"]["allocation_pct"]
edge3_b1_pct = sm_b1["EDGE3"]["allocation_pct"]
edge3_b2_pct = sm_b2["EDGE3"]["allocation_pct"]
b_target_edge3 = map_b["EDGE3"]["confidence_adjusted_target_pct"]  # what B converges to

check(
    "A: |EDGE3 pct - 0.5| < 0.15  (close to neutral, low confidence pulls bias toward neutral)",
    abs(edge3_a_pct - 0.5) < 0.15,
    f"EDGE3={edge3_a_pct:.4f}",
)
check(
    "A: EDGE3 > EDGE4  (slight bias survives even at low confidence)",
    edge3_a_pct > edge4_a_pct,
    f"EDGE3={edge3_a_pct:.4f} vs EDGE4={edge4_a_pct:.4f}",
)
check(
    "B: EDGE3 allocation_pct > 0.6  (high confidence strong bias)",
    edge3_b_pct > 0.6,
    f"EDGE3={edge3_b_pct:.4f}",
)
check(
    "C: EDGE4 allocation_pct > 0.6  (high confidence reversed bias)",
    edge4_c_pct > 0.6,
    f"EDGE4={edge4_c_pct:.4f}",
)
check(
    "A sum == 1.0",
    abs(sum(map_a[s]["allocation_pct"] for s in STRATEGIES) - 1.0) < 0.0001,
)
check(
    "B sum == 1.0",
    abs(sum(map_b[s]["allocation_pct"] for s in STRATEGIES) - 1.0) < 0.0001,
)
check(
    "C sum == 1.0",
    abs(sum(map_c[s]["allocation_pct"] for s in STRATEGIES) - 1.0) < 0.0001,
)
check(
    "Smoothing B1 applied  (smoothing_applied == True)",
    sm_b1["EDGE3"].get("smoothing_applied") is True,
    f"smoothing_applied={sm_b1['EDGE3'].get('smoothing_applied')}",
)
check(
    "Smoothing B2 closer to target than B1  (EMA convergence)",
    abs(edge3_b2_pct - b_target_edge3) < abs(edge3_b1_pct - b_target_edge3),
    f"B1={edge3_b1_pct:.4f}, B2={edge3_b2_pct:.4f}, target={b_target_edge3:.4f}",
)

passed = sum(1 for _, ok in results if ok)
total  = len(results)
print(f"\n  {'='*46}")
print(f"  TOTAL: {passed}/{total} PASSED {'✓ ALL OK' if passed == total else '✗ FAILURES'}")

# ---------------------------------------------------------------------------
# INTERPRETATIE
# ---------------------------------------------------------------------------
print(f"\n{'#'*68}")
print("  INTERPRETATIE")
print(f"{'#'*68}")

conf_logic_ok    = all(ok for _, ok in results[:4])
norm_ok          = all(ok for _, ok in results[4:7])
smoothing_ok     = all(ok for _, ok in results[7:])

print(f"  Confidence gating werkt logisch:  {'JA' if conf_logic_ok else 'NEE'}")
print(f"    → lage confidence trekt bias richting neutraal (scenario A)")
print(f"    → hoge confidence laat volle bias door (scenario B/C)")
print()
print(f"  Normalisatie correct (som=1.0):   {'JA' if norm_ok else 'NEE'}")
print()
print(f"  Smoothing zichtbaar en correct:   {'JA' if smoothing_ok else 'NEE'}")
print(f"    → EMA trekt allocatie geleidelijk richting nieuw target")
print(f"    → A→B1→B2: {sm_a['EDGE3']['allocation_pct']:.4f} → {edge3_b1_pct:.4f} → {edge3_b2_pct:.4f}  (target={b_target_edge3:.4f})")
print()
