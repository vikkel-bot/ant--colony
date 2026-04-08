"""
AC57: Multi-Cycle Stability Validation for Decision Quality Gate
Pure synthetic simulation layer. No execution, no orders, no state changes.

Sits after AC56 (Decision Quality Gate), validates that gate logic is stable
across multiple synthetic cycles before transitioning to AC-58 simulation.

Reads:   (no runtime files — generates synthetic cycles internally)
Writes:
  allocation_decision_quality_stability.json
  allocation_decision_quality_stability.tsv

Scenario families (6):
  A: Noisy small drift — should remain BLOCK throughout
  B: Sustained conviction with material drift — should stay PASS consistently
  C: Regime flip (BULL → BEAR mid-run) — should demote at flip point
  D: Oscillating target / churn pressure — gate should penalise but not oscillate wildly
  E: Sparse evidence (feedback_confidence degrades to 0) — stable HOLD→BLOCK
  F: Constraint pressure (budget excluded + immaterial drift) — always BLOCK

Stability metrics per scenario:
  cycles_total, pass_count, hold_count, block_count
  gate_flip_count, gate_flip_rate
  avg_quality_score, avg_churn_penalty
  promotion_count (HOLD→PASS or BLOCK→PASS|HOLD)
  demotion_count  (PASS→HOLD|BLOCK or HOLD→BLOCK)
  stability_assessment: STABLE / MARGINALLY_STABLE / UNSTABLE

Overall verdict:
  stability_verdict: STABLE_FOR_AC58 / NOT_STABLE_STOP

Usage: python ant_colony/build_allocation_quality_stability_lite.py
"""
import json
import importlib.util
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR  = Path(r"C:\Trading\ANT_OUT")
OUT_PATH = OUT_DIR / "allocation_decision_quality_stability.json"
OUT_TSV  = OUT_DIR / "allocation_decision_quality_stability.tsv"

VERSION = "stability_v1"

# Stability thresholds
FLIP_RATE_STABLE     = 0.20   # ≤ 20% flips → STABLE
FLIP_RATE_MARGINAL   = 0.35   # ≤ 35% flips → MARGINALLY_STABLE
                               # > 35% flips → UNSTABLE

TSV_HEADERS = [
    "scenario", "cycles_total",
    "pass_count", "hold_count", "block_count",
    "gate_flip_count", "gate_flip_rate",
    "avg_quality_score", "avg_churn_penalty",
    "promotion_count", "demotion_count",
    "stability_assessment",
]


# ---------------------------------------------------------------------------
# Load scoring functions from AC56 production module
# ---------------------------------------------------------------------------

def _load_dq_module():
    mod_path = Path(__file__).parent / "build_allocation_decision_quality_lite.py"
    spec = importlib.util.spec_from_file_location("dq", mod_path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_dq = _load_dq_module()

score_drift_materiality = _dq.score_drift_materiality
score_conviction        = _dq.score_conviction
score_regime_compat     = _dq.score_regime_compat
score_churn             = _dq.score_churn
compute_quality_score   = _dq.compute_quality_score
determine_gate          = _dq.determine_gate


# ---------------------------------------------------------------------------
# Single-cycle scoring helper (mirrors test harness run_gate)
# ---------------------------------------------------------------------------

def run_cycle(drift_pct, feedback_conf, regime_type, rebal_selected,
              delta_eur=50.0, actual_eur=100.0, target_eur=150.0) -> dict:
    drift_mat, drift_r   = score_drift_materiality(drift_pct)
    conviction, conv_r   = score_conviction(feedback_conf, regime_type)
    regime_sc, regime_r  = score_regime_compat(regime_type)
    churn, churn_r       = score_churn(delta_eur, actual_eur, target_eur)
    budget_ok            = 1.0 if rebal_selected else 0.0

    score = compute_quality_score(drift_mat, conviction, budget_ok, regime_sc, churn)
    gate, reasons = determine_gate(
        score, rebal_selected, drift_pct, drift_mat,
        [drift_r, conv_r, regime_r, churn_r],
        regime_score=regime_sc,
    )
    return {"score": score, "gate": gate, "churn": churn, "reasons": reasons}


# ---------------------------------------------------------------------------
# Stability metrics from a sequence of cycle results
# ---------------------------------------------------------------------------

def analyse_stability(results: list) -> dict:
    gates  = [r["gate"] for r in results]
    scores = [r["score"] for r in results]
    churns = [r["churn"] for r in results]

    n = len(gates)

    pass_count  = gates.count("PASS")
    hold_count  = gates.count("HOLD")
    block_count = gates.count("BLOCK")

    # Count gate flips (consecutive change)
    flips = sum(1 for i in range(1, n) if gates[i] != gates[i - 1])
    flip_rate = round(flips / max(n - 1, 1), 4)

    # Promotions: BLOCK→HOLD, BLOCK→PASS, HOLD→PASS
    _RANK = {"PASS": 2, "HOLD": 1, "BLOCK": 0}
    promotions = sum(
        1 for i in range(1, n)
        if _RANK[gates[i]] > _RANK[gates[i - 1]]
    )
    demotions = sum(
        1 for i in range(1, n)
        if _RANK[gates[i]] < _RANK[gates[i - 1]]
    )

    avg_score = round(sum(scores) / n, 4) if n else 0.0
    avg_churn = round(sum(churns) / n, 4) if n else 0.0

    if flip_rate <= FLIP_RATE_STABLE:
        assessment = "STABLE"
    elif flip_rate <= FLIP_RATE_MARGINAL:
        assessment = "MARGINALLY_STABLE"
    else:
        assessment = "UNSTABLE"

    return {
        "cycles_total":        n,
        "pass_count":          pass_count,
        "hold_count":          hold_count,
        "block_count":         block_count,
        "gate_flip_count":     flips,
        "gate_flip_rate":      flip_rate,
        "avg_quality_score":   avg_score,
        "avg_churn_penalty":   avg_churn,
        "promotion_count":     promotions,
        "demotion_count":      demotions,
        "stability_assessment": assessment,
    }


# ---------------------------------------------------------------------------
# Scenario generators
# ---------------------------------------------------------------------------

def scenario_a_noisy_drift(n=20):
    """
    Small drifts oscillating around the immaterial threshold.
    Expectation: all BLOCK (drift never clears DRIFT_IMMATERIAL).
    """
    import math
    results = []
    for i in range(n):
        drift = 0.02 + 0.025 * abs(math.sin(i))   # oscillates 0.02–0.045, never > 0.05
        results.append(run_cycle(
            drift_pct=drift, feedback_conf=0.6, regime_type="BULL",
            rebal_selected=True, delta_eur=5.0, actual_eur=100.0, target_eur=105.0,
        ))
    return results


def scenario_b_sustained_conviction(n=20):
    """
    Strong drift + high conviction + BULL throughout.
    Expectation: consistently PASS, zero or one flip.
    """
    results = []
    for i in range(n):
        # slight wobble in conviction but always > 0.85
        conf  = 0.88 + 0.07 * ((i % 3) - 1) / 3   # 0.85–0.95
        drift = 0.30 + 0.05 * (i % 2)              # 0.30 or 0.35
        results.append(run_cycle(
            drift_pct=drift, feedback_conf=conf, regime_type="BULL",
            rebal_selected=True, delta_eur=60.0, actual_eur=200.0, target_eur=260.0,
        ))
    return results


def scenario_c_regime_flip(n=20):
    """
    Cycles 0–9: BULL (PASS expected).
    Cycles 10–19: BEAR (HOLD/BLOCK expected — regime cap).
    Expectation: one clean demotion at cycle 10.
    """
    results = []
    for i in range(n):
        regime = "BULL" if i < 10 else "BEAR"
        results.append(run_cycle(
            drift_pct=0.40, feedback_conf=0.90, regime_type=regime,
            rebal_selected=True, delta_eur=80.0, actual_eur=200.0, target_eur=280.0,
        ))
    return results


def scenario_d_churn_oscillation(n=20):
    """
    Alternates between low-churn and extreme-churn cycles.
    Expectation: churn penalty visible, gate oscillates PASS/HOLD (not BLOCK).
    """
    results = []
    for i in range(n):
        if i % 2 == 0:
            # low churn cycle
            delta, actual, target = 30.0, 200.0, 230.0
        else:
            # extreme churn (delta ≈ 95% of reference)
            delta, actual, target = 285.0, 50.0, 300.0
        results.append(run_cycle(
            drift_pct=0.35, feedback_conf=0.80, regime_type="BULL",
            rebal_selected=True,
            delta_eur=delta, actual_eur=actual, target_eur=target,
        ))
    return results


def scenario_e_sparse_evidence(n=20):
    """
    feedback_confidence degrades linearly from 0.80 to 0.0 over cycles.
    Expectation: progressive demotion from PASS/HOLD toward BLOCK.
    """
    results = []
    for i in range(n):
        conf = max(0.0, 0.80 - (0.80 / (n - 1)) * i)
        results.append(run_cycle(
            drift_pct=0.25, feedback_conf=conf, regime_type="BULL",
            rebal_selected=True, delta_eur=50.0, actual_eur=200.0, target_eur=250.0,
        ))
    return results


def scenario_f_constraint_pressure(n=20):
    """
    Alternates budget_excluded and immaterial_drift — both hard-BLOCK triggers.
    Expectation: 100% BLOCK, zero flips.
    """
    results = []
    for i in range(n):
        if i % 2 == 0:
            results.append(run_cycle(
                drift_pct=0.50, feedback_conf=0.90, regime_type="BULL",
                rebal_selected=False,   # budget excluded
                delta_eur=100.0, actual_eur=200.0, target_eur=300.0,
            ))
        else:
            results.append(run_cycle(
                drift_pct=0.02, feedback_conf=0.90, regime_type="BULL",
                rebal_selected=True,    # tiny drift → immaterial
                delta_eur=2.0, actual_eur=100.0, target_eur=102.0,
            ))
    return results


# ---------------------------------------------------------------------------
# Overall verdict
# ---------------------------------------------------------------------------

def overall_verdict(scenarios: list) -> str:
    """
    STABLE_FOR_AC58 if no UNSTABLE scenario among quality-bearing families
    (B, C, D, E — the ones that should produce non-BLOCK outcomes).
    Noisy (A) and constraint (F) are expected all-BLOCK, so instability there
    means noise leaking into PASS, which is also a failure.
    """
    for s in scenarios:
        if s["stability_assessment"] == "UNSTABLE":
            return "NOT_STABLE_STOP"
    return "STABLE_FOR_AC58"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def write_tsv(path: Path, headers: list, rows: list):
    lines = ["\t".join(headers)]
    for row in rows:
        lines.append("\t".join(
            "" if row.get(h) is None else str(row.get(h))
            for h in headers
        ))
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    families = [
        ("A_noisy_drift",           scenario_a_noisy_drift()),
        ("B_sustained_conviction",  scenario_b_sustained_conviction()),
        ("C_regime_flip",           scenario_c_regime_flip()),
        ("D_churn_oscillation",     scenario_d_churn_oscillation()),
        ("E_sparse_evidence",       scenario_e_sparse_evidence()),
        ("F_constraint_pressure",   scenario_f_constraint_pressure()),
    ]

    scenario_rows = []
    for name, results in families:
        metrics = analyse_stability(results)
        metrics["scenario"] = name
        scenario_rows.append(metrics)

    verdict = overall_verdict(scenario_rows)

    out = {
        "component":        "build_allocation_quality_stability_lite",
        "version":          VERSION,
        "ts_utc":           ts,
        "stability_verdict": verdict,
        "flip_rate_stable_threshold":   FLIP_RATE_STABLE,
        "flip_rate_marginal_threshold": FLIP_RATE_MARGINAL,
        "scenarios":        scenario_rows,
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(OUT_PATH, out)
        write_tsv(OUT_TSV, TSV_HEADERS, scenario_rows)
    except Exception as e:
        print(f"[WARN] Could not write output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k != "scenarios"}, indent=2))
    print(f"\nScenario detail:")
    for s in scenario_rows:
        print(f"  {s['scenario']:<30} flips={s['gate_flip_count']:>2}/{s['cycles_total']-1}"
              f"  rate={s['gate_flip_rate']:.2f}  assessment={s['stability_assessment']}"
              f"  PASS={s['pass_count']} HOLD={s['hold_count']} BLOCK={s['block_count']}")
    print(f"\n>>> STABILITY VERDICT: {verdict}")


if __name__ == "__main__":
    main()
