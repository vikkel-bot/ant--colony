"""
AC58: Portfolio Transition Simulation
Pure simulation layer. No execution, no orders, no state changes.

Sits after AC57 (Multi-Cycle Stability). Simulates what a portfolio transition
would look like for all PASS-gated candidates from allocation_decision_quality.json.
HOLD/BLOCK candidates are noted but never simulated for execution.

Reads:
  allocation_decision_quality.json    — gate, score, drift, conviction per position
  allocation_portfolio_drift.json     — actual/target notionals
  rebalance_intents.json              — delta, action, budget selection

Writes:
  portfolio_transition_simulation.json
  portfolio_transition_simulation.tsv

Per-candidate simulation fields:
  market, strategy_key, position_key
  decision_quality_gate, decision_quality_score
  current_weight, target_weight, delta_weight
  actual_notional_eur, target_notional_eur, transition_notional_estimate
  turnover_class: LOW / MEDIUM / HIGH / EXTREME  (mirrors churn breakpoints)
  budget_pressure_before_eur, budget_pressure_after_eur
  conflict_flags: list of detected conflicts (empty = no conflicts)
  simulation_action: SIMULATE_TRANSITION / NO_ACTION
  simulation_reasons: pipe-separated rationale

Conflict detection (any of these → NO_ACTION):
  GATE_NOT_PASS          — candidate is HOLD or BLOCK
  BUDGET_NOT_SELECTED    — rebalance_selected == False
  DELTA_ZERO             — transition delta is zero
  CONFLICT_DIRECTION     — proposed delta direction contradicts drift direction
  CONFLICT_OVER_BUDGET   — transition would exceed remaining sim budget

Simulation budget cap:
  MAX_SIMULATION_NOTIONAL_PCT = 0.40  (40% of equity per sim run)
  Applied cumulatively across all PASS candidates, sorted by quality score desc.

Usage: python ant_colony/build_portfolio_transition_simulation_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR  = Path(r"C:\Trading\ANT_OUT")

DQ_PATH       = OUT_DIR / "allocation_decision_quality.json"
DRIFT_PATH    = OUT_DIR / "allocation_portfolio_drift.json"
REBALANCE_PATH = OUT_DIR / "rebalance_intents.json"

OUT_PATH     = OUT_DIR / "portfolio_transition_simulation.json"
OUT_TSV_PATH = OUT_DIR / "portfolio_transition_simulation.tsv"

VERSION = "transition_simulation_v1"

# Simulation budget: how much notional can be simulated in one run
MAX_SIMULATION_NOTIONAL_PCT = 0.40  # 40% of equity

# Turnover class breakpoints (mirrors AC56 churn thresholds)
_TURNOVER_LOW     = 0.20
_TURNOVER_MEDIUM  = 0.50
_TURNOVER_HIGH    = 0.90

TSV_HEADERS = [
    "market", "strategy_key", "position_key",
    "decision_quality_gate", "decision_quality_score",
    "current_weight", "target_weight", "delta_weight",
    "actual_notional_eur", "target_notional_eur", "transition_notional_estimate",
    "turnover_class",
    "budget_pressure_before_eur", "budget_pressure_after_eur",
    "conflict_flags", "simulation_action", "simulation_reasons",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_ts():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def to_float(v, default=0.0):
    try:
        f = float(v)
        return f if f == f else float(default)
    except Exception:
        return float(default)


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def write_tsv(path: Path, headers: list, rows: list):
    lines = ["\t".join(headers)]
    for row in rows:
        lines.append("\t".join(
            "" if row.get(h) is None else str(row.get(h, ""))
            for h in headers
        ))
    path.write_text("\n".join(lines), encoding="utf-8")


def classify_turnover(abs_delta: float, reference: float) -> str:
    ref = max(reference, 1.0)
    ratio = abs_delta / ref
    if ratio < _TURNOVER_LOW:
        return "LOW"
    if ratio < _TURNOVER_MEDIUM:
        return "MEDIUM"
    if ratio < _TURNOVER_HIGH:
        return "HIGH"
    return "EXTREME"


def detect_conflicts(gate: str, rebal_selected: bool,
                     delta_eur: float, drift_pct: float,
                     sim_budget_remaining: float) -> list:
    """
    Returns list of conflict flag strings. Empty = no conflicts.
    """
    conflicts = []

    if gate != "PASS":
        conflicts.append("GATE_NOT_PASS")

    if not rebal_selected:
        conflicts.append("BUDGET_NOT_SELECTED")

    if abs(delta_eur) < 0.01:
        conflicts.append("DELTA_ZERO")

    # Direction conflict: a correct delta corrects the drift, so signs should differ.
    # drift_pct = (actual - target) / equity
    # drift > 0  → actual > target → over-allocated  → delta must be NEGATIVE (reduce)
    # drift < 0  → actual < target → under-allocated → delta must be POSITIVE (increase)
    # Conflict fires when sign(delta) == sign(drift) — both going the same direction
    # means amplifying the imbalance rather than correcting it.
    if abs(drift_pct) > 0.01 and abs(delta_eur) > 0.01:
        drift_sign = 1 if drift_pct > 0 else -1
        delta_sign = 1 if delta_eur > 0 else -1
        if drift_sign == delta_sign:
            conflicts.append("CONFLICT_DIRECTION")

    if abs(delta_eur) > sim_budget_remaining + 0.01:
        conflicts.append("CONFLICT_OVER_BUDGET")

    return conflicts


# ---------------------------------------------------------------------------
# Main simulation logic (also importable for tests)
# ---------------------------------------------------------------------------

def simulate_transitions(dq_records: list, drift_index: dict, rebal_index: dict,
                          equity: float) -> list:
    """
    Run transition simulation over DQ records.
    Returns list of simulation row dicts.
    """
    sim_budget = round(equity * MAX_SIMULATION_NOTIONAL_PCT, 2)
    sim_budget_used = 0.0

    # Sort PASS candidates by quality score desc, then others after
    def sort_key(r):
        gate = r.get("decision_quality_gate", "BLOCK")
        score = to_float(r.get("decision_quality_score", 0.0))
        return (0 if gate == "PASS" else 1, -score)

    sorted_records = sorted(dq_records, key=sort_key)

    rows = []
    for rec in sorted_records:
        pk     = rec.get("position_key", "")
        market = rec.get("market", "")
        strategy = rec.get("strategy", "")

        gate  = rec.get("decision_quality_gate", "BLOCK")
        score = to_float(rec.get("decision_quality_score", 0.0))

        drift_row = drift_index.get(pk) or {}
        rebal_row = rebal_index.get(pk) or {}

        actual_eur  = to_float(drift_row.get("actual_notional_eur", 0.0))
        target_eur  = to_float(drift_row.get("target_notional_eur", 0.0))
        alloc_pct   = to_float(drift_row.get("allocation_pct", 0.0))
        drift_pct   = to_float(drift_row.get("drift_pct", 0.0))

        rebal_selected = bool(rebal_row.get("rebalance_selected", False))
        delta_eur      = to_float(rebal_row.get("rebalance_capped_delta_eur", 0.0))

        abs_delta = abs(delta_eur)
        reference = max(abs(actual_eur), abs(target_eur), 1.0)
        turnover  = classify_turnover(abs_delta, reference)

        # Current and target weights as fraction of equity (use alloc_pct as target)
        current_weight = round(actual_eur / max(equity, 1.0), 6) if equity > 0 else 0.0
        target_weight  = round(alloc_pct, 6)
        delta_weight   = round(target_weight - current_weight, 6)

        # Transition estimate = abs(delta)
        transition_est = abs_delta

        budget_before = round(sim_budget - sim_budget_used, 2)

        conflicts = detect_conflicts(
            gate, rebal_selected, delta_eur, drift_pct, budget_before
        )

        if conflicts:
            sim_action   = "NO_ACTION"
            sim_reasons  = conflicts + [f"NO_ACTION_REASON={'|'.join(conflicts)}"]
            budget_after = budget_before  # no change
        else:
            sim_action   = "SIMULATE_TRANSITION"
            sim_reasons  = [
                f"GATE_PASS_SCORE_{score:.4f}",
                f"TURNOVER_{turnover}",
                f"BUDGET_REMAINING_{budget_before:.2f}",
            ]
            sim_budget_used = round(sim_budget_used + transition_est, 2)
            budget_after    = round(sim_budget - sim_budget_used, 2)

        rows.append({
            "market":                       market,
            "strategy_key":                 strategy,
            "position_key":                 pk,
            "decision_quality_gate":        gate,
            "decision_quality_score":       score,
            "current_weight":               current_weight,
            "target_weight":                target_weight,
            "delta_weight":                 delta_weight,
            "actual_notional_eur":          actual_eur,
            "target_notional_eur":          target_eur,
            "transition_notional_estimate": transition_est,
            "turnover_class":               turnover,
            "budget_pressure_before_eur":   budget_before,
            "budget_pressure_after_eur":    budget_after,
            "conflict_flags":               "|".join(conflicts) if conflicts else "",
            "simulation_action":            sim_action,
            "simulation_reasons":           "|".join(sim_reasons),
        })

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    dq_data     = load_json(DQ_PATH, {}) or {}
    drift_data  = load_json(DRIFT_PATH, {}) or {}
    rebal_data  = load_json(REBALANCE_PATH, {}) or {}

    equity    = to_float(dq_data.get("equity") or drift_data.get("equity", 0.0))
    cycle_id  = dq_data.get("cycle_id") or drift_data.get("cycle_id")

    dq_records = dq_data.get("records") or []

    drift_index = {r["position_key"]: r for r in (drift_data.get("rows") or [])
                   if r.get("position_key")}
    rebal_index = {i["position_key"]: i for i in (rebal_data.get("intents") or [])
                   if i.get("position_key")}

    rows = simulate_transitions(dq_records, drift_index, rebal_index, equity)

    sim_count    = sum(1 for r in rows if r["simulation_action"] == "SIMULATE_TRANSITION")
    no_act_count = sum(1 for r in rows if r["simulation_action"] == "NO_ACTION")

    out = {
        "component":    "build_portfolio_transition_simulation_lite",
        "version":      VERSION,
        "ts_utc":       ts,
        "cycle_id":     cycle_id,
        "equity":       equity,
        "simulation_budget_eur":    round(equity * MAX_SIMULATION_NOTIONAL_PCT, 2),
        "max_simulation_notional_pct": MAX_SIMULATION_NOTIONAL_PCT,
        "rows_total":          len(rows),
        "simulate_count":      sim_count,
        "no_action_count":     no_act_count,
        "source_dq_records":   len(dq_records),
        "source_drift_rows":   len(drift_index),
        "source_rebal_intents": len(rebal_index),
        "rows": rows,
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(OUT_PATH, out)
        write_tsv(OUT_TSV_PATH, TSV_HEADERS, rows)
    except Exception as e:
        print(f"[WARN] Could not write output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k != "rows"}, indent=2))


if __name__ == "__main__":
    main()
