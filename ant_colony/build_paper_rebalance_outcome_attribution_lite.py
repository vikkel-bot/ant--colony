"""
AC60: Paper Rebalance Outcome Attribution
Pure observability + governance attribution layer. No execution, no orders.

Sits after AC59 (Paper-Only Conditional Rebalance Execution). Evaluates each
paper-executed rebalance candidate to determine whether the execution was
governance-wise beneficial, neutral, or harmful.

Central question:
  Was this paper rebalance, after the fact, better than doing nothing?

Attribution is entirely synthetic (paper governance metrics):
  - Before state:  drift_pct from AC52 (pre-execution)
  - After state:   estimated post-execution drift = drift_before - (delta_eur / equity)
  - Baseline:      no-action scenario = drift stays unchanged
  - All comparison uses governance metrics, NOT market P&L

Reads:
  paper_rebalance_execution_summary.json  — executed/skipped records (AC59)
  allocation_portfolio_drift.json         — drift_pct, actual/target per position (AC52)
  portfolio_transition_simulation.json    — budget_pressure before/after per candidate (AC58)
  allocation_decision_quality.json        — churn_penalty, quality_score per candidate (AC56)

Writes:
  paper_rebalance_outcome_attribution.json
  paper_rebalance_outcome_attribution.tsv

Outcome labels (closed set):
  HELPFUL          — drift and allocation fit improved, churn acceptable
  NEUTRAL          — marginal improvement or mixed signals
  HARMFUL          — drift worsened or churn disproportionate
  PENDING          — record present but evaluation window not complete
  INSUFFICIENT_DATA — missing critical before/after data
  NOT_EXECUTED     — candidate was SKIPPED in AC59 (no attribution possible)

Evaluation status (per record):
  READY            — all required data present; full attribution computed
  PENDING_WINDOW   — execution exists but drift data unavailable for this cycle
  INSUFFICIENT_DATA — critical field(s) missing
  NOT_EXECUTED     — execution_status == SKIPPED

Thresholds:
  DRIFT_IMPROVEMENT_MEANINGFUL = 0.02   (2% of equity)
  ALLOC_FIT_IMPROVEMENT_MEANINGFUL = 0.01
  CHURN_HARMFUL_THRESHOLD = 0.40       (CHURN_HIGH penalty level)
  CHURN_EXTREME_THRESHOLD  = 0.60

Usage: python ant_colony/build_paper_rebalance_outcome_attribution_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

EXEC_SUMMARY_PATH = OUT_DIR / "paper_rebalance_execution_summary.json"
DRIFT_PATH        = OUT_DIR / "allocation_portfolio_drift.json"
SIM_PATH          = OUT_DIR / "portfolio_transition_simulation.json"
DQ_PATH           = OUT_DIR / "allocation_decision_quality.json"

OUT_PATH     = OUT_DIR / "paper_rebalance_outcome_attribution.json"
OUT_TSV_PATH = OUT_DIR / "paper_rebalance_outcome_attribution.tsv"

VERSION = "outcome_attribution_v1"

# Attribution thresholds
DRIFT_IMPROVEMENT_MEANINGFUL     = 0.02   # 2% of equity
ALLOC_FIT_IMPROVEMENT_MEANINGFUL = 0.01
CHURN_HARMFUL_THRESHOLD          = 0.40   # CHURN_HIGH level from AC56
CHURN_EXTREME_THRESHOLD          = 0.60   # CHURN_EXTREME level from AC56

# Outcome labels
LABEL_HELPFUL           = "HELPFUL"
LABEL_NEUTRAL           = "NEUTRAL"
LABEL_HARMFUL           = "HARMFUL"
LABEL_PENDING           = "PENDING"
LABEL_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
LABEL_NOT_EXECUTED      = "NOT_EXECUTED"

# Evaluation statuses
STATUS_READY             = "READY"
STATUS_PENDING_WINDOW    = "PENDING_WINDOW"
STATUS_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
STATUS_NOT_EXECUTED      = "NOT_EXECUTED"

TSV_HEADERS = [
    "audit_id", "market", "strategy_key", "position_key",
    "paper_only", "decision_quality_gate", "transition_simulation_status",
    "rebalance_execution_action", "execution_ts_utc",
    "evaluation_status", "outcome_label", "outcome_score",
    "drift_before", "drift_after", "drift_improvement",
    "budget_pressure_before", "budget_pressure_after", "budget_pressure_change",
    "allocation_fit_before", "allocation_fit_after", "allocation_fit_improvement",
    "churn_cost_proxy", "baseline_comparison_status",
    "attribution_reasons",
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


def to_float(v, default=None):
    if v is None:
        return default
    try:
        f = float(v)
        return f if f == f else default
    except Exception:
        return default


def to_float_safe(v, default=0.0):
    r = to_float(v, None)
    return r if r is not None else default


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


def alloc_fit(drift_pct: float) -> float:
    """Allocation fit = 1 - abs(drift_pct), clamped to [0, 1]."""
    return round(max(0.0, min(1.0, 1.0 - abs(drift_pct))), 4)


# ---------------------------------------------------------------------------
# Core attribution logic (importable for tests)
# ---------------------------------------------------------------------------

def attribute_outcome(
    exec_record: dict,
    drift_before: float | None,
    budget_before: float | None,
    budget_after: float | None,
    churn_cost_proxy: float | None,
    equity: float,
    sim_action: str | None,
) -> dict:
    """
    Compute attribution for a single paper execution record.

    Parameters:
      exec_record      — row from AC59 execution summary
      drift_before     — drift_pct before execution (from AC52)
      budget_before    — sim budget pressure before (from AC58)
      budget_after     — sim budget pressure after (from AC58)
      churn_cost_proxy — churn penalty from AC56 (0.0–0.60)
      equity           — portfolio equity at time of execution
      sim_action       — simulation_action from AC58 ("SIMULATE_TRANSITION" etc.)

    Returns:
      dict with all per-record attribution fields
    """
    pk          = exec_record.get("position_key", "")
    exec_status = exec_record.get("execution_status", "SKIPPED")
    delta_eur   = to_float_safe(exec_record.get("executed_delta_eur", 0.0))
    exec_ts     = exec_record.get("ts_utc", "")
    gate        = exec_record.get("decision_quality_gate", "BLOCK")
    rebal_action = exec_record.get("rebalance_action", "")
    exec_id     = exec_record.get("execution_id", pk)

    # --- Evaluation status determination ---
    if exec_status == "SKIPPED":
        return _not_executed_record(exec_id, exec_record)

    if drift_before is None or equity <= 0:
        return _insufficient_data_record(
            exec_id, exec_record,
            reason="MISSING_DRIFT_BEFORE" if drift_before is None else "MISSING_EQUITY",
        )

    if to_float(delta_eur) is None or abs(delta_eur) < 0.001:
        return _insufficient_data_record(exec_id, exec_record, reason="DELTA_ZERO_OR_MISSING")

    # --- Core attribution computation ---
    # Estimated post-execution drift:
    # drift_before = (actual - target) / equity
    # After delta applied: actual_new = actual + delta
    # drift_after_est = (actual_new - target) / equity = drift_before + delta/equity
    drift_after_est = round(drift_before + (delta_eur / equity), 6)

    drift_improvement     = round(abs(drift_before) - abs(drift_after_est), 6)
    alloc_fit_before      = alloc_fit(drift_before)
    alloc_fit_after       = alloc_fit(drift_after_est)
    alloc_fit_improvement = round(alloc_fit_after - alloc_fit_before, 6)

    # Budget pressure change
    if budget_before is not None and budget_after is not None:
        budget_pressure_change = round(budget_after - budget_before, 2)
        budget_comparison = "BUDGET_CONSUMED" if budget_pressure_change < -0.01 else "BUDGET_STABLE"
    else:
        budget_pressure_change = None
        budget_comparison = "BUDGET_DATA_MISSING"

    churn = churn_cost_proxy if churn_cost_proxy is not None else 0.0

    # Baseline comparison: no-action → drift stays at drift_before
    # Our outcome: drift moves to drift_after_est
    # Attribution vs baseline = drift_improvement (positive = better than baseline)
    if drift_improvement > DRIFT_IMPROVEMENT_MEANINGFUL:
        baseline_comparison = "BETTER_THAN_BASELINE"
    elif drift_improvement < -DRIFT_IMPROVEMENT_MEANINGFUL:
        baseline_comparison = "WORSE_THAN_BASELINE"
    else:
        baseline_comparison = "NEUTRAL_VS_BASELINE"

    # --- Outcome score ---
    # Simple weighted combination:
    # + drift improvement (main signal)
    # + allocation fit improvement
    # - churn cost (governance cost)
    raw_score = (
          3.0 * drift_improvement
        + 2.0 * alloc_fit_improvement
        - 1.0 * churn
    )
    outcome_score = round(max(-1.0, min(1.0, raw_score)), 4)

    # --- Outcome label ---
    reasons = []
    label = _determine_label(
        drift_improvement, alloc_fit_improvement, churn,
        drift_before, drift_after_est, reasons,
    )

    # Augment reasons with context
    reasons.append(f"DRIFT_BEFORE_{drift_before:.4f}")
    reasons.append(f"DRIFT_AFTER_EST_{drift_after_est:.4f}")
    reasons.append(f"CHURN_COST_{churn:.2f}")
    reasons.append(f"BASELINE_{baseline_comparison}")

    return {
        "audit_id":                     exec_id,
        "market":                       exec_record.get("market", ""),
        "strategy_key":                 exec_record.get("strategy_key", ""),
        "position_key":                 pk,
        "paper_only":                   True,
        "decision_quality_gate":        gate,
        "transition_simulation_status": sim_action or "UNKNOWN",
        "rebalance_execution_action":   rebal_action,
        "execution_ts_utc":             exec_ts,
        "evaluation_status":            STATUS_READY,
        "outcome_label":                label,
        "outcome_score":                outcome_score,
        "drift_before":                 round(drift_before, 6),
        "drift_after":                  drift_after_est,
        "drift_improvement":            drift_improvement,
        "budget_pressure_before":       budget_before,
        "budget_pressure_after":        budget_after,
        "budget_pressure_change":       budget_pressure_change,
        "allocation_fit_before":        alloc_fit_before,
        "allocation_fit_after":         alloc_fit_after,
        "allocation_fit_improvement":   alloc_fit_improvement,
        "churn_cost_proxy":             churn,
        "baseline_comparison_status":   baseline_comparison,
        "attribution_reasons":          "|".join(reasons),
    }


def _determine_label(
    drift_improvement: float,
    alloc_fit_improvement: float,
    churn: float,
    drift_before: float,
    drift_after_est: float,
    reasons: list,
) -> str:
    """
    Determine HELPFUL / NEUTRAL / HARMFUL based on governance metrics.
    Rules are explicit and ordered; reasons list is appended in-place.
    """
    # Rule 1: Drift worsened → HARMFUL
    if drift_improvement < -DRIFT_IMPROVEMENT_MEANINGFUL:
        reasons.append("HARMFUL_DRIFT_WORSENED")
        return LABEL_HARMFUL

    # Rule 2: Extreme churn with no meaningful improvement → HARMFUL
    if churn >= CHURN_EXTREME_THRESHOLD and drift_improvement < DRIFT_IMPROVEMENT_MEANINGFUL:
        reasons.append("HARMFUL_EXTREME_CHURN_NO_BENEFIT")
        return LABEL_HARMFUL

    # Rule 3: Meaningful drift improvement and acceptable churn → HELPFUL
    if (drift_improvement >= DRIFT_IMPROVEMENT_MEANINGFUL
            and alloc_fit_improvement >= ALLOC_FIT_IMPROVEMENT_MEANINGFUL
            and churn < CHURN_HARMFUL_THRESHOLD):
        reasons.append("HELPFUL_DRIFT_AND_FIT_IMPROVED")
        return LABEL_HELPFUL

    # Rule 4: Meaningful drift improvement but high churn → NEUTRAL (value, but costly)
    if (drift_improvement >= DRIFT_IMPROVEMENT_MEANINGFUL
            and churn >= CHURN_HARMFUL_THRESHOLD):
        reasons.append("NEUTRAL_IMPROVEMENT_BUT_HIGH_CHURN")
        return LABEL_NEUTRAL

    # Rule 5: Marginal improvement (below meaningful threshold) → NEUTRAL
    if drift_improvement >= 0:
        reasons.append("NEUTRAL_MARGINAL_IMPROVEMENT")
        return LABEL_NEUTRAL

    # Default fallback → NEUTRAL (residual case: small worsening within noise)
    reasons.append("NEUTRAL_WITHIN_NOISE_BAND")
    return LABEL_NEUTRAL


def _not_executed_record(exec_id: str, exec_record: dict) -> dict:
    return {
        "audit_id":                     exec_id,
        "market":                       exec_record.get("market", ""),
        "strategy_key":                 exec_record.get("strategy_key", ""),
        "position_key":                 exec_record.get("position_key", ""),
        "paper_only":                   True,
        "decision_quality_gate":        exec_record.get("decision_quality_gate", ""),
        "transition_simulation_status": exec_record.get("simulation_action", ""),
        "rebalance_execution_action":   exec_record.get("rebalance_action", ""),
        "execution_ts_utc":             exec_record.get("ts_utc", ""),
        "evaluation_status":            STATUS_NOT_EXECUTED,
        "outcome_label":                LABEL_NOT_EXECUTED,
        "outcome_score":                None,
        "drift_before":                 None,
        "drift_after":                  None,
        "drift_improvement":            None,
        "budget_pressure_before":       None,
        "budget_pressure_after":        None,
        "budget_pressure_change":       None,
        "allocation_fit_before":        None,
        "allocation_fit_after":         None,
        "allocation_fit_improvement":   None,
        "churn_cost_proxy":             None,
        "baseline_comparison_status":   "NOT_APPLICABLE",
        "attribution_reasons":          f"NOT_EXECUTED|{exec_record.get('skip_reason', '')}",
    }


def _insufficient_data_record(exec_id: str, exec_record: dict, reason: str) -> dict:
    return {
        "audit_id":                     exec_id,
        "market":                       exec_record.get("market", ""),
        "strategy_key":                 exec_record.get("strategy_key", ""),
        "position_key":                 exec_record.get("position_key", ""),
        "paper_only":                   True,
        "decision_quality_gate":        exec_record.get("decision_quality_gate", ""),
        "transition_simulation_status": exec_record.get("simulation_action", ""),
        "rebalance_execution_action":   exec_record.get("rebalance_action", ""),
        "execution_ts_utc":             exec_record.get("ts_utc", ""),
        "evaluation_status":            STATUS_INSUFFICIENT_DATA,
        "outcome_label":                LABEL_INSUFFICIENT_DATA,
        "outcome_score":                None,
        "drift_before":                 None,
        "drift_after":                  None,
        "drift_improvement":            None,
        "budget_pressure_before":       None,
        "budget_pressure_after":        None,
        "budget_pressure_change":       None,
        "allocation_fit_before":        None,
        "allocation_fit_after":         None,
        "allocation_fit_improvement":   None,
        "churn_cost_proxy":             None,
        "baseline_comparison_status":   "INSUFFICIENT_DATA",
        "attribution_reasons":          f"INSUFFICIENT_DATA|{reason}",
    }


# ---------------------------------------------------------------------------
# Batch attribution (importable)
# ---------------------------------------------------------------------------

def build_attribution_records(
    exec_records: list,
    drift_index: dict,
    sim_index: dict,
    dq_index: dict,
    equity: float,
) -> list:
    """
    Run attribution for every record in exec_records.

    drift_index : position_key → drift row (AC52)
    sim_index   : position_key → simulation row (AC58)
    dq_index    : position_key → decision quality row (AC56)
    equity      : portfolio equity
    """
    results = []
    for rec in exec_records:
        pk = rec.get("position_key", "")

        drift_row = drift_index.get(pk) or {}
        sim_row   = sim_index.get(pk) or {}
        dq_row    = dq_index.get(pk) or {}

        drift_before = to_float(drift_row.get("drift_pct"), None)
        budget_before = to_float(sim_row.get("budget_pressure_before_eur"), None)
        budget_after  = to_float(sim_row.get("budget_pressure_after_eur"), None)
        churn_cost    = to_float(dq_row.get("churn_penalty"), None)
        sim_action    = sim_row.get("simulation_action")

        out = attribute_outcome(
            exec_record=rec,
            drift_before=drift_before,
            budget_before=budget_before,
            budget_after=budget_after,
            churn_cost_proxy=churn_cost,
            equity=equity,
            sim_action=sim_action,
        )
        results.append(out)
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    exec_data  = load_json(EXEC_SUMMARY_PATH, {}) or {}
    drift_data = load_json(DRIFT_PATH, {}) or {}
    sim_data   = load_json(SIM_PATH, {}) or {}
    dq_data    = load_json(DQ_PATH, {}) or {}

    cycle_id = exec_data.get("cycle_id") or drift_data.get("cycle_id") or "UNKNOWN"
    equity   = to_float_safe(exec_data.get("equity") or drift_data.get("equity", 0.0))

    exec_records = exec_data.get("records") or []

    drift_index = {r["position_key"]: r for r in (drift_data.get("rows") or [])
                   if r.get("position_key")}
    sim_index   = {r["position_key"]: r for r in (sim_data.get("rows") or [])
                   if r.get("position_key")}
    dq_index    = {r["position_key"]: r for r in (dq_data.get("records") or [])
                   if r.get("position_key")}

    records = build_attribution_records(exec_records, drift_index, sim_index, dq_index, equity)

    ready_records = [r for r in records if r["evaluation_status"] == STATUS_READY]
    scores        = [r["outcome_score"] for r in ready_records if r["outcome_score"] is not None]
    drift_imps    = [r["drift_improvement"] for r in ready_records if r["drift_improvement"] is not None]
    budget_chgs   = [r["budget_pressure_change"] for r in ready_records if r["budget_pressure_change"] is not None]
    churns        = [r["churn_cost_proxy"] for r in ready_records if r["churn_cost_proxy"] is not None]

    def avg(lst): return round(sum(lst) / len(lst), 4) if lst else None

    summary = {
        "records_total":           len(records),
        "pending_count":           sum(1 for r in records if r["outcome_label"] == LABEL_PENDING),
        "insufficient_data_count": sum(1 for r in records if r["outcome_label"] == LABEL_INSUFFICIENT_DATA),
        "not_executed_count":      sum(1 for r in records if r["outcome_label"] == LABEL_NOT_EXECUTED),
        "helpful_count":           sum(1 for r in records if r["outcome_label"] == LABEL_HELPFUL),
        "neutral_count":           sum(1 for r in records if r["outcome_label"] == LABEL_NEUTRAL),
        "harmful_count":           sum(1 for r in records if r["outcome_label"] == LABEL_HARMFUL),
        "avg_outcome_score":       avg(scores),
        "avg_drift_improvement":   avg(drift_imps),
        "avg_budget_pressure_change": avg(budget_chgs),
        "avg_churn_cost_proxy":    avg(churns),
    }

    out = {
        "component":  "build_paper_rebalance_outcome_attribution_lite",
        "version":    VERSION,
        "ts_utc":     ts,
        "cycle_id":   cycle_id,
        "equity":     equity,
        "paper_only": True,
        "source_files": {
            "paper_rebalance_execution_summary": str(EXEC_SUMMARY_PATH),
            "allocation_portfolio_drift":        str(DRIFT_PATH),
            "portfolio_transition_simulation":   str(SIM_PATH),
            "allocation_decision_quality":       str(DQ_PATH),
        },
        "attribution_thresholds": {
            "drift_improvement_meaningful":     DRIFT_IMPROVEMENT_MEANINGFUL,
            "alloc_fit_improvement_meaningful": ALLOC_FIT_IMPROVEMENT_MEANINGFUL,
            "churn_harmful_threshold":          CHURN_HARMFUL_THRESHOLD,
            "churn_extreme_threshold":          CHURN_EXTREME_THRESHOLD,
        },
        "summary": summary,
        "records": records,
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(OUT_PATH, out)
        write_tsv(OUT_TSV_PATH, TSV_HEADERS, records)
    except Exception as e:
        print(f"[WARN] Could not write output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k != "records"}, indent=2))

    for r in records:
        print(f"  {r['position_key']:<30} {r['outcome_label']:<20}"
              f" eval={r['evaluation_status']:<20}"
              f" score={str(r['outcome_score']):<8}"
              f" drift_imp={str(r['drift_improvement'])}")


if __name__ == "__main__":
    main()
