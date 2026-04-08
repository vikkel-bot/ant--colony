"""
AC56+AC62: Decision Quality Gate for Allocation and Rebalance
Pure observability + scoring layer. No execution, no orders, no state changes.

Sits after rebalance budget audit (AC55), before any future execution transition.
AC62 wires AC-61 conviction modifier into conviction scoring (conservative).

Reads:
  allocation_portfolio_drift.json         — drift, severity, cause, actual/target per position
  rebalance_intents.json                  — rebalance action, delta, budget selection per intent
  execution_summary.json                  — feedback_confidence, regime_type per strategy (AC46+AC48)
  allocation_feedback_integration.json    — conviction modifier per strategy_key (AC61, optional)

Writes:
  allocation_decision_quality.json
  allocation_decision_quality.tsv

Score components:
  drift_materiality_score  (weight 0.40) — how meaningful is the drift?
  conviction_score         (weight 0.35) — how strong is the allocation signal?
  budget_ok_score          (weight 0.10) — is the intent budget-approved?
  regime_compat_score      (weight 0.15) — does regime support this direction?
  churn_penalty            (weight 0.20) — penalty for high-turnover changes

AC62 conviction modifier (fail-closed):
  effective_feedback_confidence = base_feedback_confidence × modifier
  modifier from AC61 per strategy_key (default 1.00 if missing/fallback)
  modifier band: [0.90, 1.05] — cannot dominate base conviction

Gate (fail-closed):
  PASS  score >= 0.55 AND budget selected AND drift material
  HOLD  score >= 0.30
  BLOCK score < 0.30 OR budget excluded OR drift immaterial

Usage: python ant_colony/build_allocation_decision_quality_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

DRIFT_PATH        = OUT_DIR / "allocation_portfolio_drift.json"
REBALANCE_PATH    = OUT_DIR / "rebalance_intents.json"
EXEC_SUMMARY_PATH = OUT_DIR / "execution_summary.json"
FEEDBACK_PATH     = OUT_DIR / "allocation_feedback_integration.json"   # AC61

OUT_PATH     = OUT_DIR / "allocation_decision_quality.json"
OUT_TSV_PATH = OUT_DIR / "allocation_decision_quality.tsv"

TSV_HEADERS = [
    "market", "strategy", "position_key",
    "drift_pct", "drift_severity", "rebalance_action",
    "base_feedback_confidence", "allocation_conviction_modifier",
    "effective_feedback_confidence", "feedback_modifier_applied",
    "drift_materiality_score", "conviction_score", "churn_penalty",
    "decision_quality_score", "decision_quality_gate",
    "decision_quality_reasons",
]

VERSION = "decision_quality_v2"  # bumped for AC62 conviction modifier wiring

# AC62: hard bounds on modifier effect — prevents any single feedback record
# from dominating the conviction score
MODIFIER_MIN = 0.90
MODIFIER_MAX = 1.05
MODIFIER_NEUTRAL = 1.00

# Score weights (must sum to ≤ 1.0 for positive terms)
W_DRIFT      = 0.40
W_CONVICTION = 0.35
W_BUDGET     = 0.10
W_REGIME     = 0.15
W_CHURN_PEN  = 0.20   # subtracted

# Gate thresholds
GATE_PASS_MIN = 0.55
GATE_HOLD_MIN = 0.30

# Drift materiality breakpoints (abs drift_pct)
DRIFT_IMMATERIAL  = 0.05   # < 5%  → score 0.00 → hard BLOCK
DRIFT_MARGINAL    = 0.10   # 5-10% → score 0.25
DRIFT_MEDIUM_BRKT = 0.20   # 10-20%→ score 0.55
DRIFT_LARGE_BRKT  = 0.40   # 20-40%→ score 0.80
                            # ≥ 40%  → score 1.00

# Churn ratio breakpoints (|delta| / max(actual, target))
CHURN_LOW  = 0.20   # < 20%  → penalty 0.00
CHURN_MED  = 0.50   # 20-50% → penalty 0.20
CHURN_HIGH = 0.90   # 50-90% → penalty 0.40
                    # ≥ 90%   → penalty 0.60 (extreme turnover)

# Regime compatibility scalars
_REGIME_COMPAT = {
    "BULL":     1.0,
    "TREND":    1.0,
    "SIDEWAYS": 0.6,
    "SIDE":     0.6,
    "BEAR":     0.3,
    "UNKNOWN":  0.5,
}


# ---------------------------------------------------------------------------
# Scoring helpers (all importable for tests)
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
            "" if row.get(h) is None else str(row.get(h))
            for h in headers
        ))
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# AC62: Conviction modifier helpers (importable for tests)
# ---------------------------------------------------------------------------

def load_feedback_index(path: Path) -> dict:
    """
    Load AC61 allocation_feedback_integration.json and return an index
    keyed by strategy_key. Returns empty dict on missing/broken file (fail-closed).
    """
    data = load_json(path, {})
    if not data or not isinstance(data, dict):
        return {}
    index = {}
    for rec in (data.get("records") or []):
        sk = str(rec.get("strategy_key") or "").strip()
        if sk:
            index[sk] = rec
    return index


def apply_conviction_modifier(
    base_feedback_confidence: float,
    strategy_key: str,
    feedback_index: dict,
) -> tuple:
    """
    Apply AC61 conviction modifier to base_feedback_confidence.

    Returns (effective_confidence, modifier, bias_class, feedback_status,
             cooldown_flag, modifier_applied, modifier_reason).

    Fail-closed: if strategy_key not in index, returns (base, 1.00, ...).
    Modifier is hard-clamped to [MODIFIER_MIN, MODIFIER_MAX] regardless of
    AC61 values, preventing any single feedback record from dominating.
    """
    fb_rec = feedback_index.get(strategy_key)

    if fb_rec is None:
        return (
            round(max(0.0, min(1.0, base_feedback_confidence)), 4),
            MODIFIER_NEUTRAL,
            "NEUTRAL",
            "NO_FEEDBACK_DATA",
            False,
            False,
            "FALLBACK_NO_AC61_RECORD",
        )

    raw_modifier   = to_float(fb_rec.get("allocation_conviction_modifier", MODIFIER_NEUTRAL))
    modifier       = round(max(MODIFIER_MIN, min(MODIFIER_MAX, raw_modifier)), 4)
    bias_class     = str(fb_rec.get("allocation_bias_class") or "NEUTRAL")
    feedback_status = str(fb_rec.get("feedback_status") or "UNKNOWN")
    cooldown_flag  = bool(fb_rec.get("cooldown_flag", False))

    effective_conf = round(max(0.0, min(1.0, base_feedback_confidence * modifier)), 4)
    modifier_applied = (modifier != MODIFIER_NEUTRAL)
    modifier_reason = (
        f"AC61_{bias_class}_MOD_{modifier}"
        if modifier_applied
        else "AC61_NEUTRAL_MOD_1.00"
    )

    return (
        effective_conf,
        modifier,
        bias_class,
        feedback_status,
        cooldown_flag,
        modifier_applied,
        modifier_reason,
    )


def score_drift_materiality(drift_pct: float) -> tuple:
    """
    Returns (score 0.0–1.0, reason_str).
    Scores how meaningful the drift is. Immaterial drift → 0.0.
    """
    abs_drift = abs(drift_pct)
    if abs_drift < DRIFT_IMMATERIAL:
        return 0.0, "DRIFT_IMMATERIAL"
    if abs_drift < DRIFT_MARGINAL:
        return 0.25, "DRIFT_MARGINAL"
    if abs_drift < DRIFT_MEDIUM_BRKT:
        return 0.55, "DRIFT_MEDIUM"
    if abs_drift < DRIFT_LARGE_BRKT:
        return 0.80, "DRIFT_LARGE"
    return 1.00, "DRIFT_EXTREME"


def score_conviction(feedback_confidence: float, regime_type: str) -> tuple:
    """
    Returns (score 0.0–1.0, reason_str).
    Conviction = feedback_confidence scaled by regime alignment.
    """
    conf  = max(0.0, min(1.0, to_float(feedback_confidence, 0.0)))
    reg   = str(regime_type or "UNKNOWN").upper().strip()
    compat = _REGIME_COMPAT.get(reg, 0.5)
    score  = round(conf * compat, 4)

    if conf >= 0.85:
        conf_label = "CONVICTION_STRONG"
    elif conf >= 0.50:
        conf_label = "CONVICTION_MEDIUM"
    else:
        conf_label = "CONVICTION_WEAK"

    if compat >= 0.9:
        reg_label = "REGIME_ALIGNED"
    elif compat >= 0.55:
        reg_label = "REGIME_NEUTRAL"
    else:
        reg_label = "REGIME_CONSTRAINED"

    return score, f"{conf_label}|{reg_label}"


def score_regime_compat(regime_type: str) -> tuple:
    """Returns (score 0.0–1.0, reason_str) for regime alignment standalone."""
    reg   = str(regime_type or "UNKNOWN").upper().strip()
    score = _REGIME_COMPAT.get(reg, 0.5)
    if score >= 0.9:
        return score, "REGIME_FULLY_ALIGNED"
    if score >= 0.55:
        return score, "REGIME_PARTIALLY_ALIGNED"
    return score, "REGIME_ADVERSE"


def score_churn(delta_eur: float, actual_eur: float, target_eur: float) -> tuple:
    """
    Returns (penalty 0.0–0.6, reason_str).
    Churn = |delta| relative to the larger of actual or target exposure.
    Higher churn → higher penalty.
    """
    reference = max(abs(actual_eur), abs(target_eur), 1.0)
    ratio     = abs(delta_eur) / reference
    if ratio < CHURN_LOW:
        return 0.00, "CHURN_LOW"
    if ratio < CHURN_MED:
        return 0.20, "CHURN_MEDIUM"
    if ratio < CHURN_HIGH:
        return 0.40, "CHURN_HIGH"
    return 0.60, "CHURN_EXTREME"


def compute_quality_score(
    drift_mat: float,
    conviction: float,
    budget_ok: float,
    regime_compat: float,
    churn_pen: float,
) -> float:
    """
    Weighted combination of score components minus churn penalty.
    Clamped to [0.0, 1.0].
    """
    raw = (
        W_DRIFT      * drift_mat
      + W_CONVICTION * conviction
      + W_BUDGET     * budget_ok
      + W_REGIME     * regime_compat
      - W_CHURN_PEN  * churn_pen
    )
    return round(max(0.0, min(1.0, raw)), 4)


def determine_gate(
    score: float,
    rebalance_selected: bool,
    drift_pct: float,
    drift_mat_score: float,
    component_reasons: list,
    regime_score: float = 1.0,
) -> tuple:
    """
    Returns (gate: PASS|HOLD|BLOCK, reasons: list[str]).
    Fail-closed: BLOCK on hard constraints before score gate.

    Hard constraints (checked in priority order):
      1. Budget excluded                   → BLOCK
      2. Drift immaterial                  → BLOCK
      3. Adverse regime (score < 0.40)     → cap at HOLD (never PASS in BEAR)
      4. Score gate: ≥ 0.55 PASS / ≥ 0.30 HOLD / else BLOCK
    """
    reasons = list(component_reasons)

    # 1. Hard BLOCK: budget excluded
    if not rebalance_selected:
        reasons.append("BUDGET_EXCLUDED")
        return "BLOCK", reasons

    # 2. Hard BLOCK: drift too small to justify portfolio movement
    if drift_mat_score == 0.0:
        reasons.append("DRIFT_TOO_SMALL")
        return "BLOCK", reasons

    # 3. Score-based gate
    if score >= GATE_PASS_MIN:
        # Soft cap: adverse regime (BEAR) cannot produce PASS — cap at HOLD
        if regime_score < 0.40:
            reasons.append("REGIME_CAP_HOLD")
            return "HOLD", reasons
        reasons.append(f"QUALITY_SCORE_PASS_{score:.4f}")
        return "PASS", reasons
    if score >= GATE_HOLD_MIN:
        reasons.append(f"QUALITY_SCORE_HOLD_{score:.4f}")
        return "HOLD", reasons

    reasons.append(f"QUALITY_SCORE_BLOCK_{score:.4f}")
    return "BLOCK", reasons


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    drift_data    = load_json(DRIFT_PATH, {}) or {}
    rebal_data    = load_json(REBALANCE_PATH, {}) or {}
    exec_summary  = load_json(EXEC_SUMMARY_PATH, {}) or {}

    # AC62: load AC61 feedback integration (fail-closed: empty dict if missing)
    feedback_index = load_feedback_index(FEEDBACK_PATH)

    cycle_id  = drift_data.get("cycle_id") or rebal_data.get("cycle_id")
    equity    = to_float(drift_data.get("equity", 0.0))

    # Index drift rows by position_key
    drift_index = {}
    for row in (drift_data.get("rows") or []):
        pk = row.get("position_key")
        if pk:
            drift_index[pk] = row

    # Index rebalance intents by position_key
    rebal_index = {}
    for intent in (rebal_data.get("intents") or []):
        pk = intent.get("position_key")
        if pk:
            rebal_index[pk] = intent

    # Index execution_summary strategies by position_key
    exec_index = {}
    for market, mkt_row in (exec_summary.get("markets") or {}).items():
        for strategy, sr in (mkt_row.get("strategies") or {}).items():
            pk = f"{market}__{strategy}"
            exec_index[pk] = sr or {}

    # Build quality records for every candidate that has a rebalance intent
    records    = []
    gate_counts = {"PASS": 0, "HOLD": 0, "BLOCK": 0}

    # AC62: counters for modifier summary
    mod_applied_count    = 0
    mod_neutral_fallback = 0
    mod_positive_count   = 0
    mod_negative_count   = 0
    mod_caution_count    = 0

    for pk, intent in sorted(rebal_index.items()):
        drift_row = drift_index.get(pk) or {}
        exec_sr   = exec_index.get(pk) or {}

        market   = intent.get("market")
        strategy = intent.get("strategy")

        # --- Core inputs ---
        drift_pct       = to_float(drift_row.get("drift_pct", 0.0))
        drift_severity  = str(drift_row.get("drift_severity") or "LOW")
        rebal_selected  = bool(intent.get("rebalance_selected", False))
        rebal_action    = str(intent.get("rebalance_action") or "REBALANCE_HOLD")
        delta_eur       = to_float(intent.get("rebalance_capped_delta_eur", 0.0))
        actual_eur      = to_float(drift_row.get("actual_notional_eur", 0.0))
        target_eur      = to_float(drift_row.get("target_notional_eur", 0.0))
        alloc_pct       = to_float(drift_row.get("allocation_pct", 0.0))

        # From execution_summary (fail-safe defaults)
        base_feedback_conf = to_float(exec_sr.get("feedback_confidence", 0.0))
        regime_type        = str(exec_sr.get("regime_type") or
                                  drift_row.get("regime_type") or "UNKNOWN")

        # AC62: apply conviction modifier from AC61 feedback integration
        (effective_conf, modifier, bias_class,
         fb_status, cooldown_flag,
         modifier_applied, modifier_reason) = apply_conviction_modifier(
            base_feedback_conf, pk, feedback_index
        )

        # Track modifier summary counts
        if modifier_applied:
            mod_applied_count += 1
            if "POSITIVE" in bias_class:
                mod_positive_count += 1
            elif "CAUTION" in bias_class:
                mod_caution_count += 1
            elif "NEGATIVE" in bias_class:
                mod_negative_count += 1
        else:
            mod_neutral_fallback += 1

        # --- Score components (conviction uses effective_conf, not raw) ---
        drift_mat_score, drift_mat_reason   = score_drift_materiality(drift_pct)
        conviction_score, conviction_reason = score_conviction(effective_conf, regime_type)
        regime_score, regime_reason         = score_regime_compat(regime_type)
        churn_pen, churn_reason             = score_churn(delta_eur, actual_eur, target_eur)
        budget_ok_score = 1.0 if rebal_selected else 0.0

        quality_score = compute_quality_score(
            drift_mat_score, conviction_score, budget_ok_score, regime_score, churn_pen
        )

        # --- Gate ---
        component_reasons = [
            drift_mat_reason, conviction_reason, regime_reason, churn_reason,
        ]
        if modifier_applied:
            component_reasons.append(modifier_reason)
        gate, gate_reasons = determine_gate(
            quality_score, rebal_selected, drift_pct,
            drift_mat_score, component_reasons,
            regime_score=regime_score,
        )
        gate_counts[gate] = gate_counts.get(gate, 0) + 1

        records.append({
            # Identity
            "market":         market,
            "strategy":       strategy,
            "position_key":   pk,
            "cycle_id":       cycle_id,
            # Inputs
            "current_weight":       alloc_pct,
            "target_weight":        alloc_pct,
            "drift_pct":            drift_pct,
            "drift_severity":       drift_severity,
            "drift_cause":          drift_row.get("drift_cause"),
            "rebalance_action":     rebal_action,
            "rebalance_selected":   rebal_selected,
            "regime_type":          regime_type,
            "actual_notional_eur":  actual_eur,
            "target_notional_eur":  target_eur,
            "rebalance_delta_eur":  delta_eur,
            # AC62: conviction modifier audit trail
            "base_feedback_confidence":       base_feedback_conf,
            "allocation_conviction_modifier": modifier,
            "effective_feedback_confidence":  effective_conf,
            "allocation_bias_class":          bias_class,
            "feedback_status":                fb_status,
            "cooldown_flag":                  cooldown_flag,
            "feedback_modifier_applied":      modifier_applied,
            "feedback_modifier_reason":       modifier_reason,
            # legacy field alias for compatibility
            "feedback_confidence":            effective_conf,
            # Score components
            "drift_materiality_score":  drift_mat_score,
            "conviction_score":         conviction_score,
            "budget_ok_score":          budget_ok_score,
            "regime_compat_score":      regime_score,
            "churn_penalty":            churn_pen,
            # Final
            "decision_quality_score":   quality_score,
            "decision_quality_gate":    gate,
            "decision_quality_reasons": "|".join(gate_reasons),
        })

    pass_count  = gate_counts.get("PASS", 0)
    hold_count  = gate_counts.get("HOLD", 0)
    block_count = gate_counts.get("BLOCK", 0)

    out = {
        "component":  "build_allocation_decision_quality_lite",
        "version":    VERSION,
        "ts_utc":     ts,
        "cycle_id":   cycle_id,
        "equity":     equity,
        "rows_total": len(records),
        "pass_count":  pass_count,
        "hold_count":  hold_count,
        "block_count": block_count,
        "gate_counts": gate_counts,
        "score_weights": {
            "drift_materiality": W_DRIFT,
            "conviction":        W_CONVICTION,
            "budget_ok":         W_BUDGET,
            "regime_compat":     W_REGIME,
            "churn_penalty_neg": W_CHURN_PEN,
        },
        "gate_thresholds": {
            "pass_min": GATE_PASS_MIN,
            "hold_min": GATE_HOLD_MIN,
        },
        # AC62: modifier summary
        "feedback_modifier_records_total":        len(records),
        "feedback_modifier_applied_count":        mod_applied_count,
        "feedback_modifier_neutral_fallback_count": mod_neutral_fallback,
        "feedback_modifier_positive_count":       mod_positive_count,
        "feedback_modifier_negative_count":       mod_negative_count,
        "feedback_modifier_caution_count":        mod_caution_count,
        "source_feedback_keys":                   len(feedback_index),
        "source_drift_rows":   len(drift_index),
        "source_rebal_intents": len(rebal_index),
        "source_exec_strategies": len(exec_index),
        "records": records,
    }

    write_json(OUT_PATH, out)
    write_tsv(OUT_TSV_PATH, TSV_HEADERS, records)

    print(json.dumps({k: v for k, v in out.items() if k != "records"}, indent=2))


if __name__ == "__main__":
    main()
