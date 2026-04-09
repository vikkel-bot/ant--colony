"""
AC65: Memory-Aware Impact Observability
Pure observability artefact. No execution, no scoring changes, no strategy mutation.

Sits after AC-64 (Memory-Aware Conviction Integration).

Central question:
  Has persistent memory from AC-63 actually, safely, and by design influenced
  conviction and decision quality — and if so, when, how often, and in which direction?

This layer reads the AC-64 decision_quality output and reconstructs before/after
metrics to make memory impact fully transparent.

Before/after model:
  - pre_memory_effective_confidence  = base_feedback_confidence × cycle_modifier
  - post_memory_effective_confidence = effective_feedback_confidence  (from AC-64)
  - pre_memory_dq_score: recomputed inline using same weights as AC-56/64
  - post_memory_dq_score: decision_quality_score from AC-64 output
  All inputs come from AC-64 JSON — no assumptions, no reconstruction from memory.

Impact classes (closed set):
  NO_MEMORY            — memory record absent entirely
  NO_EFFECT            — memory neutral; no correction applied
  NEGATIVE_DAMPENING   — memory applied a negative correction
  CAUTION_DAMPENING    — cooldown gate or NEGATIVE_CAUTION bias damped conviction
  POSITIVE_REINFORCEMENT — memory applied a small positive correction
  BLOCKED_BY_CONFLICT  — positive memory blocked by cycle being negative/caution
  BLOCKED_LOW_CONFIDENCE — confidence gate prevented positive effect
  BLOCKED_ABSENT_MEMORY — alias for NO_MEMORY in blocked-gate context

This layer does NOT:
  - change scoring logic
  - apply new modifiers
  - write to execution pipeline

This layer DOES:
  - compute before/after modifier, confidence, and quality metrics
  - classify impact per record
  - verify safe-band compliance
  - summarise gate behaviour across all records

Reads:
  allocation_decision_quality.json   — AC-64 output with full audit trail

Writes:
  allocation_memory_impact_observability.json
  allocation_memory_impact_observability.tsv

Usage: python ant_colony/build_allocation_memory_impact_observability_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

DQ_PATH      = OUT_DIR / "allocation_decision_quality.json"
OUT_PATH     = OUT_DIR / "allocation_memory_impact_observability.json"
OUT_TSV_PATH = OUT_DIR / "allocation_memory_impact_observability.tsv"

VERSION = "memory_impact_observability_v1"

# Score weights — mirror AC-56/64 constants exactly
_W_DRIFT     = 0.40
_W_CONV      = 0.35
_W_BUDGET    = 0.10
_W_REGIME    = 0.15
_W_CHURN_PEN = 0.20
_GATE_PASS   = 0.55
_GATE_HOLD   = 0.30

# Modifier safe band — mirrors AC-56/64
MODIFIER_BAND_MIN = 0.90
MODIFIER_BAND_MAX = 1.05

# Impact classes (closed set)
IMPACT_NO_MEMORY          = "NO_MEMORY"
IMPACT_NO_EFFECT          = "NO_EFFECT"
IMPACT_NEGATIVE_DAMPENING = "NEGATIVE_DAMPENING"
IMPACT_CAUTION_DAMPENING  = "CAUTION_DAMPENING"
IMPACT_POSITIVE_REINFORCE = "POSITIVE_REINFORCEMENT"
IMPACT_BLOCKED_CONFLICT   = "BLOCKED_BY_CONFLICT"
IMPACT_BLOCKED_LOW_CONF   = "BLOCKED_LOW_CONFIDENCE"
IMPACT_BLOCKED_ABSENT     = "BLOCKED_ABSENT_MEMORY"

_GATE_TO_IMPACT = {
    "MEMORY_ABSENT":          IMPACT_NO_MEMORY,
    "MEMORY_CONF_TOO_LOW":    IMPACT_BLOCKED_LOW_CONF,
    "COOLDOWN_GATE_OPEN":     IMPACT_CAUTION_DAMPENING,
    "NEGATIVE_GATE_OPEN":     IMPACT_NEGATIVE_DAMPENING,  # may be upgraded to CAUTION below
    "CONFLICT_BLOCKED":       IMPACT_BLOCKED_CONFLICT,
    "POSITIVE_CONF_TOO_LOW":  IMPACT_BLOCKED_LOW_CONF,
    "RECENT_HARMFUL_BLOCKED": IMPACT_BLOCKED_LOW_CONF,
    "POSITIVE_GATE_OPEN":     IMPACT_POSITIVE_REINFORCE,
    "MEMORY_NEUTRAL":         IMPACT_NO_EFFECT,
}

TSV_HEADERS = [
    "strategy_key", "market",
    "base_feedback_confidence",
    "cycle_modifier", "cycle_bias_class",
    "memory_modifier", "memory_bias_class", "memory_confidence",
    "effective_modifier_final", "modifier_delta",
    "pre_memory_effective_confidence", "post_memory_effective_confidence", "confidence_delta",
    "decision_quality_score_before_memory", "decision_quality_score_after_memory", "dq_score_delta",
    "decision_quality_gate_before_memory", "decision_quality_gate_after_memory", "dq_gate_changed",
    "memory_available", "memory_influence_gate", "memory_modifier_applied",
    "cooldown_flag", "impact_class", "impact_direction",
    "safe_band_ok", "observability_flags",
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


def _safe(v, lo=0.0, hi=1.0):
    return round(max(lo, min(hi, v)), 4)


# ---------------------------------------------------------------------------
# Core observability logic (importable for tests)
# ---------------------------------------------------------------------------

def determine_impact_class(
    gate_name: str,
    memory_bias_class: str,
    modifier_delta: float,
    applied: bool,
) -> str:
    """
    Map gate_name + applied status → closed impact class string.

    NEGATIVE_GATE_OPEN with NEGATIVE_CAUTION bias → CAUTION_DAMPENING.
    All other mappings via _GATE_TO_IMPACT table.
    Fallback on modifier_delta sign for any unmapped gate.
    """
    if gate_name == "NEGATIVE_GATE_OPEN" and memory_bias_class == "NEGATIVE_CAUTION":
        return IMPACT_CAUTION_DAMPENING

    if gate_name in _GATE_TO_IMPACT:
        return _GATE_TO_IMPACT[gate_name]

    # Fallback: derive from modifier delta
    if modifier_delta < -0.0001:
        return IMPACT_NEGATIVE_DAMPENING
    if modifier_delta > 0.0001:
        return IMPACT_POSITIVE_REINFORCE
    return IMPACT_NO_EFFECT


def determine_impact_direction(modifier_delta: float) -> str:
    if modifier_delta < -0.0001:
        return "NEGATIVE"
    if modifier_delta > 0.0001:
        return "POSITIVE"
    return "NEUTRAL"


def compute_pre_memory_metrics(dq_rec: dict) -> dict:
    """
    Recompute conviction and decision quality using only the cycle modifier
    (i.e., as if memory gate had not been applied).

    Uses the same weight constants as AC-56/64. All inputs from the AC-64 record.
    Returns a dict with pre-memory conviction, DQ score, and gate.

    This is deterministic: same input record → same pre-memory metrics.
    """
    base_conf      = to_float(dq_rec.get("base_feedback_confidence", 0.0))
    cycle_mod      = to_float(dq_rec.get("allocation_conviction_modifier", 1.00))
    drift_mat      = to_float(dq_rec.get("drift_materiality_score", 0.0))
    churn_pen      = to_float(dq_rec.get("churn_penalty", 0.0))
    budget_ok      = to_float(dq_rec.get("budget_ok_score", 0.0))
    regime_compat  = to_float(dq_rec.get("regime_compat_score", 0.5))
    rebal_selected = bool(dq_rec.get("rebalance_selected", budget_ok >= 1.0))

    # Pre-memory effective confidence = base × cycle_modifier (clamped)
    pre_conf = _safe(base_conf * cycle_mod)

    # Pre-memory conviction = pre_conf × regime_compat (mirrors score_conviction)
    pre_conviction = _safe(pre_conf * regime_compat)

    # Pre-memory quality score (mirrors compute_quality_score with same weights)
    raw = (
        _W_DRIFT     * drift_mat
      + _W_CONV      * pre_conviction
      + _W_BUDGET    * budget_ok
      + _W_REGIME    * regime_compat
      - _W_CHURN_PEN * churn_pen
    )
    pre_dq_score = _safe(raw, 0.0, 1.0)

    # Pre-memory gate (mirrors determine_gate logic)
    if not rebal_selected:
        pre_gate = "BLOCK"
    elif drift_mat == 0.0:
        pre_gate = "BLOCK"
    elif pre_dq_score >= _GATE_PASS:
        pre_gate = "HOLD" if regime_compat < 0.40 else "PASS"
    elif pre_dq_score >= _GATE_HOLD:
        pre_gate = "HOLD"
    else:
        pre_gate = "BLOCK"

    return {
        "pre_memory_effective_confidence":       pre_conf,
        "pre_memory_conviction_score":           pre_conviction,
        "decision_quality_score_before_memory":  pre_dq_score,
        "decision_quality_gate_before_memory":   pre_gate,
    }


def build_observability_record(dq_rec: dict) -> dict:
    """
    Build a single memory-impact observability record from one AC-64 DQ record.

    Computes pre-memory metrics, impact class, and observability flags.
    Fail-closed: missing fields get safe defaults.
    """
    # Identity
    pk           = str(dq_rec.get("position_key") or "")
    market       = str(dq_rec.get("market") or "")
    strategy_key = pk  # position_key format == strategy_key format

    # Cycle modifier (AC-62)
    base_conf         = to_float(dq_rec.get("base_feedback_confidence", 0.0))
    cycle_modifier    = to_float(dq_rec.get("allocation_conviction_modifier", 1.00))
    cycle_bias_class  = str(dq_rec.get("allocation_bias_class") or "NEUTRAL")

    # Memory fields (AC-64)
    memory_modifier   = to_float(dq_rec.get("memory_modifier", 1.00))
    memory_bias_class = str(dq_rec.get("memory_bias_class") or "NO_MEMORY")
    memory_confidence = to_float(dq_rec.get("memory_confidence", 0.0))
    mem_applied       = bool(dq_rec.get("memory_modifier_applied", False))
    mem_gate          = str(dq_rec.get("memory_influence_gate") or "MEMORY_ABSENT")
    mem_reason        = str(dq_rec.get("memory_influence_reason") or "")
    cooldown_flag     = bool(dq_rec.get("cooldown_flag", False))

    # Final modifier and post-memory confidence (AC-64)
    final_modifier       = to_float(dq_rec.get("effective_modifier_final", cycle_modifier))
    post_memory_conf     = to_float(dq_rec.get("effective_feedback_confidence", 0.0))
    post_dq_score        = to_float(dq_rec.get("decision_quality_score", 0.0))
    post_gate            = str(dq_rec.get("decision_quality_gate") or "BLOCK")

    # Pre-memory reconstruction
    pre = compute_pre_memory_metrics(dq_rec)
    pre_conf      = pre["pre_memory_effective_confidence"]
    pre_dq_score  = pre["decision_quality_score_before_memory"]
    pre_gate      = pre["decision_quality_gate_before_memory"]

    # Delta metrics
    modifier_delta    = round(final_modifier - cycle_modifier, 4)
    confidence_delta  = round(post_memory_conf - pre_conf, 4)
    dq_score_delta    = round(post_dq_score - pre_dq_score, 4)
    dq_gate_changed   = (pre_gate != post_gate)

    # Impact classification
    impact_class     = determine_impact_class(mem_gate, memory_bias_class, modifier_delta, mem_applied)
    impact_direction = determine_impact_direction(modifier_delta)

    # Memory availability flag
    memory_available = (mem_gate != "MEMORY_ABSENT")

    # Safe band check
    safe_band_ok = (MODIFIER_BAND_MIN <= final_modifier <= MODIFIER_BAND_MAX)

    # Observability flags
    obs_flags = []
    if not safe_band_ok:
        obs_flags.append("SAFE_BAND_VIOLATION")
    if dq_gate_changed:
        obs_flags.append("DQ_GATE_CHANGED")
    if confidence_delta < -0.0001:
        obs_flags.append("MEMORY_DAMPENED")
    elif confidence_delta > 0.0001:
        obs_flags.append("MEMORY_REINFORCED")
    if cooldown_flag:
        obs_flags.append("COOLDOWN_ACTIVE")
    if cycle_bias_class in ("NEGATIVE_CAUTION", "NEGATIVE") and impact_class == IMPACT_POSITIVE_REINFORCE:
        obs_flags.append("UNEXPECTED_POSITIVE_ON_NEGATIVE_CYCLE")
    if not obs_flags:
        obs_flags.append("NOMINAL")

    return {
        "strategy_key":              strategy_key,
        "market":                    market,
        "base_feedback_confidence":  base_conf,
        "cycle_modifier":            cycle_modifier,
        "cycle_bias_class":          cycle_bias_class,
        "memory_modifier":           memory_modifier,
        "memory_bias_class":         memory_bias_class,
        "memory_confidence":         memory_confidence,
        "effective_modifier_final":  final_modifier,
        "modifier_delta":            modifier_delta,
        "pre_memory_effective_confidence":      pre_conf,
        "post_memory_effective_confidence":     post_memory_conf,
        "confidence_delta":          confidence_delta,
        "decision_quality_score_before_memory": pre_dq_score,
        "decision_quality_score_after_memory":  post_dq_score,
        "dq_score_delta":            dq_score_delta,
        "decision_quality_gate_before_memory":  pre_gate,
        "decision_quality_gate_after_memory":   post_gate,
        "dq_gate_changed":           dq_gate_changed,
        "memory_available":          memory_available,
        "memory_influence_gate":     mem_gate,
        "memory_modifier_applied":   mem_applied,
        "memory_influence_reason":   mem_reason,
        "cooldown_flag":             cooldown_flag,
        "impact_class":              impact_class,
        "impact_direction":          impact_direction,
        "safe_band_ok":              safe_band_ok,
        "observability_flags":       "|".join(obs_flags),
    }


def build_observability_report(dq_records: list) -> list:
    """Build observability records for all AC-64 DQ records."""
    return [build_observability_record(r) for r in (dq_records or [])]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    dq_data  = load_json(DQ_PATH, {}) or {}
    cycle_id = dq_data.get("cycle_id") or "UNKNOWN"

    dq_records = dq_data.get("records") or []

    records = build_observability_report(dq_records)
    total = len(records)

    if total == 0:
        print("[WARN] No DQ records to process.")
        avg = lambda _: 0.0
    else:
        def avg(key):
            return round(sum(r.get(key, 0.0) for r in records) / total, 4)

    # Summary
    memory_available_count    = sum(1 for r in records if r["memory_available"])
    memory_considered_count   = sum(1 for r in records if r["memory_available"])
    memory_applied_count      = sum(1 for r in records if r["memory_modifier_applied"])
    memory_neutral_fallback   = sum(1 for r in records if r["impact_class"] == IMPACT_NO_EFFECT)
    memory_blocked_count      = sum(1 for r in records if r["impact_class"].startswith("BLOCKED"))
    memory_positive_applied   = sum(1 for r in records if r["impact_class"] == IMPACT_POSITIVE_REINFORCE)
    memory_negative_applied   = sum(1 for r in records if r["impact_class"] == IMPACT_NEGATIVE_DAMPENING)
    memory_caution_applied    = sum(1 for r in records if r["impact_class"] == IMPACT_CAUTION_DAMPENING)
    memory_conflict_blocked   = sum(1 for r in records if r["impact_class"] == IMPACT_BLOCKED_CONFLICT)
    memory_low_conf_blocked   = sum(1 for r in records if r["impact_class"] == IMPACT_BLOCKED_LOW_CONF)
    memory_absent_blocked     = sum(1 for r in records if r["impact_class"] == IMPACT_NO_MEMORY)
    dq_gate_changed_count     = sum(1 for r in records if r["dq_gate_changed"])
    safe_band_violations      = sum(1 for r in records if not r["safe_band_ok"])
    dq_score_changed_count    = sum(1 for r in records if abs(r["dq_score_delta"]) > 0.0001)

    summary = {
        "records_total":                   total,
        "memory_available_count":          memory_available_count,
        "memory_considered_count":         memory_considered_count,
        "memory_applied_count":            memory_applied_count,
        "memory_neutral_fallback_count":   memory_neutral_fallback,
        "memory_blocked_count":            memory_blocked_count,
        "memory_positive_applied_count":   memory_positive_applied,
        "memory_negative_applied_count":   memory_negative_applied,
        "memory_caution_applied_count":    memory_caution_applied,
        "memory_conflict_blocked_count":   memory_conflict_blocked,
        "memory_low_confidence_blocked_count": memory_low_conf_blocked,
        "memory_absent_blocked_count":     memory_absent_blocked,
        "avg_cycle_modifier":              avg("cycle_modifier"),
        "avg_memory_modifier":             avg("memory_modifier"),
        "avg_final_modifier":              avg("effective_modifier_final"),
        "avg_modifier_delta":              avg("modifier_delta"),
        "avg_pre_memory_effective_confidence":  avg("pre_memory_effective_confidence"),
        "avg_post_memory_effective_confidence": avg("post_memory_effective_confidence"),
        "avg_confidence_delta":            avg("confidence_delta"),
        "decision_quality_changed_count":  dq_score_changed_count,
        "decision_quality_gate_changed_count": dq_gate_changed_count,
        "safe_band_violations_count":      safe_band_violations,
    }

    out = {
        "component":   "build_allocation_memory_impact_observability_lite",
        "version":     VERSION,
        "ts_utc":      ts,
        "cycle_id":    cycle_id,
        "paper_only":  True,
        "source_files": {
            "allocation_decision_quality": str(DQ_PATH),
        },
        "observability_model": {
            "pre_memory_basis":    "base_feedback_confidence × cycle_modifier",
            "post_memory_basis":   "effective_feedback_confidence from AC-64",
            "dq_reconstruction":   "inline using AC-56/64 weights (W_DRIFT=0.40, W_CONV=0.35, W_BUDGET=0.10, W_REGIME=0.15, W_CHURN_PEN=0.20)",
            "safe_band":           [MODIFIER_BAND_MIN, MODIFIER_BAND_MAX],
            "impact_classes":      sorted(_GATE_TO_IMPACT.values()),
        },
        "summary":  summary,
        "records":  records,
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(OUT_PATH, out)
        write_tsv(OUT_TSV_PATH, TSV_HEADERS, records)
    except Exception as e:
        print(f"[WARN] Could not write output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k != "records"}, indent=2))
    for r in records:
        print(
            f"  {r['strategy_key']:<24}"
            f" class={r['impact_class']:<25}"
            f" delta_mod={r['modifier_delta']:+.4f}"
            f" delta_conf={r['confidence_delta']:+.4f}"
            f" flags={r['observability_flags']}"
        )


if __name__ == "__main__":
    main()
