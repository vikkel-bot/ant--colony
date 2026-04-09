"""
AC66: Memory Impact Review & Policy Tuning
Pure review and advisory layer. No execution, no threshold changes, no scoring changes.

Sits after AC-65 (Memory-Aware Impact Observability).

Central question:
  Does memory-aware conviction integration behave in practice as designed —
  and if not, which policy adjustments would be logical?

This layer does NOT:
  - change any threshold or modifier value
  - mutate AC-64 or AC-63 logic
  - auto-tune parameters
  - make live decisions

This layer DOES:
  - read AC-65 impact observability records
  - compute review metrics (usage, safety, positive/negative policy, cooldown, conflicts)
  - apply explicit policy-health rules
  - generate conservatively labeled recommendations
  - produce a strategy-key-level summary
  - write a structured review artefact

Recommendation labels (closed set):
  KEEP_POLICY                         — no threshold exceeded
  INSUFFICIENT_REVIEW_DATA            — sample too small for reliable review
  REVIEW_POSITIVE_GATE                — positive_applied_rate above threshold
  REVIEW_NEGATIVE_SENSITIVITY         — negative+caution combined rate high
  REVIEW_COOLDOWN_LENGTH              — cooldown seen too frequently
  REVIEW_MEMORY_CONFIDENCE_THRESHOLD  — memory often blocked by low confidence
  REVIEW_SAFE_BAND                    — safe-band violation detected
  REVIEW_CONFLICT_POLICY              — conflict block rate above threshold

Policy status (top-level):
  KEEP_POLICY             — all metrics within bounds
  REVIEW_SUGGESTED        — low-priority review signals only
  REVIEW_REQUIRED         — medium or high priority recommendation triggered
  INSUFFICIENT_REVIEW_DATA — sample too small

Reads:
  allocation_memory_impact_observability.json  — AC-65 impact records

Writes:
  allocation_memory_policy_review.json

Usage: python ant_colony/build_allocation_memory_policy_review_lite.py
"""
import importlib.util as _ilu
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR  = Path(r"C:\Trading\ANT_OUT")
OBS_PATH = OUT_DIR / "allocation_memory_impact_observability.json"
OUT_PATH = OUT_DIR / "allocation_memory_policy_review.json"

VERSION = "memory_policy_review_v1"

# ---------------------------------------------------------------------------
# AC-68: load review thresholds from canonical policy loader.
# Fail-closed: if loader unavailable, inline defaults (same values) are used.
# Not-policy values (usage watch bands 0.10/0.80) remain hardcoded below.
# ---------------------------------------------------------------------------
def _load_ac66_policy():
    try:
        _path = Path(__file__).parent / "policy" / "load_allocation_memory_policy_lite.py"
        _spec = _ilu.spec_from_file_location("_policy_loader", _path)
        _mod = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        _policy, _fb, _reason = _mod.load_policy()
        _audit = _mod.get_policy_audit(_policy, _fb, _reason, ["review_thresholds"])
        return _policy["groups"].get("review_thresholds", {}), _fb, _reason, _audit
    except Exception as _exc:
        _reason = f"LOADER_UNAVAILABLE:{_exc}"
        _audit = {
            "policy_name": "UNKNOWN", "policy_version": "UNKNOWN", "effective_from": None,
            "load_reason": _reason, "fallback_used": True,
            "fingerprint": "UNAVAILABLE", "groups_consumed": ["review_thresholds"],
        }
        return {}, True, _reason, _audit

_ac66_review, _POLICY_FALLBACK_USED, _POLICY_LOAD_REASON, _POLICY_AUDIT = _load_ac66_policy()

# Review thresholds — sourced from policy (fail-closed to inline defaults)
MIN_REVIEWABLE_RECORDS = _ac66_review.get("review_min_records",                   5)
POSITIVE_RATE_REVIEW   = _ac66_review.get("review_positive_applied_rate_warn",  0.30)
POSITIVE_RATE_WATCH    = _ac66_review.get("review_positive_applied_rate_watch", 0.20)
NEGATIVE_RATE_REVIEW   = _ac66_review.get("review_negative_applied_rate_warn",  0.70)
COOLDOWN_RATE_REVIEW   = _ac66_review.get("review_cooldown_rate_warn",           0.50)
CONFLICT_RATE_REVIEW   = _ac66_review.get("review_conflict_block_rate_warn",     0.30)
LOW_CONF_RATE_REVIEW   = _ac66_review.get("review_low_conf_blocked_rate_warn",   0.50)
AVG_DELTA_WATCH        = _ac66_review.get("review_avg_delta_watch",              0.02)

# AC69: usage watch bands — sourced from policy (fail-closed to inline defaults)
MEMORY_APPLIED_RATE_LOW_WATCH  = _ac66_review.get("review_memory_applied_rate_low_watch",  0.10)
MEMORY_APPLIED_RATE_HIGH_WATCH = _ac66_review.get("review_memory_applied_rate_high_watch", 0.80)

# ---------------------------------------------------------------------------
# Recommendation labels
# ---------------------------------------------------------------------------

REC_KEEP          = "KEEP_POLICY"
REC_INSUFFICIENT  = "INSUFFICIENT_REVIEW_DATA"
REC_POS_GATE      = "REVIEW_POSITIVE_GATE"
REC_NEG_SENS      = "REVIEW_NEGATIVE_SENSITIVITY"
REC_COOLDOWN      = "REVIEW_COOLDOWN_LENGTH"
REC_CONF_THRESH   = "REVIEW_MEMORY_CONFIDENCE_THRESHOLD"
REC_SAFE_BAND     = "REVIEW_SAFE_BAND"
REC_CONFLICT      = "REVIEW_CONFLICT_POLICY"

# Assessment labels (per section)
ASSESS_HEALTHY     = "HEALTHY"
ASSESS_WATCH       = "WATCH"
ASSESS_REVIEW      = "REVIEW"
ASSESS_INSUFFICIENT = "INSUFFICIENT_DATA"

# Impact class names (mirrored from AC-65 for classification)
IMPACT_NO_MEMORY      = "NO_MEMORY"
IMPACT_NO_EFFECT      = "NO_EFFECT"
IMPACT_NEG_DAMP       = "NEGATIVE_DAMPENING"
IMPACT_CAUTION_DAMP   = "CAUTION_DAMPENING"
IMPACT_POS_REINFORCE  = "POSITIVE_REINFORCEMENT"
IMPACT_BLOCK_CONFLICT = "BLOCKED_BY_CONFLICT"
IMPACT_BLOCK_LOW_CONF = "BLOCKED_LOW_CONFIDENCE"
IMPACT_BLOCK_ABSENT   = "BLOCKED_ABSENT_MEMORY"


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


def _rate(count: int, total: int) -> float:
    return round(count / total, 4) if total > 0 else 0.0


def _avg(values: list) -> float:
    return round(sum(values) / len(values), 4) if values else 0.0


def _rec(label: str, priority: str, scope: str, trigger: str, evidence: str) -> dict:
    return {
        "recommendation_label": label,
        "priority":             priority,
        "scope":                scope,
        "trigger_reason":       trigger,
        "evidence_summary":     evidence,
    }


# ---------------------------------------------------------------------------
# Core review functions (importable for tests)
# ---------------------------------------------------------------------------

def compute_review_metrics(records: list) -> dict:
    """
    Aggregate AC-65 impact records into review metrics.
    Returns a flat dict of counts, rates, and averages.
    Empty records → minimal safe dict.
    """
    total = len(records)
    if total == 0:
        return {
            "total": 0, "reviewable": 0,
            "memory_available_count": 0, "memory_applied_count": 0,
            "memory_blocked_count": 0,
            "positive_applied_count": 0, "negative_applied_count": 0,
            "caution_applied_count": 0, "conflict_blocked_count": 0,
            "low_conf_blocked_count": 0, "absent_count": 0,
            "neutral_fallback_count": 0,
            "safe_band_violations": 0, "dq_gate_changed_count": 0,
            "cooldown_seen_count": 0,
            "avg_modifier_delta": 0.0, "avg_confidence_delta": 0.0,
            "avg_dq_score_delta": 0.0, "avg_abs_modifier_delta": 0.0,
            "memory_applied_rate": 0.0, "memory_blocked_rate": 0.0,
            "positive_applied_rate": 0.0, "negative_applied_rate": 0.0,
            "caution_applied_rate": 0.0, "conflict_block_rate": 0.0,
            "neutral_fallback_rate": 0.0, "cooldown_rate": 0.0,
            "low_conf_blocked_rate_of_available": 0.0,
        }

    impact_counts = Counter(r.get("impact_class", "") for r in records)

    mem_available   = sum(1 for r in records if r.get("memory_available", False))
    mem_applied     = sum(1 for r in records if r.get("memory_modifier_applied", False))
    pos_applied     = impact_counts.get(IMPACT_POS_REINFORCE, 0)
    neg_applied     = impact_counts.get(IMPACT_NEG_DAMP, 0)
    caution_applied = impact_counts.get(IMPACT_CAUTION_DAMP, 0)
    conflict_blocked = impact_counts.get(IMPACT_BLOCK_CONFLICT, 0)
    low_conf_blocked = impact_counts.get(IMPACT_BLOCK_LOW_CONF, 0)
    absent_count    = impact_counts.get(IMPACT_NO_MEMORY, 0) + impact_counts.get(IMPACT_BLOCK_ABSENT, 0)
    neutral_fallback = impact_counts.get(IMPACT_NO_EFFECT, 0)
    mem_blocked     = conflict_blocked + low_conf_blocked

    safe_violations = sum(1 for r in records if not r.get("safe_band_ok", True))
    dq_gate_changed = sum(1 for r in records if r.get("dq_gate_changed", False))
    cooldown_seen   = sum(1 for r in records if r.get("cooldown_flag", False))

    mod_deltas  = [to_float(r.get("modifier_delta", 0.0)) for r in records]
    conf_deltas = [to_float(r.get("confidence_delta", 0.0)) for r in records]
    dq_deltas   = [to_float(r.get("dq_score_delta", 0.0)) for r in records]

    low_conf_rate_of_avail = (
        _rate(low_conf_blocked, mem_available) if mem_available > 0 else 0.0
    )

    return {
        "total":                     total,
        "reviewable":                total,
        "memory_available_count":    mem_available,
        "memory_applied_count":      mem_applied,
        "memory_blocked_count":      mem_blocked,
        "positive_applied_count":    pos_applied,
        "negative_applied_count":    neg_applied,
        "caution_applied_count":     caution_applied,
        "conflict_blocked_count":    conflict_blocked,
        "low_conf_blocked_count":    low_conf_blocked,
        "absent_count":              absent_count,
        "neutral_fallback_count":    neutral_fallback,
        "safe_band_violations":      safe_violations,
        "dq_gate_changed_count":     dq_gate_changed,
        "cooldown_seen_count":       cooldown_seen,
        "avg_modifier_delta":        _avg(mod_deltas),
        "avg_confidence_delta":      _avg(conf_deltas),
        "avg_dq_score_delta":        _avg(dq_deltas),
        "avg_abs_modifier_delta":    _avg([abs(d) for d in mod_deltas]),
        # Rates (normalised to total)
        "memory_applied_rate":       _rate(mem_applied, total),
        "memory_blocked_rate":       _rate(mem_blocked, total),
        "positive_applied_rate":     _rate(pos_applied, total),
        "negative_applied_rate":     _rate(neg_applied, total),
        "caution_applied_rate":      _rate(caution_applied, total),
        "conflict_block_rate":       _rate(conflict_blocked, total),
        "neutral_fallback_rate":     _rate(neutral_fallback + absent_count, total),
        "cooldown_rate":             _rate(cooldown_seen, total),
        "low_conf_blocked_rate_of_available": low_conf_rate_of_avail,
    }


def assess_data_sufficiency(metrics: dict) -> tuple:
    reasons = [
        f"TOTAL_RECORDS={metrics['total']}",
        f"MIN_REQUIRED={MIN_REVIEWABLE_RECORDS}",
    ]
    if metrics["total"] < MIN_REVIEWABLE_RECORDS:
        reasons.append("BELOW_MIN_REVIEWABLE_THRESHOLD")
        return ASSESS_INSUFFICIENT, reasons
    reasons.append("SUFFICIENT_SAMPLE")
    return ASSESS_HEALTHY, reasons


def assess_usage(metrics: dict) -> tuple:
    applied = metrics["memory_applied_rate"]
    reasons = [
        f"APPLIED_RATE={applied:.4f}",
        f"AVAILABLE={metrics['memory_available_count']}/{metrics['total']}",
        f"BLOCKED_RATE={metrics['memory_blocked_rate']:.4f}",
    ]
    if applied < MEMORY_APPLIED_RATE_LOW_WATCH:
        reasons.append("MEMORY_RARELY_APPLIED")
        return ASSESS_WATCH, reasons
    if applied > MEMORY_APPLIED_RATE_HIGH_WATCH:
        reasons.append("MEMORY_VERY_FREQUENTLY_APPLIED")
        return ASSESS_WATCH, reasons
    return ASSESS_HEALTHY, reasons


def assess_safety(metrics: dict) -> tuple:
    violations = metrics["safe_band_violations"]
    avg_abs    = metrics["avg_abs_modifier_delta"]
    reasons = [
        f"SAFE_BAND_VIOLATIONS={violations}",
        f"AVG_ABS_MOD_DELTA={avg_abs:.4f}",
    ]
    if violations > 0:
        reasons.append("SAFE_BAND_VIOLATED")
        return ASSESS_REVIEW, reasons
    if avg_abs > AVG_DELTA_WATCH:
        reasons.append("IMPACT_MAGNITUDE_ELEVATED")
        return ASSESS_WATCH, reasons
    return ASSESS_HEALTHY, reasons


def assess_positive_policy(metrics: dict) -> tuple:
    pos_rate = metrics["positive_applied_rate"]
    reasons = [f"POSITIVE_APPLIED_RATE={pos_rate:.4f}"]

    if pos_rate > POSITIVE_RATE_REVIEW:
        reasons.append(f"EXCEEDS_REVIEW_THRESHOLD_{POSITIVE_RATE_REVIEW}")
        return ASSESS_REVIEW, reasons
    if pos_rate > POSITIVE_RATE_WATCH:
        reasons.append("ELEVATED_WATCH")
        return ASSESS_WATCH, reasons
    if pos_rate == 0.0 and metrics["memory_available_count"] > 0:
        reasons.append("POSITIVE_NEVER_APPLIED_DESPITE_MEMORY_AVAILABLE")
        return ASSESS_WATCH, reasons
    return ASSESS_HEALTHY, reasons


def assess_negative_policy(metrics: dict) -> tuple:
    neg_combined = round(
        metrics["negative_applied_rate"] + metrics["caution_applied_rate"], 4
    )
    reasons = [
        f"NEGATIVE_RATE={metrics['negative_applied_rate']:.4f}",
        f"CAUTION_RATE={metrics['caution_applied_rate']:.4f}",
        f"COMBINED_RATE={neg_combined:.4f}",
    ]
    if neg_combined > NEGATIVE_RATE_REVIEW:
        reasons.append(f"COMBINED_EXCEEDS_THRESHOLD_{NEGATIVE_RATE_REVIEW}")
        return ASSESS_REVIEW, reasons
    return ASSESS_HEALTHY, reasons


def assess_cooldown_policy(metrics: dict) -> tuple:
    cooldown_rate = metrics["cooldown_rate"]
    caution_rate  = metrics["caution_applied_rate"]
    reasons = [
        f"COOLDOWN_SEEN_RATE={cooldown_rate:.4f}",
        f"CAUTION_APPLIED_RATE={caution_rate:.4f}",
    ]
    if cooldown_rate > COOLDOWN_RATE_REVIEW:
        reasons.append(f"COOLDOWN_RATE_EXCEEDS_THRESHOLD_{COOLDOWN_RATE_REVIEW}")
        return ASSESS_REVIEW, reasons
    return ASSESS_HEALTHY, reasons


def generate_recommendations(metrics: dict, assessments: dict) -> list:
    """
    Translate assessment results into explicit, labeled recommendations.
    If data is insufficient, only INSUFFICIENT_REVIEW_DATA is returned.
    """
    recs = []

    # Data sufficiency is the hard gate — no further review without it
    if assessments["data_sufficiency"][0] == ASSESS_INSUFFICIENT:
        recs.append(_rec(
            REC_INSUFFICIENT, "HIGH", "system",
            "RECORDS_BELOW_MIN_REVIEWABLE",
            f"total={metrics['total']} min_required={MIN_REVIEWABLE_RECORDS}",
        ))
        return recs

    # Safe band (highest priority when triggered)
    if metrics["safe_band_violations"] > 0:
        recs.append(_rec(
            REC_SAFE_BAND, "HIGH", "modifier_band",
            "SAFE_BAND_VIOLATION_DETECTED",
            f"violations={metrics['safe_band_violations']}",
        ))

    # Positive gate
    if assessments["positive_policy"][0] == ASSESS_REVIEW:
        recs.append(_rec(
            REC_POS_GATE, "MEDIUM", "positive_gate",
            "POSITIVE_APPLIED_RATE_ABOVE_THRESHOLD",
            f"positive_rate={metrics['positive_applied_rate']:.4f} threshold={POSITIVE_RATE_REVIEW}",
        ))

    # Negative sensitivity
    neg_combined = round(
        metrics["negative_applied_rate"] + metrics["caution_applied_rate"], 4
    )
    if neg_combined > NEGATIVE_RATE_REVIEW:
        recs.append(_rec(
            REC_NEG_SENS, "MEDIUM", "negative_gate",
            "NEGATIVE_CAUTION_COMBINED_RATE_HIGH",
            f"combined_rate={neg_combined:.4f} threshold={NEGATIVE_RATE_REVIEW}",
        ))

    # Cooldown
    if assessments["cooldown_policy"][0] == ASSESS_REVIEW:
        recs.append(_rec(
            REC_COOLDOWN, "LOW", "cooldown",
            "COOLDOWN_SEEN_TOO_FREQUENTLY",
            f"cooldown_rate={metrics['cooldown_rate']:.4f} threshold={COOLDOWN_RATE_REVIEW}",
        ))

    # Conflict policy
    if metrics["conflict_block_rate"] > CONFLICT_RATE_REVIEW:
        recs.append(_rec(
            REC_CONFLICT, "LOW", "conflict_gate",
            "CONFLICT_BLOCK_RATE_ABOVE_THRESHOLD",
            f"conflict_rate={metrics['conflict_block_rate']:.4f} threshold={CONFLICT_RATE_REVIEW}",
        ))

    # Memory confidence threshold
    if metrics["low_conf_blocked_rate_of_available"] > LOW_CONF_RATE_REVIEW:
        recs.append(_rec(
            REC_CONF_THRESH, "LOW", "confidence_gate",
            "LOW_CONF_BLOCKED_FREQUENTLY",
            f"low_conf_rate_of_available={metrics['low_conf_blocked_rate_of_available']:.4f} threshold={LOW_CONF_RATE_REVIEW}",
        ))

    # Default: healthy
    if not recs:
        recs.append(_rec(
            REC_KEEP, "LOW", "system",
            "ALL_REVIEW_METRICS_WITHIN_BOUNDS",
            "no policy thresholds exceeded",
        ))

    return recs


def compute_policy_status(recommendations: list) -> str:
    """Derive top-level policy status from recommendation labels and priorities."""
    labels = [r["recommendation_label"] for r in recommendations]

    if REC_INSUFFICIENT in labels:
        return "INSUFFICIENT_REVIEW_DATA"

    review_recs = [
        r for r in recommendations
        if r["recommendation_label"] not in (REC_KEEP, REC_INSUFFICIENT)
    ]
    if any(r["priority"] in ("HIGH", "MEDIUM") for r in review_recs):
        return "REVIEW_REQUIRED"
    if review_recs:
        return "REVIEW_SUGGESTED"
    return "KEEP_POLICY"


def build_strategy_key_reviews(records: list) -> list:
    """
    Build compact per-strategy_key review summaries.
    Groups AC-65 records by strategy_key (= position_key).
    """
    grouped: dict = defaultdict(list)
    for r in records:
        sk = str(r.get("strategy_key") or r.get("position_key") or "UNKNOWN")
        grouped[sk].append(r)

    reviews = []
    for sk in sorted(grouped):
        sk_recs = grouped[sk]
        n = len(sk_recs)

        applied    = sum(1 for r in sk_recs if r.get("memory_modifier_applied", False))
        cooldown   = any(r.get("cooldown_flag", False) for r in sk_recs)
        mod_deltas = [to_float(r.get("modifier_delta", 0.0)) for r in sk_recs]
        conf_deltas = [to_float(r.get("confidence_delta", 0.0)) for r in sk_recs]
        dominant   = Counter(r.get("impact_class", "") for r in sk_recs).most_common(1)[0][0]

        note = "NOMINAL"
        if any(r.get("safe_band_ok") is False for r in sk_recs):
            note = "SAFE_BAND_ISSUE"
        elif dominant == IMPACT_CAUTION_DAMP:
            note = "PERSISTENT_CAUTION"
        elif dominant == IMPACT_POS_REINFORCE:
            note = "MEMORY_REINFORCING"
        elif dominant in (IMPACT_NO_MEMORY, IMPACT_BLOCK_LOW_CONF):
            note = "MEMORY_NOT_CONTRIBUTING"
        elif dominant == IMPACT_BLOCK_CONFLICT:
            note = "CONFLICT_BLOCKING_MEMORY"

        reviews.append({
            "strategy_key":           sk,
            "records_count":          n,
            "memory_applied_rate":    _rate(applied, n),
            "avg_modifier_delta":     _avg(mod_deltas),
            "avg_confidence_delta":   _avg(conf_deltas),
            "cooldown_seen":          cooldown,
            "dominant_impact_class":  dominant,
            "strategy_policy_note":   note,
        })

    return reviews


def build_policy_review(records: list) -> dict:
    """
    Build the complete policy review object from AC-65 observability records.
    Returns the full review dict (no file I/O).
    """
    metrics     = compute_review_metrics(records)
    assessments = {
        "data_sufficiency":  assess_data_sufficiency(metrics),
        "usage":             assess_usage(metrics),
        "safety":            assess_safety(metrics),
        "positive_policy":   assess_positive_policy(metrics),
        "negative_policy":   assess_negative_policy(metrics),
        "cooldown_policy":   assess_cooldown_policy(metrics),
    }
    recommendations    = generate_recommendations(metrics, assessments)
    policy_status      = compute_policy_status(recommendations)
    strategy_reviews   = build_strategy_key_reviews(records)

    def _section(name, assessment_label, reasons):
        return {
            f"{name}_assessment": assessment_label,
            f"{name}_reasons":    "|".join(reasons),
        }

    summary = {
        "records_total":                    metrics["total"],
        "reviewable_records_count":         metrics["reviewable"],
        "insufficient_review_data_count":   max(0, MIN_REVIEWABLE_RECORDS - metrics["total"]),
        "memory_applied_rate":              metrics["memory_applied_rate"],
        "memory_blocked_rate":              metrics["memory_blocked_rate"],
        "positive_applied_rate":            metrics["positive_applied_rate"],
        "negative_applied_rate":            metrics["negative_applied_rate"],
        "caution_applied_rate":             metrics["caution_applied_rate"],
        "conflict_block_rate":              metrics["conflict_block_rate"],
        "neutral_fallback_rate":            metrics["neutral_fallback_rate"],
        "avg_modifier_delta":               metrics["avg_modifier_delta"],
        "avg_confidence_delta":             metrics["avg_confidence_delta"],
        "avg_dq_score_delta":               metrics["avg_dq_score_delta"],
        "safe_band_violations_count":       metrics["safe_band_violations"],
        "gate_change_rate":                 _rate(metrics["dq_gate_changed_count"], metrics["total"]),
        "policy_status":                    policy_status,
        "policy_recommendation_count":      len(recommendations),
    }

    review_sections = {
        "usage_review": _section("memory_usage", assessments["usage"][0], assessments["usage"][1]),
        "safety_review": {
            "safe_band_assessment":     assessments["safety"][0],
            "impact_magnitude_assessment": (
                ASSESS_WATCH if metrics["avg_abs_modifier_delta"] > AVG_DELTA_WATCH else ASSESS_HEALTHY
            ),
            "safety_reasons": "|".join(assessments["safety"][1]),
        },
        "positive_policy_review": _section("positive_policy", assessments["positive_policy"][0], assessments["positive_policy"][1]),
        "negative_policy_review": _section("negative_policy", assessments["negative_policy"][0], assessments["negative_policy"][1]),
        "cooldown_review": _section("cooldown_policy", assessments["cooldown_policy"][0], assessments["cooldown_policy"][1]),
        "data_sufficiency_review": {
            "review_data_status":  assessments["data_sufficiency"][0],
            "review_data_reasons": "|".join(assessments["data_sufficiency"][1]),
        },
    }

    return {
        "metrics":            metrics,
        "assessments":        {k: v[0] for k, v in assessments.items()},
        "summary":            summary,
        "policy_review":      review_sections,
        "recommendations":    recommendations,
        "strategy_key_reviews": strategy_reviews,
        "policy_status":      policy_status,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    obs_data = load_json(OBS_PATH, {}) or {}
    cycle_id = obs_data.get("cycle_id") or "UNKNOWN"
    obs_records = obs_data.get("records") or []

    review = build_policy_review(obs_records)

    out = {
        "component":   "build_allocation_memory_policy_review_lite",
        "version":     VERSION,
        "ts_utc":      ts,
        "cycle_id":    cycle_id,
        "paper_only":  True,
        "source_files": {
            "allocation_memory_impact_observability": str(OBS_PATH),
        },
        "review_constants": {
            "min_reviewable_records":    MIN_REVIEWABLE_RECORDS,
            "positive_rate_review":      POSITIVE_RATE_REVIEW,
            "negative_rate_review":      NEGATIVE_RATE_REVIEW,
            "cooldown_rate_review":      COOLDOWN_RATE_REVIEW,
            "conflict_rate_review":      CONFLICT_RATE_REVIEW,
            "low_conf_rate_review":      LOW_CONF_RATE_REVIEW,
            "avg_delta_watch":           AVG_DELTA_WATCH,
        },
        "policy_audit":         _POLICY_AUDIT,
        "summary":              review["summary"],
        "policy_status":        review["policy_status"],
        "policy_review":        review["policy_review"],
        "recommendations":      review["recommendations"],
        "strategy_key_reviews": review["strategy_key_reviews"],
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(OUT_PATH, out)
    except Exception as e:
        print(f"[WARN] Could not write output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k not in ("strategy_key_reviews",)}, indent=2))
    print(f"\n  strategy_key_reviews ({len(review['strategy_key_reviews'])} keys):")
    for skr in review["strategy_key_reviews"]:
        print(
            f"    {skr['strategy_key']:<24}"
            f" applied_rate={skr['memory_applied_rate']:.2f}"
            f" dominant={skr['dominant_impact_class']}"
            f" note={skr['strategy_policy_note']}"
        )


if __name__ == "__main__":
    main()
