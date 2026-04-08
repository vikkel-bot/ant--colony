"""
AC61: Allocation Feedback Integration Layer (Outcome → Allocation)
Pure observability + integration artefact. No execution, no orders, no strategy mutation.

Sits after AC60 (Paper Rebalance Outcome Attribution). Reads outcome-attribution
records and translates them into per-strategy-key feedback state that can
inform future allocation conviction.

Central question:
  What does the history of paper rebalance outcomes tell us about how to
  adjust allocation conviction for each strategy_key going forward?

This layer does NOT:
  - mutate strategy logic (EDGE3/EDGE4/etc. untouched)
  - learn autonomously
  - make aggressive allocation changes
  - give positive bias when evidence is insufficient

This layer DOES:
  - aggregate READY outcome records per strategy_key
  - compute a small, conservative allocation_conviction_modifier
  - flag cooldown when negative signals are strong
  - enforce sample-size discipline (fail-closed when too few READY records)

Reads:
  paper_rebalance_outcome_attribution.json  — per-record outcome labels (AC60)

Writes:
  allocation_feedback_integration.json
  allocation_feedback_integration.tsv

Aggregation:
  Only READY outcome records count.
  PENDING / INSUFFICIENT_DATA / NOT_EXECUTED records are ignored.

Net outcome signal (per strategy_key):
  net_outcome_signal = (helpful_count - harmful_count) / ready_count  ∈ [-1, 1]
  confidence_weight  = f(ready_count)  ∈ [0.0, 1.0]
  effective_signal   = net_outcome_signal × confidence_weight

Bias classes:
  POSITIVE           effective_signal >= +0.20 AND ready_count >= MIN_READY
  NEUTRAL            -0.20 < effective_signal < +0.20 (or not enough evidence)
  NEGATIVE           effective_signal <= -0.20 AND ready_count >= MIN_READY
  NEGATIVE_CAUTION   effective_signal <= -0.50 OR (harmful_ratio >= 0.60 AND MIN_READY met)
  INSUFFICIENT_EVIDENCE  ready_count < MIN_READY_OUTCOMES

Conviction modifier band: [0.90, 1.05]
  POSITIVE:           1.05
  NEUTRAL:            1.00
  NEGATIVE:           0.95
  NEGATIVE_CAUTION:   0.90
  INSUFFICIENT_EVIDENCE: 1.00  (neutral fallback — never positive bias on thin evidence)

Cooldown:
  cooldown_flag = True when allocation_bias_class == NEGATIVE_CAUTION

Integration hook:
  The allocation_conviction_modifier in this artefact is designed to be read by
  the conviction scoring step in AC56 (build_allocation_decision_quality_lite.py)
  to scale feedback_confidence. Hook is NOT yet wired (planned AC-62 follow-up).
  The artefact is clearly structured and ready for that integration.

Usage: python ant_colony/build_allocation_feedback_integration_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

ATTRIBUTION_PATH = OUT_DIR / "paper_rebalance_outcome_attribution.json"

OUT_PATH     = OUT_DIR / "allocation_feedback_integration.json"
OUT_TSV_PATH = OUT_DIR / "allocation_feedback_integration.tsv"

VERSION = "feedback_integration_v1"

# Sample-size discipline
MIN_READY_OUTCOMES    = 3    # fewer → INSUFFICIENT_EVIDENCE
FULL_CONFIDENCE_AT    = 6    # at this count, confidence_weight reaches 1.0

# Bias thresholds (applied to effective_signal = net_signal × confidence_weight)
POSITIVE_THRESHOLD = 0.20    # effective_signal >= this → POSITIVE
NEGATIVE_THRESHOLD = -0.20   # effective_signal <= this → NEGATIVE
CAUTION_THRESHOLD  = -0.50   # effective_signal <= this → NEGATIVE_CAUTION
CAUTION_HARMFUL_RATIO = 0.60 # harmful_ratio >= this (+ MIN_READY met) → CAUTION

# Conviction modifier values (narrow band)
MODIFIER_POSITIVE  = 1.05
MODIFIER_NEUTRAL   = 1.00
MODIFIER_NEGATIVE  = 0.95
MODIFIER_CAUTION   = 0.90
MODIFIER_BAND_MIN  = 0.90   # hard floor (safeguard against future drift)
MODIFIER_BAND_MAX  = 1.05   # hard ceiling

# Outcome labels from AC60
_READY_OUTCOME_LABELS = {"HELPFUL", "NEUTRAL", "HARMFUL"}
_READY_EVAL_STATUS    = "READY"

# Feedback status values
FEEDBACK_READY              = "READY"
FEEDBACK_INSUFFICIENT       = "INSUFFICIENT_EVIDENCE"
FEEDBACK_NO_READY_OUTCOMES  = "NO_READY_OUTCOMES"

# Bias classes
BIAS_POSITIVE     = "POSITIVE"
BIAS_NEUTRAL      = "NEUTRAL"
BIAS_NEGATIVE     = "NEGATIVE"
BIAS_CAUTION      = "NEGATIVE_CAUTION"
BIAS_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"

TSV_HEADERS = [
    "strategy_key", "market",
    "ready_outcomes_count", "helpful_count", "neutral_count", "harmful_count",
    "helpful_ratio", "harmful_ratio",
    "net_outcome_signal", "confidence_weight", "effective_signal",
    "allocation_bias", "allocation_bias_class", "allocation_conviction_modifier",
    "cooldown_flag", "feedback_status", "integration_reasons",
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


# ---------------------------------------------------------------------------
# Core feedback logic (importable for tests)
# ---------------------------------------------------------------------------

def compute_confidence_weight(ready_count: int) -> float:
    """
    Linearly scales from 0.0 (0 records) to 1.0 (FULL_CONFIDENCE_AT records).
    Clamped to [0.0, 1.0].
    """
    if ready_count <= 0:
        return 0.0
    return round(min(1.0, ready_count / FULL_CONFIDENCE_AT), 4)


def determine_bias(
    net_outcome_signal: float,
    effective_signal: float,
    harmful_ratio: float,
    ready_count: int,
) -> tuple:
    """
    Returns (bias_class: str, reasons: list[str]).
    Rules applied in priority order.
    """
    reasons = []

    if ready_count < MIN_READY_OUTCOMES:
        reasons.append(f"INSUFFICIENT_EVIDENCE_READY_COUNT_{ready_count}")
        return BIAS_INSUFFICIENT, reasons

    # Rule 1: NEGATIVE_CAUTION — strong negative signal or high harmful ratio
    if effective_signal <= CAUTION_THRESHOLD:
        reasons.append(f"CAUTION_EFFECTIVE_SIGNAL_{effective_signal:.4f}")
        return BIAS_CAUTION, reasons

    if harmful_ratio >= CAUTION_HARMFUL_RATIO:
        reasons.append(f"CAUTION_HARMFUL_RATIO_{harmful_ratio:.4f}")
        return BIAS_CAUTION, reasons

    # Rule 2: NEGATIVE
    if effective_signal <= NEGATIVE_THRESHOLD:
        reasons.append(f"NEGATIVE_SIGNAL_{effective_signal:.4f}")
        return BIAS_NEGATIVE, reasons

    # Rule 3: POSITIVE
    if effective_signal >= POSITIVE_THRESHOLD:
        reasons.append(f"POSITIVE_SIGNAL_{effective_signal:.4f}")
        return BIAS_POSITIVE, reasons

    # Rule 4: NEUTRAL (mixed or near-zero signal)
    reasons.append(f"NEUTRAL_SIGNAL_{effective_signal:.4f}")
    return BIAS_NEUTRAL, reasons


def modifier_for_bias(bias_class: str) -> float:
    """Returns conviction modifier for a given bias class."""
    _MAP = {
        BIAS_POSITIVE:     MODIFIER_POSITIVE,
        BIAS_NEUTRAL:      MODIFIER_NEUTRAL,
        BIAS_NEGATIVE:     MODIFIER_NEGATIVE,
        BIAS_CAUTION:      MODIFIER_CAUTION,
        BIAS_INSUFFICIENT: MODIFIER_NEUTRAL,  # fail-closed: no positive bias
    }
    raw = _MAP.get(bias_class, MODIFIER_NEUTRAL)
    return round(max(MODIFIER_BAND_MIN, min(MODIFIER_BAND_MAX, raw)), 4)


def build_feedback_record(
    strategy_key: str,
    market: str,
    ready_outcomes: list,
) -> dict:
    """
    Build a single per-strategy-key feedback record from its READY outcomes.

    ready_outcomes: list of AC60 records with evaluation_status == READY.
    """
    n = len(ready_outcomes)
    helpful = sum(1 for r in ready_outcomes if r.get("outcome_label") == "HELPFUL")
    neutral  = sum(1 for r in ready_outcomes if r.get("outcome_label") == "NEUTRAL")
    harmful  = sum(1 for r in ready_outcomes if r.get("outcome_label") == "HARMFUL")

    helpful_ratio = round(helpful / n, 4) if n > 0 else 0.0
    harmful_ratio = round(harmful / n, 4) if n > 0 else 0.0

    net_signal         = round((helpful - harmful) / max(n, 1), 4)
    confidence_weight  = compute_confidence_weight(n)
    effective_signal   = round(net_signal * confidence_weight, 4)

    bias_class, bias_reasons = determine_bias(
        net_signal, effective_signal, harmful_ratio, n
    )

    modifier   = modifier_for_bias(bias_class)
    cooldown   = (bias_class == BIAS_CAUTION)

    if n == 0:
        fb_status = FEEDBACK_NO_READY_OUTCOMES
    elif n < MIN_READY_OUTCOMES:
        fb_status = FEEDBACK_INSUFFICIENT
    else:
        fb_status = FEEDBACK_READY

    # Bias label (readable alias for allocation engine)
    alloc_bias = bias_class.lower().replace("_", " ").capitalize()

    # Augment reasons with context
    reasons = list(bias_reasons)
    reasons.append(f"READY_N={n}")
    reasons.append(f"HELPFUL={helpful}_NEUTRAL={neutral}_HARMFUL={harmful}")
    reasons.append(f"MODIFIER={modifier}")
    if cooldown:
        reasons.append("COOLDOWN_ACTIVE")

    return {
        "strategy_key":                 strategy_key,
        "market":                       market,
        "ready_outcomes_count":         n,
        "helpful_count":                helpful,
        "neutral_count":                neutral,
        "harmful_count":                harmful,
        "helpful_ratio":                helpful_ratio,
        "harmful_ratio":                harmful_ratio,
        "net_outcome_signal":           net_signal,
        "confidence_weight":            confidence_weight,
        "effective_signal":             effective_signal,
        "allocation_bias":              alloc_bias,
        "allocation_bias_class":        bias_class,
        "allocation_conviction_modifier": modifier,
        "cooldown_flag":                cooldown,
        "feedback_status":              fb_status,
        "integration_reasons":          "|".join(reasons),
    }


def build_feedback_integration(attribution_records: list) -> list:
    """
    Aggregate AC60 outcome records per strategy_key and return feedback records.

    Only READY evaluation_status records are used for signal computation.
    Strategy keys with zero eligible records still get a record (NO_READY_OUTCOMES).
    """
    # Group all attribution records by strategy_key; track market
    grouped: dict[str, dict] = {}

    for rec in attribution_records:
        sk  = str(rec.get("strategy_key") or "").strip()
        mkt = str(rec.get("market") or "").strip()
        if not sk:
            # Fall back to position_key-derived key if strategy_key missing
            pk = str(rec.get("position_key") or "")
            if "__" in pk:
                sk = pk.split("__", 1)[1]
            else:
                sk = pk or "UNKNOWN"

        if sk not in grouped:
            grouped[sk] = {"market": mkt, "all": [], "ready": []}

        grouped[sk]["all"].append(rec)

        eval_status  = str(rec.get("evaluation_status") or "")
        outcome_label = str(rec.get("outcome_label") or "")
        if eval_status == _READY_EVAL_STATUS and outcome_label in _READY_OUTCOME_LABELS:
            grouped[sk]["ready"].append(rec)

    records = []
    for sk, data in sorted(grouped.items()):
        records.append(build_feedback_record(
            strategy_key=sk,
            market=data["market"],
            ready_outcomes=data["ready"],
        ))

    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    attr_data = load_json(ATTRIBUTION_PATH, {}) or {}
    cycle_id  = attr_data.get("cycle_id") or "UNKNOWN"
    equity    = to_float(attr_data.get("equity", 0.0))

    attribution_records = attr_data.get("records") or []

    records = build_feedback_integration(attribution_records)

    # Summary
    helpful_total  = sum(r["helpful_count"] for r in records)
    neutral_total  = sum(r["neutral_count"] for r in records)
    harmful_total  = sum(r["harmful_count"] for r in records)
    ready_used     = sum(r["ready_outcomes_count"] for r in records)
    pending_ignored = len([a for a in attribution_records
                           if a.get("evaluation_status") != _READY_EVAL_STATUS])

    positive_count  = sum(1 for r in records if r["allocation_bias_class"] == BIAS_POSITIVE)
    negative_count  = sum(1 for r in records if r["allocation_bias_class"] in (BIAS_NEGATIVE, BIAS_CAUTION))
    neutral_count   = sum(1 for r in records if r["allocation_bias_class"] in (BIAS_NEUTRAL, BIAS_INSUFFICIENT))

    summary = {
        "records_total":          len(records),
        "ready_records_used":     ready_used,
        "pending_records_ignored": pending_ignored,
        "helpful_count":          helpful_total,
        "neutral_count":          neutral_total,
        "harmful_count":          harmful_total,
        "strategy_keys_total":    len(records),
        "positive_bias_count":    positive_count,
        "negative_bias_count":    negative_count,
        "neutral_bias_count":     neutral_count,
    }

    out = {
        "component":  "build_allocation_feedback_integration_lite",
        "version":    VERSION,
        "ts_utc":     ts,
        "cycle_id":   cycle_id,
        "equity":     equity,
        "paper_only": True,
        "source_files": {
            "paper_rebalance_outcome_attribution": str(ATTRIBUTION_PATH),
        },
        "integration_constants": {
            "min_ready_outcomes":    MIN_READY_OUTCOMES,
            "full_confidence_at":    FULL_CONFIDENCE_AT,
            "positive_threshold":    POSITIVE_THRESHOLD,
            "negative_threshold":    NEGATIVE_THRESHOLD,
            "caution_threshold":     CAUTION_THRESHOLD,
            "caution_harmful_ratio": CAUTION_HARMFUL_RATIO,
            "modifier_band":         [MODIFIER_BAND_MIN, MODIFIER_BAND_MAX],
        },
        "integration_hook": (
            "allocation_conviction_modifier can scale feedback_confidence in "
            "AC56 score_conviction(). Hook not yet wired; planned AC-62."
        ),
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
        print(f"  {r['strategy_key']:<20} bias={r['allocation_bias_class']:<22}"
              f" modifier={r['allocation_conviction_modifier']}"
              f" cooldown={r['cooldown_flag']}"
              f" ready_n={r['ready_outcomes_count']}")


if __name__ == "__main__":
    main()
