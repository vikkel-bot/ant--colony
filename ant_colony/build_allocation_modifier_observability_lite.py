"""
AC63 Part B: Modifier Observability Dashboarding
Pure observability artefact. No execution, no orders, no state changes.

Sits after AC-61 (feedback integration), AC-62 (conviction modifier wiring),
and AC-63 Part A (persistent feedback memory).

Central question:
  What is the full modifier picture per strategy_key — comparing cycle-local
  modifier (AC-61/AC-62) with memory-informed modifier (AC-63), and projecting
  what the effective conviction would be if memory were wired?

This layer does NOT:
  - change actual AC-62 modifier execution (that uses AC-61 cycle modifier only)
  - write to any execution pipeline
  - apply memory modifier to real conviction scoring

This layer DOES:
  - read AC-61 cycle modifier, AC-62 base confidence, AC-63 memory state
  - compute memory-informed modifier from rolling window
  - blend cycle and memory modifiers conservatively
  - produce a structured observability artefact per strategy_key
  - surface cooldown persistence, bias class, memory confidence

Memory-informed modifier:
  memory_net_signal    = (helpful_win - harmful_win) / max(window_size, 1)
  memory_eff_signal    = memory_net_signal × memory_confidence
  memory_bias_class    → same thresholds as AC-61
  memory_modifier      → same modifier values as AC-61

Blending (when memory_confidence >= MEMORY_MIN_CONFIDENCE):
  mem_weight           = min(MEMORY_WEIGHT_MAX, memory_confidence × MEMORY_WEIGHT_MAX)
  effective_modifier   = cycle_modifier × (1 - mem_weight) + memory_modifier × mem_weight
  effective_modifier   → clamped to [MODIFIER_MIN, MODIFIER_MAX]

When memory_confidence < MEMORY_MIN_CONFIDENCE:
  effective_modifier   = cycle_modifier  (memory does not influence)

Reads:
  allocation_feedback_integration.json    — cycle modifier per strategy_key (AC61)
  allocation_decision_quality.json        — base_feedback_confidence per position (AC62)
  allocation_feedback_memory.json         — rolling window memory state (AC63 Part A)

Writes:
  allocation_modifier_observability.json
  allocation_modifier_observability.tsv

Usage: python ant_colony/build_allocation_modifier_observability_lite.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR = Path(r"C:\Trading\ANT_OUT")

FEEDBACK_PATH     = OUT_DIR / "allocation_feedback_integration.json"
DQ_PATH           = OUT_DIR / "allocation_decision_quality.json"
MEMORY_PATH       = OUT_DIR / "allocation_feedback_memory.json"

OUT_PATH     = OUT_DIR / "allocation_modifier_observability.json"
OUT_TSV_PATH = OUT_DIR / "allocation_modifier_observability.tsv"

VERSION = "modifier_observability_v1"

# Memory modifier thresholds — mirrors AC-61
MEMORY_POSITIVE_THRESHOLD    = 0.20
MEMORY_NEGATIVE_THRESHOLD    = -0.20
MEMORY_CAUTION_THRESHOLD     = -0.50
MEMORY_CAUTION_HARMFUL_RATIO = 0.60

# Modifier values — mirrors AC-61
MODIFIER_POSITIVE  = 1.05
MODIFIER_NEUTRAL   = 1.00
MODIFIER_NEGATIVE  = 0.95
MODIFIER_CAUTION   = 0.90
MODIFIER_MIN       = 0.90
MODIFIER_MAX       = 1.05

# Memory blend parameters
MEMORY_MIN_CONFIDENCE = 0.40   # below this: memory does not influence blend
MEMORY_WEIGHT_MAX     = 0.40   # memory gets at most 40% weight in blend

# Bias classes
BIAS_POSITIVE     = "POSITIVE"
BIAS_NEUTRAL      = "NEUTRAL"
BIAS_NEGATIVE     = "NEGATIVE"
BIAS_CAUTION      = "NEGATIVE_CAUTION"
BIAS_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
BIAS_NO_MEMORY    = "NO_MEMORY"

TSV_HEADERS = [
    "strategy_key", "market",
    "base_feedback_confidence",
    "cycle_modifier", "cycle_bias_class",
    "memory_modifier", "memory_bias_class", "memory_confidence", "memory_status",
    "effective_modifier", "memory_blended",
    "effective_feedback_confidence",
    "ac62_effective_confidence",
    "cooldown_flag", "cooldown_cycles_remaining",
    "modifier_applied", "modifier_reason",
    "observability_flags",
    "feedback_status",
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
# Memory modifier logic (importable for tests)
# ---------------------------------------------------------------------------

def compute_memory_modifier(memory_rec: dict) -> tuple:
    """
    Compute modifier from AC-63 memory record.

    Returns (modifier: float, bias_class: str, reasons: list[str]).
    Fail-closed: if memory_rec is None or confidence too low → (1.00, NO_MEMORY, ...).
    """
    if memory_rec is None:
        return MODIFIER_NEUTRAL, BIAS_NO_MEMORY, ["NO_MEMORY_RECORD"]

    memory_confidence = to_float(memory_rec.get("memory_confidence", 0.0))

    if memory_confidence < MEMORY_MIN_CONFIDENCE:
        return (
            MODIFIER_NEUTRAL,
            BIAS_INSUFFICIENT,
            [f"MEMORY_CONFIDENCE_LOW_{memory_confidence:.4f}"],
        )

    window = memory_rec.get("rolling_window") or []
    n = len(window)
    if n == 0:
        return MODIFIER_NEUTRAL, BIAS_NO_MEMORY, ["EMPTY_WINDOW"]

    labels       = [e["outcome_label"] for e in window]
    helpful      = labels.count("HELPFUL")
    harmful      = labels.count("HARMFUL")
    harmful_ratio = round(harmful / max(n, 1), 4)

    net_signal       = round((helpful - harmful) / max(n, 1), 4)
    effective_signal = round(net_signal * memory_confidence, 4)

    reasons = [f"MEMORY_EFF_SIGNAL={effective_signal:.4f}", f"MEMORY_N={n}"]

    # Apply same priority rules as AC-61
    if effective_signal <= MEMORY_CAUTION_THRESHOLD:
        reasons.insert(0, f"MEMORY_CAUTION_SIGNAL")
        return MODIFIER_CAUTION, BIAS_CAUTION, reasons

    if harmful_ratio >= MEMORY_CAUTION_HARMFUL_RATIO:
        reasons.insert(0, f"MEMORY_CAUTION_HARMFUL_RATIO={harmful_ratio:.4f}")
        return MODIFIER_CAUTION, BIAS_CAUTION, reasons

    if effective_signal <= MEMORY_NEGATIVE_THRESHOLD:
        reasons.insert(0, f"MEMORY_NEGATIVE_SIGNAL")
        return MODIFIER_NEGATIVE, BIAS_NEGATIVE, reasons

    if effective_signal >= MEMORY_POSITIVE_THRESHOLD:
        reasons.insert(0, f"MEMORY_POSITIVE_SIGNAL")
        return MODIFIER_POSITIVE, BIAS_POSITIVE, reasons

    reasons.insert(0, "MEMORY_NEUTRAL_SIGNAL")
    return MODIFIER_NEUTRAL, BIAS_NEUTRAL, reasons


def blend_modifiers(
    cycle_modifier: float,
    memory_modifier: float,
    memory_confidence: float,
) -> tuple:
    """
    Conservatively blend cycle and memory modifiers.

    Returns (effective_modifier: float, was_blended: bool, reason: str).

    When memory_confidence < MEMORY_MIN_CONFIDENCE: returns cycle_modifier unchanged.
    Otherwise: weighted blend, memory gets at most MEMORY_WEIGHT_MAX of the weight.
    Always clamped to [MODIFIER_MIN, MODIFIER_MAX].
    """
    if memory_confidence < MEMORY_MIN_CONFIDENCE:
        clamped = round(max(MODIFIER_MIN, min(MODIFIER_MAX, cycle_modifier)), 4)
        return clamped, False, f"MEMORY_CONF_TOO_LOW_{memory_confidence:.4f}_CYCLE_ONLY"

    # Memory weight scales with confidence, capped at MEMORY_WEIGHT_MAX
    mem_weight = round(min(MEMORY_WEIGHT_MAX, memory_confidence * MEMORY_WEIGHT_MAX), 4)
    cyc_weight = round(1.0 - mem_weight, 4)

    blended = cycle_modifier * cyc_weight + memory_modifier * mem_weight
    clamped = round(max(MODIFIER_MIN, min(MODIFIER_MAX, blended)), 4)

    reason = (
        f"BLEND_CYC={cycle_modifier}_W={cyc_weight:.2f}"
        f"_MEM={memory_modifier}_W={mem_weight:.2f}"
        f"_CONF={memory_confidence:.2f}"
    )
    return clamped, True, reason


# ---------------------------------------------------------------------------
# Core observability record builder (importable for tests)
# ---------------------------------------------------------------------------

def build_observability_record(
    strategy_key: str,
    market: str,
    base_feedback_confidence: float,
    ac62_effective_confidence: float,
    cycle_modifier: float,
    cycle_bias_class: str,
    feedback_status: str,
    memory_rec: dict,
) -> dict:
    """
    Build a single observability record for one strategy_key.

    Computes memory modifier, blends with cycle modifier, projects effective conviction.
    Does NOT change AC-62 execution — projection only.
    """
    memory_modifier, memory_bias_class, mem_reasons = compute_memory_modifier(memory_rec)
    memory_confidence = to_float((memory_rec or {}).get("memory_confidence", 0.0))
    memory_status     = str((memory_rec or {}).get("memory_status") or "NO_MEMORY")
    cooldown_from_mem = bool((memory_rec or {}).get("cooldown_flag", False))
    cooldown_remaining = int((memory_rec or {}).get("cooldown_cycles_remaining") or 0)

    effective_modifier, was_blended, blend_reason = blend_modifiers(
        cycle_modifier, memory_modifier, memory_confidence
    )

    projected_eff_conf = round(
        max(0.0, min(1.0, base_feedback_confidence * effective_modifier)), 4
    )

    modifier_applied = (effective_modifier != MODIFIER_NEUTRAL)

    # Observability flags
    obs_flags = []
    if cooldown_from_mem:
        obs_flags.append("MEMORY_COOLDOWN_ACTIVE")
    if cycle_bias_class == BIAS_CAUTION:
        obs_flags.append("CYCLE_CAUTION")
    if memory_bias_class == BIAS_CAUTION:
        obs_flags.append("MEMORY_CAUTION")
    if cycle_bias_class != memory_bias_class and memory_confidence >= MEMORY_MIN_CONFIDENCE:
        obs_flags.append("CYCLE_MEMORY_BIAS_DISAGREE")
    if effective_modifier < cycle_modifier:
        obs_flags.append("MEMORY_DAMPENS_CYCLE")
    elif effective_modifier > cycle_modifier:
        obs_flags.append("MEMORY_AMPLIFIES_CYCLE")
    if memory_confidence < MEMORY_MIN_CONFIDENCE:
        obs_flags.append("MEMORY_CONFIDENCE_INSUFFICIENT")
    if not obs_flags:
        obs_flags.append("NOMINAL")

    modifier_reason = "|".join([blend_reason] + mem_reasons[:2])

    return {
        "strategy_key":               strategy_key,
        "market":                     market,
        "base_feedback_confidence":   round(base_feedback_confidence, 4),
        "cycle_modifier":             round(cycle_modifier, 4),
        "cycle_bias_class":           cycle_bias_class,
        "memory_modifier":            round(memory_modifier, 4),
        "memory_bias_class":          memory_bias_class,
        "memory_confidence":          round(memory_confidence, 4),
        "memory_status":              memory_status,
        "effective_modifier":         effective_modifier,
        "memory_blended":             was_blended,
        "effective_feedback_confidence": projected_eff_conf,
        "ac62_effective_confidence":  round(ac62_effective_confidence, 4),
        "cooldown_flag":              cooldown_from_mem,
        "cooldown_cycles_remaining":  cooldown_remaining,
        "modifier_applied":           modifier_applied,
        "modifier_reason":            modifier_reason,
        "observability_flags":        "|".join(obs_flags),
        "feedback_status":            feedback_status,
    }


def build_observability_report(
    feedback_records: list,
    dq_records: list,
    memory_state: dict,
) -> list:
    """
    Build observability records for all known strategy_keys.

    feedback_records: list from AC-61 allocation_feedback_integration.json
    dq_records:       list from AC-62 allocation_decision_quality.json
    memory_state:     dict keyed by strategy_key from AC-63 memory

    Returns: list of observability dicts.
    """
    # Index feedback by strategy_key
    feedback_index = {}
    for r in (feedback_records or []):
        sk = str(r.get("strategy_key") or "").strip()
        if sk:
            feedback_index[sk] = r

    # Index DQ records by position_key (= strategy_key format)
    dq_index = {}
    for r in (dq_records or []):
        pk = str(r.get("position_key") or "").strip()
        if pk:
            dq_index[pk] = r

    # Union of all known strategy_keys
    all_keys = (
        set(feedback_index.keys()) |
        set(dq_index.keys()) |
        set(memory_state.keys())
    )

    records = []
    for sk in sorted(all_keys):
        fb_rec  = feedback_index.get(sk) or {}
        dq_rec  = dq_index.get(sk) or {}
        mem_rec = memory_state.get(sk)

        market = (
            str(fb_rec.get("market") or "").strip() or
            str(dq_rec.get("market") or "").strip() or
            str((mem_rec or {}).get("market") or "").strip()
        )

        base_conf       = to_float(dq_rec.get("base_feedback_confidence", 0.0))
        ac62_eff_conf   = to_float(dq_rec.get("effective_feedback_confidence", base_conf))
        cycle_modifier  = to_float(fb_rec.get("allocation_conviction_modifier", MODIFIER_NEUTRAL))
        cycle_bias      = str(fb_rec.get("allocation_bias_class") or BIAS_NEUTRAL)
        feedback_status = str(fb_rec.get("feedback_status") or "NO_FEEDBACK_DATA")

        records.append(build_observability_record(
            strategy_key=sk,
            market=market,
            base_feedback_confidence=base_conf,
            ac62_effective_confidence=ac62_eff_conf,
            cycle_modifier=cycle_modifier,
            cycle_bias_class=cycle_bias,
            feedback_status=feedback_status,
            memory_rec=mem_rec,
        ))

    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ts = utc_now_ts()

    fb_data  = load_json(FEEDBACK_PATH, {}) or {}
    dq_data  = load_json(DQ_PATH, {}) or {}
    mem_data = load_json(MEMORY_PATH, {}) or {}

    cycle_id = (
        fb_data.get("cycle_id") or
        dq_data.get("cycle_id") or
        mem_data.get("cycle_id") or
        "UNKNOWN"
    )

    feedback_records = fb_data.get("records") or []
    dq_records       = dq_data.get("records") or []
    memory_state     = mem_data.get("strategy_keys") or {}

    records = build_observability_report(feedback_records, dq_records, memory_state)

    # Summary
    total         = len(records)
    applied_count = sum(1 for r in records if r["modifier_applied"])
    neutral_fb    = sum(1 for r in records if not r["modifier_applied"])
    positive_c    = sum(1 for r in records if r["cycle_bias_class"] == BIAS_POSITIVE)
    negative_c    = sum(1 for r in records if r["cycle_bias_class"] in (BIAS_NEGATIVE, BIAS_CAUTION))
    caution_c     = sum(1 for r in records if r["cycle_bias_class"] == BIAS_CAUTION)
    mem_caution_c = sum(1 for r in records if r["memory_bias_class"] == BIAS_CAUTION)
    cooldown_c    = sum(1 for r in records if r["cooldown_flag"])
    blended_c     = sum(1 for r in records if r["memory_blended"])

    avg_base_conf = (
        round(sum(r["base_feedback_confidence"] for r in records) / total, 4)
        if total > 0 else 0.0
    )
    avg_eff_conf  = (
        round(sum(r["effective_feedback_confidence"] for r in records) / total, 4)
        if total > 0 else 0.0
    )
    avg_eff_mod   = (
        round(sum(r["effective_modifier"] for r in records) / total, 4)
        if total > 0 else 0.0
    )

    summary = {
        "records_total":               total,
        "modifier_applied_count":      applied_count,
        "neutral_fallback_count":      neutral_fb,
        "positive_count":              positive_c,
        "negative_count":              negative_c,
        "caution_count":               caution_c,
        "memory_caution_count":        mem_caution_c,
        "cooldown_count":              cooldown_c,
        "memory_blended_count":        blended_c,
        "avg_base_feedback_confidence":    avg_base_conf,
        "avg_effective_feedback_confidence": avg_eff_conf,
        "avg_effective_modifier":      avg_eff_mod,
    }

    out = {
        "component":   "build_allocation_modifier_observability_lite",
        "version":     VERSION,
        "ts_utc":      ts,
        "cycle_id":    cycle_id,
        "paper_only":  True,
        "source_files": {
            "allocation_feedback_integration": str(FEEDBACK_PATH),
            "allocation_decision_quality":     str(DQ_PATH),
            "allocation_feedback_memory":      str(MEMORY_PATH),
        },
        "observability_constants": {
            "memory_min_confidence": MEMORY_MIN_CONFIDENCE,
            "memory_weight_max":     MEMORY_WEIGHT_MAX,
            "modifier_band":         [MODIFIER_MIN, MODIFIER_MAX],
        },
        "note": (
            "effective_modifier and effective_feedback_confidence are projections only. "
            "AC-62 currently uses cycle_modifier (from AC-61) for actual conviction scoring. "
            "Memory wiring planned for AC-64."
        ),
        "summary": summary,
        "records": records,
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(OUT_PATH, out)
        write_tsv(OUT_TSV_PATH, TSV_HEADERS, records)
    except Exception as e:
        print(f"[WARN] Could not write observability output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k != "records"}, indent=2))
    for r in records:
        print(
            f"  {r['strategy_key']:<24}"
            f" cycle={r['cycle_modifier']:.2f}({r['cycle_bias_class']:<20})"
            f" mem={r['memory_modifier']:.2f}({r['memory_bias_class']:<20})"
            f" eff={r['effective_modifier']:.2f}"
            f" cooldown={r['cooldown_flag']}"
            f" flags={r['observability_flags']}"
        )


if __name__ == "__main__":
    main()
