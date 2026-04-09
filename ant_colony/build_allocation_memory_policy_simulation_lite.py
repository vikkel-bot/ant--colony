"""
AC-67: Memory Policy Simulation / What-if Engine

Part A — Policy abstraction: loads baseline memory policy from
  ant_colony/policy/allocation_memory_policy.json

Part B — What-if simulation: re-classifies AC-65 observability records
  under candidate policies using the gate_reclassification_v1 method.

Design principles:
  - No live execution, no auto-activation.
  - Same input + same policy = same output (deterministic).
  - Missing/invalid policy file → safe default fallback (fail-closed).
  - Candidate policies are simulation-only; nothing is activated.
  - Conservative: unsafe parameters → HIGH_RISK classification.

Simulation method (gate_reclassification_v1):
  For each AC-65 observability record, re-run the AC-64 gate hierarchy
  using the candidate policy's thresholds and weights. This determines
  what impact class and modifier delta would have resulted under the
  candidate policy, given the same underlying market/memory data.

  Note: RECENT_HARMFUL_BLOCKED records cannot be re-simulated without
  the full rolling window; these are preserved at their original gate
  outcome (conservative).

Reads:
  allocation_memory_impact_observability.json  (AC-65)
  ant_colony/policy/allocation_memory_policy.json  (baseline policy)

Writes:
  allocation_memory_policy_simulation.json
  allocation_memory_policy_simulation.tsv  (per-candidate summary)

Usage: python ant_colony/build_allocation_memory_policy_simulation_lite.py
"""
import copy
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR  = Path(r"C:\Trading\ANT_OUT")
OBS_PATH = OUT_DIR / "allocation_memory_impact_observability.json"
OUT_PATH = OUT_DIR / "allocation_memory_policy_simulation.json"
OUT_TSV  = OUT_DIR / "allocation_memory_policy_simulation.tsv"

POLICY_DIR   = Path(__file__).parent / "policy"
POLICY_PATH  = POLICY_DIR / "allocation_memory_policy.json"
CANDIDATES_PATH = POLICY_DIR / "allocation_memory_candidates.json"  # optional external

VERSION = "memory_policy_simulation_v1"

# ---------------------------------------------------------------------------
# Default policy — mirrors AC-63/64/65/66 hardcoded constants exactly
# Used when policy file is absent or invalid (fail-closed fallback)
# ---------------------------------------------------------------------------

DEFAULT_POLICY: dict = {
    "policy_name":    "baseline_default",
    "policy_version": "v1",
    "description":    "Default memory policy — mirrors AC-63/64/65/66 hardcoded constants exactly",
    "paper_only":     True,
    "groups": {
        "memory_gate": {
            "memory_confidence_min_negative":  0.50,
            "memory_confidence_min_positive":  0.75,
            "negative_blend_weight":           0.50,
            "positive_blend_weight":           0.30,
            "negative_correction_cap":         0.05,
            "positive_correction_cap":         0.03,
            "modifier_band_min":               0.90,
            "modifier_band_max":               1.05,
            "recent_harmful_lookback":         3,
            "recent_harmful_block_threshold":  2,
            "conflict_policy_mode":            "BLOCK_ON_CONFLICT",
        },
        "memory_rolling_window": {
            "window_size":              10,
            "full_memory_at":           8,
            "cooldown_cycles_default":  3,
        },
        "review_thresholds": {
            "review_min_records":                   5,
            "review_positive_applied_rate_warn":    0.30,
            "review_positive_applied_rate_watch":   0.20,
            "review_negative_applied_rate_warn":    0.70,
            "review_cooldown_rate_warn":            0.50,
            "review_conflict_block_rate_warn":      0.30,
            "review_low_conf_blocked_rate_warn":    0.50,
            "review_avg_delta_watch":               0.02,
        },
    },
}

# ---------------------------------------------------------------------------
# Safety limits for candidate policy validation
# Parameters outside these ranges → policy_safe=False → HIGH_RISK
# ---------------------------------------------------------------------------

_SAFE_LIMITS: dict = {
    "memory_confidence_min_negative": (0.20, 0.80),
    "memory_confidence_min_positive": (0.50, 0.95),
    "negative_blend_weight":          (0.10, 0.90),
    "positive_blend_weight":          (0.10, 0.60),
    "negative_correction_cap":        (0.01, 0.10),
    "positive_correction_cap":        (0.005, 0.06),
    "modifier_band_min":              (0.85, 0.92),
    "modifier_band_max":              (1.03, 1.07),
    "cooldown_cycles_default":        (1, 10),
}

# Always use baseline band for safe_band_ok check in simulations
_BASELINE_BAND_MIN = 0.90
_BASELINE_BAND_MAX = 1.05
_MODIFIER_NEUTRAL  = 1.00

# Risk thresholds for classification
_HIGH_RISK_POSITIVE_MULTIPLIER = 1.50  # pos_rate > warn × 1.5 → HIGH_RISK

# ---------------------------------------------------------------------------
# Built-in candidate policies (overlays on baseline)
# Each candidate changes only specific parameters; all others from baseline.
# ---------------------------------------------------------------------------

BUILTIN_CANDIDATES: list = [
    {
        "policy_name":        "stricter_positive_gate",
        "policy_description": "Raise positive gate confidence 0.75→0.85 (fewer positive corrections)",
        "overlay": {"memory_gate": {"memory_confidence_min_positive": 0.85}},
    },
    {
        "policy_name":        "looser_positive_gate",
        "policy_description": "Lower positive gate confidence 0.75→0.60 (more positive corrections)",
        "overlay": {"memory_gate": {"memory_confidence_min_positive": 0.60}},
    },
    {
        "policy_name":        "stronger_negative_dampening",
        "policy_description": "Increase negative blend 0.50→0.70 and cap 0.05→0.07 (stronger braking)",
        "overlay": {"memory_gate": {"negative_blend_weight": 0.70, "negative_correction_cap": 0.07}},
    },
    {
        "policy_name":        "weaker_negative_dampening",
        "policy_description": "Decrease negative blend 0.50→0.25 and cap 0.05→0.025 (gentler braking)",
        "overlay": {"memory_gate": {"negative_blend_weight": 0.25, "negative_correction_cap": 0.025}},
    },
    {
        "policy_name":        "higher_memory_confidence_threshold",
        "policy_description": "Raise negative gate confidence 0.50→0.65 (memory acts less often)",
        "overlay": {"memory_gate": {"memory_confidence_min_negative": 0.65}},
    },
    {
        "policy_name":        "conflict_allow_on_positive",
        "policy_description": "Allow positive memory when cycle is negative (loosen conflict gate)",
        "overlay": {"memory_gate": {"conflict_policy_mode": "ALLOW_ON_CONFLICT"}},
    },
    {
        "policy_name":        "unsafe_extreme",
        "policy_description": "Extreme parameters beyond safe limits — expected HIGH_RISK/TOO_RISKY",
        "overlay": {"memory_gate": {
            "positive_correction_cap": 0.15,
            "negative_blend_weight":   0.95,
            "negative_correction_cap": 0.15,
        }},
    },
]

# TSV headers for per-candidate summary
TSV_HEADERS = [
    "policy_name",
    "policy_safe",
    "policy_risk_class",
    "policy_recommendation",
    "simulated_positive_applied_rate",
    "simulated_negative_applied_rate",
    "simulated_caution_applied_rate",
    "simulated_conflict_block_rate",
    "simulated_neutral_fallback_rate",
    "simulated_memory_applied_rate",
    "simulated_avg_modifier_delta",
    "simulated_safe_band_violation_count",
    "delta_positive_applied_rate",
    "delta_negative_applied_rate",
    "delta_memory_applied_rate",
    "delta_avg_modifier_delta",
    "changed_parameters_count",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def utc_now_ts() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_float(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if f == f else float(default)  # NaN guard
    except Exception:
        return float(default)


def _rate(count: int, total: int) -> float:
    return round(count / total, 4) if total > 0 else 0.0


def _avg(vals: list) -> float:
    return round(sum(vals) / len(vals), 4) if vals else 0.0


def load_json_file(path: Path, default):
    if not path.exists():
        return None, f"FILE_NOT_FOUND:{path}"
    try:
        return json.loads(path.read_text(encoding="utf-8-sig")), None
    except Exception as e:
        return None, f"PARSE_ERROR:{e}"


def write_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def write_tsv(path: Path, rows: list) -> None:
    lines = ["\t".join(TSV_HEADERS)]
    for row in rows:
        lines.append("\t".join("" if row.get(h) is None else str(row.get(h, "")) for h in TSV_HEADERS))
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Part A — Policy abstraction: load, flatten, validate
# ---------------------------------------------------------------------------

def load_policy(path: Path) -> tuple:
    """
    Load baseline policy from JSON.
    Returns (policy_dict, fallback_used: bool, load_reason: str).
    On any error → returns DEFAULT_POLICY with fallback_used=True.
    """
    data, err = load_json_file(path, None)
    if err:
        return copy.deepcopy(DEFAULT_POLICY), True, f"FALLBACK_DEFAULT|{err}"
    if not isinstance(data, dict) or "groups" not in data:
        return copy.deepcopy(DEFAULT_POLICY), True, "FALLBACK_DEFAULT|INVALID_STRUCTURE"

    # Deep-merge: DEFAULT_POLICY fills any missing keys in loaded policy
    merged = copy.deepcopy(DEFAULT_POLICY)
    merged["policy_name"]    = data.get("policy_name",    merged["policy_name"])
    merged["policy_version"] = data.get("policy_version", merged["policy_version"])
    merged["description"]    = data.get("description",    merged["description"])
    for group, defaults in merged["groups"].items():
        loaded_group = data.get("groups", {}).get(group, {})
        for k, v in loaded_group.items():
            if k.startswith("_"):
                continue  # skip comments
            if k in defaults:
                merged["groups"][group][k] = v

    return merged, False, "LOADED_FROM_FILE"


def flatten_policy(policy: dict) -> dict:
    """Flatten grouped policy into a single dict of param → value."""
    flat: dict = {}
    for group_vals in policy.get("groups", {}).values():
        if isinstance(group_vals, dict):
            for k, v in group_vals.items():
                if not k.startswith("_"):
                    flat[k] = v
    return flat


def apply_overlay(baseline_policy: dict, overlay: dict) -> dict:
    """
    Apply a candidate overlay to a baseline policy (deep copy, no mutation).
    overlay format: {group_name: {param: new_value}}
    """
    result = copy.deepcopy(baseline_policy)
    for group, vals in overlay.items():
        if group in result.get("groups", {}):
            for k, v in vals.items():
                result["groups"][group][k] = v
    return result


def validate_policy(flat_policy: dict) -> tuple:
    """
    Check candidate policy against safety limits.
    Returns (is_safe: bool, violations: list[str]).
    """
    violations: list = []

    for param, (lo, hi) in _SAFE_LIMITS.items():
        if param in flat_policy:
            val = _to_float(flat_policy[param], lo)
            if val < lo or val > hi:
                violations.append(f"{param}={val} outside_safe_range[{lo},{hi}]")

    # Cross-constraint: positive confidence gate must be >= negative gate
    neg_gate = _to_float(flat_policy.get("memory_confidence_min_negative", 0.50))
    pos_gate = _to_float(flat_policy.get("memory_confidence_min_positive", 0.75))
    if pos_gate < neg_gate:
        violations.append(
            f"positive_conf_gate={pos_gate} < negative_conf_gate={neg_gate} (inverts asymmetry)"
        )

    return len(violations) == 0, violations


def compute_changed_params(flat_base: dict, flat_cand: dict) -> dict:
    """Return dict of param → {baseline, candidate} for any differing values."""
    changed: dict = {}
    for k in sorted(set(flat_base) | set(flat_cand)):
        bv = flat_base.get(k)
        cv = flat_cand.get(k)
        if bv != cv:
            changed[k] = {"baseline": bv, "candidate": cv}
    return changed


# ---------------------------------------------------------------------------
# Part B — What-if simulation: gate_reclassification_v1
# ---------------------------------------------------------------------------

def simulate_record(obs_rec: dict, p: dict) -> dict:
    """
    Re-simulate one AC-65 observability record under candidate policy p.
    p is a flat policy dict (from flatten_policy).

    Re-runs the AC-64 gate hierarchy with p's thresholds / weights.
    RECENT_HARMFUL_BLOCKED is preserved as-is (no rolling window available).
    Fail-closed: missing fields → no-effect fallback.

    Returns a dict of sim_* fields.
    """
    NEUTRAL = _MODIFIER_NEUTRAL

    mem_available = bool(obs_rec.get("memory_available", False))
    mem_conf      = _to_float(obs_rec.get("memory_confidence", 0.0))
    mem_modifier  = _to_float(obs_rec.get("memory_modifier", NEUTRAL))
    mem_bias      = str(obs_rec.get("memory_bias_class") or "NEUTRAL")
    cycle_mod     = _to_float(obs_rec.get("cycle_modifier", NEUTRAL))
    cycle_bias    = str(obs_rec.get("cycle_bias_class") or "NEUTRAL")
    cooldown      = bool(obs_rec.get("cooldown_flag", False))
    orig_gate     = str(obs_rec.get("memory_influence_gate") or "MEMORY_ABSENT")
    base_conf     = _to_float(obs_rec.get("base_feedback_confidence", 0.0))

    band_min = _to_float(p.get("modifier_band_min", _BASELINE_BAND_MIN))
    band_max = _to_float(p.get("modifier_band_max", _BASELINE_BAND_MAX))

    def _no_effect(gate: str, impact_class: str) -> dict:
        return {
            "sim_impact_class":            impact_class,
            "sim_modifier_delta":          0.0,
            "sim_confidence_delta":        0.0,
            "sim_memory_modifier_applied": False,
            "sim_safe_band_ok":            True,
            "sim_gate_name":               gate,
            "sim_reason":                  gate,
        }

    # Gate 1 — no memory data
    if not mem_available:
        return _no_effect("MEMORY_ABSENT", "NO_MEMORY")

    # Gate 2 — confidence below negative gate threshold
    conf_gate_neg = _to_float(p.get("memory_confidence_min_negative", 0.50))
    if mem_conf < conf_gate_neg:
        return _no_effect("MEMORY_CONF_TOO_LOW", "BLOCKED_LOW_CONFIDENCE")

    # Gate 3 — negative / cooldown path
    is_negative = (mem_modifier < NEUTRAL) or cooldown
    if is_negative:
        neg_blend  = _to_float(p.get("negative_blend_weight", 0.50))
        neg_cap    = _to_float(p.get("negative_correction_cap", 0.05))
        raw_corr   = (mem_modifier - NEUTRAL) * neg_blend
        correction = round(max(-neg_cap, min(0.0, raw_corr)), 4)
        final      = round(max(band_min, min(band_max, cycle_mod + correction)), 4)
        delta      = round(final - cycle_mod, 4)
        gate       = "COOLDOWN_GATE_OPEN" if cooldown else "NEGATIVE_GATE_OPEN"
        if cooldown:
            impact = "CAUTION_DAMPENING"
        elif mem_bias == "NEGATIVE_CAUTION":
            impact = "CAUTION_DAMPENING"
        else:
            impact = "NEGATIVE_DAMPENING"
        safe = (_BASELINE_BAND_MIN <= final <= _BASELINE_BAND_MAX)
        return {
            "sim_impact_class":            impact,
            "sim_modifier_delta":          delta,
            "sim_confidence_delta":        round(base_conf * delta, 4),
            "sim_memory_modifier_applied": True,
            "sim_safe_band_ok":            safe,
            "sim_gate_name":               gate,
            "sim_reason":                  f"{gate}|blend={neg_blend}|cap={neg_cap}|corr={correction}",
        }

    # Gate 4 — positive path
    is_positive = (mem_modifier > NEUTRAL) and (not cooldown)
    if is_positive:
        # Gate 4a — conflict check
        conflict_mode = str(p.get("conflict_policy_mode", "BLOCK_ON_CONFLICT"))
        if conflict_mode == "BLOCK_ON_CONFLICT":
            if cycle_bias in ("NEGATIVE", "NEGATIVE_CAUTION"):
                return _no_effect("CONFLICT_BLOCKED", "BLOCKED_BY_CONFLICT")

        # Gate 4b — positive confidence gate
        conf_gate_pos = _to_float(p.get("memory_confidence_min_positive", 0.75))
        if mem_conf < conf_gate_pos:
            return _no_effect("POSITIVE_CONF_TOO_LOW", "BLOCKED_LOW_CONFIDENCE")

        # Gate 4c — recent harmful (preserved; no window data in AC-65 records)
        if orig_gate == "RECENT_HARMFUL_BLOCKED":
            return _no_effect("RECENT_HARMFUL_BLOCKED", "BLOCKED_LOW_CONFIDENCE")

        # Gate 4d — positive correction
        pos_blend  = _to_float(p.get("positive_blend_weight", 0.30))
        pos_cap    = _to_float(p.get("positive_correction_cap", 0.03))
        raw_corr   = (mem_modifier - NEUTRAL) * pos_blend
        correction = round(min(pos_cap, max(0.0, raw_corr)), 4)
        final      = round(max(band_min, min(band_max, cycle_mod + correction)), 4)
        delta      = round(final - cycle_mod, 4)
        safe       = (_BASELINE_BAND_MIN <= final <= _BASELINE_BAND_MAX)
        return {
            "sim_impact_class":            "POSITIVE_REINFORCEMENT",
            "sim_modifier_delta":          delta,
            "sim_confidence_delta":        round(base_conf * delta, 4),
            "sim_memory_modifier_applied": True,
            "sim_safe_band_ok":            safe,
            "sim_gate_name":               "POSITIVE_GATE_OPEN",
            "sim_reason":                  f"POSITIVE_GATE_OPEN|blend={pos_blend}|cap={pos_cap}|corr={correction}",
        }

    # Gate 5 — neutral memory signal
    return _no_effect("MEMORY_NEUTRAL", "NO_EFFECT")


def aggregate_simulated_metrics(sim_records: list, n_total: int) -> dict:
    """Aggregate per-record simulation results into summary metrics."""
    n = n_total or len(sim_records)
    if n == 0 or not sim_records:
        return {
            "simulated_memory_applied_rate":       0.0,
            "simulated_positive_applied_rate":     0.0,
            "simulated_negative_applied_rate":     0.0,
            "simulated_caution_applied_rate":      0.0,
            "simulated_conflict_block_rate":       0.0,
            "simulated_neutral_fallback_rate":     0.0,
            "simulated_avg_modifier_delta":        0.0,
            "simulated_avg_confidence_delta":      0.0,
            "simulated_safe_band_violation_count": 0,
        }

    impact_counts = Counter(r["sim_impact_class"] for r in sim_records)
    applied    = sum(1 for r in sim_records if r["sim_memory_modifier_applied"])
    violations = sum(1 for r in sim_records if not r["sim_safe_band_ok"])
    mod_deltas  = [r["sim_modifier_delta"]    for r in sim_records]
    conf_deltas = [r["sim_confidence_delta"]  for r in sim_records]

    neutral_fallback = (
        impact_counts.get("NO_EFFECT", 0)
        + impact_counts.get("NO_MEMORY", 0)
        + impact_counts.get("BLOCKED_ABSENT_MEMORY", 0)
    )

    return {
        "simulated_memory_applied_rate":       _rate(applied, n),
        "simulated_positive_applied_rate":     _rate(impact_counts.get("POSITIVE_REINFORCEMENT", 0), n),
        "simulated_negative_applied_rate":     _rate(impact_counts.get("NEGATIVE_DAMPENING", 0), n),
        "simulated_caution_applied_rate":      _rate(impact_counts.get("CAUTION_DAMPENING", 0), n),
        "simulated_conflict_block_rate":       _rate(impact_counts.get("BLOCKED_BY_CONFLICT", 0), n),
        "simulated_neutral_fallback_rate":     _rate(neutral_fallback, n),
        "simulated_avg_modifier_delta":        _avg(mod_deltas),
        "simulated_avg_confidence_delta":      _avg(conf_deltas),
        "simulated_safe_band_violation_count": violations,
    }


def compute_delta(base_metrics: dict, sim_metrics: dict) -> dict:
    """Compute sim − baseline delta for each comparable metric."""
    key_map = {
        "simulated_positive_applied_rate":     "positive_applied_rate",
        "simulated_negative_applied_rate":     "negative_applied_rate",
        "simulated_caution_applied_rate":      "caution_applied_rate",
        "simulated_conflict_block_rate":       "conflict_block_rate",
        "simulated_neutral_fallback_rate":     "neutral_fallback_rate",
        "simulated_memory_applied_rate":       "memory_applied_rate",
        "simulated_avg_modifier_delta":        "avg_modifier_delta",
        "simulated_avg_confidence_delta":      "avg_confidence_delta",
        "simulated_safe_band_violation_count": "safe_band_violation_count",
    }
    delta: dict = {}
    for full_key, short_key in key_map.items():
        bv = _to_float(base_metrics.get(full_key, 0.0))
        cv = _to_float(sim_metrics.get(full_key, 0.0))
        delta[short_key] = round(cv - bv, 4)
    return delta


def classify_risk(
    sim_metrics: dict,
    flat_cand: dict,
    flat_base: dict,
    policy_safe: bool,
    n_total: int,
) -> tuple:
    """
    Classify risk of a candidate policy based on simulated metrics.
    Returns (risk_class: str, reasons: list[str]).
    """
    min_recs = int(_to_float(flat_base.get("review_min_records", 5)))
    if n_total < min_recs:
        return "INSUFFICIENT_DATA", [f"RECORDS={n_total} BELOW_MIN={min_recs}"]

    # Unsafe parameters → always HIGH_RISK
    if not policy_safe:
        return "HIGH_RISK", ["POLICY_PARAMETER_OUTSIDE_SAFE_LIMITS"]

    # Safe band violations in simulation → HIGH_RISK
    violations = sim_metrics.get("simulated_safe_band_violation_count", 0)
    if violations > 0:
        return "HIGH_RISK", [f"SIMULATION_SAFE_BAND_VIOLATIONS={violations}"]

    reasons: list = []
    pos_warn    = _to_float(flat_base.get("review_positive_applied_rate_warn", 0.30))
    neg_warn    = _to_float(flat_base.get("review_negative_applied_rate_warn", 0.70))
    delta_watch = _to_float(flat_base.get("review_avg_delta_watch", 0.02))

    pos_rate = sim_metrics.get("simulated_positive_applied_rate", 0.0)
    if pos_rate > pos_warn * _HIGH_RISK_POSITIVE_MULTIPLIER:
        return "HIGH_RISK", [f"POSITIVE_RATE_VERY_HIGH={pos_rate:.4f} THRESHOLD={pos_warn * _HIGH_RISK_POSITIVE_MULTIPLIER:.4f}"]

    medium: list = []
    if pos_rate > pos_warn:
        medium.append(f"POSITIVE_RATE_ABOVE_WARN={pos_rate:.4f}")

    neg_combined = (
        sim_metrics.get("simulated_negative_applied_rate", 0.0)
        + sim_metrics.get("simulated_caution_applied_rate", 0.0)
    )
    if neg_combined > neg_warn:
        medium.append(f"NEG_CAUTION_COMBINED_RATE={neg_combined:.4f}")

    avg_abs_delta = abs(sim_metrics.get("simulated_avg_modifier_delta", 0.0))
    if avg_abs_delta > delta_watch * 2:
        medium.append(f"AVG_MODIFIER_DELTA_ELEVATED={avg_abs_delta:.4f}")

    if medium:
        return "MEDIUM_RISK", medium

    return "LOW_RISK", ["ALL_METRICS_WITHIN_BOUNDS"]


def generate_recommendation(
    risk_class: str,
    sim_metrics: dict,
    delta: dict,
) -> str:
    """
    Map risk class + delta to a recommendation label.
    Returns one of: NO_CHANGE | WORTH_REVIEW | TOO_RISKY | INSUFFICIENT_DATA
                    | CANDIDATE_FOR_MANUAL_TRIAL
    """
    if risk_class == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    if risk_class == "HIGH_RISK":
        return "TOO_RISKY"

    if risk_class == "LOW_RISK":
        abs_deltas = [abs(v) for v in delta.values() if isinstance(v, (int, float))]
        if max(abs_deltas, default=0.0) < 0.03:
            return "NO_CHANGE"
        return "WORTH_REVIEW"

    if risk_class == "MEDIUM_RISK":
        violations = sim_metrics.get("simulated_safe_band_violation_count", 0)
        pos_delta  = delta.get("positive_applied_rate", 0.0)
        if violations == 0 and abs(pos_delta) <= 0.10:
            return "CANDIDATE_FOR_MANUAL_TRIAL"
        return "WORTH_REVIEW"

    return "WORTH_REVIEW"


# ---------------------------------------------------------------------------
# Main simulation builder (importable for tests)
# ---------------------------------------------------------------------------

def build_simulation(
    obs_records: list,
    baseline_policy: dict,
    candidate_specs: list,
) -> dict:
    """
    Build the complete what-if simulation from AC-65 records and candidate specs.
    Returns the full simulation result dict. No file I/O.

    candidate_specs: list of {policy_name, policy_description, overlay}
    """
    flat_base = flatten_policy(baseline_policy)

    # Baseline metrics (re-simulate baseline to get consistent numbers)
    base_sim   = [simulate_record(r, flat_base) for r in obs_records]
    base_metrics = aggregate_simulated_metrics(base_sim, len(obs_records))

    comparisons: list = []
    completed = failed = deltas_detected = 0

    for spec in candidate_specs:
        try:
            cand_policy  = apply_overlay(baseline_policy, spec.get("overlay", {}))
            flat_cand    = flatten_policy(cand_policy)
            is_safe, unsafe_reasons = validate_policy(flat_cand)

            sim_recs     = [simulate_record(r, flat_cand) for r in obs_records]
            sim_metrics  = aggregate_simulated_metrics(sim_recs, len(obs_records))

            changed      = compute_changed_params(flat_base, flat_cand)
            delta        = compute_delta(base_metrics, sim_metrics)

            risk_class, risk_reasons = classify_risk(
                sim_metrics, flat_cand, flat_base, is_safe, len(obs_records)
            )
            recommendation = generate_recommendation(risk_class, sim_metrics, delta)

            if any(abs(_to_float(v)) > 0.001 for v in delta.values()):
                deltas_detected += 1

            comparisons.append({
                "policy_name":                    spec["policy_name"],
                "policy_description":             spec.get("policy_description", ""),
                "changed_parameters":             changed,
                "policy_safe":                    is_safe,
                "policy_safe_violations":         unsafe_reasons,
                "simulated_records_total":        len(obs_records),
                **sim_metrics,
                "delta_vs_baseline":              delta,
                "policy_risk_class":              risk_class,
                "policy_recommendation":          recommendation,
                "simulation_reasons":             risk_reasons,
                # TSV helper fields
                "delta_positive_applied_rate":    delta.get("positive_applied_rate", 0.0),
                "delta_negative_applied_rate":    delta.get("negative_applied_rate", 0.0),
                "delta_memory_applied_rate":      delta.get("memory_applied_rate", 0.0),
                "delta_avg_modifier_delta":       delta.get("avg_modifier_delta", 0.0),
                "changed_parameters_count":       len(changed),
            })
            completed += 1

        except Exception as exc:
            failed += 1
            comparisons.append({
                "policy_name":           spec.get("policy_name", "UNKNOWN"),
                "policy_description":    spec.get("policy_description", ""),
                "policy_safe":           False,
                "policy_safe_violations": [f"SIMULATION_ERROR:{exc}"],
                "simulated_records_total": len(obs_records),
                "policy_risk_class":     "INSUFFICIENT_DATA",
                "policy_recommendation": "INSUFFICIENT_DATA",
                "simulation_reasons":    [f"ERROR:{exc}"],
            })

    summary = {
        "candidate_policies_total":       len(candidate_specs),
        "baseline_policy_name":           baseline_policy.get("policy_name", "baseline_default"),
        "records_total":                  len(obs_records),
        "reviewable_records_count":       len(obs_records),
        "simulations_completed":          completed,
        "simulations_failed":             failed,
        "policy_deltas_detected_count":   deltas_detected,
        "recommendations_generated_count": completed,
    }

    return {
        "summary":            summary,
        "baseline_policy":    flat_base,
        "baseline_metrics":   base_metrics,
        "policy_comparisons": comparisons,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ts = utc_now_ts()

    # Load baseline policy (fail-closed)
    baseline_policy, fallback_used, policy_load_reason = load_policy(POLICY_PATH)

    # Load external candidate specs (optional — supplement BUILTIN_CANDIDATES)
    extra_candidates: list = []
    ext_data, ext_err = load_json_file(CANDIDATES_PATH, None)
    if ext_data and isinstance(ext_data, dict) and "candidates" in ext_data:
        extra_candidates = ext_data["candidates"]

    candidate_specs = BUILTIN_CANDIDATES + extra_candidates

    # Load AC-65 observability records
    obs_data, obs_err = load_json_file(OBS_PATH, None)
    obs_records: list = []
    if obs_data and isinstance(obs_data, dict):
        obs_records = obs_data.get("records", [])

    # Run simulation
    simulation = build_simulation(obs_records, baseline_policy, candidate_specs)

    out = {
        "component":  "build_allocation_memory_policy_simulation_lite",
        "version":    VERSION,
        "ts_utc":     ts,
        "paper_only": True,
        "source_files": {
            "obs_path":    str(OBS_PATH),
            "policy_path": str(POLICY_PATH),
        },
        "policy_load_info": {
            "fallback_used":        fallback_used,
            "policy_load_reason":   policy_load_reason,
            "external_candidates":  len(extra_candidates),
        },
        "simulation_config": {
            "simulation_method":     "gate_reclassification_v1",
            "builtin_candidates":    len(BUILTIN_CANDIDATES),
            "external_candidates":   len(extra_candidates),
            "baseline_policy_name":  baseline_policy.get("policy_name", "baseline_default"),
        },
        "summary":            simulation["summary"],
        "baseline_policy":    simulation["baseline_policy"],
        "baseline_metrics":   simulation["baseline_metrics"],
        "candidate_policies": simulation["policy_comparisons"],
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(OUT_PATH, out)
        write_tsv(OUT_TSV, [c for c in simulation["policy_comparisons"] if "simulated_records_total" in c])
    except Exception as e:
        print(f"[WARN] Could not write output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k != "candidate_policies"}, indent=2))
    print(f"\n  policy_comparisons ({len(simulation['policy_comparisons'])} candidates):")
    for c in simulation["policy_comparisons"]:
        print(
            f"    {c['policy_name']:<35}"
            f" risk={c.get('policy_risk_class', '?'):<16}"
            f" rec={c.get('policy_recommendation', '?')}"
        )


if __name__ == "__main__":
    main()
