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
import importlib.util as _ilu
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


OUT_DIR  = Path(r"C:\Trading\ANT_OUT")
OBS_PATH = OUT_DIR / "allocation_memory_impact_observability.json"
OUT_PATH = OUT_DIR / "allocation_memory_policy_simulation.json"
OUT_TSV  = OUT_DIR / "allocation_memory_policy_simulation.tsv"

POLICY_DIR      = Path(__file__).parent / "policy"
POLICY_PATH     = POLICY_DIR / "allocation_memory_policy.json"
CANDIDATES_PATH = POLICY_DIR / "allocation_memory_candidates.json"  # optional external

VERSION = "memory_policy_simulation_v1"

# ---------------------------------------------------------------------------
# AC-68: canonical policy loader — single source of defaults and load logic.
# DEFAULT_POLICY and load_policy() are imported from the canonical loader.
# No embedded copy of DEFAULT_POLICY lives here.
# ---------------------------------------------------------------------------

def _import_canonical_loader():
    _path = Path(__file__).parent / "policy" / "load_allocation_memory_policy_lite.py"
    _spec = _ilu.spec_from_file_location("_policy_loader", _path)
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    return _mod

def _import_scenario_registry():
    _path = Path(__file__).parent / "policy" / "load_scenario_registry_lite.py"
    _spec = _ilu.spec_from_file_location("_registry_loader", _path)
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    return _mod

_canonical_loader = _import_canonical_loader()
_registry_loader  = _import_scenario_registry()

# Re-export for test/caller compatibility: sim.DEFAULT_POLICY and sim.load_policy()
DEFAULT_POLICY = copy.deepcopy(_canonical_loader.DEFAULT_POLICY)
load_policy    = _canonical_loader.load_policy

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
# Part A — Policy abstraction: flatten, validate (load_policy → canonical loader)
# ---------------------------------------------------------------------------

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
# AC-73: Scenario evaluation + recommendation ranking
# ---------------------------------------------------------------------------

# Ordinal ranking: lower = more actionable / less risky.
_RECOMMENDATION_RANK: dict = {
    "CANDIDATE_FOR_MANUAL_TRIAL": 1,
    "WORTH_REVIEW":               2,
    "NO_CHANGE":                  3,
    "INSUFFICIENT_DATA":          4,
    "TOO_RISKY":                  5,
}


def _evaluation_reason_for(comp: dict) -> str:
    """
    Build a short, human-readable reason string for a comparison entry.
    Used in evaluate_scenario_comparisons(); not part of the core scoring.
    """
    rec   = comp.get("policy_recommendation", "")
    risk  = comp.get("policy_risk_class", "")
    n_ch  = comp.get("changed_parameters_count", 0)
    delta = comp.get("delta_vs_baseline") or {}
    risk_reasons = comp.get("simulation_reasons") or []

    if rec == "TOO_RISKY":
        first = risk_reasons[0] if risk_reasons else risk
        return f"HIGH_RISK|{first}"
    if rec == "INSUFFICIENT_DATA":
        n = comp.get("simulated_records_total", 0)
        return f"INSUFFICIENT_DATA|records={n}"
    if rec == "NO_CHANGE":
        return f"LOW_RISK_NO_DELTA|changed_params={n_ch}"
    if rec == "WORTH_REVIEW":
        d_pos = _to_float(delta.get("positive_applied_rate", 0.0))
        d_neg = _to_float(delta.get("negative_applied_rate", 0.0))
        return f"{risk}|NOTABLE_DELTA|d_pos={d_pos:+.4f}|d_neg={d_neg:+.4f}"
    if rec == "CANDIDATE_FOR_MANUAL_TRIAL":
        d_pos = _to_float(delta.get("positive_applied_rate", 0.0))
        return f"MEDIUM_RISK_VIABLE|d_pos={d_pos:+.4f}|changed_params={n_ch}"
    return f"{rec}|{risk}"


def evaluate_scenario_comparisons(comparisons: list) -> tuple:
    """
    Enrich comparison entries with evaluation fields and build a top-level summary.

    Each entry is enriched with:
      risk_rank         — int (1=most actionable, 5=rejected); see _RECOMMENDATION_RANK
      evaluation_reason — short explanatory string

    Returns:
      (enriched_comparisons: list, scenario_evaluation_summary: dict)

    Pure function — input list is not mutated; returns new list.
    Deterministic: tie-breaking within same rank is by scenario_id (alphabetical).
    """
    enriched: list = []
    for comp in comparisons:
        ec = dict(comp)
        rec = ec.get("policy_recommendation", "")
        ec["risk_rank"]        = _RECOMMENDATION_RANK.get(rec, 99)
        ec["evaluation_reason"] = _evaluation_reason_for(ec)
        enriched.append(ec)

    # Ranked order: (risk_rank, scenario_id) for determinism
    ranked = sorted(enriched, key=lambda c: (c["risk_rank"], c.get("scenario_id", "")))

    # Best candidate: first with rank <= 2 (CANDIDATE_FOR_MANUAL_TRIAL or WORTH_REVIEW)
    best = next((c for c in ranked if c["risk_rank"] <= 2), None)

    rejected     = sorted(c["scenario_id"] for c in enriched
                          if c.get("policy_recommendation") == "TOO_RISKY"
                          and c.get("scenario_id"))
    insufficient = sorted(c["scenario_id"] for c in enriched
                          if c.get("policy_recommendation") == "INSUFFICIENT_DATA"
                          and c.get("scenario_id"))

    summary = {
        "comparison_count":               len(comparisons),
        "best_candidate_scenario_id":     best["scenario_id"] if best else None,
        "best_candidate_recommendation":  best.get("policy_recommendation") if best else None,
        "best_candidate_risk_class":      best.get("policy_risk_class") if best else None,
        "rejected_scenarios":             rejected,
        "rejected_count":                 len(rejected),
        "insufficient_data_scenarios":    insufficient,
        "insufficient_data_count":        len(insufficient),
        "worth_review_count":             sum(1 for c in enriched
                                              if c.get("policy_recommendation") == "WORTH_REVIEW"),
        "no_change_count":                sum(1 for c in enriched
                                              if c.get("policy_recommendation") == "NO_CHANGE"),
        "candidate_for_trial_count":      sum(1 for c in enriched
                                              if c.get("policy_recommendation") == "CANDIDATE_FOR_MANUAL_TRIAL"),
        "ranked_scenario_ids":            [c.get("scenario_id", c.get("policy_name", ""))
                                           for c in ranked],
    }

    return enriched, summary


# ---------------------------------------------------------------------------
# AC-74: Allocation advisory layer
# ---------------------------------------------------------------------------

# Advisory confidence per actionable recommendation.
# These are fixed, conservative values — not computed from returns.
_ADVISORY_CONFIDENCE: dict = {
    "CANDIDATE_FOR_MANUAL_TRIAL": 0.70,
    "WORTH_REVIEW":               0.50,
}

# Recommendations that are safe to surface as an advisory.
# HIGH_RISK/TOO_RISKY, INSUFFICIENT_DATA, and NO_CHANGE never qualify.
_ADVISORY_ELIGIBLE_RECS: frozenset = frozenset({
    "WORTH_REVIEW",
    "CANDIDATE_FOR_MANUAL_TRIAL",
})

# AC-75: Stable reason codes for the queen intake contract.
# These are the canonical codes; the free-form advisory_reason string
# provides human-readable detail but this code is machine-stable.
_ADVISORY_REASON_CODES: dict = {
    # ADVISORY_ACTIVE paths
    "WORTH_REVIEW":               "ACTIVE_WORTH_REVIEW",
    "CANDIDATE_FOR_MANUAL_TRIAL": "ACTIVE_CANDIDATE_FOR_TRIAL",
    # BASELINE_HOLD paths (used as keys for HOLD resolution)
    "_HOLD_NO_CANDIDATES":               "HOLD_NO_CANDIDATES",
    "_HOLD_ALL_REJECTED_OR_INSUFFICIENT": "HOLD_ALL_REJECTED_OR_INSUFFICIENT",
    "_HOLD_ALL_NO_CHANGE":               "HOLD_ALL_NO_CHANGE",
    "_HOLD_NO_SUITABLE_CANDIDATE":       "HOLD_NO_SUITABLE_CANDIDATE",
    "_HOLD_NO_ELIGIBLE_REC":             "HOLD_NO_ELIGIBLE_REC",
}

# Version tag for the queen intake contract shape.
# Bump when field names or semantics change.
QUEEN_INTAKE_CONTRACT_VERSION: str = "v1"


def build_allocation_advisory(
    evaluation_summary: dict,
    comparisons: list,
) -> dict:
    """
    Translate scenario evaluation output into an allocation advisory.

    Advisory is STRICTLY simulation-only and read-only.
    It carries no execution coupling; advisory_simulation_only=True is always set.

    Eligible scenarios (WORTH_REVIEW / CANDIDATE_FOR_MANUAL_TRIAL) become
    ADVISORY_ACTIVE. Any other outcome → BASELINE_HOLD.

    HIGH_RISK / TOO_RISKY / INSUFFICIENT_DATA / NO_CHANGE are never advisory-active.

    Returns a dict with:
      advisory_status                — "ADVISORY_ACTIVE" | "BASELINE_HOLD"
      advisory_scenario_id           — scenario_id of advisory (or "baseline")
      advisory_action                — "CONSIDER_VARIANT" | "KEEP_CURRENT_POLICY"
      advisory_confidence            — float [0.0, 1.0]
      advisory_reason                — short deterministic explanation string
      advisory_simulation_only       — always True (explicit safety marker)
      advisory_reason_code           — stable machine-readable code (AC-75)
      queen_intake_ready             — True iff ADVISORY_ACTIVE (AC-75)
      queen_intake_contract_version  — shape version tag (AC-75)
    """
    best_id   = evaluation_summary.get("best_candidate_scenario_id")
    best_rec  = evaluation_summary.get("best_candidate_recommendation")
    best_risk = evaluation_summary.get("best_candidate_risk_class", "")
    n_comps   = evaluation_summary.get("comparison_count", 0)

    # Advisory-active path: best_candidate exists and has an eligible recommendation
    if best_id is not None and best_rec in _ADVISORY_ELIGIBLE_RECS:
        confidence = _ADVISORY_CONFIDENCE.get(best_rec, 0.40)

        # Enrich with changed_parameters count from the comparison entry
        adv_comp  = next((c for c in comparisons if c.get("scenario_id") == best_id), None)
        n_changed = adv_comp.get("changed_parameters_count", 0) if adv_comp else 0

        return {
            "advisory_status":               "ADVISORY_ACTIVE",
            "advisory_scenario_id":          best_id,
            "advisory_action":               "CONSIDER_VARIANT",
            "advisory_confidence":           confidence,
            "advisory_reason":               (
                f"{best_rec}|risk={best_risk}"
                f"|changed_params={n_changed}"
                f"|basis={n_comps}_scenarios"
            ),
            "advisory_simulation_only":      True,
            "advisory_reason_code":          _ADVISORY_REASON_CODES.get(best_rec, "ACTIVE_UNKNOWN"),
            "queen_intake_ready":            True,
            "queen_intake_contract_version": QUEEN_INTAKE_CONTRACT_VERSION,
        }

    # Baseline-hold path: explain why no advisory was generated
    parts: list = []
    if n_comps == 0:
        hold_code = "_HOLD_NO_CANDIDATES"
        parts.append("NO_CANDIDATES_SIMULATED")
    elif best_id is None:
        rej   = evaluation_summary.get("rejected_count", 0)
        insuf = evaluation_summary.get("insufficient_data_count", 0)
        nc    = evaluation_summary.get("no_change_count", 0)
        if rej + insuf == n_comps:
            hold_code = "_HOLD_ALL_REJECTED_OR_INSUFFICIENT"
            parts.append(f"ALL_{n_comps}_REJECTED_OR_INSUFFICIENT")
        elif nc == n_comps:
            hold_code = "_HOLD_ALL_NO_CHANGE"
            parts.append(f"ALL_{n_comps}_NO_CHANGE")
        else:
            hold_code = "_HOLD_NO_SUITABLE_CANDIDATE"
            parts.append("NO_SUITABLE_CANDIDATE")
    else:
        hold_code = "_HOLD_NO_ELIGIBLE_REC"
        parts.append(f"NO_ELIGIBLE_RECOMMENDATION_rec={best_rec}")
    parts.append(f"basis={n_comps}_scenarios")

    return {
        "advisory_status":               "BASELINE_HOLD",
        "advisory_scenario_id":          "baseline",
        "advisory_action":               "KEEP_CURRENT_POLICY",
        "advisory_confidence":           1.00,
        "advisory_reason":               "|".join(parts),
        "advisory_simulation_only":      True,
        "advisory_reason_code":          _ADVISORY_REASON_CODES.get(hold_code, "HOLD_UNKNOWN"),
        "queen_intake_ready":            False,
        "queen_intake_contract_version": QUEEN_INTAKE_CONTRACT_VERSION,
    }


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
                # AC-72: stable scenario identity + fingerprint
                "scenario_id":                    spec.get("scenario_id", spec["policy_name"]),
                "scenario_type":                  spec.get("scenario_type", "variant"),
                "fingerprint":                    _canonical_loader.policy_fingerprint(cand_policy),
                # Existing fields
                "policy_name":                    spec["policy_name"],
                "policy_description":             spec.get("policy_description", spec.get("description", "")),
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

    # AC-73: enrich comparisons with risk_rank/evaluation_reason; build evaluation summary
    enriched_comparisons, evaluation_summary = evaluate_scenario_comparisons(comparisons)

    # AC-74: allocation advisory derived from evaluation summary
    advisory = build_allocation_advisory(evaluation_summary, enriched_comparisons)

    return {
        "summary":                     summary,
        "baseline_policy":             flat_base,
        "baseline_metrics":            base_metrics,
        "policy_comparisons":          enriched_comparisons,
        "scenario_evaluation_summary": evaluation_summary,
        "allocation_advisory":         advisory,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ts = utc_now_ts()

    # Load baseline policy (fail-closed)
    baseline_policy, fallback_used, policy_load_reason = load_policy(POLICY_PATH)

    # AC-72: load scenario registry as primary source for candidate specs.
    # Fail-closed: if registry unavailable, falls back to BUILTIN_CANDIDATES.
    registry_scenarios, reg_fallback, reg_reason = _registry_loader.load_registry()
    variant_scenarios = _registry_loader.get_variant_scenarios(registry_scenarios)

    if variant_scenarios:
        # Convert registry format to simulator candidate_spec format (add compat fields)
        candidate_specs = [
            {
                **s,
                "policy_name":        s.get("scenario_id", s.get("scenario_name", "unknown")),
                "policy_description": s.get("description", ""),
            }
            for s in variant_scenarios
        ]
        registry_source = "REGISTRY"
    else:
        # Fallback to builtin candidates if registry produced nothing
        candidate_specs = list(BUILTIN_CANDIDATES)
        registry_source = "BUILTIN_FALLBACK"

    # Load external candidate specs (optional — supplement active candidate_specs)
    extra_candidates: list = []
    ext_data, ext_err = load_json_file(CANDIDATES_PATH, None)
    if ext_data and isinstance(ext_data, dict) and "candidates" in ext_data:
        extra_candidates = ext_data["candidates"]
        candidate_specs = candidate_specs + extra_candidates

    # Load AC-65 observability records
    obs_data, obs_err = load_json_file(OBS_PATH, None)
    obs_records: list = []
    if obs_data and isinstance(obs_data, dict):
        obs_records = obs_data.get("records", [])

    # Run simulation
    simulation = build_simulation(obs_records, baseline_policy, candidate_specs)

    baseline_fingerprint = _canonical_loader.policy_fingerprint(baseline_policy)

    out = {
        "component":  "build_allocation_memory_policy_simulation_lite",
        "version":    VERSION,
        "ts_utc":     ts,
        "paper_only": True,
        "source_files": {
            "obs_path":      str(OBS_PATH),
            "policy_path":   str(POLICY_PATH),
            "registry_path": str(_registry_loader.REGISTRY_PATH),
        },
        "policy_load_info": {
            "fallback_used":        fallback_used,
            "policy_load_reason":   policy_load_reason,
            "baseline_fingerprint": baseline_fingerprint,
            "external_candidates":  len(extra_candidates),
        },
        # AC-72: registry metadata
        "registry_info": {
            "registry_source":       registry_source,
            "registry_fallback_used": reg_fallback,
            "registry_reason":       reg_reason,
            "scenarios_total":       len(registry_scenarios),
            "variant_scenarios":     len(variant_scenarios),
            "scenario_ids":          _registry_loader.get_scenario_ids(registry_scenarios),
        },
        "simulation_config": {
            "simulation_method":     "gate_reclassification_v1",
            "builtin_candidates":    len(BUILTIN_CANDIDATES),
            "external_candidates":   len(extra_candidates),
            "baseline_policy_name":  baseline_policy.get("policy_name", "baseline_default"),
        },
        "summary":                     simulation["summary"],
        "scenario_evaluation_summary": simulation["scenario_evaluation_summary"],
        "allocation_advisory":         simulation["allocation_advisory"],
        "baseline_policy":             simulation["baseline_policy"],
        "baseline_metrics":            simulation["baseline_metrics"],
        "candidate_policies":          simulation["policy_comparisons"],
    }

    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        write_json(OUT_PATH, out)
        write_tsv(OUT_TSV, [c for c in simulation["policy_comparisons"] if "simulated_records_total" in c])
    except Exception as e:
        print(f"[WARN] Could not write output: {e}")

    print(json.dumps({k: v for k, v in out.items() if k != "candidate_policies"}, indent=2))
    ev  = simulation["scenario_evaluation_summary"]
    adv = simulation["allocation_advisory"]
    print(f"\n  allocation_advisory: status={adv['advisory_status']}"
          f" scenario={adv['advisory_scenario_id']}"
          f" confidence={adv['advisory_confidence']:.2f}")
    print(f"  advisory_reason: {adv['advisory_reason']}")
    print(f"  best_candidate: {ev.get('best_candidate_scenario_id')} "
          f"({ev.get('best_candidate_recommendation')})")
    print(f"  rejected: {ev.get('rejected_scenarios')}")
    print(f"\n  policy_comparisons ({len(simulation['policy_comparisons'])} candidates, ranked):")
    for c in sorted(simulation["policy_comparisons"],
                    key=lambda x: (x.get("risk_rank", 99), x.get("scenario_id", ""))):
        print(
            f"    [{c.get('risk_rank', '?')}] {c.get('scenario_id', c['policy_name']):<38}"
            f" risk={c.get('policy_risk_class', '?'):<16}"
            f" rec={c.get('policy_recommendation', '?')}"
        )


if __name__ == "__main__":
    main()
