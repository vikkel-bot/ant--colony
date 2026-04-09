"""
AC-67 / AC-68: Canonical policy loader for allocation_memory_policy.json

This is the SINGLE CANONICAL LOADER for the allocation memory policy surface.
All modules (AC-63, AC-64, AC-66, AC-67) must load policy exclusively through
this file. No other embedded DEFAULT_POLICY or load_policy() should exist.

Schema (grouped, canonical as of AC-68):
  groups.memory_gate          — AC-64 gate thresholds and blend weights
  groups.memory_rolling_window — AC-63 window size and cooldown
  groups.review_thresholds    — AC-66 review trigger thresholds

Fail-closed: any error (missing file, bad JSON, invalid structure) returns
DEFAULT_POLICY (built-in defaults) unchanged — never raises.

Usage (from another ant_colony module via importlib):
    import importlib.util, pathlib
    _spec = importlib.util.spec_from_file_location(
        "_policy_loader",
        pathlib.Path(__file__).parent / "policy" / "load_allocation_memory_policy_lite.py"
    )
    _loader = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_loader)
    policy, fallback_used, reason = _loader.load_policy()
    memory_gate   = policy["groups"]["memory_gate"]
    memory_window = policy["groups"]["memory_rolling_window"]
    review_cfg    = policy["groups"]["review_thresholds"]

Returns:
    (policy_dict, fallback_used: bool, load_reason: str)
"""
import copy
import json
from pathlib import Path

POLICY_PATH = Path(__file__).parent / "allocation_memory_policy.json"

# ---------------------------------------------------------------------------
# Built-in defaults — mirror AC-63/64/65/66 hardcoded constants exactly.
# Used as fallback when policy file is absent or invalid (fail-closed).
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
            "bias_caution_signal_threshold":  -0.50,
            "bias_caution_harmful_ratio":      0.60,
            "bias_negative_signal_threshold": -0.20,
            "bias_positive_signal_threshold":  0.20,
        },
        "memory_rolling_window": {
            "window_size":              10,
            "full_memory_at":           8,
            "cooldown_cycles_default":  3,
            "memory_min_confidence":    0.40,
            "sparse_window_threshold":  3,
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
            "review_memory_applied_rate_low_watch":  0.10,
            "review_memory_applied_rate_high_watch": 0.80,
        },
    },
}

# Required groups that must be present for a policy file to be accepted
_REQUIRED_GROUPS = ("memory_gate", "memory_rolling_window", "review_thresholds")

# Required keys per group
_REQUIRED_KEYS: dict = {
    "memory_gate": (
        "memory_confidence_min_negative",
        "memory_confidence_min_positive",
        "negative_blend_weight",
        "positive_blend_weight",
        "modifier_band_min",
        "modifier_band_max",
    ),
    "memory_rolling_window": (
        "window_size",
        "cooldown_cycles_default",
    ),
    "review_thresholds": (
        "review_min_records",
    ),
}


def _validate_structure(data: dict) -> tuple:
    """
    Validate that a loaded policy dict has the required groups and keys.
    Returns (ok: bool, reason: str).
    """
    if not isinstance(data, dict):
        return False, "NOT_A_DICT"
    if "groups" not in data:
        return False, "MISSING_KEY:groups"
    groups = data["groups"]
    for group in _REQUIRED_GROUPS:
        if group not in groups:
            return False, f"MISSING_GROUP:{group}"
        for key in _REQUIRED_KEYS.get(group, ()):
            if key not in groups[group]:
                return False, f"MISSING_KEY:{group}.{key}"
    return True, "OK"


def load_policy(path: Path = POLICY_PATH) -> tuple:
    """
    Load the allocation memory policy from *path*.

    Behaviour:
    - File missing                  → fallback to DEFAULT_POLICY (fail-closed)
    - File present but bad JSON     → fallback to DEFAULT_POLICY (fail-closed)
    - File present, invalid struct  → fallback to DEFAULT_POLICY (fail-closed)
    - File present, valid           → deep-merge with defaults (missing keys filled)

    Returns:
        (policy_dict, fallback_used: bool, load_reason: str)
    """
    try:
        path = Path(path)
    except Exception:
        return copy.deepcopy(DEFAULT_POLICY), True, "FALLBACK_DEFAULT|INVALID_PATH"

    if not path.exists():
        return copy.deepcopy(DEFAULT_POLICY), True, f"FALLBACK_DEFAULT|FILE_NOT_FOUND:{path}"

    try:
        raw = path.read_text(encoding="utf-8-sig")
        data = json.loads(raw)
    except Exception as exc:
        return copy.deepcopy(DEFAULT_POLICY), True, f"FALLBACK_DEFAULT|PARSE_ERROR:{exc}"

    ok, reason = _validate_structure(data)
    if not ok:
        return copy.deepcopy(DEFAULT_POLICY), True, f"FALLBACK_DEFAULT|INVALID_STRUCTURE:{reason}"

    # Deep-merge: start from defaults, overlay loaded values group by group.
    # Unknown keys in the file are ignored; missing keys filled from defaults.
    merged = copy.deepcopy(DEFAULT_POLICY)
    merged["policy_name"]    = data.get("policy_name",    merged["policy_name"])
    merged["policy_version"] = data.get("policy_version", merged["policy_version"])
    merged["description"]    = data.get("description",    merged["description"])

    for group, defaults in merged["groups"].items():
        loaded_group = data.get("groups", {}).get(group, {})
        for k, v in loaded_group.items():
            if k.startswith("_"):
                continue  # skip JSON comments
            if k in defaults:
                merged["groups"][group][k] = v

    return merged, False, "LOADED_FROM_FILE"


def get_flat_policy(path: Path = POLICY_PATH) -> dict:
    """
    Convenience: load policy and flatten all group keys into a single dict.
    Returns flat dict — always succeeds (falls back to defaults on any error).
    """
    policy, _, _ = load_policy(path)
    flat: dict = {}
    for group_vals in policy.get("groups", {}).values():
        if isinstance(group_vals, dict):
            for k, v in group_vals.items():
                if not k.startswith("_"):
                    flat[k] = v
    return flat
