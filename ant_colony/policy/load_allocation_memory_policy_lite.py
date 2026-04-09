"""
AC-67 / AC-68 / AC-70: Canonical policy loader for allocation_memory_policy.json

This is the SINGLE CANONICAL LOADER for the allocation memory policy surface.
All modules (AC-63, AC-64, AC-66, AC-67) must load policy exclusively through
this file. No other embedded DEFAULT_POLICY or load_policy() should exist.

Schema (grouped, canonical as of AC-68):
  groups.memory_gate          — AC-64 gate thresholds and blend weights
  groups.memory_rolling_window — AC-63 window size and cooldown
  groups.review_thresholds    — AC-66 review trigger thresholds

Fail-closed: any error (missing file, bad JSON, invalid structure) returns
DEFAULT_POLICY (built-in defaults) unchanged — never raises.

AC-70 governance layer (applied after merge, before return):
  1. Per-key type validation and coercion (float/int/str).
     Wrong type → per-key fallback to DEFAULT_POLICY value; correction noted.
  2. Per-key numeric bounds check.
     Out-of-range → per-key fallback to DEFAULT_POLICY value; correction noted.
  3. Per-group cross-constraint check.
     Constraint violated → full group falls back to DEFAULT_POLICY; noted.
  fallback_used=True only when the entire policy falls back (file not loadable).
  Per-key and per-group corrections use fallback_used=False with enriched reason.

Load reason format:
  "LOADED_FROM_FILE"                               — clean load, no corrections
  "LOADED_WITH_CORRECTIONS:k1,k2,..."              — per-key corrections applied
  "LOADED_WITH_SECTION_FALLBACK:grp[why];..."      — group-level fallback applied
  "FALLBACK_DEFAULT|<reason>"                      — whole-policy fallback

Drift utility:
  check_policy_drift(path) — compares JSON key set against DEFAULT_POLICY;
  returns a dict useful for governance tests and CI checks.

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
import hashlib
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
    "effective_from": None,
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
            "review_min_records":                    5,
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

# ---------------------------------------------------------------------------
# AC-70 governance: schema contract
# ---------------------------------------------------------------------------

# Expected Python type for each policy key.
# float keys accept int or float (coerced); int keys require integer values;
# str keys require str.
KEY_TYPES: dict = {
    # memory_gate
    "memory_confidence_min_negative":  float,
    "memory_confidence_min_positive":  float,
    "negative_blend_weight":           float,
    "positive_blend_weight":           float,
    "negative_correction_cap":         float,
    "positive_correction_cap":         float,
    "modifier_band_min":               float,
    "modifier_band_max":               float,
    "recent_harmful_lookback":         int,
    "recent_harmful_block_threshold":  int,
    "conflict_policy_mode":            str,
    "bias_caution_signal_threshold":   float,
    "bias_caution_harmful_ratio":      float,
    "bias_negative_signal_threshold":  float,
    "bias_positive_signal_threshold":  float,
    # memory_rolling_window
    "window_size":             int,
    "full_memory_at":          int,
    "cooldown_cycles_default": int,
    "memory_min_confidence":   float,
    "sparse_window_threshold": int,
    # review_thresholds
    "review_min_records":                    int,
    "review_positive_applied_rate_warn":    float,
    "review_positive_applied_rate_watch":   float,
    "review_negative_applied_rate_warn":    float,
    "review_cooldown_rate_warn":            float,
    "review_conflict_block_rate_warn":      float,
    "review_low_conf_blocked_rate_warn":    float,
    "review_avg_delta_watch":               float,
    "review_memory_applied_rate_low_watch":  float,
    "review_memory_applied_rate_high_watch": float,
}

# Inclusive numeric bounds for each key. Values outside → per-key fallback.
KEY_BOUNDS: dict = {
    # Probabilities / rates: [0.0, 1.0]
    "memory_confidence_min_negative":  (0.0, 1.0),
    "memory_confidence_min_positive":  (0.0, 1.0),
    "negative_blend_weight":           (0.0, 1.0),
    "positive_blend_weight":           (0.0, 1.0),
    "negative_correction_cap":         (0.0, 1.0),
    "positive_correction_cap":         (0.0, 1.0),
    "bias_caution_harmful_ratio":      (0.0, 1.0),
    "memory_min_confidence":           (0.0, 1.0),
    "review_positive_applied_rate_warn":    (0.0, 1.0),
    "review_positive_applied_rate_watch":   (0.0, 1.0),
    "review_negative_applied_rate_warn":    (0.0, 1.0),
    "review_cooldown_rate_warn":            (0.0, 1.0),
    "review_conflict_block_rate_warn":      (0.0, 1.0),
    "review_low_conf_blocked_rate_warn":    (0.0, 1.0),
    "review_avg_delta_watch":               (0.0, 1.0),
    "review_memory_applied_rate_low_watch":  (0.0, 1.0),
    "review_memory_applied_rate_high_watch": (0.0, 1.0),
    # Modifier band
    "modifier_band_min": (0.5, 1.5),
    "modifier_band_max": (0.5, 1.5),
    # Signal thresholds
    "bias_caution_signal_threshold":  (-1.0, 0.0),
    "bias_negative_signal_threshold": (-1.0, 0.0),
    "bias_positive_signal_threshold": ( 0.0, 1.0),
    # Positive integers
    "window_size":             (1, 1000),
    "full_memory_at":          (1, 1000),
    "cooldown_cycles_default": (0, 100),
    "sparse_window_threshold": (1, 100),
    "recent_harmful_lookback": (1, 50),
    "recent_harmful_block_threshold": (1, 20),
    "review_min_records":      (1, 10000),
}

# Cross-constraints within each group.
# Format: (key_a, op, key_b)  where op is "<" or "<=".
# Violation of any constraint → full group falls back to DEFAULT_POLICY.
GROUP_CONSTRAINTS: dict = {
    "memory_gate": [
        ("modifier_band_min",               "<",  "modifier_band_max"),
        ("memory_confidence_min_negative",  "<=", "memory_confidence_min_positive"),
        ("bias_caution_signal_threshold",   "<=", "bias_negative_signal_threshold"),
        ("bias_negative_signal_threshold",  "<",  "bias_positive_signal_threshold"),
    ],
    "memory_rolling_window": [
        ("full_memory_at",        "<=", "window_size"),
        ("sparse_window_threshold", "<=", "full_memory_at"),
    ],
    "review_thresholds": [
        ("review_positive_applied_rate_watch", "<=", "review_positive_applied_rate_warn"),
        ("review_memory_applied_rate_low_watch", "<", "review_memory_applied_rate_high_watch"),
    ],
}

# ---------------------------------------------------------------------------
# Required groups / keys for structure validation (unchanged from AC-68)
# ---------------------------------------------------------------------------

_REQUIRED_GROUPS = ("memory_gate", "memory_rolling_window", "review_thresholds")

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


# ---------------------------------------------------------------------------
# AC-70 governance helpers
# ---------------------------------------------------------------------------

def _normalize_key_value(key: str, value, default_value):
    """
    Validate and coerce one policy key value.
    Returns (coerced_value, error_tag_or_None).
    error_tag is non-None when the value was replaced with default_value.
    """
    expected = KEY_TYPES.get(key)
    if expected is None:
        return value, None  # unknown key — pass through unchanged

    # Reject booleans masquerading as int/float (JSON true/false)
    if isinstance(value, bool):
        return default_value, f"TYPE:{key}=bool"

    # Type coercion
    if expected is float:
        if isinstance(value, (int, float)):
            value = float(value)
        else:
            return default_value, f"TYPE:{key}={type(value).__name__}"
        # NaN / inf guard
        if value != value or abs(value) == float("inf"):
            return default_value, f"INVALID_FLOAT:{key}"
    elif expected is int:
        if isinstance(value, float) and value == int(value):
            value = int(value)
        elif not isinstance(value, int):
            return default_value, f"TYPE:{key}={type(value).__name__}"
    elif expected is str:
        if not isinstance(value, str):
            return default_value, f"TYPE:{key}={type(value).__name__}"
        if not value.strip():
            return default_value, f"EMPTY_STR:{key}"

    # Bounds check (numeric only)
    bounds = KEY_BOUNDS.get(key)
    if bounds is not None and isinstance(value, (int, float)):
        lo, hi = bounds
        if not (lo <= value <= hi):
            return default_value, f"BOUNDS:{key}={value} [{lo},{hi}]"

    return value, None


def _check_group_constraints(group_name: str, group_vals: dict) -> list:
    """
    Check cross-key constraints within a group.
    Returns list of violation strings (empty = all OK).
    """
    violations = []
    for key_a, op, key_b in GROUP_CONSTRAINTS.get(group_name, []):
        if key_a not in group_vals or key_b not in group_vals:
            continue
        a = group_vals[key_a]
        b = group_vals[key_b]
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            continue
        if op == "<" and not (a < b):
            violations.append(f"{key_a}={a}!<{key_b}={b}")
        elif op == "<=" and not (a <= b):
            violations.append(f"{key_a}={a}!<={key_b}={b}")
    return violations


# ---------------------------------------------------------------------------
# Structure validation
# ---------------------------------------------------------------------------

def _validate_structure(data: dict) -> tuple:
    """
    Validate that a loaded policy dict has required groups and keys.
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


# ---------------------------------------------------------------------------
# Core load function
# ---------------------------------------------------------------------------

def load_policy(path: Path = POLICY_PATH) -> tuple:
    """
    Load the allocation memory policy from *path*.

    Behaviour:
    - File missing / unreadable       → FALLBACK_DEFAULT (fail-closed)
    - Bad JSON                        → FALLBACK_DEFAULT (fail-closed)
    - Invalid structure               → FALLBACK_DEFAULT (fail-closed)
    - Valid file, clean values        → LOADED_FROM_FILE
    - Valid file, type/bounds errors  → LOADED_WITH_CORRECTIONS (per-key fix)
    - Valid file, cross-constraints   → LOADED_WITH_SECTION_FALLBACK (group fix)

    Returns:
        (policy_dict, fallback_used: bool, load_reason: str)
        fallback_used=True only when the entire policy falls back (unreadable file).
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

    ok, struct_reason = _validate_structure(data)
    if not ok:
        return copy.deepcopy(DEFAULT_POLICY), True, f"FALLBACK_DEFAULT|INVALID_STRUCTURE:{struct_reason}"

    # Deep-merge: start from defaults, overlay loaded values.
    # Unknown keys in the file are ignored; missing keys filled from defaults.
    merged = copy.deepcopy(DEFAULT_POLICY)
    merged["policy_name"]    = data.get("policy_name",    merged["policy_name"])
    merged["policy_version"] = data.get("policy_version", merged["policy_version"])
    merged["description"]    = data.get("description",    merged["description"])
    merged["effective_from"] = data.get("effective_from", merged.get("effective_from"))

    for group, defaults in merged["groups"].items():
        loaded_group = data.get("groups", {}).get(group, {})
        for k, v in loaded_group.items():
            if k.startswith("_"):
                continue  # skip JSON comments
            if k in defaults:
                merged["groups"][group][k] = v

    # --- AC-70 governance: validate and normalize merged values ---
    corrections: list = []
    section_fallbacks: list = []

    for group_name, group_vals in merged["groups"].items():
        defaults = DEFAULT_POLICY["groups"][group_name]

        # Per-key type / bounds normalization
        for key in list(group_vals.keys()):
            val, err = _normalize_key_value(key, group_vals[key], defaults.get(key))
            if err:
                corrections.append(err)
                group_vals[key] = defaults.get(key, group_vals[key])
            else:
                group_vals[key] = val

        # Cross-constraint check (after per-key normalization)
        violations = _check_group_constraints(group_name, group_vals)
        if violations:
            # Full group fallback — inconsistent state is unsafe
            merged["groups"][group_name] = copy.deepcopy(defaults)
            section_fallbacks.append(f"{group_name}:[{';'.join(violations)}]")

    if section_fallbacks:
        tag = "LOADED_WITH_SECTION_FALLBACK:" + ",".join(section_fallbacks)
        if corrections:
            tag += "|CORRECTIONS:" + ",".join(corrections)
        return merged, False, tag

    if corrections:
        return merged, False, "LOADED_WITH_CORRECTIONS:" + ",".join(corrections)

    return merged, False, "LOADED_FROM_FILE"


# ---------------------------------------------------------------------------
# Drift detection — governance utility
# ---------------------------------------------------------------------------

def check_policy_drift(path: Path = POLICY_PATH) -> dict:
    """
    Compare the key set of the policy JSON file against DEFAULT_POLICY.

    Returns a dict:
      drift_detected              : bool
      keys_in_default_not_in_file : list — present in DEFAULT_POLICY but absent in file
      keys_in_file_not_in_default : list — present in file but not in DEFAULT_POLICY
      file_error                  : str or None — if file could not be read/parsed

    Keys from JSON comment fields (_*) are excluded.
    This is a governance/CI utility; it does not affect load_policy() behaviour.
    """
    if not Path(path).exists():
        return {
            "drift_detected": True,
            "file_error": f"FILE_NOT_FOUND:{path}",
            "keys_in_default_not_in_file": [],
            "keys_in_file_not_in_default": [],
        }
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {
            "drift_detected": True,
            "file_error": f"PARSE_ERROR:{exc}",
            "keys_in_default_not_in_file": [],
            "keys_in_file_not_in_default": [],
        }

    default_keys: set = set()
    for gv in DEFAULT_POLICY["groups"].values():
        default_keys.update(k for k in gv if not k.startswith("_"))

    file_keys: set = set()
    for gv in data.get("groups", {}).values():
        if isinstance(gv, dict):
            file_keys.update(k for k in gv if not k.startswith("_"))

    missing = sorted(default_keys - file_keys)
    extra   = sorted(file_keys   - default_keys)

    return {
        "drift_detected": bool(missing or extra),
        "file_error": None,
        "keys_in_default_not_in_file": missing,
        "keys_in_file_not_in_default": extra,
    }


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# AC-71 observability helpers
# ---------------------------------------------------------------------------

def policy_fingerprint(policy: dict) -> str:
    """
    Stable 16-char SHA-256 fingerprint of the policy *groups* values.

    Only the `groups` dict is hashed — top-level metadata (policy_name,
    policy_version, effective_from) does not affect the fingerprint.
    This means bumping the version string without changing values produces
    the same fingerprint; changing any actual value produces a new one.

    Returns "FINGERPRINT_ERROR" on any exception (safe fallback).
    """
    try:
        canonical = json.dumps(
            policy.get("groups", {}),
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]
    except Exception:
        return "FINGERPRINT_ERROR"


def get_policy_audit(
    policy: dict,
    fallback_used: bool,
    load_reason: str,
    groups_consumed: list = None,
) -> dict:
    """
    Build a standardized policy audit dict for inclusion in runtime outputs.

    Intended usage: call inside each module's _load_acXX_policy() helper,
    then include the returned dict as "policy_audit" in the module's output JSON.

    Fields:
      policy_name      — from policy file (or "baseline_default" if in-code default)
      policy_version   — from policy file metadata
      effective_from   — from policy file metadata (None when using in-code defaults)
      load_reason      — canonical loader load_reason string (AC-70)
      fallback_used    — True only when entire policy is the in-code fallback
      fingerprint      — 16-char SHA-256 of groups values (AC-71)
      groups_consumed  — which policy groups this module reads (sorted list)
    """
    return {
        "policy_name":     policy.get("policy_name",    "UNKNOWN"),
        "policy_version":  policy.get("policy_version", "UNKNOWN"),
        "effective_from":  policy.get("effective_from", None),
        "load_reason":     load_reason,
        "fallback_used":   fallback_used,
        "fingerprint":     policy_fingerprint(policy),
        "groups_consumed": sorted(groups_consumed or []),
    }


def get_flat_policy(path: Path = POLICY_PATH) -> dict:
    """
    Load policy and flatten all group keys into a single dict.
    Always succeeds (falls back to defaults on any error).
    """
    policy, _, _ = load_policy(path)
    flat: dict = {}
    for group_vals in policy.get("groups", {}).values():
        if isinstance(group_vals, dict):
            for k, v in group_vals.items():
                if not k.startswith("_"):
                    flat[k] = v
    return flat
