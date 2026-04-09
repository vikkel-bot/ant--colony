"""
AC-72: Scenario Registry Loader

Loads the named policy scenario registry from
  ant_colony/policy/allocation_memory_scenario_registry.json

Design principles:
  - Registry stores ONLY overlay diffs — no full policy values.
  - Canonical baseline values always come from the canonical loader
    (load_allocation_memory_policy_lite.py). No second source of truth.
  - Fail-closed: missing/invalid registry file → default registry
    (baseline scenario only). Never raises.
  - apply_overlay() is a pure function; no mutation of inputs.

Registry format (one entry per scenario):
  scenario_id   — stable string identifier (do NOT rename without schema bump)
  scenario_name — human-readable name (may change)
  description   — free-text description
  scenario_type — "baseline" | "variant"
  overlay       — {group: {key: value}} diff to apply on top of baseline

Scenario IDs are the primary key. Policy values live exclusively in the
canonical policy file (allocation_memory_policy.json) and loader defaults.

Usage:
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location(
        "_registry", Path(__file__).parent / "load_scenario_registry_lite.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    scenarios, fallback_used, reason = mod.load_registry()
"""
import copy
import json
from pathlib import Path

REGISTRY_PATH = Path(__file__).parent / "allocation_memory_scenario_registry.json"

VERSION = "scenario_registry_v1"

# Minimal fail-closed default — baseline scenario only.
# Returned when the registry file is missing or invalid.
_DEFAULT_REGISTRY: list = [
    {
        "scenario_id":   "baseline",
        "scenario_name": "baseline_default",
        "description":   "Canonical baseline policy — no overlay applied.",
        "scenario_type": "baseline",
        "overlay":       {},
    }
]

# Required keys in every scenario entry.
_REQUIRED_SCENARIO_KEYS = ("scenario_id", "scenario_type", "overlay")


# ---------------------------------------------------------------------------
# Registry load + validation
# ---------------------------------------------------------------------------

def _validate_scenario(s: dict) -> tuple:
    """Validate one scenario entry. Returns (ok: bool, reason: str)."""
    for k in _REQUIRED_SCENARIO_KEYS:
        if k not in s:
            return False, f"MISSING_KEY:{k}"
    if not isinstance(s["scenario_id"], str) or not s["scenario_id"].strip():
        return False, "INVALID_SCENARIO_ID"
    if s["scenario_type"] not in ("baseline", "variant"):
        return False, f"UNKNOWN_SCENARIO_TYPE:{s['scenario_type']}"
    if not isinstance(s["overlay"], dict):
        return False, "OVERLAY_NOT_DICT"
    return True, "OK"


def load_registry(path: Path = REGISTRY_PATH) -> tuple:
    """
    Load the scenario registry from *path*.

    Returns:
        (scenarios: list[dict], fallback_used: bool, reason: str)
        fallback_used=True only when the file could not be loaded at all.
        Per-scenario validation errors silently drop the invalid entry.
    """
    try:
        path = Path(path)
    except Exception:
        return copy.deepcopy(_DEFAULT_REGISTRY), True, "FALLBACK_DEFAULT|INVALID_PATH"

    if not path.exists():
        return copy.deepcopy(_DEFAULT_REGISTRY), True, f"FALLBACK_DEFAULT|FILE_NOT_FOUND:{path}"

    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return copy.deepcopy(_DEFAULT_REGISTRY), True, f"FALLBACK_DEFAULT|PARSE_ERROR:{exc}"

    if not isinstance(data, dict) or "scenarios" not in data:
        return copy.deepcopy(_DEFAULT_REGISTRY), True, "FALLBACK_DEFAULT|MISSING_SCENARIOS_KEY"

    raw_scenarios = data.get("scenarios", [])
    if not isinstance(raw_scenarios, list):
        return copy.deepcopy(_DEFAULT_REGISTRY), True, "FALLBACK_DEFAULT|SCENARIOS_NOT_LIST"

    # Per-scenario validation — drop invalid entries, keep valid ones
    valid: list = []
    dropped: list = []
    for s in raw_scenarios:
        if not isinstance(s, dict):
            dropped.append("NON_DICT_ENTRY")
            continue
        ok, reason = _validate_scenario(s)
        if ok:
            valid.append(s)
        else:
            dropped.append(f"{s.get('scenario_id', '?')}:{reason}")

    if not valid:
        return copy.deepcopy(_DEFAULT_REGISTRY), True, "FALLBACK_DEFAULT|NO_VALID_SCENARIOS"

    reason = f"LOADED:{len(valid)}_SCENARIOS"
    if dropped:
        reason += f"|DROPPED:{len(dropped)}"

    return valid, False, reason


# ---------------------------------------------------------------------------
# Overlay application
# ---------------------------------------------------------------------------

def apply_overlay(baseline_policy: dict, overlay: dict) -> dict:
    """
    Apply a scenario overlay to the baseline policy (deep copy, no mutation).

    overlay format: {group_name: {param: new_value}}
    Only keys that already exist in baseline groups are updated.
    Unknown group names and unknown keys within groups are silently ignored
    (prevents registry from injecting unvalidated keys into the policy).
    """
    result = copy.deepcopy(baseline_policy)
    for group, vals in (overlay or {}).items():
        if isinstance(vals, dict) and group in result.get("groups", {}):
            for k, v in vals.items():
                if k in result["groups"][group]:
                    result["groups"][group][k] = v
    return result


# ---------------------------------------------------------------------------
# Registry inspection helpers
# ---------------------------------------------------------------------------

def get_scenario_ids(scenarios: list) -> list:
    """Return sorted list of scenario_ids from a registry list."""
    return sorted(
        s.get("scenario_id", "") for s in scenarios
        if isinstance(s, dict) and s.get("scenario_id")
    )


def get_baseline_scenario(scenarios: list) -> dict:
    """Return the first baseline scenario, or a minimal default if none found."""
    for s in scenarios:
        if isinstance(s, dict) and s.get("scenario_type") == "baseline":
            return s
    return copy.deepcopy(_DEFAULT_REGISTRY[0])


def get_variant_scenarios(scenarios: list) -> list:
    """Return only variant scenarios (scenario_type == 'variant')."""
    return [s for s in scenarios if isinstance(s, dict) and s.get("scenario_type") == "variant"]
