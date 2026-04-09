"""
AC-85: Cross-Asset Regime Overlay + Allocation Bias

Builds a regime-aware overlay on top of the AC-84 allocation envelope.
Shows how regime information *would* colour the cross-asset allocation —
still purely observational. No live activation, no portfolio mutation, no
execution-impact.

Design principles:
  - overlay_non_binding=True and overlay_simulation_only=True always.
  - Fail-closed: invalid envelope → OVERLAY_REJECTED; missing regime data →
    bias defaults to neutral (0.0), reason "NO_REGIME_DATA".
  - Deterministic: same envelope + same market_regimes → same output.
  - Pure core function (build_regime_overlay) — no I/O, no side effects.
  - Simple, stable bias table (see _BIAS_TABLE); no optimizer.

Regime data shape (per market, from cb20_regime.json):
    {
      "market":        str,
      "trend_regime":  "BULL" | "BEAR" | "SIDEWAYS",
      "vol_regime":    "LOW"  | "HIGH" | "EXTREME",
      "gate":          "ALLOW" | "BLOCK",
      "size_mult":     float,
    }

Bias logic (deterministic table, no optimizer):
  gate == BLOCK                          → bias_scalar = 0.0
  trend/vol combination → bias_scalar from _BIAS_TABLE
  unknown combination                    → bias_scalar = 0.0 (neutral)
  missing regime data for a market       → bias_scalar = 0.0 (neutral)

  Asset-class bias = mean of market biases within that class.

Overlay status values:
  OVERLAY_ACTIVE    — envelope is ACTIVE and ≥1 market processed
  OVERLAY_BASELINE  — envelope is BASELINE_HOLD or EMPTY
  OVERLAY_REJECTED  — invalid/missing input

Usage (importable):
    from build_regime_overlay_lite import build_regime_overlay
    overlay = build_regime_overlay(allocation_envelope, market_regimes)

    # Full chain from specs:
    from build_regime_overlay_lite import build_overlay_from_specs
    result = build_overlay_from_specs(
        market_specs, total_equity_eur=10_000.0,
        market_regimes={"BTC-EUR": {...}, ...}
    )

Output fields:
    regime_overlay_status          — "OVERLAY_ACTIVE"|"OVERLAY_BASELINE"|"OVERLAY_REJECTED"
    regime_mode                    — "REGIME_AWARE"|"REGIME_BASELINE"|"REGIME_REJECTED"
    asset_class_regimes            — {asset_class: regime aggregate}
    market_regimes_summary         — {market: regime entry}
    allocation_bias_by_asset_class — {asset_class: {bias_scalar, bias_reason_code}}
    allocation_bias_by_market      — {market: {bias_scalar, bias_reason_code}}
    bias_reason                    — human-readable
    bias_reason_code               — machine-stable
    overlay_non_binding            — always True
    overlay_simulation_only        — always True
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

VERSION = "regime_overlay_v1"

# Overlay status values
OVERLAY_ACTIVE   = "OVERLAY_ACTIVE"
OVERLAY_BASELINE = "OVERLAY_BASELINE"
OVERLAY_REJECTED = "OVERLAY_REJECTED"

# Regime mode values
MODE_REGIME_AWARE     = "REGIME_AWARE"
MODE_REGIME_BASELINE  = "REGIME_BASELINE"
MODE_REGIME_REJECTED  = "REGIME_REJECTED"

# Bias reason codes (machine-stable)
BIAS_GATE_BLOCKED       = "GATE_BLOCKED"
BIAS_BULL_LOW_VOL       = "BULL_LOW_VOL"
BIAS_BULL_HIGH_VOL      = "BULL_HIGH_VOL"
BIAS_BULL_EXTREME_VOL   = "BULL_EXTREME_VOL"
BIAS_SIDEWAYS_LOW_VOL   = "SIDEWAYS_LOW_VOL"
BIAS_SIDEWAYS_HIGH_VOL  = "SIDEWAYS_HIGH_VOL"
BIAS_SIDEWAYS_EXTR_VOL  = "SIDEWAYS_EXTREME_VOL"
BIAS_BEAR_LOW_VOL       = "BEAR_LOW_VOL"
BIAS_BEAR_HIGH_VOL      = "BEAR_HIGH_VOL"
BIAS_BEAR_EXTREME_VOL   = "BEAR_EXTREME_VOL"
BIAS_NO_REGIME          = "NO_REGIME_DATA"
BIAS_NEUTRAL            = "NEUTRAL"

# Envelope status values (mirrored from AC-84 for routing)
_ENV_ACTIVE   = "ENVELOPE_ACTIVE"
_ENV_BASELINE = "ENVELOPE_BASELINE"
_ENV_REJECTED = "ENVELOPE_REJECTED"

# Inline market → asset class (mirrors AC-84; avoids @dataclass importlib issue)
_MARKET_TO_ASSET_CLASS: dict = {
    "BTC-EUR": "crypto",
    "ETH-EUR": "crypto",
    "SOL-EUR": "crypto",
    "XRP-EUR": "crypto",
    "ADA-EUR": "crypto",
    "BNB-EUR": "crypto",
}
_DEFAULT_ASSET_CLASS = "crypto"

_TOL = 1e-9

# ---------------------------------------------------------------------------
# Deterministic bias table: (trend_regime, vol_regime) → (bias_scalar, code)
# ---------------------------------------------------------------------------
# bias_scalar is a signed float in [-1.0, +1.0]:
#   +value → favour increasing allocation weight for this market
#   -value → favour decreasing allocation weight
#   0.0    → neutral / no change
#
# These are observational signals only — they never mutate allocation.

_BIAS_TABLE: dict = {
    ("BULL",     "LOW"):     (+0.10, BIAS_BULL_LOW_VOL),
    ("BULL",     "HIGH"):    (+0.05, BIAS_BULL_HIGH_VOL),
    ("BULL",     "EXTREME"): (+0.00, BIAS_BULL_EXTREME_VOL),
    ("SIDEWAYS", "LOW"):     (+0.00, BIAS_SIDEWAYS_LOW_VOL),
    ("SIDEWAYS", "HIGH"):    (-0.05, BIAS_SIDEWAYS_HIGH_VOL),
    ("SIDEWAYS", "EXTREME"): (-0.10, BIAS_SIDEWAYS_EXTR_VOL),
    ("BEAR",     "LOW"):     (-0.10, BIAS_BEAR_LOW_VOL),
    ("BEAR",     "HIGH"):    (-0.20, BIAS_BEAR_HIGH_VOL),
    ("BEAR",     "EXTREME"): (-0.30, BIAS_BEAR_EXTREME_VOL),
}


# ---------------------------------------------------------------------------
# Core overlay function (pure, no I/O)
# ---------------------------------------------------------------------------

def build_regime_overlay(
    allocation_envelope: object,
    market_regimes: object = None,
) -> dict:
    """
    Build a regime-aware overlay on top of an AC-84 allocation envelope.

    overlay_non_binding=True and overlay_simulation_only=True always.
    No execution, no cash movement, no state mutation.

    Args:
        allocation_envelope: dict returned by build_allocation_envelope() (AC-84).
        market_regimes:      optional dict {market: regime_dict} with keys
                             trend_regime, vol_regime, gate, size_mult.
                             Missing markets → neutral bias.

    Returns:
        Regime overlay dict.
    """
    if not isinstance(allocation_envelope, dict):
        return _rejected_overlay("allocation_envelope is not a dict")

    if "allocation_envelope_status" not in allocation_envelope:
        return _rejected_overlay("allocation_envelope missing allocation_envelope_status")

    env_status = allocation_envelope.get("allocation_envelope_status", "")

    # Baseline / rejected envelope → baseline overlay
    if env_status in (_ENV_BASELINE, _ENV_REJECTED, ""):
        return _baseline_overlay(env_status)

    if env_status != _ENV_ACTIVE:
        return _baseline_overlay(env_status)

    # Normalise market_regimes input
    if not isinstance(market_regimes, dict):
        market_regimes = {}

    # Collect market entries from envelope
    market_alloc_list = allocation_envelope.get("market_allocations") or []

    market_regimes_out:  dict = {}
    bias_by_market:      dict = {}
    asset_class_data:    dict = {}  # asset_class → {biases, trend_counts, vol_counts, sizes, gate_blocked}

    for ma in market_alloc_list:
        if not isinstance(ma, dict):
            continue
        market     = str(ma.get("market") or "UNKNOWN")
        asset_class = str(ma.get("asset_class") or _classify_market(market))

        regime = market_regimes.get(market)
        regime_available = isinstance(regime, dict)

        if regime_available:
            trend   = str(regime.get("trend_regime") or "").upper()
            vol     = str(regime.get("vol_regime")   or "").upper()
            gate    = str(regime.get("gate")          or "ALLOW").upper()
            size_m  = _safe_float(regime.get("size_mult"), 1.0)
        else:
            trend  = ""
            vol    = ""
            gate   = "ALLOW"
            size_m = 1.0

        # Compute market bias
        bias_scalar, bias_code = _compute_market_bias(trend, vol, gate, regime_available)

        market_regimes_out[market] = {
            "trend_regime":      trend if regime_available else None,
            "vol_regime":        vol   if regime_available else None,
            "gate":              gate  if regime_available else None,
            "size_mult":         size_m,
            "regime_available":  regime_available,
        }
        bias_by_market[market] = {
            "bias_scalar":      round(bias_scalar, 4),
            "bias_reason_code": bias_code,
        }

        # Accumulate into asset class
        if asset_class not in asset_class_data:
            asset_class_data[asset_class] = {
                "biases":        [],
                "trend_counts":  {},
                "vol_counts":    {},
                "sizes":         [],
                "gate_blocked":  0,
                "market_count":  0,
            }
        d = asset_class_data[asset_class]
        d["biases"].append(bias_scalar)
        d["market_count"] += 1
        if regime_available:
            d["trend_counts"][trend] = d["trend_counts"].get(trend, 0) + 1
            d["vol_counts"][vol]     = d["vol_counts"].get(vol, 0) + 1
            d["sizes"].append(size_m)
        if gate == "BLOCK":
            d["gate_blocked"] += 1

    # Build asset_class_regimes and allocation_bias_by_asset_class
    asset_class_regimes:   dict = {}
    bias_by_asset_class:   dict = {}

    for ac, d in sorted(asset_class_data.items()):
        avg_bias     = round(sum(d["biases"]) / max(len(d["biases"]), 1), 4)
        avg_size     = round(sum(d["sizes"]) / max(len(d["sizes"]), 1), 4) if d["sizes"] else 1.0
        trend_maj    = _majority(d["trend_counts"])
        vol_maj      = _majority(d["vol_counts"])

        asset_class_regimes[ac] = {
            "trend_majority":      trend_maj,
            "vol_majority":        vol_maj,
            "avg_size_mult":       avg_size,
            "gate_blocked_count":  d["gate_blocked"],
            "market_count":        d["market_count"],
        }

        ac_bias_code = _asset_class_bias_code(trend_maj, vol_maj, d["gate_blocked"], d["market_count"])
        bias_by_asset_class[ac] = {
            "bias_scalar":      avg_bias,
            "bias_reason_code": ac_bias_code,
        }

    # Overall bias summary
    all_biases = [b["bias_scalar"] for b in bias_by_market.values()]
    overall_bias = round(sum(all_biases) / max(len(all_biases), 1), 4) if all_biases else 0.0
    bias_reason, bias_reason_code = _overall_bias_summary(overall_bias, len(market_regimes))

    return {
        "regime_overlay_status":          OVERLAY_ACTIVE,
        "regime_mode":                    MODE_REGIME_AWARE,
        "asset_class_regimes":            asset_class_regimes,
        "market_regimes_summary":         market_regimes_out,
        "allocation_bias_by_asset_class": bias_by_asset_class,
        "allocation_bias_by_market":      bias_by_market,
        "bias_reason":                    bias_reason,
        "bias_reason_code":               bias_reason_code,
        "overlay_non_binding":            True,
        "overlay_simulation_only":        True,
    }


# ---------------------------------------------------------------------------
# Bias computation helpers
# ---------------------------------------------------------------------------

def _compute_market_bias(
    trend: str, vol: str, gate: str, regime_available: bool
) -> tuple:
    """Return (bias_scalar, bias_reason_code) for a single market."""
    if not regime_available:
        return (0.0, BIAS_NO_REGIME)
    if gate == "BLOCK":
        return (0.0, BIAS_GATE_BLOCKED)
    entry = _BIAS_TABLE.get((trend, vol))
    if entry is not None:
        return entry
    return (0.0, BIAS_NEUTRAL)


def _majority(counts: dict) -> str:
    """Return the key with the highest count, or empty string if no data."""
    if not counts:
        return ""
    return max(counts, key=lambda k: counts[k])


def _asset_class_bias_code(
    trend_maj: str, vol_maj: str, gate_blocked: int, market_count: int
) -> str:
    """Derive a machine-stable bias reason code for an asset class."""
    if market_count == 0:
        return BIAS_NO_REGIME
    if gate_blocked == market_count:
        return BIAS_GATE_BLOCKED
    entry = _BIAS_TABLE.get((trend_maj, vol_maj))
    if entry is not None:
        return entry[1]
    if not trend_maj:
        return BIAS_NO_REGIME
    return BIAS_NEUTRAL


def _overall_bias_summary(overall_bias: float, regime_count: int) -> tuple:
    """Return (bias_reason, bias_reason_code) for the top-level summary."""
    if regime_count == 0:
        return ("no regime data provided — neutral bias applied", BIAS_NO_REGIME)
    if overall_bias > _TOL:
        return (
            f"positive regime bias ({overall_bias:+.3f}) — allocation would favour active markets",
            "POSITIVE_BIAS",
        )
    if overall_bias < -_TOL:
        return (
            f"negative regime bias ({overall_bias:+.3f}) — allocation would favour caution",
            "NEGATIVE_BIAS",
        )
    return ("neutral regime bias — no directional signal", "NEUTRAL_BIAS")


# ---------------------------------------------------------------------------
# Fail-closed helpers
# ---------------------------------------------------------------------------

def _rejected_overlay(reason: str) -> dict:
    return {
        "regime_overlay_status":          OVERLAY_REJECTED,
        "regime_mode":                    MODE_REGIME_REJECTED,
        "asset_class_regimes":            {},
        "market_regimes_summary":         {},
        "allocation_bias_by_asset_class": {},
        "allocation_bias_by_market":      {},
        "bias_reason":                    reason,
        "bias_reason_code":               "OVERLAY_INVALID_INPUT",
        "overlay_non_binding":            True,
        "overlay_simulation_only":        True,
    }


def _baseline_overlay(env_status: str) -> dict:
    return {
        "regime_overlay_status":          OVERLAY_BASELINE,
        "regime_mode":                    MODE_REGIME_BASELINE,
        "asset_class_regimes":            {},
        "market_regimes_summary":         {},
        "allocation_bias_by_asset_class": {},
        "allocation_bias_by_market":      {},
        "bias_reason":                    f"envelope is {env_status} — regime overlay not applied",
        "bias_reason_code":               "OVERLAY_BASELINE_HOLD",
        "overlay_non_binding":            True,
        "overlay_simulation_only":        True,
    }


def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _classify_market(market: str) -> str:
    return _MARKET_TO_ASSET_CLASS.get(market, _DEFAULT_ASSET_CLASS)


# ---------------------------------------------------------------------------
# Module loader helper
# ---------------------------------------------------------------------------

def _load_envelope_module():
    path = Path(__file__).parent / "build_allocation_envelope_lite.py"
    spec = importlib.util.spec_from_file_location("_envelope", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Convenience: full chain AC-81 + AC-82 + AC-84 + AC-85
# ---------------------------------------------------------------------------

def build_overlay_from_specs(
    market_specs: object,
    total_equity_eur: float,
    market_regimes: object = None,
    market_capital_fractions: object = None,
) -> dict:
    """
    Full chain: market_specs → splits (AC-81) → capital allocation (AC-82)
                             → allocation envelope (AC-84) → regime overlay (AC-85).

    Returns dict with keys:
        splits_result, capital_allocation, allocation_envelope, regime_overlay.
    overlay_non_binding=True and overlay_simulation_only=True always.
    """
    _env_mod = _load_envelope_module()
    pipeline = _env_mod.build_envelope_from_specs(
        market_specs, total_equity_eur, market_capital_fractions
    )
    overlay = build_regime_overlay(
        pipeline["allocation_envelope"],
        market_regimes or {},
    )
    return {
        "splits_result":       pipeline["splits_result"],
        "capital_allocation":  pipeline["capital_allocation"],
        "allocation_envelope": pipeline["allocation_envelope"],
        "regime_overlay":      overlay,
    }


# ---------------------------------------------------------------------------
# Optional main (CLI demo)
# ---------------------------------------------------------------------------

def main() -> None:
    import json

    specs = [
        {
            "market": "BTC-EUR",
            "strategies": [
                {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION", "weight_fraction": 0.6},
                {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT",        "weight_fraction": 0.4},
            ],
        },
        {
            "market": "ETH-EUR",
            "strategies": [
                {"strategy_id": "EDGE3", "strategy_family": "MEAN_REVERSION"},
                {"strategy_id": "EDGE4", "strategy_family": "BREAKOUT"},
            ],
        },
        {
            "market": "SOL-EUR",
            "strategies": [],
        },
    ]
    regimes = {
        "BTC-EUR": {"trend_regime": "SIDEWAYS", "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        "ETH-EUR": {"trend_regime": "BEAR",     "vol_regime": "LOW",  "gate": "ALLOW", "size_mult": 1.0},
        # SOL-EUR intentionally missing → NO_REGIME_DATA
    }
    result = build_overlay_from_specs(specs, total_equity_eur=10_000.0, market_regimes=regimes)
    ov = result["regime_overlay"]
    print(json.dumps({
        "regime_overlay_status":          ov["regime_overlay_status"],
        "regime_mode":                    ov["regime_mode"],
        "bias_reason":                    ov["bias_reason"],
        "bias_reason_code":               ov["bias_reason_code"],
        "overlay_non_binding":            ov["overlay_non_binding"],
        "overlay_simulation_only":        ov["overlay_simulation_only"],
        "asset_class_regimes":            ov["asset_class_regimes"],
        "allocation_bias_by_asset_class": ov["allocation_bias_by_asset_class"],
        "allocation_bias_by_market":      ov["allocation_bias_by_market"],
    }, indent=2))


if __name__ == "__main__":
    main()
