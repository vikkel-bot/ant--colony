"""
AC-141: Cross-Market Allocation Guard (Research-Only)

Adds a safety guard layer above the AC-140 queen candidate portfolio summary
to detect overconcentration per strategy, per regime, and per market weight.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No optimizer, no rebalance engine -- safety gating and observability only.

Input:  AC-140 portfolio summary dict (from build_queen_candidate_portfolio_summary_lite)
Output: guard result dict with guard_status, guard_reasons, exposure_summary

Guard checks (fixed thresholds, configurable for testing):
  1. Strategy concentration: any strategy used by > MAX_STRATEGY_CONCENTRATION
     fraction of active markets -> GUARD_FAIL
  2. Regime concentration: any regime used by > MAX_REGIME_CONCENTRATION
     fraction of active markets -> GUARD_FAIL
  3. Market weight: any single market with chosen_allocation_weight / total_weight
     > MAX_MARKET_WEIGHT_FRACTION -> GUARD_FAIL

If all checks pass -> GUARD_PASS.
If no active markets -> GUARD_PASS (nothing to guard).
Invalid input -> GUARD_FAIL with reason code.

Usage (importable):
    from ant_colony.build_cross_market_allocation_guard_lite import check_guard
    result = check_guard(portfolio_summary)

Usage (CLI):
    python ant_colony/build_cross_market_allocation_guard_lite.py \\
        --summary data/research/queen_candidate_portfolio_summary.json
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_MODULE_DIR = Path(__file__).resolve().parent
_REPO_ROOT  = _MODULE_DIR.parent

DEFAULT_SUMMARY_PATH = _REPO_ROOT / "data" / "research" / \
                       "queen_candidate_portfolio_summary.json"
DEFAULT_OUTPUT_PATH  = _REPO_ROOT / "data" / "research" / \
                       "cross_market_allocation_guard.json"

SNAPSHOT_VERSION = "cross_market_allocation_guard_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# Fixed concentration thresholds (fraction of active markets)
MAX_STRATEGY_CONCENTRATION: float = 0.80
MAX_REGIME_CONCENTRATION:   float = 0.80
MAX_MARKET_WEIGHT_FRACTION: float = 0.70

# Guard status values
GUARD_PASS = "GUARD_PASS"
GUARD_FAIL = "GUARD_FAIL"

# Machine-stable reason codes
REASON_CODES: dict = {
    "PASS":                      "GUARD_ALL_CHECKS_PASSED",
    "NO_ACTIVE_MARKETS":         "GUARD_PASS_NO_ACTIVE_MARKETS",
    "STRATEGY_CONCENTRATION":    "GUARD_FAIL_STRATEGY_CONCENTRATION",
    "REGIME_CONCENTRATION":      "GUARD_FAIL_REGIME_CONCENTRATION",
    "MARKET_WEIGHT":             "GUARD_FAIL_MARKET_WEIGHT_CONCENTRATION",
    "INVALID_INPUT":             "GUARD_FAIL_INVALID_INPUT",
}


# ---------------------------------------------------------------------------
# Core guard function (pure, no I/O)
# ---------------------------------------------------------------------------

def check_guard(
    portfolio_summary: object,
    max_strategy_concentration: float = MAX_STRATEGY_CONCENTRATION,
    max_regime_concentration:   float = MAX_REGIME_CONCENTRATION,
    max_market_weight_fraction: float = MAX_MARKET_WEIGHT_FRACTION,
) -> dict:
    """
    Run cross-market allocation guard checks on an AC-140 portfolio summary.

    Returns a guard result dict. research_only=True always.
    Fail-closed: invalid input -> GUARD_FAIL.

    Args:
        portfolio_summary:          dict from build_portfolio_summary() (AC-140).
        max_strategy_concentration: threshold for strategy concentration (0–1).
        max_regime_concentration:   threshold for regime concentration (0–1).
        max_market_weight_fraction: threshold for single-market weight (0–1).
    """
    if not isinstance(portfolio_summary, dict):
        return _guard_result(
            status=GUARD_FAIL,
            reasons=["portfolio_summary is not a dict"],
            reason_codes=[REASON_CODES["INVALID_INPUT"]],
            exposure={},
            active_checked=0,
        )

    market_summaries = portfolio_summary.get("market_summaries") or {}
    colony_summary   = portfolio_summary.get("colony_summary") or {}
    active_count     = int(colony_summary.get("active_markets_count", 0))

    if not isinstance(market_summaries, dict):
        return _guard_result(
            status=GUARD_FAIL,
            reasons=["market_summaries is not a dict"],
            reason_codes=[REASON_CODES["INVALID_INPUT"]],
            exposure={},
            active_checked=0,
        )

    # Collect active market data
    active_markets = [
        (m, s) for m, s in market_summaries.items()
        if isinstance(s, dict) and s.get("intake_status") == "CANDIDATE_ACTIVE"
    ]

    if not active_markets:
        return _guard_result(
            status=GUARD_PASS,
            reasons=["no active markets — nothing to guard"],
            reason_codes=[REASON_CODES["NO_ACTIVE_MARKETS"]],
            exposure=_empty_exposure(),
            active_checked=0,
        )

    n = len(active_markets)

    # --- Compute exposures ---

    # Strategy exposure: fraction of active markets on each strategy
    strat_counts: dict[str, int] = {}
    for _, s in active_markets:
        strat = s.get("chosen_strategy") or "unknown"
        strat_counts[strat] = strat_counts.get(strat, 0) + 1
    strategy_exposure = {k: round(v / n, 6) for k, v in strat_counts.items()}
    max_strat_conc = max(strategy_exposure.values()) if strategy_exposure else 0.0

    # Regime exposure
    regime_counts: dict[str, int] = {}
    for _, s in active_markets:
        regime = s.get("chosen_regime") or "unknown"
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
    regime_exposure = {k: round(v / n, 6) for k, v in regime_counts.items()}
    max_regime_conc = max(regime_exposure.values()) if regime_exposure else 0.0

    # Market weight exposure: each market's weight / total weight
    weights = {
        m: _safe_float(s.get("chosen_allocation_weight"), 0.0)
        for m, s in active_markets
    }
    total_weight = sum(weights.values())
    if total_weight > 0.0:
        market_weight_exposure = {
            m: round(w / total_weight, 6) for m, w in weights.items()
        }
    else:
        market_weight_exposure = {m: 0.0 for m, _ in active_markets}
    max_mkt_weight = max(market_weight_exposure.values()) if market_weight_exposure else 0.0

    exposure = {
        "strategy_exposure":          strategy_exposure,
        "regime_exposure":            regime_exposure,
        "market_weight_exposure":     market_weight_exposure,
        "max_strategy_concentration": round(max_strat_conc, 6),
        "max_regime_concentration":   round(max_regime_conc, 6),
        "max_single_market_weight":   round(max_mkt_weight, 6),
    }

    # --- Run checks ---
    reasons: list[str] = []
    reason_codes: list[str] = []
    fail = False

    # Check 1: strategy concentration
    for strat, frac in strategy_exposure.items():
        if frac > max_strategy_concentration:
            reasons.append(
                f"strategy '{strat}' in {frac:.0%} of active markets "
                f"(max {max_strategy_concentration:.0%})"
            )
            reason_codes.append(REASON_CODES["STRATEGY_CONCENTRATION"])
            fail = True

    # Check 2: regime concentration
    for regime, frac in regime_exposure.items():
        if frac > max_regime_concentration:
            reasons.append(
                f"regime '{regime}' in {frac:.0%} of active markets "
                f"(max {max_regime_concentration:.0%})"
            )
            reason_codes.append(REASON_CODES["REGIME_CONCENTRATION"])
            fail = True

    # Check 3: market weight
    for market, frac in market_weight_exposure.items():
        if frac > max_market_weight_fraction:
            reasons.append(
                f"market '{market}' weight fraction {frac:.0%} "
                f"(max {max_market_weight_fraction:.0%})"
            )
            reason_codes.append(REASON_CODES["MARKET_WEIGHT"])
            fail = True

    if fail:
        return _guard_result(
            status=GUARD_FAIL,
            reasons=reasons,
            reason_codes=reason_codes,
            exposure=exposure,
            active_checked=n,
        )

    return _guard_result(
        status=GUARD_PASS,
        reasons=["all concentration checks passed"],
        reason_codes=[REASON_CODES["PASS"]],
        exposure=exposure,
        active_checked=n,
    )


# ---------------------------------------------------------------------------
# File-based helpers
# ---------------------------------------------------------------------------

def check_guard_from_file(
    summary_path: Path = DEFAULT_SUMMARY_PATH,
    **kwargs,
) -> dict:
    """
    Load AC-140 portfolio summary from disk and run check_guard().

    On load error -> GUARD_FAIL (does not re-raise).
    """
    if not summary_path.exists():
        return _guard_result(
            status=GUARD_FAIL,
            reasons=[f"portfolio summary not found: {summary_path}"],
            reason_codes=[REASON_CODES["INVALID_INPUT"]],
            exposure={},
            active_checked=0,
        )
    try:
        data = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _guard_result(
            status=GUARD_FAIL,
            reasons=[f"could not load summary: {exc}"],
            reason_codes=[REASON_CODES["INVALID_INPUT"]],
            exposure={},
            active_checked=0,
        )
    return check_guard(data, **kwargs)


def write_guard_result(result: dict, out_path: Path) -> None:
    """Write guard result as pretty-printed JSON. Creates parent dirs if absent."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
        f.write("\n")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _guard_result(
    status: str,
    reasons: list,
    reason_codes: list,
    exposure: dict,
    active_checked: int,
) -> dict:
    return {
        "version":              SNAPSHOT_VERSION,
        "ts_utc":               _now_utc_iso(),
        "guard_status":         status,
        "guard_pass":           (status == GUARD_PASS),
        "guard_reasons":        reasons,
        "guard_reason_codes":   reason_codes,
        "exposure_summary":     exposure,
        "active_markets_checked": active_checked,
        "research_only":        True,
        "flags":                dict(FLAGS),
    }


def _empty_exposure() -> dict:
    return {
        "strategy_exposure":          {},
        "regime_exposure":            {},
        "market_weight_exposure":     {},
        "max_strategy_concentration": 0.0,
        "max_regime_concentration":   0.0,
        "max_single_market_weight":   0.0,
    }


def _safe_float(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-141: Cross-market allocation guard (research-only)."
    )
    p.add_argument(
        "--summary", dest="summary", default=str(DEFAULT_SUMMARY_PATH),
        help="Path to AC-140 queen_candidate_portfolio_summary.json",
    )
    p.add_argument(
        "--out", dest="output", default=str(DEFAULT_OUTPUT_PATH),
        help="Output path for guard result JSON",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = check_guard_from_file(Path(args.summary))
    write_guard_result(result, Path(args.output))
    print()
    print("=== AC-141 CROSS-MARKET ALLOCATION GUARD ===")
    print(f"{'status':<12}: {result['guard_status']}")
    print(f"{'checked':<12}: {result['active_markets_checked']}")
    for r in result["guard_reasons"]:
        print(f"{'reason':<12}: {r}")
    print(f"{'file':<12}: {args.output}")
    print()
    return 0 if result["guard_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
