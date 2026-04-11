"""
AC-140: Queen Candidate Portfolio Summary (Research-Only)

Bundles AC-136 candidate intake results from multiple markets into one
compact colony-wide queen summary.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No new allocation logic -- bundling/summarising from AC-136 intake output only.

Input:  dict of {market: intake_result} from queen_research_candidate_intake_lite
Output: compact colony-wide portfolio summary dict

Fail-closed:
  - missing / invalid per-market intake -> included as non-active, no crash
  - empty market set -> valid empty summary, no crash

Usage (importable):
    from ant_colony.build_queen_candidate_portfolio_summary_lite import build_portfolio_summary
    summary = build_portfolio_summary({"BTC-EUR": intake_btc, "ETH-EUR": intake_eth})

Usage (CLI):
    python ant_colony/build_queen_candidate_portfolio_summary_lite.py
    python ant_colony/build_queen_candidate_portfolio_summary_lite.py \\
        --markets BTC-EUR ETH-EUR ADA-EUR
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_MODULE_DIR = Path(__file__).resolve().parent
_REPO_ROOT  = _MODULE_DIR.parent

DEFAULT_SNAPSHOT_DIR  = _REPO_ROOT / "data" / "research"
DEFAULT_SNAPSHOT_NAME = "queen_candidate_decision_snapshot.json"
DEFAULT_OUTPUT_PATH   = _REPO_ROOT / "data" / "research" / \
                        "queen_candidate_portfolio_summary.json"

SNAPSHOT_VERSION = "queen_candidate_portfolio_summary_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# Intake statuses from AC-136
_ACTIVE  = "CANDIDATE_ACTIVE"
_HOLD    = "CANDIDATE_HOLD"
_INVALID = "CANDIDATE_INVALID"


# ---------------------------------------------------------------------------
# Dominant helper (same pattern as AC-132/133)
# ---------------------------------------------------------------------------

def _dominant(freq: dict) -> Optional[str]:
    """Highest count, tie-break alphabetical. Returns None if freq is empty."""
    if not freq:
        return None
    return min(freq, key=lambda k: (-freq[k], k))


# ---------------------------------------------------------------------------
# Core builder (pure, no I/O)
# ---------------------------------------------------------------------------

def build_portfolio_summary(
    market_intakes: dict,
    ts_utc: Optional[str] = None,
) -> dict:
    """
    Bundle per-market AC-136 intake results into a colony-wide summary.

    Args:
        market_intakes: {market_name: intake_result_dict}
                        intake_result_dict may be any dict or non-dict (safe fallback).
        ts_utc:         optional ISO timestamp string (injected for determinism).

    Returns:
        Colony-wide portfolio summary dict. research_only=True always.
    """
    if not isinstance(market_intakes, dict):
        market_intakes = {}

    markets = sorted(market_intakes.keys())
    market_summaries: dict[str, dict] = {}

    strategy_freq: dict[str, int] = {}
    regime_freq:   dict[str, int] = {}
    active_count   = 0

    for market in markets:
        intake = market_intakes.get(market)
        # Safe: intake may be missing, None, or not a dict
        if not isinstance(intake, dict):
            intake = {}

        status  = intake.get("intake_status", _INVALID)
        is_active = (status == _ACTIVE)

        entry = {
            "intake_status":            status,
            "chosen_timeframe":         intake.get("chosen_timeframe"),
            "chosen_strategy":          intake.get("chosen_strategy"),
            "chosen_regime":            intake.get("chosen_regime"),
            "chosen_allocation_weight": intake.get("chosen_allocation_weight"),
            "snapshot_ts_utc":          intake.get("snapshot_ts_utc"),
        }
        market_summaries[market] = entry

        if is_active:
            active_count += 1
            strat  = intake.get("chosen_strategy")
            regime = intake.get("chosen_regime")
            if strat:
                strategy_freq[strat] = strategy_freq.get(strat, 0) + 1
            if regime:
                regime_freq[regime] = regime_freq.get(regime, 0) + 1

    dominant_strategy = _dominant(strategy_freq)
    dominant_regime   = _dominant(regime_freq)

    return {
        "version":          SNAPSHOT_VERSION,
        "ts_utc":           ts_utc or _now_utc_iso(),
        "markets":          markets,
        "market_summaries": market_summaries,
        "colony_summary": {
            "total_markets":             len(markets),
            "active_markets_count":      active_count,
            "dominant_strategy":         dominant_strategy,
            "dominant_regime":           dominant_regime,
            "total_candidate_decisions": active_count,
        },
        "research_only": True,
        "flags":         dict(FLAGS),
    }


# ---------------------------------------------------------------------------
# File-based helpers
# ---------------------------------------------------------------------------

def build_portfolio_summary_from_paths(
    market_paths: dict,
    ts_utc: Optional[str] = None,
    max_age_hours: int = 24,
) -> dict:
    """
    Load intake results per market using AC-136 load_and_consume, then bundle.

    Args:
        market_paths: {market_name: Path to queen_candidate_decision_snapshot.json}
        ts_utc:       optional timestamp override.
        max_age_hours: passed to load_and_consume for freshness check.

    Returns:
        Colony-wide portfolio summary dict (same structure as build_portfolio_summary).
    """
    from ant_colony.queen_research_candidate_intake_lite import load_and_consume

    market_intakes: dict[str, dict] = {}
    for market, path in (market_paths or {}).items():
        try:
            result = load_and_consume(Path(path), max_age_hours=max_age_hours)
        except Exception as exc:  # pragma: no cover — belt-and-suspenders
            result = {
                "intake_status":            _INVALID,
                "intake_valid":             False,
                "intake_reason":            f"unexpected error: {exc}",
                "intake_reason_code":       "CANDIDATE_INVALID_UNKNOWN",
                "chosen_timeframe":         None,
                "chosen_strategy":          None,
                "chosen_regime":            None,
                "chosen_allocation_weight": None,
                "snapshot_ts_utc":          None,
                "research_only":            True,
            }
        market_intakes[market] = result

    return build_portfolio_summary(market_intakes, ts_utc=ts_utc)


def write_summary(summary: dict, out_path: Path) -> None:
    """Write summary as pretty-printed JSON. Creates parent dirs if absent."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
        f.write("\n")


def build_and_write_summary(
    market_paths: dict,
    out_path: Path = DEFAULT_OUTPUT_PATH,
    ts_utc: Optional[str] = None,
    max_age_hours: int = 24,
) -> dict:
    """End-to-end: load per-market snapshots → build summary → write."""
    summary = build_portfolio_summary_from_paths(
        market_paths, ts_utc=ts_utc, max_age_hours=max_age_hours
    )
    write_summary(summary, out_path)
    return summary


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AC-140: Queen candidate portfolio summary (research-only)."
    )
    p.add_argument(
        "--markets", nargs="+", default=["BTC-EUR", "ETH-EUR", "ADA-EUR"],
        help="Market names to include (default: BTC-EUR ETH-EUR ADA-EUR)",
    )
    p.add_argument(
        "--snapshot-dir", dest="snapshot_dir", default=str(DEFAULT_SNAPSHOT_DIR),
        help="Directory containing queen_candidate_decision_snapshot.json files",
    )
    p.add_argument(
        "--snapshot-name", dest="snapshot_name", default=DEFAULT_SNAPSHOT_NAME,
        help=f"Snapshot filename (default: {DEFAULT_SNAPSHOT_NAME})",
    )
    p.add_argument(
        "--out", dest="output", default=str(DEFAULT_OUTPUT_PATH),
        help="Output path for portfolio summary JSON",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    snap_dir = Path(args.snapshot_dir)
    market_paths = {m: snap_dir / args.snapshot_name for m in args.markets}

    summary = build_and_write_summary(
        market_paths=market_paths,
        out_path=Path(args.output),
    )
    cs = summary["colony_summary"]
    print()
    print("=== AC-140 QUEEN CANDIDATE PORTFOLIO SUMMARY ===")
    print(f"{'markets':<12}: {cs['total_markets']}")
    print(f"{'active':<12}: {cs['active_markets_count']}")
    print(f"{'dom_strategy':<12}: {cs['dominant_strategy']}")
    print(f"{'dom_regime':<12}: {cs['dominant_regime']}")
    print(f"{'file':<12}: {args.output}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
