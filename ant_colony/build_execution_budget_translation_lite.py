"""
AC-142: Execution Budget Translation (Research-Only)

Translates AC-140 allocation weights from the queen candidate portfolio summary
into concrete budget fractions (and optionally EUR amounts) per market.

NO pipeline impact. NO execution. NO ANT_OUT writes. Research-only.
No live sizing optimizer, no broker logic. Controlled translation only.

Input:  AC-140 portfolio summary dict (from build_queen_candidate_portfolio_summary_lite)
Output: budget translation dict with budget_per_market, budget_summary

Budget logic:
  - budget_fraction per market = chosen_allocation_weight / sum(all active weights)
  - If all weights are zero or no active markets: equal distribution (1/n)
  - If no active markets at all: fallback with budget_fraction=0.0 per market
  - budget_eur = budget_fraction * total_budget_eur if total_budget_eur provided
  - budget_context explains how the budget was derived

Fail-closed:
  - invalid portfolio_summary -> fallback with no budgets, reason in summary
  - missing allocation weight -> treated as 0.0 for normalization

Usage (importable):
    from ant_colony.build_execution_budget_translation_lite import translate_to_budget
    result = translate_to_budget(portfolio_summary, total_budget_eur=10000.0)

Usage (CLI):
    python ant_colony/build_execution_budget_translation_lite.py \\
        --summary data/research/queen_candidate_portfolio_summary.json \\
        --budget-eur 10000
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
                       "execution_budget_translation.json"

SNAPSHOT_VERSION = "execution_budget_translation_v1"
FLAGS = {"research_only": True, "pipeline_impact": False}

# Budget context labels
_CTX_WEIGHT_NORMALIZED = "allocation_weight_normalized"
_CTX_EQUAL_SPLIT       = "equal_split_zero_weights"
_CTX_FALLBACK          = "fallback_no_active_markets"
_CTX_INVALID           = "fallback_invalid_input"


# ---------------------------------------------------------------------------
# Core budget translation function (pure, no I/O)
# ---------------------------------------------------------------------------

def translate_to_budget(
    portfolio_summary: object,
    total_budget_eur: Optional[float] = None,
) -> dict:
    """
    Translate AC-140 allocation weights into budget fractions per market.

    Args:
        portfolio_summary: dict from build_portfolio_summary() (AC-140).
        total_budget_eur:  optional total EUR budget; if provided, budget_eur
                           is computed per market. None -> budget_eur=None.

    Returns:
        Budget translation dict. research_only=True always.
        pipeline_impact=False always.
    """
    # Validate total_budget_eur if provided
    budget_eur_total: Optional[float] = None
    if total_budget_eur is not None:
        budget_eur_total = _safe_float(total_budget_eur, None)
        if budget_eur_total is None or budget_eur_total < 0.0:
            budget_eur_total = None  # ignore invalid value; fallback to no EUR

    if not isinstance(portfolio_summary, dict):
        return _fallback_result(
            markets=[],
            budget_eur_total=budget_eur_total,
            context=_CTX_INVALID,
            reason="portfolio_summary is not a dict",
        )

    market_summaries = portfolio_summary.get("market_summaries") or {}
    if not isinstance(market_summaries, dict):
        return _fallback_result(
            markets=[],
            budget_eur_total=budget_eur_total,
            context=_CTX_INVALID,
            reason="market_summaries is not a dict",
        )

    all_markets = sorted(market_summaries.keys())

    # Collect active markets and their allocation weights
    active_entries: list[tuple[str, float]] = []
    for market in all_markets:
        s = market_summaries.get(market) or {}
        if not isinstance(s, dict):
            continue
        if s.get("intake_status") != "CANDIDATE_ACTIVE":
            continue
        weight = _safe_float(s.get("chosen_allocation_weight"), 0.0)
        active_entries.append((market, weight))

    if not active_entries:
        # No active markets — produce zero-budget entries for all markets
        budget_per_market = {
            m: _budget_entry(
                market=m,
                budget_fraction=0.0,
                budget_eur=None,
                source_weight=0.0,
                intake_status=(market_summaries.get(m) or {}).get("intake_status", "CANDIDATE_INVALID"),
                context=_CTX_FALLBACK,
            )
            for m in all_markets
        }
        return _make_result(
            budget_per_market=budget_per_market,
            total_fraction=0.0,
            active_count=0,
            budget_eur_total=budget_eur_total,
            budget_source=_CTX_FALLBACK,
            fallback_used=True,
        )

    # Normalize weights
    total_weight = sum(w for _, w in active_entries)
    context: str
    if total_weight > 0.0:
        fractions = {m: w / total_weight for m, w in active_entries}
        context = _CTX_WEIGHT_NORMALIZED
    else:
        n = len(active_entries)
        fractions = {m: 1.0 / n for m, _ in active_entries}
        context = _CTX_EQUAL_SPLIT

    # Build per-market budget entries
    budget_per_market: dict[str, dict] = {}
    total_fraction = 0.0

    for market in all_markets:
        s = market_summaries.get(market) or {}
        intake_status = s.get("intake_status", "CANDIDATE_INVALID") if isinstance(s, dict) else "CANDIDATE_INVALID"
        source_weight = _safe_float(s.get("chosen_allocation_weight") if isinstance(s, dict) else None, 0.0)

        if market in fractions:
            frac = round(fractions[market], 6)
            eur  = round(frac * budget_eur_total, 4) if budget_eur_total is not None else None
            total_fraction += frac
            ctx = context
        else:
            frac = 0.0
            eur  = None
            ctx  = _CTX_FALLBACK

        budget_per_market[market] = _budget_entry(
            market=market,
            budget_fraction=frac,
            budget_eur=eur,
            source_weight=source_weight,
            intake_status=intake_status,
            context=ctx,
        )

    return _make_result(
        budget_per_market=budget_per_market,
        total_fraction=round(total_fraction, 6),
        active_count=len(active_entries),
        budget_eur_total=budget_eur_total,
        budget_source=context,
        fallback_used=(context == _CTX_EQUAL_SPLIT),
    )


# ---------------------------------------------------------------------------
# File-based helpers
# ---------------------------------------------------------------------------

def translate_from_file(
    summary_path: Path = DEFAULT_SUMMARY_PATH,
    total_budget_eur: Optional[float] = None,
) -> dict:
    """
    Load AC-140 portfolio summary from disk and run translate_to_budget().

    On load error -> fallback result (does not re-raise).
    """
    if not summary_path.exists():
        return _fallback_result(
            markets=[],
            budget_eur_total=total_budget_eur,
            context=_CTX_INVALID,
            reason=f"portfolio summary not found: {summary_path}",
        )
    try:
        data = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _fallback_result(
            markets=[],
            budget_eur_total=total_budget_eur,
            context=_CTX_INVALID,
            reason=f"could not load summary: {exc}",
        )
    return translate_to_budget(data, total_budget_eur=total_budget_eur)


def write_budget_translation(result: dict, out_path: Path) -> None:
    """Write budget translation as pretty-printed JSON."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
        f.write("\n")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _budget_entry(
    market: str,
    budget_fraction: float,
    budget_eur: Optional[float],
    source_weight: float,
    intake_status: str,
    context: str,
) -> dict:
    return {
        "budget_fraction":  budget_fraction,
        "budget_eur":       budget_eur,
        "source_weight":    source_weight,
        "intake_status":    intake_status,
        "budget_context":   context,
    }


def _make_result(
    budget_per_market: dict,
    total_fraction: float,
    active_count: int,
    budget_eur_total: Optional[float],
    budget_source: str,
    fallback_used: bool,
) -> dict:
    total_budget_eur_allocated = None
    if budget_eur_total is not None:
        total_budget_eur_allocated = round(
            sum(e["budget_eur"] for e in budget_per_market.values()
                if e["budget_eur"] is not None),
            4,
        )
    return {
        "version":           SNAPSHOT_VERSION,
        "ts_utc":            _now_utc_iso(),
        "budget_per_market": budget_per_market,
        "budget_summary": {
            "total_budget_eur":         budget_eur_total,
            "total_budget_eur_allocated": total_budget_eur_allocated,
            "total_fraction_allocated": total_fraction,
            "active_markets":           active_count,
            "budget_source":            budget_source,
        },
        "fallback_used":  fallback_used,
        "research_only":  True,
        "flags":          dict(FLAGS),
    }


def _fallback_result(
    markets: list,
    budget_eur_total: Optional[float],
    context: str,
    reason: str,
) -> dict:
    budget_per_market = {
        m: _budget_entry(m, 0.0, None, 0.0, "CANDIDATE_INVALID", context)
        for m in markets
    }
    return {
        "version":           SNAPSHOT_VERSION,
        "ts_utc":            _now_utc_iso(),
        "budget_per_market": budget_per_market,
        "budget_summary": {
            "total_budget_eur":           budget_eur_total,
            "total_budget_eur_allocated": None,
            "total_fraction_allocated":   0.0,
            "active_markets":             0,
            "budget_source":              context,
        },
        "fallback_used":  True,
        "fallback_reason": reason,
        "research_only":  True,
        "flags":          dict(FLAGS),
    }


def _safe_float(value: object, default) -> Optional[float]:
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
        description="AC-142: Execution budget translation (research-only)."
    )
    p.add_argument(
        "--summary", dest="summary", default=str(DEFAULT_SUMMARY_PATH),
        help="Path to AC-140 queen_candidate_portfolio_summary.json",
    )
    p.add_argument(
        "--budget-eur", dest="budget_eur", type=float, default=None,
        help="Total budget in EUR (optional)",
    )
    p.add_argument(
        "--out", dest="output", default=str(DEFAULT_OUTPUT_PATH),
        help="Output path for budget translation JSON",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = translate_from_file(
        summary_path=Path(args.summary),
        total_budget_eur=args.budget_eur,
    )
    write_budget_translation(result, Path(args.output))
    bs = result["budget_summary"]
    print()
    print("=== AC-142 EXECUTION BUDGET TRANSLATION ===")
    print(f"{'active':<12}: {bs['active_markets']}")
    print(f"{'total_frac':<12}: {bs['total_fraction_allocated']:.4f}")
    print(f"{'budget_eur':<12}: {bs['total_budget_eur']}")
    print(f"{'source':<12}: {bs['budget_source']}")
    print(f"{'fallback':<12}: {result['fallback_used']}")
    print(f"{'file':<12}: {args.output}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
