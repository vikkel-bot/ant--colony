"""
AC-105: Data Freshness / Source Health Review Layer

Reads existing local paper outputs and produces a compact health assessment
that separates data/source health problems from content review problems.

Sources:
  PRIMARY   : C:\\Trading\\ANT_OUT\\marketdata_scenario_adapter.json  (AC-103)
  SECONDARY : C:\\Trading\\ANT_OUT\\worker_market_data.json           (raw refresh)
  OPTIONAL  : C:\\Trading\\ANT_OUT\\feedback_analysis.json            (AC-100)

Output:
  C:\\Trading\\ANT_OUT\\source_health_review.json

Health classification:
  HEALTHY   — no markets stale or missing
  DEGRADED  — some stale, none missing, fewer than half affected
  CRITICAL  — any missing, or half or more of markets are stale

freshness_blocking_review:
  True when health_status == CRITICAL (data quality prevents reliable review)

No execution. No API calls. Paper-only. Non-binding. Simulation-only.
live_activation_allowed=False always.

Usage:
    python ant_colony/build_source_health_review_lite.py
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ADAPTER_PATH  = Path(r"C:\Trading\ANT_OUT\marketdata_scenario_adapter.json")
MD_PATH       = Path(r"C:\Trading\ANT_OUT\worker_market_data.json")
ANALYSIS_PATH = Path(r"C:\Trading\ANT_OUT\feedback_analysis.json")
OUTPUT_PATH   = Path(r"C:\Trading\ANT_OUT\source_health_review.json")

VERSION   = "source_health_review_v1"
COMPONENT = "build_source_health_review_lite"

FLAGS = {
    "non_binding":             True,
    "simulation_only":         True,
    "paper_only":              True,
    "live_activation_allowed": False,
}

# Seed classes that indicate the price data itself is OK (content review needed)
_FRESH_CLASSES = {"ALL_CLEAR", "ZERO_INTENTS", "HOLD_REVIEW"}
# Seed classes that indicate a data/source health problem
_STALE_CLASSES   = {"DATA_STALE"}
_MISSING_CLASSES = {"DATA_MISSING"}


# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------

def build_source_health(
    adapter_data: dict | None,
    md_data:      dict | None,
    now_utc:      datetime.datetime,
) -> dict:
    """
    Build source health review from loaded source dicts.
    Pure function — no I/O, no side effects.

    Args:
        adapter_data: parsed marketdata_scenario_adapter.json, or None
        md_data:      parsed worker_market_data.json, or None
        now_utc:      current UTC datetime

    Returns:
        source_health_review dict
    """
    # ── Extract market classifications from adapter ───────────────────────────
    markets_list: list[dict] = []
    if adapter_data and isinstance(adapter_data, dict):
        markets_list = adapter_data.get("markets") or []

    fresh_markets:   list[str] = []
    stale_markets:   list[str] = []
    missing_markets: list[str] = []

    for m in markets_list:
        if not isinstance(m, dict):
            continue
        name       = str(m.get("market") or "UNKNOWN")
        seed_class = str(m.get("review_seed_class") or "HOLD_REVIEW")
        if seed_class in _FRESH_CLASSES:
            fresh_markets.append(name)
        elif seed_class in _STALE_CLASSES:
            stale_markets.append(name)
        elif seed_class in _MISSING_CLASSES:
            missing_markets.append(name)
        else:
            # Unknown class — treat as stale (conservative)
            stale_markets.append(name)

    total   = len(fresh_markets) + len(stale_markets) + len(missing_markets)
    n_fresh = len(fresh_markets)
    n_stale = len(stale_markets)
    n_miss  = len(missing_markets)

    # ── Health classification ────────────────────────────────────────────────
    # CRITICAL  : any missing  OR  stale >= half of all markets
    # DEGRADED  : stale > 0, none missing, stale < half
    # HEALTHY   : no stale, no missing

    if n_miss > 0 or (total > 0 and n_stale * 2 >= total):
        source_health_status = "CRITICAL"
    elif n_stale > 0:
        source_health_status = "DEGRADED"
    else:
        source_health_status = "HEALTHY"

    freshness_blocking_review: bool = source_health_status == "CRITICAL"

    # ── Primary reason ───────────────────────────────────────────────────────
    if source_health_status == "HEALTHY":
        primary_reason      = "All market data sources are fresh and present."
        primary_reason_code = "ALL_SOURCES_FRESH"
    elif n_miss > 0 and n_stale > 0:
        primary_reason      = (f"{n_miss} market(s) missing and {n_stale} stale — "
                               "review chain structurally impaired.")
        primary_reason_code = "SOURCES_MISSING_AND_STALE"
    elif n_miss > 0:
        primary_reason      = (f"{n_miss} market(s) have no price data — "
                               "review chain structurally impaired.")
        primary_reason_code = "SOURCES_MISSING"
    else:
        primary_reason      = (f"{n_stale} of {total} market(s) stale — "
                               "data freshness blocking reliable review.")
        primary_reason_code = ("ALL_SOURCES_STALE" if n_stale == total
                               else "MAJORITY_SOURCES_STALE")

    affected_markets = sorted(stale_markets + missing_markets)

    # ── Optional: last refresh ts from worker_market_data ────────────────────
    md_refresh_ts: str = ""
    if md_data and isinstance(md_data, dict):
        md_refresh_ts = str(md_data.get("ts_utc") or "")

    return {
        "version":   VERSION,
        "component": COMPONENT,
        "ts_utc":    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_health_status":      source_health_status,
        "markets_total":             total,
        "markets_fresh":             n_fresh,
        "markets_stale":             n_stale,
        "markets_missing":           n_miss,
        "freshness_blocking_review": freshness_blocking_review,
        "primary_reason":            primary_reason,
        "primary_reason_code":       primary_reason_code,
        "affected_markets":          affected_markets,
        "sources": {
            "adapter_loaded":   adapter_data is not None,
            "md_loaded":        md_data is not None,
            "md_refresh_ts":    md_refresh_ts,
        },
        "flags": dict(FLAGS),
    }


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict | None:
    """Load JSON. Returns None if absent or corrupt (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_source_health(result: dict, path: Path = OUTPUT_PATH) -> None:
    """Write result to JSON file, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")


def run_health_review(
    adapter_path: Path = ADAPTER_PATH,
    md_path:      Path = MD_PATH,
    output_path:  Path = OUTPUT_PATH,
    now_utc:      datetime.datetime | None = None,
) -> dict:
    """Load sources, build health review, write output, return result."""
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    adapter_data = _load_json(adapter_path)
    md_data      = _load_json(md_path)

    result = build_source_health(adapter_data, md_data, now_utc)
    write_source_health(result, output_path)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    result = run_health_review()
    status = result["source_health_status"]
    print(f"Source health     : {status}")
    print(f"Markets total     : {result['markets_total']}")
    print(f"  fresh           : {result['markets_fresh']}")
    print(f"  stale           : {result['markets_stale']}")
    print(f"  missing         : {result['markets_missing']}")
    print(f"Blocking review   : {result['freshness_blocking_review']}")
    print(f"Reason            : {result['primary_reason_code']}")
    print(f"Detail            : {result['primary_reason']}")
    if result["affected_markets"]:
        print(f"Affected markets  : {', '.join(result['affected_markets'])}")
    print(f"Output            : {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
