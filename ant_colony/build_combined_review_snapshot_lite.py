"""
AC-107: Combined Review Snapshot (Daily Human Overview)

Reads existing local outputs and produces one compact object for human
daily review. No new logic — only bundles and summarises existing signals.

Sources:
  PRIMARY   : C:\\Trading\\ANT_OUT\\feedback_analysis.json    (AC-100)
  PRIMARY   : C:\\Trading\\ANT_OUT\\source_health_review.json (AC-105)
  OPTIONAL  : C:\\Trading\\ANT_OUT\\marketdata_scenario_adapter.json (AC-103)

Output:
  C:\\Trading\\ANT_OUT\\combined_review_snapshot.json

Overview status (priority order):
  CRITICAL  — source_health.status == CRITICAL
  ATTENTION — review_health.needs_attention == True
  WATCH     — alignment == MEDIUM  OR  source_health.status == DEGRADED
  HEALTHY   — all clear

No execution. No API calls. Paper-only. Non-binding. Simulation-only.
live_activation_allowed=False always.

Usage:
    python ant_colony/build_combined_review_snapshot_lite.py
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ANALYSIS_PATH  = Path(r"C:\Trading\ANT_OUT\feedback_analysis.json")
HEALTH_PATH    = Path(r"C:\Trading\ANT_OUT\source_health_review.json")
ADAPTER_PATH   = Path(r"C:\Trading\ANT_OUT\marketdata_scenario_adapter.json")
OUTPUT_PATH    = Path(r"C:\Trading\ANT_OUT\combined_review_snapshot.json")

VERSION   = "combined_review_snapshot_v1"
COMPONENT = "build_combined_review_snapshot_lite"

FLAGS = {
    "non_binding":             True,
    "simulation_only":         True,
    "paper_only":              True,
    "live_activation_allowed": False,
}

# ---------------------------------------------------------------------------
# Deterministic text maps (no free-form generation)
# ---------------------------------------------------------------------------

_TOP_RISK_MAP = {
    "ALL_SOURCES_STALE":        "Source freshness: all market data stale",
    "ALL_SOURCES_FRESH":        "No source risk",
    "SOURCES_MISSING":          "Source availability: price data missing",
    "SOURCES_MISSING_AND_STALE":"Source availability and freshness impaired",
    "MAJORITY_SOURCES_STALE":   "Source freshness: majority of markets stale",
}

_HUMAN_CTX_MAP = {
    "CRITICAL_DISAGREE":        "Disagreement on critical cases — review required",
    "HIGH_DISAGREE_RATE":       "Elevated disagree rate across reviewed cases",
    "LOW_CONFIRM_RATE":         "Low confirmation rate — operator uncertain",
    "NONE":                     "No attention trigger — review aligned",
}


def _top_risk(sh_reason_code: str, rv_reason_code: str) -> str:
    if sh_reason_code and sh_reason_code != "ALL_SOURCES_FRESH":
        return _TOP_RISK_MAP.get(sh_reason_code, f"Source issue: {sh_reason_code}")
    if rv_reason_code and rv_reason_code != "NONE":
        return _HUMAN_CTX_MAP.get(rv_reason_code, f"Review issue: {rv_reason_code}")
    return _TOP_RISK_MAP.get("ALL_SOURCES_FRESH", "No risk identified")


def _human_context(rv_reason_code: str, sh_status: str) -> str:
    if rv_reason_code and rv_reason_code != "NONE":
        return _HUMAN_CTX_MAP.get(rv_reason_code, f"Review flag: {rv_reason_code}")
    if sh_status == "CRITICAL":
        return "Data freshness blocking review — verify source pipeline"
    if sh_status == "DEGRADED":
        return "Some markets stale — results may be partial"
    return _HUMAN_CTX_MAP.get("NONE", "No attention trigger")


# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------

def build_snapshot(
    analysis_data: dict | None,
    health_data:   dict | None,
    now_utc:       datetime.datetime,
) -> dict:
    """
    Build combined review snapshot from loaded source dicts.
    Pure function — no I/O, no side effects.

    Args:
        analysis_data: parsed feedback_analysis.json, or None
        health_data:   parsed source_health_review.json, or None
        now_utc:       current UTC datetime

    Returns:
        combined_review_snapshot dict
    """
    # ── Extract source health fields ─────────────────────────────────────────
    sh: dict = {}
    if health_data and isinstance(health_data, dict):
        sh = health_data

    sh_status   = str(sh.get("source_health_status", "UNKNOWN"))
    sh_blocking = bool(sh.get("freshness_blocking_review", False))
    sh_code     = str(sh.get("primary_reason_code", "UNKNOWN"))
    sh_total    = int(sh.get("markets_total",   0))
    sh_fresh    = int(sh.get("markets_fresh",   0))
    sh_stale    = int(sh.get("markets_stale",   0))
    sh_miss     = int(sh.get("markets_missing", 0))

    # ── Extract review health fields ─────────────────────────────────────────
    rv: dict = {}
    if analysis_data and isinstance(analysis_data, dict):
        rv = analysis_data

    rv_signals    = rv.get("signals", {}) if isinstance(rv.get("signals"), dict) else {}
    rv_rates      = rv.get("rates",   {}) if isinstance(rv.get("rates"),   dict) else {}
    rv_totals     = rv.get("totals",  {}) if isinstance(rv.get("totals"),  dict) else {}

    alignment       = str(rv_signals.get("system_human_alignment", "UNKNOWN"))
    needs_attention = bool(rv_signals.get("needs_attention", False))
    attn_code       = str(rv_signals.get("attention_reason_code", "NONE"))
    entries         = int(rv_totals.get("entries",        0))
    confirm_rate    = float(rv_rates.get("confirm_rate",  0.0))
    disagree_rate   = float(rv_rates.get("disagree_rate", 0.0))
    uncertain_rate  = float(rv_rates.get("uncertain_rate",0.0))

    # ── Overview status (priority order) ─────────────────────────────────────
    if sh_status == "CRITICAL":
        overview_status      = "CRITICAL"
        overview_reason_code = sh_code
    elif needs_attention:
        overview_status      = "ATTENTION"
        overview_reason_code = attn_code
    elif alignment == "MEDIUM" or sh_status == "DEGRADED":
        overview_status      = "WATCH"
        overview_reason_code = (sh_code if sh_status == "DEGRADED"
                                else f"ALIGNMENT_{alignment}")
    else:
        overview_status      = "HEALTHY"
        overview_reason_code = "ALL_CLEAR"

    # ── Summary text (deterministic from maps) ───────────────────────────────
    top_risk      = _top_risk(sh_code, attn_code)
    human_context = _human_context(attn_code, sh_status)

    return {
        "version":   VERSION,
        "component": COMPONENT,
        "ts_utc":    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overview_status":      overview_status,
        "overview_reason_code": overview_reason_code,
        "source_health": {
            "status":          sh_status,
            "blocking_review": sh_blocking,
            "reason_code":     sh_code,
            "markets_total":   sh_total,
            "markets_fresh":   sh_fresh,
            "markets_stale":   sh_stale,
            "markets_missing": sh_miss,
        },
        "review_health": {
            "alignment":            alignment,
            "needs_attention":      needs_attention,
            "attention_reason_code":attn_code,
            "entries":              entries,
            "confirm_rate":         round(confirm_rate,  4),
            "disagree_rate":        round(disagree_rate, 4),
            "uncertain_rate":       round(uncertain_rate,4),
        },
        "summary": {
            "top_risk":      top_risk,
            "human_context": human_context,
        },
        "sources": {
            "analysis_loaded": analysis_data is not None,
            "health_loaded":   health_data   is not None,
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


def write_snapshot(result: dict, path: Path = OUTPUT_PATH) -> None:
    """Write snapshot to JSON file, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")


def run_snapshot(
    analysis_path: Path = ANALYSIS_PATH,
    health_path:   Path = HEALTH_PATH,
    output_path:   Path = OUTPUT_PATH,
    now_utc:       datetime.datetime | None = None,
) -> dict:
    """Load sources → build snapshot → write output → return result."""
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    analysis_data = _load_json(analysis_path)
    health_data   = _load_json(health_path)

    result = build_snapshot(analysis_data, health_data, now_utc)
    write_snapshot(result, output_path)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    result = run_snapshot()
    print(f"Overview status   : {result['overview_status']}")
    print(f"Reason code       : {result['overview_reason_code']}")
    print()
    sh = result["source_health"]
    print(f"Source health     : {sh['status']}  "
          f"(fresh={sh['markets_fresh']} stale={sh['markets_stale']} "
          f"missing={sh['markets_missing']})")
    print(f"  blocking_review : {sh['blocking_review']}")
    rv = result["review_health"]
    print(f"Review health     : alignment={rv['alignment']}  "
          f"entries={rv['entries']}  "
          f"confirm={rv['confirm_rate']:.1%}  "
          f"disagree={rv['disagree_rate']:.1%}")
    print(f"  needs_attention : {rv['needs_attention']}"
          + (f"  ({rv['attention_reason_code']})" if rv["needs_attention"] else ""))
    print()
    sm = result["summary"]
    print(f"Top risk          : {sm['top_risk']}")
    print(f"Human context     : {sm['human_context']}")
    print(f"Output            : {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
