"""
AC-103: Paper Market Data Adapter → Scenario Input

Reads existing paper market data artifacts and classifies each market
for scenario ingestion into the anomaly/review stack.

Sources (both optional, fail-closed if absent/corrupt):
  PRIMARY   : C:\\Trading\\ANT_OUT\\worker_market_data.json
              (worker_market_data_refresh_lite — price per market)
  SECONDARY : C:\\Trading\\ANT_OUT\\rebalance_intents.json
              (build_rebalance_intents_lite — active intents per market)

Output:
  C:\\Trading\\ANT_OUT\\marketdata_scenario_adapter.json

Classification:
  DATA_MISSING  — price entry absent or last_price == 0
  DATA_STALE    — price present but older than STALE_THRESHOLD_H hours
  ALL_CLEAR     — price fresh + intents present
  ZERO_INTENTS  — price fresh + no intents for this market
  HOLD_REVIEW   — ambiguous / incomplete state (fallback)

No execution. No API calls. Paper-only. Non-binding. Simulation-only.
live_activation_allowed=False always.

Usage:
    python ant_colony/build_marketdata_scenario_adapter_lite.py
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

MD_PATH      = Path(r"C:\Trading\ANT_OUT\worker_market_data.json")
INTENTS_PATH = Path(r"C:\Trading\ANT_OUT\rebalance_intents.json")
OUTPUT_PATH  = Path(r"C:\Trading\ANT_OUT\marketdata_scenario_adapter.json")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STALE_THRESHOLD_H: float = 24.0   # hours — price older than this is STALE

VERSION   = "marketdata_scenario_adapter_v1"
COMPONENT = "build_marketdata_scenario_adapter_lite"

FLAGS = {
    "non_binding":           True,
    "simulation_only":       True,
    "paper_only":            True,
    "live_activation_allowed": False,
}

# ---------------------------------------------------------------------------
# Pure core
# ---------------------------------------------------------------------------

def classify_market(
    market:          str,
    price_entry:     dict | None,
    intents_markets: set,
    now_utc:         datetime.datetime,
    stale_hours:     float = STALE_THRESHOLD_H,
) -> dict:
    """
    Classify one market from its price entry and intent presence.
    Pure function — no I/O, no side effects.

    Args:
        market:          market symbol, e.g. "BTC-EUR"
        price_entry:     dict from worker_market_data.json markets[market],
                         or None if not present
        intents_markets: set of market symbols that have at least one intent
        now_utc:         current UTC datetime (injected for testability)
        stale_hours:     staleness threshold in hours

    Returns:
        dict with keys: market, price_present, price_fresh, data_state,
                        intents_present, review_seed_class
    """
    # ── price presence ───────────────────────────────────────────────────────
    last_price: float = 0.0
    price_ts: str     = ""

    if price_entry and isinstance(price_entry, dict):
        last_price = float(price_entry.get("last_price") or 0.0)
        price_ts   = str(price_entry.get("ts_utc") or "")

    price_present: bool = last_price > 0.0

    # ── staleness ────────────────────────────────────────────────────────────
    price_fresh: bool = False
    if price_present and price_ts:
        try:
            ts = datetime.datetime.fromisoformat(
                price_ts.replace("Z", "+00:00")
            )
            age_h = (now_utc - ts).total_seconds() / 3600.0
            price_fresh = age_h <= stale_hours
        except (ValueError, OverflowError):
            price_fresh = False

    # ── data_state ───────────────────────────────────────────────────────────
    if not price_present:
        data_state = "MISSING"
    elif not price_fresh:
        data_state = "STALE"
    else:
        data_state = "OK"

    # ── intents ──────────────────────────────────────────────────────────────
    intents_present: bool = market in intents_markets

    # ── review_seed_class ────────────────────────────────────────────────────
    if data_state == "MISSING":
        review_seed_class = "DATA_MISSING"
    elif data_state == "STALE":
        review_seed_class = "DATA_STALE"
    elif data_state == "OK" and intents_present:
        review_seed_class = "ALL_CLEAR"
    elif data_state == "OK" and not intents_present:
        review_seed_class = "ZERO_INTENTS"
    else:
        review_seed_class = "HOLD_REVIEW"

    return {
        "market":            market,
        "price_present":     price_present,
        "price_fresh":       price_fresh,
        "data_state":        data_state,
        "intents_present":   intents_present,
        "review_seed_class": review_seed_class,
    }


def build_adapter(
    md_data:      dict | None,
    intents_data: dict | None,
    now_utc:      datetime.datetime,
    stale_hours:  float = STALE_THRESHOLD_H,
) -> dict:
    """
    Build the full adapter output dict from loaded source dicts.
    Pure function — no I/O.

    Args:
        md_data:      parsed worker_market_data.json (or None if absent/corrupt)
        intents_data: parsed rebalance_intents.json  (or None if absent/corrupt)
        now_utc:      current UTC datetime
        stale_hours:  staleness threshold in hours

    Returns:
        marketdata_scenario_adapter dict
    """
    # Extract price entries
    price_entries: dict = {}
    if md_data and isinstance(md_data, dict):
        price_entries = md_data.get("markets") or {}

    # Extract markets that have at least one intent
    intents_markets: set = set()
    if intents_data and isinstance(intents_data, dict):
        for intent in intents_data.get("intents") or []:
            mkt = intent.get("market")
            if mkt:
                intents_markets.add(mkt)

    # Union of all known markets (from both sources)
    all_markets: set = set(price_entries.keys()) | intents_markets
    if not all_markets:
        all_markets = set()

    markets_out = []
    for market in sorted(all_markets):
        entry = classify_market(
            market          = market,
            price_entry     = price_entries.get(market),
            intents_markets = intents_markets,
            now_utc         = now_utc,
            stale_hours     = stale_hours,
        )
        markets_out.append(entry)

    return {
        "version":   VERSION,
        "component": COMPONENT,
        "ts_utc":    now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "markets":   markets_out,
        "sources": {
            "market_data_loaded":  md_data is not None,
            "intents_data_loaded": intents_data is not None,
        },
        "flags": dict(FLAGS),
    }


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> dict | None:
    """Load JSON from path. Returns None if absent or corrupt (fail-closed)."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_adapter_output(result: dict, path: Path = OUTPUT_PATH) -> None:
    """Write adapter output to JSON file, creating parent dirs if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2), encoding="utf-8")


def run_adapter(
    md_path:      Path = MD_PATH,
    intents_path: Path = INTENTS_PATH,
    output_path:  Path = OUTPUT_PATH,
    now_utc:      datetime.datetime | None = None,
) -> dict:
    """
    Full pipeline: load sources → classify → write output → return result.
    """
    if now_utc is None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)

    md_data      = _load_json(md_path)
    intents_data = _load_json(intents_path)

    result = build_adapter(md_data, intents_data, now_utc)
    write_adapter_output(result, output_path)
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    result = run_adapter()
    print(f"Markets classified : {len(result['markets'])}")
    print(f"Sources loaded     : market_data={result['sources']['market_data_loaded']}"
          f"  intents={result['sources']['intents_data_loaded']}")
    print(f"Output             : {OUTPUT_PATH}")
    print()
    for m in result["markets"]:
        print(f"  {m['market']:<12}  state={m['data_state']:<8}  "
              f"price_fresh={str(m['price_fresh']):<5}  "
              f"intents={str(m['intents_present']):<5}  "
              f"→ {m['review_seed_class']}")
    print()
    flag_line = "  " + "  ".join(f"{k}={v}" for k, v in result["flags"].items())
    print(f"Flags: {flag_line}")


if __name__ == "__main__":
    main()
