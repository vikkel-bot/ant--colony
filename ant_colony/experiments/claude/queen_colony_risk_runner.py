"""
AC_DEV - Phase 2: Colony Risk Layer Runner
==========================================
Leest portfolio state + combined colony status.
Draait colony_risk_engine per market.
Schrijft colony_risk_targets.json naar ANT_OUT.

STANDALONE - raakt bitvavo-bot_clean NIET aan.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict

# ── paden ──────────────────────────────────────────────────────────────────
ROOT        = r"C:\Trading\AC_DEV"
ANT_OUT     = os.path.join(ROOT, "ANT_OUT")
PROD_OUT    = r"C:\Trading\ANT_OUT"   # alleen lezen, nooit schrijven

OUT_PATH    = os.path.join(ANT_OUT, "colony_risk_targets.json")

# Bronbestanden (lees uit productie ANT_OUT — read-only)
PORTFOLIO_PATH = os.path.join(PROD_OUT, "paper_portfolio_state.json")
COMBINED_PATH  = os.path.join(PROD_OUT, "combined_colony_status.json")

MARKETS = ["BTC-EUR", "ETH-EUR", "SOL-EUR", "XRP-EUR", "ADA-EUR", "BNB-EUR"]

# ── helpers ────────────────────────────────────────────────────────────────
def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def load_json(path: str) -> dict:
    if not os.path.exists(path):
        print(f"  MISSING: {path}")
        return {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception as e:
        print(f"  ERROR reading {path}: {e}")
        return {}

def atomic_write(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)

# ── risk logic (inline — geen import van productie code) ───────────────────
# Asset profiles voor crypto
CRYPTO_PROFILE = {
    "base_position_frac": 0.50,
    "max_position_frac":  0.75,
    "dd_reduce_at":       0.08,
    "dd_block_at":        0.16,
    "vol_reduce_at":      0.035,
    "vol_block_at":       0.080,
}

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def decide_risk(market: str, dd_frac: float, vol_frac: float,
                upstream_gate: str = "ALLOW", upstream_size_mult: float = 1.0) -> dict:
    p = CRYPTO_PROFILE

    if upstream_gate.upper() != "ALLOW":
        return {"market": market, "gate": "BLOCK", "size_mult": 0.0,
                "reason": "UPSTREAM_BLOCK", "dd_frac": dd_frac, "vol_frac": vol_frac}

    if dd_frac >= p["dd_block_at"]:
        return {"market": market, "gate": "BLOCK", "size_mult": 0.0,
                "reason": "DD_BLOCK", "dd_frac": dd_frac, "vol_frac": vol_frac}

    if vol_frac >= p["vol_block_at"]:
        return {"market": market, "gate": "BLOCK", "size_mult": 0.0,
                "reason": "VOL_BLOCK", "dd_frac": dd_frac, "vol_frac": vol_frac}

    mult = float(upstream_size_mult)
    reasons = []

    if dd_frac >= p["dd_reduce_at"]:
        mult *= 0.5
        reasons.append("DD_REDUCE")

    if vol_frac >= p["vol_reduce_at"]:
        mult *= 0.5
        reasons.append("VOL_REDUCE")

    mult = clamp(mult, 0.0, 1.0)

    return {
        "market":     market,
        "gate":       "ALLOW",
        "size_mult":  round(mult, 4),
        "reason":     "+".join(reasons) if reasons else "RISK_OK",
        "dd_frac":    round(dd_frac, 4),
        "vol_frac":   round(vol_frac, 4),
    }

# ── input extractie ────────────────────────────────────────────────────────
def extract_dd_frac(portfolio: dict) -> float:
    """Bereken drawdown fractie uit portfolio state."""
    equity  = float(portfolio.get("equity", 0) or 0)
    peak    = float(portfolio.get("peak_equity", equity) or equity)
    if peak <= 0:
        return 0.0
    return round(max(0.0, (peak - equity) / peak), 4)

def extract_vol_frac(combined: dict, market: str) -> float:
    """Extraheer vol_frac uit combined colony status voor een market."""
    markets = combined.get("markets", {}) or {}
    m = markets.get(market, {}) or {}
    # cb20 regime info
    cb20 = m.get("cb20", {}) or {}
    vol = cb20.get("vol_frac") or cb20.get("vol") or 0.0
    try:
        return float(vol)
    except (TypeError, ValueError):
        return 0.0

def extract_upstream(combined: dict, market: str) -> tuple[str, float]:
    """Extraheer upstream gate + size_mult uit combined status."""
    markets = combined.get("markets", {}) or {}
    m = markets.get(market, {}) or {}
    edge3 = m.get("edge3", {}) or {}
    gate      = str(edge3.get("gate", "ALLOW"))
    size_mult = float(edge3.get("size_mult", 1.0) or 1.0)
    return gate, size_mult

# ── main ───────────────────────────────────────────────────────────────────
def main() -> int:
    print("=" * 55)
    print("AC_DEV Phase 2 — Colony Risk Layer")
    print("=" * 55)
    print(f"  Reading portfolio: {PORTFOLIO_PATH}")
    print(f"  Reading colony:    {COMBINED_PATH}")
    print()

    portfolio = load_json(PORTFOLIO_PATH)
    combined  = load_json(COMBINED_PATH)

    if not portfolio:
        print("ERROR: geen portfolio data. Draai productie colony eerst.")
        return 1

    dd_frac = extract_dd_frac(portfolio)
    print(f"  Portfolio equity:  {portfolio.get('equity', '?')}")
    print(f"  Drawdown frac:     {dd_frac:.4f} ({dd_frac*100:.1f}%)")
    print()

    results: Dict[str, dict] = {}
    allowed = 0
    blocked = 0

    for market in MARKETS:
        vol_frac                  = extract_vol_frac(combined, market)
        upstream_gate, size_mult  = extract_upstream(combined, market)
        decision = decide_risk(market, dd_frac, vol_frac, upstream_gate, size_mult)
        results[market] = decision

        icon = "✓" if decision["gate"] == "ALLOW" else "✗"
        print(f"  {icon} {market:<10} gate={decision['gate']:<6} "
              f"size={decision['size_mult']:.2f}  reason={decision['reason']}")

        if decision["gate"] == "ALLOW":
            allowed += 1
        else:
            blocked += 1

    output = {
        "version":          "colony_risk_targets_v1",
        "ts_utc":           utc_now(),
        "source_component": "queen_colony_risk_runner",
        "source":           "AC_DEV",
        "portfolio_dd_frac": dd_frac,
        "markets_total":    len(MARKETS),
        "allowed_count":    allowed,
        "blocked_count":    blocked,
        "markets":          results,
    }

    atomic_write(OUT_PATH, output)
    print()
    print(f"  WROTE: {OUT_PATH}")
    print(f"  Summary: {allowed} ALLOW / {blocked} BLOCK")
    print()
    return 0

if __name__ == "__main__":
    sys.exit(main())
