from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
CORE_DIR = os.path.join(HERE, "core")
if CORE_DIR not in sys.path:
    sys.path.insert(0, CORE_DIR)

from colony_risk_engine import decide_colony_risk
from asset_profiles import get_asset_class

OUTDIR = r"C:\Trading\ANT_OUT"
STATUS_TSV = os.path.join(OUTDIR, "colony_status.tsv")
MARKET_HEALTH_JSON = os.path.join(OUTDIR, "market_health.json")
OUT_JSON = os.path.join(OUTDIR, "colony_risk_targets.json")
HB_JSON = os.path.join(OUTDIR, "queen_colony_risk_lite_heartbeat.json")

DEFAULT_MARKETS = [
    "BTC-EUR",
    "ETH-EUR",
    "SOL-EUR",
    "XRP-EUR",
    "ADA-EUR",
    "BNB-EUR",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        s = str(x).strip().replace(",", ".")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def read_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def read_colony_status(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []

    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)

        if "\t" in sample:
            reader = csv.DictReader(f, delimiter="\t")
        else:
            reader = csv.DictReader(f)

        for row in reader:
            if row:
                rows.append({str(k).strip(): ("" if v is None else str(v).strip()) for k, v in row.items()})
    return rows


def infer_market(row: Dict[str, str]) -> Optional[str]:
    for k in ("market", "symbol", "pair", "instrument"):
        v = row.get(k, "").strip()
        if v:
            return v
    return None


def infer_upstream_gate(row: Dict[str, str]) -> str:
    for k in ("gate", "cb21_gate", "combined_gate", "edge3_combined_gate"):
        v = row.get(k, "").strip().upper()
        if v:
            return v
    return "ALLOW"


def infer_upstream_size_mult(row: Dict[str, str]) -> float:
    for k in ("size_mult", "cb21_size_mult", "combined_size_mult", "edge3_combined_size_mult"):
        if k in row and str(row.get(k, "")).strip() != "":
            return safe_float(row.get(k), 1.0)
    return 1.0


def infer_vol_frac(row: Dict[str, str]) -> float:
    for k in ("vol_frac", "atr_frac", "volatility_frac"):
        if k in row and str(row.get(k, "")).strip() != "":
            return clamp(safe_float(row.get(k), 0.0), 0.0, 1.0)

    vcat = ""
    for k in ("cb20_vol", "vol", "vol_regime"):
        val = row.get(k, "").strip().upper()
        if val:
            vcat = val
            break

    mapping = {
        "LOW": 0.012,
        "MID": 0.030,
        "MED": 0.030,
        "MEDIUM": 0.030,
        "HIGH": 0.060,
    }
    return mapping.get(vcat, 0.020)


def infer_dd_frac(row: Dict[str, str]) -> float:
    for k in ("dd_frac", "drawdown_frac", "max_drawdown_frac"):
        if k in row and str(row.get(k, "")).strip() != "":
            return clamp(safe_float(row.get(k), 0.0), 0.0, 1.0)

    reason_blob = " ".join(
        row.get(k, "") for k in (
            "reason", "cb21_reason", "edge3_health_reason", "edge3_reason"
        )
    ).upper()

    if "BLOCK" in reason_blob:
        return 0.18
    if "WEAK" in reason_blob:
        return 0.09
    if "REDUCE" in reason_blob:
        return 0.07
    return 0.02


def load_market_health(path: str) -> Dict[str, dict]:
    j = read_json(path)
    if not j:
        return {}
    return j.get("markets", {}) or {}


def build_inputs(rows: List[Dict[str, str]], health: Dict[str, dict]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}

    for row in rows:
        market = infer_market(row)
        if not market:
            continue

        dd_frac = infer_dd_frac(row)
        vol_frac = infer_vol_frac(row)

        h = health.get(market, {})
        health_score = safe_float(h.get("health_score", 1.0), 1.0)
        recent_crashes = int(safe_float(h.get("recent_crash_count", 0), 0))
        last_exit_code = int(safe_float(h.get("last_exit_code", 0), 0))

        dd_boost = (1.0 - health_score) * 0.10
        dd_frac = clamp(dd_frac + dd_boost, 0.0, 1.0)

        if recent_crashes >= 2:
            dd_frac = clamp(dd_frac + 0.05, 0.0, 1.0)
        if last_exit_code not in (0,):
            dd_frac = clamp(dd_frac + 0.03, 0.0, 1.0)

        out[market] = {
            "dd_frac": dd_frac,
            "vol_frac": vol_frac,
        }

    for market in DEFAULT_MARKETS:
        if market not in out:
            h = health.get(market, {})
            health_score = safe_float(h.get("health_score", 1.0), 1.0)
            dd_frac = clamp(0.02 + (1.0 - health_score) * 0.10, 0.0, 1.0)
            out[market] = {
                "dd_frac": dd_frac,
                "vol_frac": 0.02,
            }

    return out


def build_upstream(rows: List[Dict[str, str]], health: Dict[str, dict]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}

    for row in rows:
        market = infer_market(row)
        if not market:
            continue

        gate = infer_upstream_gate(row)
        size_mult = infer_upstream_size_mult(row)

        h = health.get(market, {})
        health_gate = str(h.get("health_gate", "ALLOW")).upper()
        health_mult = safe_float(h.get("health_size_mult", 1.0), 1.0)

        if health_gate == "BLOCK":
            gate = "BLOCK"
            size_mult = 0.0
        else:
            size_mult = min(size_mult, health_mult)

        out[market] = {
            "gate": gate,
            "size_mult": size_mult,
            "health_gate": health_gate,
            "health_size_mult": health_mult,
            "health_score": safe_float(h.get("health_score", 1.0), 1.0),
            "health_reason": str(h.get("health_reason", "UNKNOWN")),
        }

    for market in DEFAULT_MARKETS:
        if market not in out:
            h = health.get(market, {})
            health_gate = str(h.get("health_gate", "ALLOW")).upper()
            health_mult = safe_float(h.get("health_size_mult", 1.0), 1.0)

            gate = "BLOCK" if health_gate == "BLOCK" else "ALLOW"
            size_mult = 0.0 if health_gate == "BLOCK" else health_mult

            out[market] = {
                "gate": gate,
                "size_mult": size_mult,
                "health_gate": health_gate,
                "health_size_mult": health_mult,
                "health_score": safe_float(h.get("health_score", 1.0), 1.0),
                "health_reason": str(h.get("health_reason", "UNKNOWN")),
            }

    return out


def write_json(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="ascii", newline="") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def main() -> int:
    os.makedirs(OUTDIR, exist_ok=True)

    ts_utc = utc_now_iso()
    rows = read_colony_status(STATUS_TSV)
    health = load_market_health(MARKET_HEALTH_JSON)

    inputs = build_inputs(rows, health)
    upstream = build_upstream(rows, health)
    decisions = decide_colony_risk(inputs=inputs, upstream=upstream)

    markets = {}
    for market, d in decisions.items():
        up = upstream.get(market, {})
        h = health.get(market, {})

        markets[market] = {
            "gate": d["gate"],
            "target_size_mult": round(float(d["size_mult"]), 6),
            "reason": d["reason"],
            "asset_class": d["asset_class"],
            "profile_base_pf": d["profile_base_pf"],
            "profile_max_pf": d["profile_max_pf"],
            "dd_frac": round(float(d["dd_frac"]), 6),
            "vol_frac": round(float(d["vol_frac"]), 6),
            "upstream_gate": up.get("gate", "ALLOW"),
            "upstream_size_mult": up.get("size_mult", 1.0),
            "health_gate": up.get("health_gate", "ALLOW"),
            "health_size_mult": up.get("health_size_mult", 1.0),
            "health_score": round(safe_float(up.get("health_score", 1.0), 1.0), 6),
            "health_reason": up.get("health_reason", "UNKNOWN"),
            "market_health_last_exit_code": h.get("last_exit_code", None),
            "market_health_recent_crash_count": h.get("recent_crash_count", 0),
            "market_health_cb21_reason": h.get("last_cb21_reason", "UNKNOWN"),
        }

    out = {
        "version": "colony_risk_lite_v2",
        "ts_utc": ts_utc,
        "mode": "enabled",
        "source_status_file": STATUS_TSV,
        "source_market_health_file": MARKET_HEALTH_JSON,
        "markets": markets,
    }

    hb = {
        "ts_utc": ts_utc,
        "component": "queen_colony_risk_lite",
        "mode": "enabled",
        "state": "ok",
        "markets": len(markets),
        "source_rows": len(rows),
        "market_health_loaded": bool(health),
    }

    write_json(OUT_JSON, out)
    write_json(HB_JSON, hb)

    print(f"{ts_utc} QUEEN_COLONY_RISK_LITE OK markets={len(markets)} rows={len(rows)} health_loaded={bool(health)} out={OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())