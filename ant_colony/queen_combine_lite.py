from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Dict, Any

OUTDIR = r"C:\Trading\ANT_OUT"

ALLOC_JSON = os.path.join(OUTDIR, "alloc_targets.json")
RISK_JSON = os.path.join(OUTDIR, "colony_risk_targets.json")
OUT_JSON = os.path.join(OUTDIR, "alloc_targets_combined.json")
HB_JSON = os.path.join(OUTDIR, "queen_combine_lite_heartbeat.json")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: str, default: Dict[str, Any]) -> Dict[str, Any]:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path: str, obj: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="ascii", newline="") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def norm_gate(x: str, default: str = "ALLOW") -> str:
    s = str(x or "").strip().upper()
    if s in ("ALLOW", "BLOCK"):
        return s
    return default


def combine_reason(alloc_reason: str, risk_reason: str, gate: str) -> str:
    ar = str(alloc_reason or "").strip()
    rr = str(risk_reason or "").strip()

    parts = []
    if ar:
        parts.append("ALLOC:" + ar)
    if rr:
        parts.append("RISK:" + rr)

    if not parts:
        return "COMBINED_" + gate

    return "|".join(parts)


def main() -> int:
    os.makedirs(OUTDIR, exist_ok=True)

    ts_utc = utc_now_iso()

    alloc = load_json(ALLOC_JSON, default={
        "version": "alloc_lite_v2_missing",
        "mode": "missing",
        "default_size_mult": 1.0,
        "markets": {}
    })

    risk = load_json(RISK_JSON, default={
        "version": "colony_risk_lite_v1_missing",
        "mode": "missing",
        "markets": {}
    })

    alloc_markets = alloc.get("markets", {}) or {}
    risk_markets = risk.get("markets", {}) or {}

    all_markets = sorted(set(list(alloc_markets.keys()) + list(risk_markets.keys())))

    default_alloc_mult = safe_float(alloc.get("default_size_mult", 1.0), 1.0)

    out_markets: Dict[str, Dict[str, Any]] = {}

    for market in all_markets:
        a = alloc_markets.get(market, {}) or {}
        r = risk_markets.get(market, {}) or {}

        alloc_mult = safe_float(a.get("target_size_mult", default_alloc_mult), default_alloc_mult)
        alloc_reason = str(a.get("reason", "ALLOC_DEFAULT"))
        alloc_gate = "BLOCK" if alloc_mult <= 0.0 else "ALLOW"

        risk_mult = safe_float(r.get("target_size_mult", 1.0), 1.0)
        risk_reason = str(r.get("reason", "RISK_DEFAULT"))
        risk_gate = norm_gate(r.get("gate", "ALLOW"), "ALLOW")

        final_gate = "BLOCK" if (alloc_gate == "BLOCK" or risk_gate == "BLOCK") else "ALLOW"
        final_mult = 0.0 if final_gate == "BLOCK" else min(alloc_mult, risk_mult)

        out_markets[market] = {
            "gate": final_gate,
            "target_size_mult": round(final_mult, 6),
            "reason": combine_reason(alloc_reason, risk_reason, final_gate),
            "alloc_target_size_mult": round(alloc_mult, 6),
            "alloc_reason": alloc_reason,
            "alloc_gate": alloc_gate,
            "risk_target_size_mult": round(risk_mult, 6),
            "risk_reason": risk_reason,
            "risk_gate": risk_gate,
            "asset_class": r.get("asset_class", "unknown"),
            "profile_base_pf": r.get("profile_base_pf", None),
            "profile_max_pf": r.get("profile_max_pf", None),
            "dd_frac": r.get("dd_frac", None),
            "vol_frac": r.get("vol_frac", None),
        }

    out = {
        "version": "alloc_combined_lite_v1",
        "ts_utc": ts_utc,
        "mode": "enabled",
        "sources": {
            "alloc": ALLOC_JSON,
            "risk": RISK_JSON
        },
        "default_size_mult": default_alloc_mult,
        "markets": out_markets
    }

    hb = {
        "ts_utc": ts_utc,
        "component": "queen_combine_lite",
        "mode": "enabled",
        "state": "ok",
        "markets": len(out_markets),
        "alloc_source_exists": os.path.exists(ALLOC_JSON),
        "risk_source_exists": os.path.exists(RISK_JSON),
    }

    write_json(OUT_JSON, out)
    write_json(HB_JSON, hb)

    print(f"{ts_utc} QUEEN_COMBINE_LITE OK markets={len(out_markets)} out={OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())