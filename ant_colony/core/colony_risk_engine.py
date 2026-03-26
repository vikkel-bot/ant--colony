from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Optional

from asset_profiles import get_profile_for_market

@dataclass
class RiskDecision:
    market: str
    asset_class: str
    gate: str
    size_mult: float
    reason: str
    profile_base_pf: float
    profile_max_pf: float
    dd_frac: float
    vol_frac: float

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def decide_market_risk(
    market: str,
    dd_frac: float,
    vol_frac: float,
    upstream_gate: str = "ALLOW",
    upstream_size_mult: float = 1.0,
) -> RiskDecision:
    p = get_profile_for_market(market)
    gate = (upstream_gate or "ALLOW").upper()

    if gate != "ALLOW":
        return RiskDecision(
            market=market,
            asset_class=p.asset_class,
            gate="BLOCK",
            size_mult=0.0,
            reason="UPSTREAM_BLOCK",
            profile_base_pf=p.base_position_frac,
            profile_max_pf=p.max_position_frac,
            dd_frac=dd_frac,
            vol_frac=vol_frac,
        )

    if dd_frac >= p.dd_block_at:
        return RiskDecision(
            market=market,
            asset_class=p.asset_class,
            gate="BLOCK",
            size_mult=0.0,
            reason="DD_BLOCK",
            profile_base_pf=p.base_position_frac,
            profile_max_pf=p.max_position_frac,
            dd_frac=dd_frac,
            vol_frac=vol_frac,
        )

    mult = float(upstream_size_mult)
    reasons = []

    if dd_frac >= p.dd_reduce_at:
        mult *= 0.5
        reasons.append("DD_REDUCE")

    if vol_frac >= p.vol_block_at:
        return RiskDecision(
            market=market,
            asset_class=p.asset_class,
            gate="BLOCK",
            size_mult=0.0,
            reason="VOL_BLOCK",
            profile_base_pf=p.base_position_frac,
            profile_max_pf=p.max_position_frac,
            dd_frac=dd_frac,
            vol_frac=vol_frac,
        )

    if vol_frac >= p.vol_reduce_at:
        mult *= 0.5
        reasons.append("VOL_REDUCE")

    mult = clamp(mult, 0.0, 1.0)

    return RiskDecision(
        market=market,
        asset_class=p.asset_class,
        gate="ALLOW",
        size_mult=mult,
        reason="+".join(reasons) if reasons else "RISK_OK",
        profile_base_pf=p.base_position_frac,
        profile_max_pf=p.max_position_frac,
        dd_frac=dd_frac,
        vol_frac=vol_frac,
    )

def decide_colony_risk(
    inputs: Dict[str, Dict[str, float]],
    upstream: Optional[Dict[str, Dict[str, float]]] = None,
) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    upstream = upstream or {}

    for market, vals in inputs.items():
        dd_frac = float(vals.get("dd_frac", 0.0))
        vol_frac = float(vals.get("vol_frac", 0.0))

        up = upstream.get(market, {})
        rd = decide_market_risk(
            market=market,
            dd_frac=dd_frac,
            vol_frac=vol_frac,
            upstream_gate=str(up.get("gate", "ALLOW")),
            upstream_size_mult=float(up.get("size_mult", 1.0)),
        )
        out[market] = asdict(rd)

    return out