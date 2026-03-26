from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

@dataclass(frozen=True)
class AssetProfile:
    asset_class: str
    base_position_frac: float
    max_position_frac: float
    dd_reduce_at: float
    dd_block_at: float
    vol_reduce_at: float
    vol_block_at: float
    notes: str

ASSET_PROFILES: Dict[str, AssetProfile] = {
    "crypto": AssetProfile(
        asset_class="crypto",
        base_position_frac=0.50,
        max_position_frac=0.75,
        dd_reduce_at=0.08,
        dd_block_at=0.16,
        vol_reduce_at=0.035,
        vol_block_at=0.080,
        notes="Higher volatility, faster adaptation, wider tolerance."
    ),
    "etf": AssetProfile(
        asset_class="etf",
        base_position_frac=0.20,
        max_position_frac=0.30,
        dd_reduce_at=0.05,
        dd_block_at=0.10,
        vol_reduce_at=0.020,
        vol_block_at=0.045,
        notes="Conservative sizing, lower volatility, steadier capital curve."
    ),
    "commodity": AssetProfile(
        asset_class="commodity",
        base_position_frac=0.10,
        max_position_frac=0.20,
        dd_reduce_at=0.04,
        dd_block_at=0.08,
        vol_reduce_at=0.018,
        vol_block_at=0.040,
        notes="Margin-aware, strict drawdown and volatility discipline."
    ),
}

MARKET_TO_ASSET_CLASS: Dict[str, str] = {
    "BTC-EUR": "crypto",
    "ETH-EUR": "crypto",
    "SOL-EUR": "crypto",
    "XRP-EUR": "crypto",
    "ADA-EUR": "crypto",
    "BNB-EUR": "crypto",
}

def get_asset_class(market: str, default: str = "crypto") -> str:
    return MARKET_TO_ASSET_CLASS.get(market, default)

def get_profile_for_market(market: str) -> AssetProfile:
    asset_class = get_asset_class(market)
    return ASSET_PROFILES[asset_class]