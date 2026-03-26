from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Optional, Dict
import math
import statistics

@dataclass
class EdgeSignal:
    edge_name: str
    side: str
    strength: float
    entry_price: float
    tp_price: float
    sl_price: float
    hold_bars: int
    reason: str

def sma(xs: List[float], n: int) -> List[float]:
    if len(xs) < n:
        return []
    out = []
    s = sum(xs[:n])
    out.append(s / n)
    for i in range(n, len(xs)):
        s += xs[i] - xs[i - n]
        out.append(s / n)
    return out

def stdev_window(xs: List[float], n: int) -> List[float]:
    if len(xs) < n:
        return []
    out = []
    for i in range(n - 1, len(xs)):
        w = xs[i - n + 1:i + 1]
        out.append(statistics.pstdev(w))
    return out

def rsi(xs: List[float], n: int = 14) -> List[float]:
    if len(xs) < n + 1:
        return []
    gains = []
    losses = []
    for i in range(1, len(xs)):
        d = xs[i] - xs[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    out = []
    for i in range(n - 1, len(gains)):
        avg_gain = sum(gains[i - n + 1:i + 1]) / n
        avg_loss = sum(losses[i - n + 1:i + 1]) / n
        rs = avg_gain / (avg_loss + 1e-12)
        out.append(100.0 - (100.0 / (1.0 + rs)))
    return out

def bollinger(closes: List[float], n: int = 20, k: float = 2.0) -> Dict[str, List[float]]:
    mid = sma(closes, n)
    sd = stdev_window(closes, n)
    if not mid or not sd:
        return {"mid": [], "upper": [], "lower": [], "width": []}
    upper = []
    lower = []
    width = []
    for m, s in zip(mid, sd):
        u = m + k * s
        l = m - k * s
        upper.append(u)
        lower.append(l)
        width.append((u - l) / m if m else 0.0)
    return {"mid": mid, "upper": upper, "lower": lower, "width": width}

def detect_extreme_mean_reversion(closes: List[float]) -> Optional[EdgeSignal]:
    if len(closes) < 50:
        return None
    rs = rsi(closes, 14)
    bb = bollinger(closes, 20, 2.0)
    if not rs or not bb["lower"]:
        return None

    cur = closes[-1]
    cur_rsi = rs[-1]
    cur_lower = bb["lower"][-1]
    cur_mid = bb["mid"][-1]
    cur_width = bb["width"][-1]

    if cur_rsi > 25.0:
        return None
    if cur_width > 0.08:
        return None
    if cur > cur_lower * 1.01:
        return None

    strength = min(1.0, ((25.0 - cur_rsi) / 25.0) + max(0.0, (0.08 - cur_width) / 0.08))
    return EdgeSignal(
        edge_name="extreme_mean_reversion",
        side="long",
        strength=round(strength, 4),
        entry_price=cur,
        tp_price=round(cur_mid, 8),
        sl_price=round(cur * 0.975, 8),
        hold_bars=6,
        reason="RSI_LT_25_AND_BB_SQUEEZE"
    )

def detect_volume_breakout(closes: List[float], volumes: List[float]) -> Optional[EdgeSignal]:
    if len(closes) < 30 or len(volumes) < 30:
        return None
    cur = closes[-1]
    prev = closes[-2]
    avg_vol = sum(volumes[-21:-1]) / 20.0
    cur_vol = volumes[-1]
    vol_mult = cur_vol / (avg_vol + 1e-12)
    ret = (cur / prev) - 1.0 if prev else 0.0

    if vol_mult < 2.0:
        return None
    if ret < 0.0075:
        return None

    strength = min(1.0, (vol_mult / 3.0) * 0.6 + (ret / 0.02) * 0.4)
    return EdgeSignal(
        edge_name="volume_breakout",
        side="long",
        strength=round(strength, 4),
        entry_price=cur,
        tp_price=round(cur * 1.02, 8),
        sl_price=round(cur * 0.985, 8),
        hold_bars=4,
        reason="VOL_GE_2X_AND_MOMENTUM"
    )

def detect_pairs_zscore(spread_series: List[float]) -> Optional[EdgeSignal]:
    if len(spread_series) < 60:
        return None
    w = spread_series[-60:]
    mu = statistics.mean(w[:-1])
    sd = statistics.pstdev(w[:-1]) + 1e-12
    z = (w[-1] - mu) / sd

    if abs(z) < 2.0:
        return None

    side = "short_spread" if z > 0 else "long_spread"
    strength = min(1.0, abs(z) / 4.0)
    return EdgeSignal(
        edge_name="pairs_zscore_reversion",
        side=side,
        strength=round(strength, 4),
        entry_price=round(w[-1], 8),
        tp_price=round(mu, 8),
        sl_price=round(w[-1] + (1.0 if z > 0 else -1.0) * sd, 8),
        hold_bars=12,
        reason="SPREAD_ZSCORE_EXTREME"
    )

def edge_snapshot(closes: List[float], volumes: Optional[List[float]] = None) -> Dict:
    out = {
        "extreme_mean_reversion": None,
        "volume_breakout": None,
    }
    sig = detect_extreme_mean_reversion(closes)
    if sig:
        out["extreme_mean_reversion"] = asdict(sig)
    if volumes:
        sig2 = detect_volume_breakout(closes, volumes)
        if sig2:
            out["volume_breakout"] = asdict(sig2)
    return out