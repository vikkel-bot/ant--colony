# cb20_regime.py
# CB-20 Regime Detection (observatie/gating layer boven EDGE3)
# - leest candle cache (data_cache/*)
# - ondersteunt candles als LIST [ts,o,h,l,c,v] én als DICT {"timestamp"/"ts", "open"/"o", "high"/"h", "low"/"l", "close"/"c", "volume"/"v"}
# - schrijft: reports/cb20_regime.json (snapshot) + reports/cb20_regime.jsonl (history)

from __future__ import annotations
import os, json, glob
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default

def load_candles_from_cache(path: str) -> List[Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # cache can be: {"meta":..., "candles":[...]} OR pure list
    if isinstance(data, dict) and "candles" in data:
        return data["candles"] or []
    if isinstance(data, list):
        return data
    return []

def pick_best_cache(market: str, interval: str) -> Optional[str]:
    pats = [
        os.path.join("data_cache", f"{market}_{interval}_*.json"),
        os.path.join("data_cache", f"{market}_{interval}_*.JSON"),
    ]
    files: List[str] = []
    for p in pats:
        files.extend(glob.glob(p))
    if not files:
        return None
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    return files[0]

def candle_get(c: Any) -> Dict[str, float]:
    """
    Normalise one candle into dict with keys: ts,o,h,l,c,v (all floats except ts int)
    Accepts:
      - list/tuple: [ts,o,h,l,c,v]
      - dict: keys like timestamp/ts, open/o, high/h, low/l, close/c, volume/v
    """
    if isinstance(c, (list, tuple)) and len(c) >= 5:
        return {
            "ts": _safe_int(c[0]),
            "o": _safe_float(c[1]),
            "h": _safe_float(c[2]),
            "l": _safe_float(c[3]),
            "c": _safe_float(c[4]),
            "v": _safe_float(c[5]) if len(c) > 5 else 0.0,
        }
    if isinstance(c, dict):
        ts = c.get("ts", c.get("timestamp", c.get("time", c.get("t", 0))))
        o  = c.get("o", c.get("open", 0.0))
        h  = c.get("h", c.get("high", 0.0))
        l  = c.get("l", c.get("low", 0.0))
        cl = c.get("c", c.get("close", 0.0))
        v  = c.get("v", c.get("volume", 0.0))
        return {"ts": _safe_int(ts), "o": _safe_float(o), "h": _safe_float(h), "l": _safe_float(l), "c": _safe_float(cl), "v": _safe_float(v)}
    return {"ts": 0, "o": 0.0, "h": 0.0, "l": 0.0, "c": 0.0, "v": 0.0}

def ema(values: List[float], period: int) -> List[Optional[float]]:
    if period <= 1:
        return [v for v in values]
    k = 2.0 / (period + 1.0)
    out: List[Optional[float]] = [None] * len(values)
    e: Optional[float] = None
    for i, v in enumerate(values):
        if e is None:
            e = v
        else:
            e = (v - e) * k + e
        out[i] = e
    return out

def true_range(h: float, l: float, prev_c: float) -> float:
    return max(h - l, abs(h - prev_c), abs(l - prev_c))

def atr(candles_n: List[Dict[str, float]], period: int) -> List[Optional[float]]:
    if len(candles_n) < 2:
        return [None] * len(candles_n)
    trs: List[float] = []
    prev_c = candles_n[0]["c"]
    trs.append(candles_n[0]["h"] - candles_n[0]["l"])
    for i in range(1, len(candles_n)):
        h = candles_n[i]["h"]; l = candles_n[i]["l"]; c = candles_n[i]["c"]
        trs.append(true_range(h, l, prev_c))
        prev_c = c
    out: List[Optional[float]] = [None] * len(candles_n)
    if len(trs) < period:
        return out
    a = sum(trs[:period]) / period
    out[period - 1] = a
    for i in range(period, len(trs)):
        a = (a * (period - 1) + trs[i]) / period
        out[i] = a
    return out

def percentile_rank(window: List[float], x: float) -> float:
    if not window:
        return 0.5
    lt = sum(1 for v in window if v < x)
    eq = sum(1 for v in window if v == x)
    return (lt + 0.5 * eq) / len(window)

@dataclass
class CB20Config:
    market: str = "BTC-EUR"
    interval: str = "4h"
    ema_period: int = 200
    slope_n: int = 20
    atr_period: int = 14
    atr_regime_window: int = 200
    vol_low_p: float = 0.30
    vol_high_p: float = 0.70

def compute_cb20_regime(candles_raw: List[Any], cfg: CB20Config) -> Dict[str, Any]:
    candles_n = [candle_get(c) for c in candles_raw]
    closes = [c["c"] for c in candles_n]
    ts = candles_n[-1]["ts"] if candles_n else 0
    close = closes[-1] if closes else 0.0

    ema_series = ema(closes, cfg.ema_period)
    ema_last = ema_series[-1] if ema_series and ema_series[-1] is not None else None

    slope = None
    if len(ema_series) > cfg.slope_n and ema_series[-1] is not None and ema_series[-1 - cfg.slope_n] is not None:
        slope = _safe_float(ema_series[-1]) - _safe_float(ema_series[-1 - cfg.slope_n])

    atr_series = atr(candles_n, cfg.atr_period)
    atr_last = atr_series[-1] if atr_series and atr_series[-1] is not None else None
    atr_pct = (atr_last / close) if (atr_last is not None and close > 0) else None

    # ATR% percentile over regime window
    atrp_hist: List[float] = []
    start_i = max(0, len(candles_n) - cfg.atr_regime_window)
    for i in range(start_i, len(candles_n)):
        if atr_series[i] is None:
            continue
        c = closes[i]
        if c <= 0:
            continue
        atrp_hist.append(_safe_float(atr_series[i]) / c)

    atrp_rank = percentile_rank(atrp_hist, atr_pct) if (atr_pct is not None and atrp_hist) else 0.5

    trend = "UNKNOWN"
    if ema_last is not None and slope is not None:
        if close > ema_last and slope > 0:
            trend = "TREND_UP"
        elif close < ema_last and slope < 0:
            trend = "TREND_DOWN"
        else:
            trend = "SIDEWAYS"

    if atrp_rank <= cfg.vol_low_p:
        vol = "VOL_LOW"
    elif atrp_rank >= cfg.vol_high_p:
        vol = "VOL_HIGH"
    else:
        vol = "VOL_MID"

    gate = "ALLOW"
    if trend == "TREND_UP" and vol == "VOL_HIGH":
        gate = "BLOCK"

    size_mult = 1.0
    if gate == "BLOCK":
        size_mult = 0.0
    elif vol == "VOL_HIGH":
        size_mult = 0.5

    return {
        "ts_utc": now_utc(),
        "market": cfg.market,
        "interval": cfg.interval,
        "candle_ts": ts,
        "close": close,
        "ema200": ema_last,
        "ema_slope_n": cfg.slope_n,
        "ema_slope": slope,
        "atr14": atr_last,
        "atr_pct": atr_pct,
        "atr_pct_rank": atrp_rank,
        "trend_regime": trend,
        "vol_regime": vol,
        "gate": gate,
        "size_mult": size_mult,
        "config": asdict(cfg),
    }

def atomic_write(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)

def append_jsonl(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")

def main():
    market = os.getenv("CB20_MARKET", "BTC-EUR").strip()
    interval = os.getenv("CB20_INTERVAL", "4h").strip()
    cfg = CB20Config(market=market, interval=interval)

    cache = pick_best_cache(market, interval)
    if not cache:
        raise SystemExit(f"CB20 ERROR: no cache found for {market} {interval} in data_cache/")

    candles = load_candles_from_cache(cache)
    if len(candles) < 50:
        raise SystemExit(f"CB20 ERROR: cache too small: {cache} candles={len(candles)}")

    snap = compute_cb20_regime(candles, cfg)

    atomic_write(os.path.join("reports", "cb20_regime.json"), snap)
    append_jsonl(os.path.join("reports", "cb20_regime.jsonl"), snap)

    line = f"{snap['ts_utc']} {snap['market']} {snap['interval']} trend={snap['trend_regime']} vol={snap['vol_regime']} gate={snap['gate']} size={snap['size_mult']}"
    atomic_write(os.path.join("reports", "cb20_regime.txt"), {"line": line})
    print(line)

if __name__ == "__main__":
    main()


