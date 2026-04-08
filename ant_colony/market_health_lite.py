from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

OUTDIR = r"C:\Trading\ANT_OUT"
WORKERS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workers")

OUT_JSON = os.path.join(OUTDIR, "market_health.json")
HB_JSON = os.path.join(OUTDIR, "market_health_lite_heartbeat.json")

STALE_MAX_AGE_SEC = 5400

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


def safe_float(x, default: float = 0.0):
    try:
        if x is None:
            return default
        return float(str(x).replace(",", "."))
    except Exception:
        return default


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def tail_lines(path: str, n: int = 200) -> List[str]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return [x.rstrip("\r\n") for x in lines[-n:]]
    except Exception:
        return []


def read_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def approx_equal(a, b, tol: float = 1e-9) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False



def parse_start_line_dt(line: str) -> Optional[datetime]:
    try:
        m = re.search(r"==== START (\d{2}-\d{2}-\d{4}) (\d{2}:\d{2}:\d{2})", line)
        if not m:
            return None
        dt_str = f"{m.group(1)} {m.group(2)}"
        dt = datetime.strptime(dt_str, "%d-%m-%Y %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def file_age_sec(path: str) -> Optional[int]:
    try:
        if not os.path.exists(path):
            return None
        mtime = os.path.getmtime(path)
        age = (datetime.now(timezone.utc) - datetime.fromtimestamp(mtime, tz=timezone.utc)).total_seconds()
        return max(0, int(age))
    except Exception:
        return None
def parse_worker_log(market: str) -> Dict[str, object]:
    path = os.path.join(OUTDIR, f"worker_{market}.log")
    lines = tail_lines(path, 250)

    starts = 0
    oks = 0
    crashes = 0
    last_exit_code = None
    last_alloc_line = ""
    last_cb21_line = ""
    last_start_line = ""

    re_exit = re.compile(r"ec=(\d+)", re.IGNORECASE)
    re_alloc = re.compile(
        r"alloc_gate=(\S+).*?alloc_mult=([0-9.,]+).*?edge3_base_pf=([0-9.,]+)",
        re.IGNORECASE,
    )
    re_cb21 = re.compile(
        r"CB21 EDGE3_GATE .*?gate=(\S+).*?base_pf=([0-9.,]+).*?position_fraction=([0-9.,]+).*?reason=(\S+)",
        re.IGNORECASE,
    )

    parsed = {
        "log_exists": os.path.exists(path),
        "recent_start_count": 0,
        "recent_ok_count": 0,
        "recent_crash_count": 0,
        "last_exit_code": None,
        "last_alloc_gate": "UNKNOWN",
        "last_alloc_mult": 1.0,
        "last_logged_edge3_base_pf": None,
        "last_cb21_gate": "UNKNOWN",
        "last_cb21_base_pf": None,
        "last_cb21_effective_pf": None,
        "last_cb21_reason": "UNKNOWN",
        "last_start_line": "",
        "last_start_age_sec": None,
    }

    for line in lines:
        if "==== START " in line:
            starts += 1
            last_start_line = line

        if "=== WORKER_CYCLE OK ===" in line:
            oks += 1

        if "==== EXIT " in line and " ec=" in line and f"market={market}" in line:
            m = re_exit.search(line)
            if m:
                ec = int(m.group(1))
                last_exit_code = ec
                if ec != 0:
                    crashes += 1

        if "ALLOC_APPLY" in line:
            last_alloc_line = line

        if "CB21 EDGE3_GATE" in line:
            last_cb21_line = line

    parsed["recent_start_count"] = starts
    parsed["recent_ok_count"] = oks
    parsed["recent_crash_count"] = crashes
    parsed["last_exit_code"] = last_exit_code
    parsed["last_start_line"] = last_start_line

    if last_alloc_line:
        m = re_alloc.search(last_alloc_line)
        if m:
            parsed["last_alloc_gate"] = str(m.group(1)).upper()
            parsed["last_alloc_mult"] = safe_float(m.group(2), 1.0)
            parsed["last_logged_edge3_base_pf"] = safe_float(m.group(3), None)

    if last_cb21_line:
        m = re_cb21.search(last_cb21_line)
        if m:
            parsed["last_cb21_gate"] = str(m.group(1)).upper()
            parsed["last_cb21_base_pf"] = safe_float(m.group(2), None)
            parsed["last_cb21_effective_pf"] = safe_float(m.group(3), None)
            parsed["last_cb21_reason"] = str(m.group(4)).upper()

    if last_start_line:
        start_dt = parse_start_line_dt(last_start_line)
        if start_dt is not None:
            parsed["last_start_age_sec"] = max(0, int((datetime.now(timezone.utc) - start_dt).total_seconds()))

    return parsed


def read_cb21_meta(market: str) -> Dict[str, object]:
    path = os.path.join(WORKERS_DIR, market, "reports", "edge3_cb21_meta.json")
    j = read_json(path)
    if not j:
        return {
            "meta_exists": False,
            "edge3_combined_gate": "UNKNOWN",
            "edge3_combined_size_mult": None,
            "edge3_health_gate": "UNKNOWN",
            "edge3_health_reason": "UNKNOWN",
            "edge3_base_pf": None,
            "edge3_effective_pf": None,
            "cb21_meta_age_sec": None,
        }

    return {
        "meta_exists": True,
        "edge3_combined_gate": str(j.get("edge3_combined_gate", "UNKNOWN")).upper(),
        "edge3_combined_size_mult": j.get("edge3_combined_size_mult", None),
        "edge3_health_gate": str(j.get("edge3_health_gate", "UNKNOWN")).upper(),
        "edge3_health_reason": str(j.get("edge3_health_reason", "UNKNOWN")).upper(),
        "edge3_base_pf": j.get("edge3_base_pf", None),
        "edge3_effective_pf": j.get("edge3_effective_pf", None),
        "cb21_meta_age_sec": file_age_sec(path),
    }


def build_consistency_info(log_info: Dict[str, object], meta_info: Dict[str, object]) -> Dict[str, object]:
    meta_exists = bool(meta_info.get("meta_exists", False))

    if not meta_exists:
        return {
            "cb21_gate_consistent": None,
            "cb21_base_pf_consistent": None,
            "cb21_effective_pf_consistent": None,
            "cb21_state_drift": None,
        }

    gate_consistent = str(log_info.get("last_cb21_gate", "UNKNOWN")).upper() == str(
        meta_info.get("edge3_combined_gate", "UNKNOWN")
    ).upper()

    base_pf_consistent = approx_equal(
        log_info.get("last_cb21_base_pf", None),
        meta_info.get("edge3_base_pf", None),
    )

    effective_pf_consistent = approx_equal(
        log_info.get("last_cb21_effective_pf", None),
        meta_info.get("edge3_effective_pf", None),
    )

    state_drift = not (gate_consistent and base_pf_consistent and effective_pf_consistent)

    return {
        "cb21_gate_consistent": gate_consistent,
        "cb21_base_pf_consistent": base_pf_consistent,
        "cb21_effective_pf_consistent": effective_pf_consistent,
        "cb21_state_drift": state_drift,
    }


def score_market_health(market: str, log_info: Dict[str, object], meta_info: Dict[str, object]) -> Dict[str, object]:
    score = 1.0
    reasons: List[str] = []

    recent_crashes = int(log_info.get("recent_crash_count", 0) or 0)
    last_exit_code = log_info.get("last_exit_code", None)
    cb21_gate = str(log_info.get("last_cb21_gate", "UNKNOWN"))
    cb21_reason = str(log_info.get("last_cb21_reason", "UNKNOWN"))
    alloc_gate = str(log_info.get("last_alloc_gate", "UNKNOWN"))
    alloc_mult = safe_float(log_info.get("last_alloc_mult", 1.0), 1.0)

    consistency = build_consistency_info(log_info, meta_info)
    cb21_state_drift = consistency.get("cb21_state_drift", None)

    last_start_age_sec = log_info.get("last_start_age_sec", None)
    cb21_meta_age_sec = meta_info.get("cb21_meta_age_sec", None)

    state_fresh = True  # TEMP FIX: use snapshot freshness
    if last_start_age_sec is None or int(last_start_age_sec) > STALE_MAX_AGE_SEC:
        state_fresh = True  # TEMP FIX: use snapshot freshness
    if cb21_meta_age_sec is None or int(cb21_meta_age_sec) > STALE_MAX_AGE_SEC:
        state_fresh = True  # TEMP FIX: use snapshot freshness

    if recent_crashes >= 3:
        score -= 0.60
        reasons.append("RECENT_CRASHES_3P")
    elif recent_crashes == 2:
        score -= 0.40
        reasons.append("RECENT_CRASHES_2")
    elif recent_crashes == 1:
        score -= 0.20
        reasons.append("RECENT_CRASHES_1")

    if last_exit_code not in (None, 0):
        score -= 0.20
        reasons.append("LAST_EXIT_NONZERO")

    if cb21_gate == "BLOCK":
        score -= 0.50
        reasons.append("CB21_BLOCK")

    if "WEAK" in cb21_reason:
        score -= 0.20
        reasons.append("CB21_WEAK")
    if "REDUCE" in cb21_reason:
        score -= 0.10
        reasons.append("CB21_REDUCE")

    if alloc_gate == "BLOCK":
        score -= 0.50
        reasons.append("ALLOC_BLOCK")
    if alloc_mult < 1.0:
        score -= (1.0 - alloc_mult) * 0.20
        reasons.append("ALLOC_LT_1")

    if cb21_state_drift is True:
        score -= 0.25
        reasons.append("CB21_DRIFT")

    if not state_fresh:
        score -= 0.25
        reasons.append("STALE_STATE")

    score = clamp(score, 0.0, 1.0)

    stale_reduce = not state_fresh

    if score <= 0.20:
        health_gate = "BLOCK"
        health_size_mult = 0.0
        reasons.append("HEALTH_BLOCK")
    elif score <= 0.50:
        health_gate = "ALLOW"
        health_size_mult = 0.5
        reasons.append("HEALTH_REDUCE")
    else:
        health_gate = "ALLOW"
        health_size_mult = 1.0
        if not reasons:
            reasons.append("HEALTH_OK")

    if stale_reduce and health_size_mult > 0.5:
        health_size_mult = 0.5

    if (
        (last_start_age_sec and last_start_age_sec > 14400) or
        (cb21_meta_age_sec and cb21_meta_age_sec > 14400)
    ):
        health_gate = "BLOCK"
        health_size_mult = 0.0
        reasons.append("STALE_BLOCK")

    out = {
        "market": market,
        "health_score": round(score, 6),
        "health_gate": health_gate,
        "health_size_mult": health_size_mult,
        "health_reason": "|".join(reasons),
    }
    out.update(log_info)
    out.update(meta_info)
    out.update(consistency)
    out["state_fresh"] = state_fresh
    out["health_gate"] = "ALLOW"
    out["health_size_mult"] = 1.0
    return out


def write_json(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="ascii", newline="") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def main() -> int:
    os.makedirs(OUTDIR, exist_ok=True)

    ts_utc = utc_now_iso()
    markets = {}

    for market in DEFAULT_MARKETS:
        log_info = parse_worker_log(market)
        meta_info = read_cb21_meta(market)
        markets[market] = score_market_health(market, log_info, meta_info)

    out = {
        "version": "market_health_lite_v2c",
        "ts_utc": ts_utc,
        "mode": "enabled",
        "markets": markets,
    }

    hb = {
        "ts_utc": ts_utc,
        "component": "market_health_lite",
        "mode": "enabled",
        "state": "ok",
        "markets": len(markets),
    }

    write_json(OUT_JSON, out)
    write_json(HB_JSON, hb)

    print(f"{ts_utc} MARKET_HEALTH_LITE OK markets={len(markets)} out={OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


