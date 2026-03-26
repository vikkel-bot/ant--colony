from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
from typing import Any, Dict, Optional

ROOT = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(ROOT, "reports")

CB20_PATH = os.path.join(REPORTS_DIR, "cb20_regime.json")
EDGE3_SNAPSHOT_PATH = os.path.join(REPORTS_DIR, "edge3_snapshot.json")
META_PATH = os.path.join(REPORTS_DIR, "edge3_cb21_meta.json")
HEALTH_PATH = os.path.join(REPORTS_DIR, "cb19_health.txt")  # 1-line status, same contract as CB19

def utc_now_iso_z() -> str:
    # "Z" style to match cb19_health formatting
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def atomic_write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n")
    os.replace(tmp, path)

def atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)

def read_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default

def env_str(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default

def file_age_seconds(path: str) -> Optional[int]:
    if not os.path.exists(path):
        return None
    try:
        mtime = dt.datetime.fromtimestamp(os.path.getmtime(path), tz=dt.timezone.utc)
        age = dt.datetime.now(dt.timezone.utc) - mtime
        return int(age.total_seconds())
    except Exception:
        return None

def health_policy(edge3_snap: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Simple defensive rules (no curve-fit):
    - if EDGE3 missing or status != OK -> BLOCK (size_mult=0)
    - else if pf < 1.0 OR equity < 950 -> ALLOW but size_mult=0.5
    - else -> ALLOW size_mult=1.0
    """
    if not edge3_snap:
        return {"health_gate": "BLOCK", "health_size_mult": 0.0, "reason": "EDGE3_SNAPSHOT_MISSING"}
    st = str(edge3_snap.get("status", ""))
    if st != "OK":
        return {"health_gate": "BLOCK", "health_size_mult": 0.0, "reason": f"EDGE3_STATUS_{st}"}

    pf = edge3_snap.get("profit_factor", None)
    eq = edge3_snap.get("ending_equity", None)

    if pf is None or eq is None:
        return {"health_gate": "ALLOW", "health_size_mult": 0.5, "reason": "EDGE3_METRICS_MISSING"}

    try:
        pf_f = float(pf)
        eq_f = float(eq)
    except Exception:
        return {"health_gate": "ALLOW", "health_size_mult": 0.5, "reason": "EDGE3_METRICS_PARSE_FAIL"}

    if pf_f < 1.0 or eq_f < 950.0:
        return {"health_gate": "ALLOW", "health_size_mult": 0.5, "reason": "EDGE3_WEAK"}
    return {"health_gate": "ALLOW", "health_size_mult": 1.0, "reason": "EDGE3_OK"}

def main() -> int:
    os.makedirs(REPORTS_DIR, exist_ok=True)

    base_pf = env_float("EDGE3_BASE_POSITION_FRACTION", 0.50)

    cb20 = read_json(CB20_PATH)
    cb20_market = cb20.get("market") if cb20 else None
    cb20_interval = cb20.get("interval") if cb20 else None
    cb20_trend = cb20.get("trend_regime") if cb20 else None
    cb20_vol = cb20.get("vol_regime") if cb20 else None
    cb20_gate = cb20.get("gate") if cb20 else "ALLOW"
    cb20_size_mult = float(cb20.get("size_mult", 1.0)) if cb20 else 1.0

    edge3_prev = read_json(EDGE3_SNAPSHOT_PATH)
    hp = health_policy(edge3_prev)

    gate = "ALLOW"
    if str(cb20_gate).upper() != "ALLOW":
        gate = "BLOCK"
    if hp["health_gate"] != "ALLOW":
        gate = "BLOCK"

    size_mult = cb20_size_mult * float(hp["health_size_mult"])
    effective_pf = base_pf * size_mult

    # Window: env override else last 730d
    now = dt.datetime.now(dt.timezone.utc)
    default_end = now
    default_start = now - dt.timedelta(days=730)

    start_iso_env = os.getenv("EDGE3_START_ISO", "").strip()
    end_iso_env = os.getenv("EDGE3_END_ISO", "").strip()

    start_iso = start_iso_env if start_iso_env else default_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end_iso_env if end_iso_env else default_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    meta = {
        "ts_utc": utc_now_iso(),
        "cb20_market": cb20_market,
        "cb20_interval": cb20_interval,
        "cb20_trend": cb20_trend,
        "cb20_vol": cb20_vol,
        "cb20_gate": cb20_gate,
        "cb20_size_mult": cb20_size_mult,
        "edge3_health_gate": hp["health_gate"],
        "edge3_health_size_mult": hp["health_size_mult"],
        "edge3_health_reason": hp["reason"],
        "edge3_combined_gate": gate,
        "edge3_combined_size_mult": size_mult,
        "edge3_base_pf": base_pf,
        "edge3_effective_pf": effective_pf,
        "edge3_start_iso": start_iso,
        "edge3_end_iso": end_iso,
    }
    atomic_write_json(META_PATH, meta)

    print(
        f"{utc_now_iso()} CB21 EDGE3_GATE gate={gate} trend={cb20_trend} vol={cb20_vol} "
        f"cb20_mult={cb20_size_mult:.2f} health_mult={float(hp['health_size_mult']):.2f} "
        f"base_pf={base_pf:.2f} -> position_fraction={effective_pf:.4f} reason={hp['reason']}"
    )

    # If blocked, still write health line (manual visibility)
    age_e3 = file_age_seconds(EDGE3_SNAPSHOT_PATH)
    e3_status = edge3_prev.get("status", "NOFILE") if edge3_prev else "NOFILE"
    pf = edge3_prev.get("profit_factor", None) if edge3_prev else None
    eq = edge3_prev.get("ending_equity", None) if edge3_prev else None

    def fmt(x: Any) -> str:
        if x is None:
            return "nan"
        try:
            return str(float(x))
        except Exception:
            return "nan"

    health_line = (
        f"{utc_now_iso_z()} gate={gate} trend={cb20_trend or 'NA'} vol={cb20_vol or 'NA'} "
        f"edge3_status={e3_status} pf={fmt(pf)} equity={fmt(eq)} snap_age_s={age_e3 if age_e3 is not None else 'nan'}"
    )
    atomic_write_text(HEALTH_PATH, health_line)

    if gate != "ALLOW" or effective_pf <= 0:
        return 0

    run_py = os.path.join(ROOT, "run_edge3_fast.py")
    if not os.path.exists(run_py):
        print(f"{utc_now_iso()} EDGE3_FATAL: missing run_edge3_fast.py")
        return 2

    market = env_str("CB20_MARKET", "BTC-EUR")
    interval = env_str("CB20_INTERVAL", "4h")

    cmd = [
        "python", run_py,
        "--market", market,
        "--interval", interval,
        "--start-iso", start_iso,
        "--end-iso", end_iso,
        "--initial-equity", "1000",
        "--taker-fee", "0.0025",
        "--maker-fee", "0.0015",
        "--slippage-bps", "3",
        "--entry-mode", "limit_maker",
        "--reclaim-limit-offset-bps", "-10",
        "--fill-prob", "0.70",
        "--stop-extra-slip-bps", "1",
        "--vol-filter", "atr_percentile",
        "--atr-period", "14",
        "--atr-regime-window", "200",
        "--atr-regime-percentile", "0.50",
        "--position-fraction", f"{effective_pf:.6f}",
        "--dump-snapshot-path", EDGE3_SNAPSHOT_PATH,
    ]

    p = subprocess.run(cmd, cwd=ROOT)
    if p.returncode != 0:
        print(f"{utc_now_iso()} EDGE3_FATAL: run_edge3_fast failed rc={p.returncode}")
        return 2

    # After run, refresh health line from updated snapshot
    edge3_new = read_json(EDGE3_SNAPSHOT_PATH)
    age_e3_2 = file_age_seconds(EDGE3_SNAPSHOT_PATH)
    e3_status_2 = edge3_new.get("status", "NOFILE") if edge3_new else "NOFILE"
    pf_2 = edge3_new.get("profit_factor", None) if edge3_new else None
    eq_2 = edge3_new.get("ending_equity", None) if edge3_new else None

    health_line_2 = (
        f"{utc_now_iso_z()} gate={gate} trend={cb20_trend or 'NA'} vol={cb20_vol or 'NA'} "
        f"edge3_status={e3_status_2} pf={fmt(pf_2)} equity={fmt(eq_2)} snap_age_s={age_e3_2 if age_e3_2 is not None else 'nan'}"
    )
    atomic_write_text(HEALTH_PATH, health_line_2)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
