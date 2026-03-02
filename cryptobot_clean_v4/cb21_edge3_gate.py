import os, json, subprocess, sys
from datetime import datetime, timezone

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def read_cb20(root="."):
    p = os.path.join(root, "reports", "cb20_regime.json")
    if not os.path.exists(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def atomic_write(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)

def main():
    root = "."
    cb20 = read_cb20(root)

    gate = str(cb20.get("gate", "ALLOW")).upper()
    size_mult = float(cb20.get("size_mult", 1.0) or 1.0)

    trend = cb20.get("trend_regime")
    vol = cb20.get("vol_regime")

    base_pf = float(os.getenv("EDGE3_BASE_PF", "0.5"))
    pf = base_pf * size_mult
    if gate == "BLOCK":
        pf = 0.0

    meta = {
        "ts_utc": now_utc(),
        "cb20_market": cb20.get("market"),
        "cb20_interval": cb20.get("interval"),
        "cb20_trend": trend,
        "cb20_vol": vol,
        "cb20_gate": gate,
        "cb20_size_mult": size_mult,
        "edge3_base_pf": base_pf,
        "edge3_effective_pf": pf,
    }
    atomic_write(os.path.join("reports", "edge3_cb21_meta.json"), meta)

    cmd = [sys.executable, "run_edge3_fast.py"] + sys.argv[1:] + ["--position-fraction", f"{pf:.4f}"]

    print(f"{meta['ts_utc']} CB21 EDGE3_GATE gate={gate} trend={trend} vol={vol} size_mult={size_mult:.2f} base_pf={base_pf:.2f} -> position_fraction={pf:.4f}")
    sys.stdout.flush()

    rc = subprocess.call(cmd)
    raise SystemExit(rc)

if __name__ == "__main__":
    main()
