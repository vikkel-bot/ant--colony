import os, json, time
from datetime import datetime, timezone

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def read_json(p):
    if not os.path.exists(p):
        return None, None
    st = os.stat(p)
    age_s = time.time() - st.st_mtime
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f), age_s

def main():
    root="."
    meta_p = os.path.join(root, "reports", "edge3_cb21_meta.json")
    snap_p = os.path.join(root, "reports", "edge3_snapshot.json")

    meta, meta_age = read_json(meta_p)
    snap, snap_age = read_json(snap_p)

    parts = [now_utc(), "EDGE3"]

    if meta:
        parts.append(f"gate={meta.get('cb20_gate')} size={meta.get('cb20_size_mult')} pf={meta.get('edge3_effective_pf')}")
        parts.append(f"trend={meta.get('cb20_trend')} vol={meta.get('cb20_vol')}")
        parts.append(f"meta_age_s={meta_age:.0f}")
    else:
        parts.append("meta=NOFILE")

    if snap:
        # snap schema kan verschillen; we pakken defensief
        eq = snap.get("ending_equity", snap.get("equity", None))
        dd = snap.get("max_dd", snap.get("max_drawdown_pct", None))
        pf = snap.get("profit_factor", None)
        ct = snap.get("closed_trades", None)
        if eq is not None: parts.append(f"eq={float(eq):.2f}")
        if pf is not None: parts.append(f"pf={float(pf):.3f}")
        if dd is not None: parts.append(f"dd={float(dd):.3f}")
        if ct is not None: parts.append(f"ct={ct}")
        parts.append(f"snap_age_s={snap_age:.0f}")
    else:
        parts.append("snap=NOFILE")

    print(" ".join(parts))

if __name__ == "__main__":
    main()
