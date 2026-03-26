import os, json, time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def main():
    meta_p = os.path.join(ROOT, "reports", "edge3_cb21_meta.json")
    snap_p = os.path.join(ROOT, "reports", "edge3_snapshot.json")

    gate="?"
    size="?"
    pf="?"
    trend=None
    vol=None
    meta_age=None

    if os.path.exists(meta_p):
        try:
            st = os.stat(meta_p)
            meta_age = time.time() - st.st_mtime
            with open(meta_p, "r", encoding="utf-8") as f:
                m = json.load(f)
            gate = m.get("cb20_gate")
            size = m.get("cb20_size_mult")
            pf = m.get("edge3_effective_pf")
            trend = m.get("cb20_trend")
            vol = m.get("cb20_vol")
        except Exception:
            pass

    snap_state = "NOFILE"
    if os.path.exists(snap_p):
        try:
            with open(snap_p, "r", encoding="utf-8") as f:
                s = json.load(f)
            snap_state = s.get("status", "UNKNOWN")
        except Exception:
            snap_state = "READERR"

    print(f"{now_utc()} EDGE3 gate={gate} size={size} pf={pf} trend={trend} vol={vol} meta_age_s={(int(meta_age) if meta_age is not None else 'NA')} snap={snap_state}")

if __name__ == "__main__":
    main()
