import os, json, time
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def main():
    p = os.path.join(ROOT, "reports", "cb20_regime.json")
    if not os.path.exists(p):
        print(f"{now_utc()} CB20 bridge: NO_FILE {p}")
        return
    try:
        st = os.stat(p)
        age_s = time.time() - st.st_mtime
        with open(p, "r", encoding="utf-8") as f:
            d = json.load(f)
        print(
            f"{now_utc()} CB20 {d.get('market')} {d.get('interval')} "
            f"trend={d.get('trend_regime')} vol={d.get('vol_regime')} "
            f"gate={d.get('gate')} size={d.get('size_mult')} age_s={age_s:.0f}"
        )
    except Exception as e:
        print(f"{now_utc()} CB20 bridge: ERROR {repr(e)}")

if __name__ == "__main__":
    main()
