import argparse, json, os, sys, time
from datetime import datetime, timezone

def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def safe_read_text(path: str) -> str:
    # handle UTF-8 BOM and regular utf-8
    with open(path, "rb") as f:
        b = f.read()
    # strip UTF-8 BOM if present
    if b.startswith(b"\xef\xbb\xbf"):
        b = b[3:]
    return b.decode("utf-8", errors="replace")

def read_json(path: str, default=None):
    try:
        txt = safe_read_text(path)
        return json.loads(txt)
    except FileNotFoundError:
        return default
    except Exception:
        return default

def parse_colony_status_tsv(tsv_path: str):
    """
    Expected header:
    ts_utc  market  gate  size_mult  cb21_reason  cb20_trend  cb20_vol
    """
    rows = []
    try:
        txt = safe_read_text(tsv_path)
        lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
        if len(lines) < 2:
            return rows
        header = lines[0].split()
        # crude but robust: split by whitespace
        for ln in lines[1:]:
            parts = ln.split()
            if len(parts) < 7:
                continue
            d = dict(zip(header[:len(parts)], parts))
            rows.append(d)
    except FileNotFoundError:
        return rows
    except Exception:
        return rows
    return rows

def decide_mult(row):
    # Conservative ruleset v2
    gate = (row.get("gate") or "").upper()
    reason = (row.get("cb21_reason") or "").upper()
    trend = (row.get("cb20_trend") or "").upper()
    vol = (row.get("cb20_vol") or "").upper()

    if gate != "ALLOW":
        return 0.0, "GATE_BLOCK"
    if reason and reason != "EDGE3_OK":
        return 0.0, f"CB21_{reason}"

    # volatility damp
    if vol == "HIGH":
        return 0.5, "VOL_HIGH"

    if trend == "BEAR" and vol == "LOW":
        return 0.5, "BEAR_LOW"

    if trend == "SIDEWAYS" and vol == "MID":
        return 1.0, "SIDEWAYS_MID"

    return 1.0, "DEFAULT"

def write_atomic(path: str, data: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
    os.replace(tmp, path)

def write_heartbeat(out_dir: str, mode: str, state: str, markets: int, extra=None):
    hb_path = os.path.join(out_dir, "queen_alloc_lite_heartbeat.json")
    o = {
        "ts_utc": utc_now_iso(),
        "component": "queen_alloc_lite",
        "mode": mode,
        "state": state,
        "markets": markets,
    }
    if extra:
        o.update(extra)
    write_atomic(hb_path, json.dumps(o, separators=(",", ":"), ensure_ascii=False))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--sleep-s", type=float, default=30.0)
    args = ap.parse_args()

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    cfg_path = os.path.join(out_dir, "queen_config.json")
    status_path = os.path.join(out_dir, "colony_status.tsv")
    targets_path = os.path.join(out_dir, "alloc_targets.json")

    while True:
        cfg = read_json(cfg_path, default={}) or {}
        mode = (cfg.get("mode") or "enabled").strip().lower()
        markets = cfg.get("markets") or ["BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","BNB-EUR"]

        rows = parse_colony_status_tsv(status_path)
        by_market = { (r.get("market") or "").upper(): r for r in rows if r.get("market") }

        out = {
            "version": "alloc_lite_v2",
            "ts_utc": utc_now_iso(),
            "mode": mode,
            "default_size_mult": 1.0,
            "markets": {}
        }

        # compute targets
        for m in markets:
            key = m.upper()
            if mode == "disabled":
                out["markets"][m] = {"target_size_mult": 1.0, "reason": "DISABLED"}
                continue

            row = by_market.get(key, {})
            mult, reason = decide_mult(row) if row else (1.0, "NO_STATUS")
            out["markets"][m] = {"target_size_mult": float(mult), "reason": reason}

        # write outputs
        try:
            write_atomic(targets_path, json.dumps(out, indent=2, ensure_ascii=False) + "\n")
            write_heartbeat(out_dir, mode, "ok", len(markets))
        except Exception as e:
            # still write heartbeat
            try:
                write_heartbeat(out_dir, mode, "crash", len(markets), extra={"error": str(e)[:200]})
            except Exception:
                pass

        if args.once:
            return
        time.sleep(max(1.0, args.sleep_s))

if __name__ == "__main__":
    main()
