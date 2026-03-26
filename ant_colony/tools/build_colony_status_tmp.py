import json
from pathlib import Path
from datetime import datetime, timezone

OUT_DIR = Path(r"C:\Trading\ANT_OUT")
WORKERS_ROOT = Path(r"C:\Trading\EDGE3\ant_colony\workers")

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

def read_last_line(path: Path):
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return text[-1] if text else None
    except Exception:
        return None

OUT_DIR.mkdir(parents=True, exist_ok=True)

workers = []
for d in sorted([p for p in WORKERS_ROOT.iterdir() if p.is_dir()], key=lambda p: p.name):
    market = d.name
    health_path = d / "health.txt"
    log_path = d / "logs" / "worker.log"
    cb20_path = d / "reports" / "cb20_regime.json"
    cb21_path = d / "reports" / "edge3_cb21_meta.json"
    snap_path = d / "reports" / "edge3_snapshot.json"

    cb20 = load_json(cb20_path)
    cb21 = load_json(cb21_path)
    health_line = read_last_line(health_path)

    worker_state = "UNKNOWN"
    gate = "UNKNOWN"
    size_mult = None
    exit_code = None

    if health_line:
        parts = {}
        for token in health_line.split():
            if "=" in token:
                k, v = token.split("=", 1)
                parts[k] = v
        worker_state = parts.get("worker", worker_state)
        gate = parts.get("gate", gate)
        try:
            exit_code = int(parts["exit"]) if "exit" in parts else None
        except Exception:
            exit_code = None

    if cb21:
        gate = cb21.get("edge3_combined_gate", gate)
        size_mult = cb21.get("edge3_combined_size_mult", size_mult)
        if worker_state == "UNKNOWN":
            worker_state = "OK"
        if exit_code is None:
            exit_code = 0

    workers.append({
        "market": market,
        "worker": worker_state,
        "gate": gate,
        "size_mult": size_mult,
        "exit": exit_code,
        "health_line": health_line,
        "latest_snapshot": str(snap_path) if snap_path.exists() else None,
        "cb20": cb20,
        "cb21": cb21,
        "log_path": str(log_path),
    })

status = {
    "ts_utc": utc_now(),
    "workers_root": str(WORKERS_ROOT),
    "worker_count": len(workers),
    "workers": workers,
}

out_path = OUT_DIR / "colony_status.json"
out_path.write_text(json.dumps(status, indent=2), encoding="utf-8")

print(f"OK wrote {out_path}")
print(f"worker_count={len(workers)}")
