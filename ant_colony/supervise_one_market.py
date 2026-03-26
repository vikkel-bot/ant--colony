from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPOROOT = os.path.abspath(os.path.join(HERE, ".."))
OUTDIR = r"C:\Trading\ANT_OUT"
PIDDIR = os.path.join(OUTDIR, "pids")

BASE_S = 2
MAX_S = 60
SLEEP_OK = 60

CANDIDATE_PY = [
    "run_edge3_fast.py",
    "worker.py",
    "run_worker.py",
    "worker_main.py",
    "main.py",
    "run.py",
    "bot.py",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dirs() -> None:
    os.makedirs(OUTDIR, exist_ok=True)
    os.makedirs(PIDDIR, exist_ok=True)


def write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="ascii", newline="") as f:
        f.write(text)


def write_json(path: str, obj: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="ascii", newline="") as f:
        json.dump(obj, f, separators=(",", ":"))
    os.replace(tmp, path)


def is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            r = subprocess.run(
                ["powershell", "-NoProfile", "-Command", f"$p=Get-Process -Id {pid} -ErrorAction SilentlyContinue; if($p){{'1'}}else{{'0'}}"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            return r.stdout.strip() == "1"
        return False
    except Exception:
        return False


def acquire_lock(market: str) -> str | None:
    lockdir = os.path.join(PIDDIR, f"lock_supervise_{market}")
    pidfile = os.path.join(lockdir, "pid.txt")

    if os.path.exists(lockdir):
        oldpid = 0
        try:
            if os.path.exists(pidfile):
                with open(pidfile, "r", encoding="ascii") as f:
                    oldpid = int((f.read() or "0").strip())
        except Exception:
            oldpid = 0

        if oldpid > 0 and is_pid_alive(oldpid):
            return None

        shutil.rmtree(lockdir, ignore_errors=True)

    os.makedirs(lockdir, exist_ok=True)
    write_text(pidfile, str(os.getpid()))
    write_text(os.path.join(lockdir, "market.txt"), market)
    write_text(os.path.join(lockdir, "ts_local.txt"), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return lockdir


def load_alloc(market: str) -> dict:
    combined = os.path.join(OUTDIR, "alloc_targets_combined.json")
    fallback = os.path.join(OUTDIR, "alloc_targets.json")

    path = combined if os.path.exists(combined) else fallback if os.path.exists(fallback) else None
    if path is None:
        return {
            "alloc_mult": 1.0,
            "alloc_reason": "NO_ALLOC_FILE",
            "alloc_gate": "ALLOW",
            "alloc_file_used": "NONE",
        }

    try:
        with open(path, "r", encoding="utf-8") as f:
            j = json.load(f)
    except Exception:
        return {
            "alloc_mult": 1.0,
            "alloc_reason": "ALLOC_PARSE_FAIL",
            "alloc_gate": "ALLOW",
            "alloc_file_used": path,
        }

    default_mult = float(j.get("default_size_mult", 1.0))
    markets = j.get("markets", {}) or {}
    obj = markets.get(market, {}) or {}

    alloc_mult = float(obj.get("target_size_mult", default_mult))
    alloc_reason = str(obj.get("reason", "DEFAULT")).replace("\r", " ").replace("\n", " ")
    alloc_gate = str(obj.get("gate", "ALLOW")).strip().upper() or "ALLOW"
    if alloc_gate == "BLOCK":
        alloc_mult = 0.0

    return {
        "alloc_mult": alloc_mult,
        "alloc_reason": alloc_reason,
        "alloc_gate": alloc_gate,
        "alloc_file_used": path,
    }


def find_worker_entry(market: str) -> tuple[str, str]:
    w = os.path.join(REPOROOT, "ant_colony", "workers", market)
    entry_cmd = os.path.join(w, "worker_cycle.cmd")
    if os.path.exists(entry_cmd):
        return ("cmd", entry_cmd)

    for name in CANDIDATE_PY:
        p = os.path.join(w, name)
        if os.path.exists(p):
            return ("py", p)

    raise FileNotFoundError(f"No worker entry found for {market} in {w}")


def log_line(path: str, line: str) -> None:
    with open(path, "a", encoding="ascii", newline="") as f:
        f.write(line + "\n")


def main() -> int:
    market = sys.argv[1] if len(sys.argv) > 1 else ""
    if not market:
        return 2

    ensure_dirs()

    lockdir = acquire_lock(market)
    if lockdir is None:
        return 0

    log_path = os.path.join(OUTDIR, f"worker_{market}.log")
    hb_path = os.path.join(OUTDIR, f"hb_{market}.json")

    try:
        entry_type, entry_path = find_worker_entry(market)
    except Exception:
        shutil.rmtree(lockdir, ignore_errors=True)
        return 3

    base_pf_raw = os.environ.get("EDGE3_BASE_POSITION_FRACTION", "0.50")
    try:
        base_pf = float(base_pf_raw.replace(",", "."))
    except Exception:
        base_pf = 0.50
        base_pf_raw = "0.50"

    sleep_fail = BASE_S

    while True:
        alloc = load_alloc(market)
        alloc_mult = float(alloc["alloc_mult"])
        edge3_pf = base_pf * alloc_mult

        env = os.environ.copy()
        env["ANT_SIZE_MULT"] = str(alloc_mult)
        env["EDGE3_BASE_POSITION_FRACTION"] = f"{edge3_pf:.6f}"

        entry_name = os.path.basename(entry_path)

        log_line(log_path, f"==== START {datetime.now().strftime('%d-%m-%Y %H:%M:%S')} market={market} entry={entry_name} ====")
        log_line(
            log_path,
            f"---- ALLOC_APPLY ts={datetime.now().strftime('%d-%m-%Y %H:%M:%S')} "
            f"market={market} alloc_file={alloc['alloc_file_used']} alloc_gate={alloc['alloc_gate']} "
            f"alloc_mult={alloc_mult} reason={alloc['alloc_reason']} "
            f"base_pf_raw={base_pf_raw} edge3_base_pf={edge3_pf:.6f} ant_size_mult={alloc_mult} ----"
        )

        write_json(hb_path, {
            "ts_utc": utc_now_iso(),
            "market": market,
            "state": "starting",
        })

        try:
            if entry_type == "cmd":
                proc = subprocess.run(
                    ["cmd", "/c", entry_path],
                    cwd=os.path.dirname(entry_path),
                    env=env,
                    stdout=open(log_path, "a", encoding="ascii"),
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            else:
                proc = subprocess.run(
                    [sys.executable, entry_path],
                    cwd=os.path.dirname(entry_path),
                    env=env,
                    stdout=open(log_path, "a", encoding="ascii"),
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            ec = int(proc.returncode)
        except Exception as e:
            log_line(log_path, f"SUPERVISOR_EXCEPTION {type(e).__name__}: {e}")
            ec = 999

        log_line(log_path, f"==== EXIT {datetime.now().strftime('%d-%m-%Y %H:%M:%S')} market={market} ec={ec} ====")

        if ec == 0:
            write_json(hb_path, {
                "ts_utc": utc_now_iso(),
                "market": market,
                "state": "ok",
                "exit_code": 0,
            })
            sleep_fail = BASE_S
            time.sleep(SLEEP_OK)
        else:
            write_json(hb_path, {
                "ts_utc": utc_now_iso(),
                "market": market,
                "state": "crash",
                "exit_code": ec,
            })
            sleep_fail = min(MAX_S, max(BASE_S, sleep_fail * 2))
            time.sleep(sleep_fail)


if __name__ == "__main__":
    raise SystemExit(main())