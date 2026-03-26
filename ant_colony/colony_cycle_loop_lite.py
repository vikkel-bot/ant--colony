import atexit
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


VERSION = "colony_cycle_loop_lite_v8"


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def write_tsv(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = list(data.keys())
    values = [str(data.get(k, "")) for k in keys]
    path.write_text("`t".join(keys) + "`n" + "`t".join(values) + "`n", encoding="utf-8")


def write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def to_int(value, default):
    try:
        return int(value)
    except Exception:
        return default


def persist_status(out_dir: Path, status: dict):
    write_json(out_dir / "colony_cycle_loop_lite.json", status)
    write_tsv(out_dir / "colony_cycle_loop_lite.tsv", status)


def persist_heartbeat(out_dir: Path, status: dict):
    heartbeat = {
        "version": VERSION,
        "ts_utc": utc_now(),
        "component": "colony_cycle_loop_lite",
        "pid": status.get("lock_pid", ""),
        "cycle_no": status.get("cycle_no", 0),
        "last_ok": status.get("last_ok", False),
        "stopped_reason": status.get("stopped_reason", ""),
        "sleep_seconds": status.get("sleep_seconds", 0),
        "runner": status.get("runner", ""),
        "last_rc": status.get("last_rc", 0),
        "last_success_ts_utc": status.get("last_success_ts_utc", ""),
        "last_success_cycle_no": status.get("last_success_cycle_no", 0),
    }
    write_json(out_dir / "colony_cycle_loop_heartbeat.json", heartbeat)
    write_tsv(out_dir / "colony_cycle_loop_heartbeat.tsv", heartbeat)


def inspect_windows_pid(pid: int) -> dict:
    result = {
        "pid": pid,
        "exists": False,
        "name": "",
        "command_line": "",
        "reason": "",
    }

    if pid <= 0:
        result["reason"] = "INVALID_PID"
        return result

    ps_cmd = """
$ErrorActionPreference = "Stop"
$pidValue = __PID__
$p = Get-CimInstance Win32_Process -Filter ("ProcessId = " + $pidValue) -ErrorAction SilentlyContinue

if ($null -eq $p) {
    [pscustomobject]@{
        exists = $false
        name = ""
        command_line = ""
        reason = "PID_NOT_RUNNING"
    } | ConvertTo-Json -Compress
}
else {
    [pscustomobject]@{
        exists = $true
        name = [string]$p.Name
        command_line = [string]$p.CommandLine
        reason = "OK"
    } | ConvertTo-Json -Compress
}
""".replace("__PID__", str(pid))

    try:
        proc = subprocess.run(
            ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd],
            capture_output=True,
            text=True,
            timeout=15
        )

        raw = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()

        if raw:
            parsed = json.loads(raw)
            result["exists"] = bool(parsed.get("exists", False))
            result["name"] = str(parsed.get("name", "") or "")
            result["command_line"] = str(parsed.get("command_line", "") or "")
            result["reason"] = str(parsed.get("reason", "") or ("OK" if result["exists"] else "PID_NOT_RUNNING"))
            return result

        if proc.returncode == 0:
            result["reason"] = "PID_NOT_RUNNING"
            return result

        result["reason"] = "PROCESS_QUERY_FAILED"
        if err:
            result["command_line"] = err
        return result

    except Exception as exc:
        result["reason"] = "PROCESS_QUERY_EXCEPTION"
        result["command_line"] = str(exc)
        return result

def classify_lock_pid(pid_text: str) -> dict:
    info = {
        "lock_pid_raw": str(pid_text or "").strip(),
        "lock_pid": "",
        "lock_state": "STALE",
        "lock_reason": "INVALID_PID",
        "process_name": "",
        "process_command_line": "",
    }

    pid_raw = info["lock_pid_raw"]
    try:
        pid = int(pid_raw)
    except Exception:
        return info

    info["lock_pid"] = str(pid)

    if pid <= 0:
        info["lock_reason"] = "INVALID_PID"
        return info

    proc_info = inspect_windows_pid(pid)
    info["process_name"] = proc_info.get("name", "")
    info["process_command_line"] = proc_info.get("command_line", "")

    if not proc_info.get("exists", False):
        info["lock_reason"] = proc_info.get("reason", "PID_NOT_RUNNING")
        return info

    name_upper = str(proc_info.get("name", "") or "").upper()
    cmd_upper = str(proc_info.get("command_line", "") or "").upper()

    if "COLONY_CYCLE_LOOP_LITE.PY" in cmd_upper:
        info["lock_state"] = "ACTIVE"
        info["lock_reason"] = "LOCK_ACTIVE"
        return info

    if pid == os.getpid():
        info["lock_state"] = "ACTIVE"
        info["lock_reason"] = "LOCK_ACTIVE_SELF"
        return info

    info["lock_reason"] = "PID_NOT_COLONY_LOOP"
    return info


def acquire_lock(lock_path: Path, out_dir: Path, base_status: dict):
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    if lock_path.exists():
        existing_pid = ""
        try:
            existing_pid = lock_path.read_text(encoding="utf-8").strip()
        except Exception:
            existing_pid = ""

        lock_info = classify_lock_pid(existing_pid)

        if lock_info.get("lock_state") == "STALE":
            stale_status = dict(base_status)
            stale_status["ts_utc"] = utc_now()
            stale_status["last_ok"] = True
            stale_status["last_rc"] = 0
            stale_status["stopped_reason"] = "LOCK_STALE_DETECTED"
            stale_status["lock_path"] = str(lock_path)
            stale_status["lock_pid"] = lock_info.get("lock_pid_raw", "")
            stale_status["lock_state"] = lock_info.get("lock_state", "")
            stale_status["lock_reason"] = lock_info.get("lock_reason", "")
            stale_status["lock_process_name"] = lock_info.get("process_name", "")
            stale_status["lock_process_command_line"] = lock_info.get("process_command_line", "")

            write_json(out_dir / "colony_cycle_loop_lite_lock_cleanup.json", stale_status)
            write_tsv(out_dir / "colony_cycle_loop_lite_lock_cleanup.tsv", stale_status)

            print("LOCK_STALE_DETECTED")
            print(f"lock_path={lock_path}")
            print(f"lock_pid={lock_info.get('lock_pid_raw', '')}")
            print(f"lock_reason={lock_info.get('lock_reason', '')}")

            try:
                lock_path.unlink()
                print("LOCK_REMOVED")
            except Exception as exc:
                failed_status = dict(stale_status)
                failed_status["last_ok"] = False
                failed_status["last_rc"] = 1
                failed_status["stopped_reason"] = "LOCK_REMOVE_FAILED"
                failed_status["lock_remove_error"] = str(exc)
                write_json(out_dir / "colony_cycle_loop_lite_lock_cleanup_failed.json", failed_status)
                write_tsv(out_dir / "colony_cycle_loop_lite_lock_cleanup_failed.tsv", failed_status)
                print("LOCK_REMOVE_FAILED")
                return False
        else:
            status = dict(base_status)
            status["ts_utc"] = utc_now()
            status["last_ok"] = False
            status["last_rc"] = 1
            status["stopped_reason"] = "LOCK_ACTIVE"
            status["lock_path"] = str(lock_path)
            status["lock_pid"] = lock_info.get("lock_pid", "") or lock_info.get("lock_pid_raw", "")
            status["lock_state"] = lock_info.get("lock_state", "")
            status["lock_reason"] = lock_info.get("lock_reason", "")
            status["lock_process_name"] = lock_info.get("process_name", "")
            status["lock_process_command_line"] = lock_info.get("process_command_line", "")

            write_json(out_dir / "colony_cycle_loop_lite_lock_conflict.json", status)
            write_tsv(out_dir / "colony_cycle_loop_lite_lock_conflict.tsv", status)

            print("LOCK_ACTIVE")
            print(f"lock_path={lock_path}")
            print(f"lock_pid={status.get('lock_pid', '')}")
            print(f"lock_reason={status.get('lock_reason', '')}")
            return False

    lock_path.write_text(str(os.getpid()), encoding="utf-8")
    return True


def release_lock(lock_path: Path):
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def main():
    root = Path(__file__).resolve().parents[1]
    out_dir = Path(r"C:\Trading\ANT_OUT")
    runner = root / "ant_colony" / "colony_cycle_runner_lite.py"
    lock_path = out_dir / "colony_cycle_loop_lite.lock"
    runner_stdout_path = out_dir / "colony_cycle_runner_lite.stdout.log"
    runner_stderr_path = out_dir / "colony_cycle_runner_lite.stderr.log"

    sleep_seconds = to_int(sys.argv[1], 30) if len(sys.argv) >= 2 else 30
    max_cycles = to_int(sys.argv[2], 0) if len(sys.argv) >= 3 else 0

    cycle_no = 0
    last_rc = 0
    last_success_ts_utc = ""
    last_success_cycle_no = 0

    status = {
        "version": VERSION,
        "ts_utc": utc_now(),
        "root": str(root),
        "runner": str(runner),
        "cycle_no": cycle_no,
        "sleep_seconds": sleep_seconds,
        "max_cycles": max_cycles,
        "last_cycle_started_utc": "",
        "last_cycle_finished_utc": "",
        "last_rc": last_rc,
        "last_ok": True,
        "stopped_reason": "STARTING",
        "lock_path": str(lock_path),
        "lock_pid": str(os.getpid()),
        "runner_stdout_log": str(runner_stdout_path),
        "runner_stderr_log": str(runner_stderr_path),
        "last_success_ts_utc": last_success_ts_utc,
        "last_success_cycle_no": last_success_cycle_no,
    }
    persist_status(out_dir, status)
    persist_heartbeat(out_dir, status)

    if not acquire_lock(lock_path, out_dir, status):
        return 1

    atexit.register(release_lock, lock_path)

    status["stopped_reason"] = "RUNNING"
    persist_status(out_dir, status)
    persist_heartbeat(out_dir, status)

    try:
        while True:
            cycle_no += 1
            ts_start = utc_now()

            proc = subprocess.run(
                [sys.executable, str(runner)],
                cwd=str(root),
                capture_output=True,
                text=True
            )

            ts_end = utc_now()
            last_rc = int(proc.returncode)
            last_ok = (last_rc == 0)

            write_text(runner_stdout_path, proc.stdout or "")
            write_text(runner_stderr_path, proc.stderr or "")

            if last_ok:
                last_success_ts_utc = ts_end
                last_success_cycle_no = cycle_no

            status = {
                "version": VERSION,
                "ts_utc": ts_end,
                "root": str(root),
                "runner": str(runner),
                "cycle_no": cycle_no,
                "sleep_seconds": sleep_seconds,
                "max_cycles": max_cycles,
                "last_cycle_started_utc": ts_start,
                "last_cycle_finished_utc": ts_end,
                "last_rc": last_rc,
                "last_ok": last_ok,
                "stopped_reason": "RUNNING",
                "lock_path": str(lock_path),
                "lock_pid": str(os.getpid()),
                "runner_stdout_log": str(runner_stdout_path),
                "runner_stderr_log": str(runner_stderr_path),
                "last_success_ts_utc": last_success_ts_utc,
                "last_success_cycle_no": last_success_cycle_no,
            }
            persist_status(out_dir, status)
            persist_heartbeat(out_dir, status)

            print(f"[{ts_end}] cycle={cycle_no} rc={last_rc} ok={str(last_ok).lower()}")

            if not last_ok:
                status["stopped_reason"] = "RUNNER_FAILED"
                persist_status(out_dir, status)
                persist_heartbeat(out_dir, status)
                return last_rc

            if max_cycles > 0 and cycle_no >= max_cycles:
                status["stopped_reason"] = "MAX_CYCLES_REACHED"
                persist_status(out_dir, status)
                persist_heartbeat(out_dir, status)
                return 0

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        status = {
            "version": VERSION,
            "ts_utc": utc_now(),
            "root": str(root),
            "runner": str(runner),
            "cycle_no": cycle_no,
            "sleep_seconds": sleep_seconds,
            "max_cycles": max_cycles,
            "last_cycle_started_utc": status.get("last_cycle_started_utc", ""),
            "last_cycle_finished_utc": status.get("last_cycle_finished_utc", ""),
            "last_rc": 0,
            "last_ok": True,
            "stopped_reason": "INTERRUPTED",
            "lock_path": str(lock_path),
            "lock_pid": str(os.getpid()),
            "runner_stdout_log": str(runner_stdout_path),
            "runner_stderr_log": str(runner_stderr_path),
            "last_success_ts_utc": last_success_ts_utc,
            "last_success_cycle_no": last_success_cycle_no,
        }
        persist_status(out_dir, status)
        persist_heartbeat(out_dir, status)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())