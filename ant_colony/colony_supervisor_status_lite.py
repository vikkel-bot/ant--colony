import json
from datetime import datetime, timezone
from pathlib import Path


VERSION = "colony_supervisor_status_lite_v7"

WATCHDOG_ACTION_PATH = Path(r"C:\Trading\ANT_OUT\colony_watchdog_start_if_needed.json")
WATCHDOG_CHECK_PATH = Path(r"C:\Trading\ANT_OUT\colony_watchdog_check_lite.json")
LOOP_HEARTBEAT_PATH = Path(r"C:\Trading\ANT_OUT\colony_cycle_loop_heartbeat.json")
LOOP_STATUS_PATH = Path(r"C:\Trading\ANT_OUT\colony_cycle_loop_lite.json")
LOOP_CONFLICT_PATH = Path(r"C:\Trading\ANT_OUT\colony_cycle_loop_lite_lock_conflict.json")
LOOP_CLEANUP_PATH = Path(r"C:\Trading\ANT_OUT\colony_cycle_loop_lite_lock_cleanup.json")
RECOVERY_STATUS_PATH = Path(r"C:\Trading\ANT_OUT\colony_recover_supervisor_status.json")

OUT_JSON = Path(r"C:\Trading\ANT_OUT\colony_supervisor_status_lite.json")

WATCHDOG_ACTION_MAX_AGE_SECONDS = 120
LOOP_MAX_AGE_SECONDS = 120
LOCK_EVENT_MAX_AGE_SECONDS = 300
RECOVERY_MAX_AGE_SECONDS = 300


def now_utc():
    return datetime.now(timezone.utc)


def iso_z(dt: datetime):
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def calc_age_and_stale(ts_utc: str, max_age_seconds: int):
    ts_utc = str(ts_utc or "")
    if not ts_utc:
        return -1, True
    try:
        dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        age = int((now_utc() - dt).total_seconds())
        return age, age > int(max_age_seconds)
    except Exception:
        return -1, True


def classify_runtime_state(last_ok: bool, stopped_reason: str):
    reason = str(stopped_reason or "").upper()
    if last_ok and reason == "RUNNING":
        return "RUNNING_OK", True
    if reason in ("ERROR", "FAILED", "CRASHED"):
        return "ERROR", False
    if reason in ("STOPPED", "INTERRUPTED", "EXITED"):
        return "STOPPED", False
    return "UNKNOWN", False


def derive_watchdog_state(loop_exists: bool, loop_is_stale: bool, live_runtime_state: str, fallback_watchdog_state: str):
    runtime = str(live_runtime_state or "").upper()
    fallback = str(fallback_watchdog_state or "").upper()
    if not loop_exists:
        return "MISSING"
    if loop_is_stale:
        return "STALE"
    if runtime == "RUNNING_OK":
        return "FRESH"
    return fallback or "UNKNOWN"


def derive_supervisor_state_and_reason(loop_exists: bool, loop_is_stale: bool, loop_runtime_state: str, watchdog_state: str):
    runtime = str(loop_runtime_state or "").upper()
    wd_state = str(watchdog_state or "").upper()

    if not loop_exists:
        return "DOWN", "SUPERVISOR_HEARTBEAT_MISSING"
    if loop_is_stale:
        return "STALE", "SUPERVISOR_LOOP_STALE"
    if wd_state == "FRESH" and runtime == "RUNNING_OK":
        return "OK", "SUPERVISOR_RUNNING_OK"
    if runtime == "ERROR":
        return "ERROR", "SUPERVISOR_LOOP_ERROR"
    if runtime == "STOPPED":
        return "STOPPED", "SUPERVISOR_LOOP_STOPPED"
    return "DEGRADED", "SUPERVISOR_STATE_DEGRADED"


def main():
    wd_action = read_json(WATCHDOG_ACTION_PATH)
    wd_check = read_json(WATCHDOG_CHECK_PATH)
    loop_hb = read_json(LOOP_HEARTBEAT_PATH)
    loop_status = read_json(LOOP_STATUS_PATH)
    loop_conflict = read_json(LOOP_CONFLICT_PATH)
    loop_cleanup = read_json(LOOP_CLEANUP_PATH)
    recovery = read_json(RECOVERY_STATUS_PATH)

    loop_ts_utc = str(loop_hb.get("ts_utc", "") or "")
    loop_age_seconds, loop_is_stale = calc_age_and_stale(loop_ts_utc, LOOP_MAX_AGE_SECONDS)

    wd_action_ts_utc = str(wd_action.get("ts_utc", "") or "")
    wd_action_age_seconds, wd_action_is_stale = calc_age_and_stale(wd_action_ts_utc, WATCHDOG_ACTION_MAX_AGE_SECONDS)

    loop_conflict_ts_utc = str(loop_conflict.get("ts_utc", "") or "")
    loop_conflict_age_seconds, loop_conflict_is_stale = calc_age_and_stale(loop_conflict_ts_utc, LOCK_EVENT_MAX_AGE_SECONDS)

    loop_cleanup_ts_utc = str(loop_cleanup.get("ts_utc", "") or "")
    loop_cleanup_age_seconds, loop_cleanup_is_stale = calc_age_and_stale(loop_cleanup_ts_utc, LOCK_EVENT_MAX_AGE_SECONDS)

    recovery_ts_utc = str(recovery.get("ts_utc", "") or "")
    recovery_age_seconds, recovery_is_stale = calc_age_and_stale(recovery_ts_utc, RECOVERY_MAX_AGE_SECONDS)

    hb_pid = str(loop_hb.get("pid", "") or "")
    hb_cycle_no = int(loop_hb.get("cycle_no", 0) or 0)
    hb_last_ok = bool(loop_hb.get("last_ok", False))
    hb_stopped_reason = str(loop_hb.get("stopped_reason", "") or "")
    hb_last_rc = int(loop_hb.get("last_rc", 0) or 0)
    hb_last_success_ts_utc = str(loop_hb.get("last_success_ts_utc", "") or "")
    hb_last_success_cycle_no = int(loop_hb.get("last_success_cycle_no", 0) or 0)

    loop_runtime_state_raw, live_engine_running = classify_runtime_state(hb_last_ok, hb_stopped_reason)

    watchdog_state = str(wd_check.get("watchdog_state", "") or "")
    watchdog_runtime_state = str(wd_check.get("runtime_state", "") or "")
    watchdog_last_known_runtime_state = str(wd_check.get("last_known_runtime_state", "") or "")
    watchdog_engine_running = bool(wd_check.get("engine_running", False))

    loop_exists = LOOP_HEARTBEAT_PATH.exists()

    if not loop_exists:
        loop_runtime_state = "MISSING"
    elif loop_is_stale:
        loop_runtime_state = "STALE"
    else:
        loop_runtime_state = loop_runtime_state_raw

    if not watchdog_runtime_state:
        watchdog_runtime_state = loop_runtime_state
    if not watchdog_last_known_runtime_state:
        watchdog_last_known_runtime_state = loop_runtime_state

    watchdog_state = derive_watchdog_state(
        loop_exists=loop_exists,
        loop_is_stale=loop_is_stale,
        live_runtime_state=loop_runtime_state,
        fallback_watchdog_state=watchdog_state,
    )

    supervisor_state, supervisor_reason = derive_supervisor_state_and_reason(
        loop_exists=loop_exists,
        loop_is_stale=loop_is_stale,
        loop_runtime_state=loop_runtime_state,
        watchdog_state=watchdog_state,
    )

    recent_watchdog_action = ""
    if not wd_action_is_stale:
        recent_watchdog_action = str(wd_action.get("action", "") or "")

    recent_lock_event = ""
    if not loop_conflict_is_stale:
        recent_lock_event = str(loop_conflict.get("reason", "") or "")
    elif not loop_cleanup_is_stale:
        recent_lock_event = str(loop_cleanup.get("reason", "") or "")

    recent_recovery_action = str(recovery.get("recent_recovery_action", "") or "")
    recovery_decision = str(recovery.get("recovery_decision", "") or "")
    recovery_attempted = bool(recovery.get("recovery_attempted", False))
    recovery_action_rc = recovery.get("recovery_action_rc", "")
    recovery_decision_reason = str(recovery.get("recovery_decision_reason", "") or "")
    previous_recovery_age_s = recovery.get("previous_recovery_age_s", "")
    recovery_cooldown_s = recovery.get("recovery_cooldown_s", 120)
    cooldown_active = bool(recovery.get("cooldown_active", False))

    if recovery_is_stale:
        recent_recovery_action = ""
        recovery_decision = ""
        recovery_attempted = False
        recovery_action_rc = ""
        recovery_decision_reason = ""
        previous_recovery_age_s = ""
        cooldown_active = False

    status = {
        "version": VERSION,
        "ts_utc": iso_z(now_utc()),
        "supervisor_state": supervisor_state,
        "supervisor_reason": supervisor_reason,

        "watchdog_action_exists": WATCHDOG_ACTION_PATH.exists(),
        "watchdog_action_ts_utc": wd_action_ts_utc,
        "watchdog_action": str(wd_action.get("action", "") or ""),
        "watchdog_result": str(wd_action.get("result", "") or ""),
        "watchdog_action_max_age_seconds": WATCHDOG_ACTION_MAX_AGE_SECONDS,
        "watchdog_action_age_seconds": wd_action_age_seconds,
        "watchdog_action_is_stale": wd_action_is_stale,
        "recent_watchdog_action": recent_watchdog_action,

        "lock_event_max_age_seconds": LOCK_EVENT_MAX_AGE_SECONDS,
        "recent_lock_event": recent_lock_event,

        "watchdog_check_exists": WATCHDOG_CHECK_PATH.exists(),
        "watchdog_state": watchdog_state,
        "watchdog_runtime_state": watchdog_runtime_state,
        "watchdog_last_known_runtime_state": watchdog_last_known_runtime_state,
        "watchdog_engine_running": watchdog_engine_running,

        "loop_heartbeat_exists": loop_exists,
        "loop_status_exists": LOOP_STATUS_PATH.exists(),
        "loop_conflict_exists": LOOP_CONFLICT_PATH.exists(),
        "loop_cleanup_exists": LOOP_CLEANUP_PATH.exists(),
        "loop_ts_utc": loop_ts_utc,
        "loop_pid": hb_pid,
        "loop_cycle_no": hb_cycle_no,
        "loop_last_ok": hb_last_ok,
        "loop_stopped_reason": hb_stopped_reason,
        "loop_runtime_state_raw": loop_runtime_state_raw,
        "loop_runtime_state": loop_runtime_state,
        "loop_last_rc": hb_last_rc,
        "loop_last_success_ts_utc": hb_last_success_ts_utc,
        "loop_last_success_cycle_no": hb_last_success_cycle_no,
        "loop_max_age_seconds": LOOP_MAX_AGE_SECONDS,
        "loop_age_seconds": loop_age_seconds,
        "loop_is_stale": loop_is_stale,
        "loop_status_stopped_reason": str(loop_status.get("stopped_reason", "") or ""),
        "loop_status_last_rc": int(loop_status.get("last_rc", 0) or 0),

        "loop_conflict_ts_utc": loop_conflict_ts_utc,
        "loop_conflict_age_seconds": loop_conflict_age_seconds,
        "loop_conflict_is_stale": loop_conflict_is_stale,
        "loop_conflict_reason": str(loop_conflict.get("reason", "") or ""),
        "loop_conflict_pid": str(loop_conflict.get("pid", "") or ""),
        "loop_conflict_process_name": str(loop_conflict.get("process_name", "") or ""),

        "loop_cleanup_ts_utc": loop_cleanup_ts_utc,
        "loop_cleanup_age_seconds": loop_cleanup_age_seconds,
        "loop_cleanup_is_stale": loop_cleanup_is_stale,
        "loop_cleanup_reason": str(loop_cleanup.get("reason", "") or ""),
        "loop_cleanup_pid": str(loop_cleanup.get("pid", "") or ""),

        "recovery_ts": recovery_ts_utc,
        "recovery_age_seconds": recovery_age_seconds,
        "recovery_is_stale": recovery_is_stale,
        "recent_recovery_action": recent_recovery_action,
        "recovery_decision": recovery_decision,
        "recovery_attempted": recovery_attempted,
        "recovery_action_rc": recovery_action_rc,
        "recovery_decision_reason": recovery_decision_reason,
        "previous_recovery_age_s": previous_recovery_age_s,
        "recovery_cooldown_s": recovery_cooldown_s,
        "cooldown_active": cooldown_active,
    }

    write_json(OUT_JSON, status)
    print(json.dumps(status, indent=2))


if __name__ == "__main__":
    main()