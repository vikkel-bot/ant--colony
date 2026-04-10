"""
AC-UI-1: Feedback Dashboard (Read-Only CLI)

Reads C:\\Trading\\ANT_OUT\\feedback_analysis.json and prints a compact
human-readable overview. No file writes. No dependencies beyond stdlib.

AC-106: also reads C:\\Trading\\ANT_OUT\\source_health_review.json to show
source/data health context alongside review/anomaly signals.

AC-108: also reads C:\\Trading\\ANT_OUT\\combined_review_snapshot.json to show
a compact daily overview in the dashboard header.

AC-110: also reads C:\\Trading\\ANT_OUT\\source_freshness_recovery_plan.json to
show the recovery plan for stale/missing sources.

AC-115: also reads C:\\Trading\\ANT_OUT\\refresh_trigger.json to show the
semi-automatic refresh trigger in the dashboard header.

Usage:
    python ant_colony/show_feedback_dashboard.py
"""
from __future__ import annotations
import json
from pathlib import Path

ANALYSIS_PATH      = Path(r"C:\Trading\ANT_OUT\feedback_analysis.json")
SOURCE_HEALTH_PATH = Path(r"C:\Trading\ANT_OUT\source_health_review.json")
SNAPSHOT_PATH      = Path(r"C:\Trading\ANT_OUT\combined_review_snapshot.json")
RECOVERY_PATH      = Path(r"C:\Trading\ANT_OUT\source_freshness_recovery_plan.json")
TRIGGER_PATH       = Path(r"C:\Trading\ANT_OUT\refresh_trigger.json")

# ANSI colours (auto-disabled on systems that don't support them)
try:
    import sys
    _COLOUR = sys.stdout.isatty()
except Exception:
    _COLOUR = False

def _c(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLOUR else text

def _green(t):  return _c(t, "32")
def _yellow(t): return _c(t, "33")
def _red(t):    return _c(t, "31")
def _bold(t):   return _c(t, "1")
def _cyan(t):   return _c(t, "36")


def _bar(count: int, total: int, width: int = 20) -> str:
    if total == 0:
        return "[" + "-" * width + "]"
    filled = round(count / total * width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def _align_colour(alignment: str) -> str:
    if alignment == "HIGH":
        return _green(alignment)
    if alignment == "MEDIUM":
        return _yellow(alignment)
    return _red(alignment)


def _health_colour(status: str) -> str:
    if status == "HEALTHY":
        return _green(status)
    if status == "DEGRADED":
        return _yellow(status)
    return _red(status)


def _attn_colour(needs: bool) -> str:
    return _red("YES") if needs else _green("NO")


def _pct(rate: float) -> str:
    return f"{rate * 100:.1f}%"


def _load_trigger(path: Path) -> tuple[dict | None, str | None]:
    """Load refresh_trigger.json. Returns (data, None) or (None, msg)."""
    if not path.exists():
        return None, "NO DATA"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (json.JSONDecodeError, OSError) as exc:
        return None, f"ERROR: {exc}"


def _load_snapshot(path: Path) -> tuple[dict | None, str | None]:
    """Load combined_review_snapshot.json. Returns (data, None) or (None, msg)."""
    if not path.exists():
        return None, "NO DATA"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (json.JSONDecodeError, OSError) as exc:
        return None, f"ERROR: {exc}"


def _load_recovery_plan(path: Path) -> tuple[dict | None, str | None]:
    """Load source_freshness_recovery_plan.json. Returns (data, None) or (None, msg)."""
    if not path.exists():
        return None, "NO DATA"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (json.JSONDecodeError, OSError) as exc:
        return None, f"ERROR: {exc}"


def _load_source_health(path: Path) -> tuple[dict | None, str | None]:
    """
    Load source_health_review.json. Returns (data, None) on success,
    (None, error_msg) on missing/corrupt.
    """
    if not path.exists():
        return None, "NO DATA"
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (json.JSONDecodeError, OSError) as exc:
        return None, f"ERROR: {exc}"


def _overview_colour(status: str) -> str:
    if status == "HEALTHY":
        return _green(status)
    if status == "WATCH":
        return _cyan(status)
    if status == "ATTENTION":
        return _yellow(status)
    return _red(status)


def show(path: Path = ANALYSIS_PATH,
         source_health_path: Path = SOURCE_HEALTH_PATH,
         snapshot_path: Path = SNAPSHOT_PATH,
         recovery_path: Path = RECOVERY_PATH,
         trigger_path: Path = TRIGGER_PATH) -> None:
    """Print feedback dashboard from analysis JSON. Handles missing file."""
    # --- Load ---
    if not path.exists():
        print(_bold("=== ANT COLONY — Feedback Dashboard ==="))
        print()
        print("  NO DATA — feedback_analysis.json not found.")
        print(f"  Expected: {path}")
        print()
        print("  Run: python ant_colony/run_ac_scenarios_lite.py")
        return

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(_bold("=== ANT COLONY — Feedback Dashboard ==="))
        print()
        print(f"  ERROR reading file: {exc}")
        return

    totals  = data.get("totals",  {})
    rates   = data.get("rates",   {})
    signals = data.get("signals", {})
    by_ac   = data.get("by_action_class", {})
    by_urg  = data.get("by_urgency", {})
    ts      = data.get("ts_utc", "unknown")

    # --- Combined snapshot (AC-108) ---
    snap_data, snap_err = _load_snapshot(snapshot_path)

    # --- Recovery plan (AC-110) ---
    rp_data, rp_err = _load_recovery_plan(recovery_path)

    # --- Source health (AC-106) ---
    sh_data, sh_err = _load_source_health(source_health_path)

    # --- Refresh trigger (AC-115) ---
    tr_data, tr_err = _load_trigger(trigger_path)

    n_entries   = totals.get("entries",   0)
    n_confirm   = totals.get("confirm",   0)
    n_disagree  = totals.get("disagree",  0)
    n_uncertain = totals.get("uncertain", 0)
    n_invalid   = totals.get("invalid",   0)

    cr  = rates.get("confirm_rate",   0.0)
    dr  = rates.get("disagree_rate",  0.0)
    ur  = rates.get("uncertain_rate", 0.0)

    alignment  = signals.get("system_human_alignment", "?")
    needs_attn = signals.get("needs_attention", False)
    attn_code  = signals.get("attention_reason_code", "NONE")

    # --- Header ---
    print()
    print(_bold("╔══════════════════════════════════════════════╗"))
    print(_bold("║   ANT COLONY — Feedback Dashboard            ║"))
    print(_bold("╚══════════════════════════════════════════════╝"))
    print(f"  as-of : {ts}")
    print()

    # --- Overview (AC-108) ---
    print(_bold("── Overview ─────────────────────────────────────"))
    if snap_data and isinstance(snap_data, dict):
        ov_status = snap_data.get("overview_status", "?")
        sm        = snap_data.get("summary", {})
        top_risk  = sm.get("top_risk",      "—") if isinstance(sm, dict) else "—"
        human_ctx = sm.get("human_context", "—") if isinstance(sm, dict) else "—"
        print(f"  status      : {_overview_colour(ov_status)}")
        print(f"  top risk    : {top_risk}")
        print(f"  human ctx   : {human_ctx}")
    elif snap_err:
        print(f"  {snap_err}")
    else:
        print("  (no data)")

    # --- Refresh trigger (AC-115) ---
    if tr_data and isinstance(tr_data, dict):
        tr_status = tr_data.get("trigger_status", "?")
        tr_rc     = tr_data.get("trigger_reason_code", "?")
        tr_og     = tr_data.get("operator_guidance", {})
        tr_action = tr_og.get("recommended_action", "?") if isinstance(tr_og, dict) else "?"
        tr_window = tr_og.get("recommended_window", "?") if isinstance(tr_og, dict) else "?"
        print(f"  refresh trigger : {tr_status}  ({tr_rc})")
        print(f"  recommended     : {tr_action} / {tr_window}")
    elif tr_err:
        print(f"  refresh trigger : {tr_err}")
    else:
        print("  refresh trigger : (no data)")
    print()

    # --- Totals ---
    print(_bold("── Totals ─────────────────────────────────────"))
    print(f"  entries   : {n_entries}")
    valid = n_confirm + n_disagree + n_uncertain
    print(f"  valid     : {valid}  (invalid: {n_invalid})")
    print()

    # --- Feedback distribution ---
    print(_bold("── Feedback Distribution ───────────────────────"))
    print(f"  CONFIRM   : {n_confirm:>4}  {_bar(n_confirm,  valid)}  {_pct(cr)}")
    print(f"  DISAGREE  : {n_disagree:>4}  {_bar(n_disagree, valid)}  {_pct(dr)}")
    print(f"  UNCERTAIN : {n_uncertain:>4}  {_bar(n_uncertain,valid)}  {_pct(ur)}")
    print()

    # --- Signals ---
    print(_bold("── Signals ─────────────────────────────────────"))
    print(f"  alignment      : {_align_colour(alignment)}")
    print(f"  needs_attention: {_attn_colour(needs_attn)}"
          + (f"  ({attn_code})" if needs_attn else ""))

    # Combined review context line (AC-106)
    if sh_data and isinstance(sh_data, dict):
        sh_status = sh_data.get("source_health_status", "UNKNOWN")
        _review_ctx = f"SOURCE_{sh_status} / REVIEW_{alignment}"
    else:
        _review_ctx = f"SOURCE_UNKNOWN / REVIEW_{alignment}"
    print(f"  review context : {_cyan(_review_ctx)}")
    print()

    # --- Source Health (AC-106) ---
    print(_bold("── Source Health ────────────────────────────────"))
    if sh_err and sh_data is None:
        print(f"  {sh_err}")
    elif sh_data and isinstance(sh_data, dict):
        sh_status   = sh_data.get("source_health_status", "?")
        sh_blocking = sh_data.get("freshness_blocking_review", False)
        sh_code     = sh_data.get("primary_reason_code", "?")
        sh_fresh    = sh_data.get("markets_fresh",   0)
        sh_stale    = sh_data.get("markets_stale",   0)
        sh_miss     = sh_data.get("markets_missing", 0)
        sh_affected = sh_data.get("affected_markets", [])
        print(f"  status          : {_health_colour(sh_status)}")
        print(f"  blocking_review : {_red('True') if sh_blocking else _green('False')}")
        print(f"  reason_code     : {sh_code}")
        print(f"  fresh/stale/miss: {sh_fresh} / {sh_stale} / {sh_miss}")
        if sh_affected:
            print(f"  affected_markets: {', '.join(sh_affected)}")
    else:
        print("  (no data)")
    print()

    # --- Recovery Plan (AC-110) ---
    print(_bold("── Recovery Plan ────────────────────────────────"))
    if rp_data and isinstance(rp_data, dict):
        rp_status = rp_data.get("recovery_status", "?")
        rp_code   = rp_data.get("recovery_reason_code", "?")
        rp_sm     = rp_data.get("summary", {})
        rp_req    = rp_sm.get("markets_requiring_recovery", 0) if isinstance(rp_sm, dict) else 0
        rp_po     = rp_data.get("priority_order") or []
        top_mkts  = ", ".join(e["market"] for e in rp_po[:3]) if rp_po else "—"
        rp_colour = _red if rp_status == "URGENT" else (_yellow if rp_status == "PLAN_READY"
                    else _green)
        print(f"  status            : {rp_colour(rp_status)}")
        print(f"  reason_code       : {rp_code}")
        print(f"  requiring_recovery: {rp_req}")
        print(f"  top priorities    : {top_mkts}")
    elif rp_err:
        print(f"  {rp_err}")
    else:
        print("  (no data)")
    print()

    # --- By action class ---
    print(_bold("── By Action Class ─────────────────────────────"))
    _print_group_table(by_ac, valid)
    print()

    # --- By urgency ---
    print(_bold("── By Urgency ───────────────────────────────────"))
    urgency_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "NONE"]
    ordered_urg = {k: by_urg[k] for k in urgency_order if k in by_urg}
    # append any extra keys not in the ordered list
    for k, v in by_urg.items():
        if k not in ordered_urg:
            ordered_urg[k] = v
    _print_group_table(ordered_urg, valid)
    print()

    # --- Flags reminder ---
    flags = data.get("flags", {})
    print(_bold("── Flags ───────────────────────────────────────"))
    print(f"  non_binding             : {flags.get('non_binding', '?')}")
    print(f"  simulation_only         : {flags.get('simulation_only', '?')}")
    print(f"  paper_only              : {flags.get('paper_only', '?')}")
    print(f"  live_activation_allowed : {flags.get('live_activation_allowed', '?')}")
    print()


def _print_group_table(groups: dict, total_valid: int) -> None:
    if not groups:
        print("  (no data)")
        return
    for key, g in groups.items():
        if not isinstance(g, dict):
            continue
        n   = g.get("entries",   0)
        c   = g.get("confirm",   0)
        d   = g.get("disagree",  0)
        u   = g.get("uncertain", 0)
        if n == 0:
            print(f"  {key:<32} {n:>4} entries  —")
            continue
        dr_local = d / (c + d + u) if (c + d + u) > 0 else 0.0
        dr_str   = _pct(dr_local)
        disagree_indicator = (
            _red(f"  disagree={dr_str}") if dr_local > 0.3
            else _yellow(f"  disagree={dr_str}") if dr_local > 0
            else f"  disagree={dr_str}"
        )
        print(f"  {key:<32} {n:>4} entries  C:{c} D:{d} U:{u}{disagree_indicator}")


if __name__ == "__main__":
    show()
