"""
AC-180: Queen Advisory

Reads queen_learning_summary.json and generates a simple, non-binding advisory
object for each (market, strategy_key, signal_key) group.

One sentence: Interprets aggregated learning metrics and attaches plain-language
advisory fields to each group — strictly observational, with no effect on
execution, allocation, or live gates.

IMPORTANT — this module is strictly non-binding:
  - no execution changes
  - no allocation changes
  - no live gate changes
  - output artifact is labelled observational_only=True, binding=False

Input (read-only):
    {base_output_dir}/{lane}/queen_learning_summary.json  — AC-179 summary

Output:
    {base_output_dir}/{lane}/queen_advisory_summary.json

Advisory fields per group
(all fields are strings; thresholds are fixed and documented inline):

  sample_size_status
      "INSUFFICIENT" — fewer than MIN_TRADES_FOR_SIGNAL (5) trades
      "MINIMAL"      — 5–9 trades
      "ADEQUATE"     — 10 or more trades
      Reason: fewer than 5 trades is noise; advice below that threshold is
      purely observational and should be treated with extra skepticism.

  execution_quality_status
      "ATTENTION_NEEDED" — queen_action_required_count / trades_count > 0
      "CLEAN"            — queen_action_required_count == 0
      Reason: any anomaly count signals the Queen should review those trades.

  signal_observation
      Observation about the win-rate relative to a 50 % baseline and the
      most-recently observed market regime:
      "POSITIVE_SIGNAL"  — win_rate > WIN_RATE_HIGH (0.60)
      "NEGATIVE_SIGNAL"  — win_rate < WIN_RATE_LOW  (0.40)
      "NEUTRAL_SIGNAL"   — 0.40 <= win_rate <= 0.60
      "NO_DATA"          — trades_count == 0
      Reason: a simple majority-wins baseline with ±10 pp tolerance band.

  advisory_note
      One plain-English sentence combining the three fields above.
      Always ends with a reminder that this note is non-binding.

Thresholds (module-level constants, easy to audit):
  MIN_TRADES_FOR_SIGNAL = 5
  WIN_RATE_HIGH         = 0.60
  WIN_RATE_LOW          = 0.40

Fail-closed: if the input file is missing or unreadable the output advisory is
written with zero groups and ok=True (not a failure — the Queen just has nothing
to advise yet). Never raises.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

MIN_TRADES_FOR_SIGNAL: int = 5
WIN_RATE_HIGH: float = 0.60
WIN_RATE_LOW: float = 0.40


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json_atomic(path: Path, obj: Any) -> None:
    """Write JSON atomically via a .tmp sibling then os.replace()."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Advisory logic
# ---------------------------------------------------------------------------

def _sample_size_status(trades_count: int) -> str:
    """
    Return sample-size status label.

    INSUFFICIENT (<5) / MINIMAL (5–9) / ADEQUATE (≥10).
    """
    if trades_count < MIN_TRADES_FOR_SIGNAL:
        return "INSUFFICIENT"
    if trades_count < 10:
        return "MINIMAL"
    return "ADEQUATE"


def _execution_quality_status(queen_action_required_count: int) -> str:
    """
    Return ATTENTION_NEEDED if any trades required queen attention, else CLEAN.
    """
    return "ATTENTION_NEEDED" if queen_action_required_count > 0 else "CLEAN"


def _signal_observation(trades_count: int, win_count: int) -> str:
    """
    Return signal observation based on win rate vs 50 % baseline ± 10 pp.

    NO_DATA → POSITIVE_SIGNAL / NEUTRAL_SIGNAL / NEGATIVE_SIGNAL.
    """
    if trades_count == 0:
        return "NO_DATA"
    win_rate = win_count / trades_count
    if win_rate > WIN_RATE_HIGH:
        return "POSITIVE_SIGNAL"
    if win_rate < WIN_RATE_LOW:
        return "NEGATIVE_SIGNAL"
    return "NEUTRAL_SIGNAL"


def _advisory_note(
    sample_status: str,
    exec_status: str,
    signal_obs: str,
    trades_count: int,
    last_regime: str,
) -> str:
    """
    Compose one plain-English advisory note from the three status fields.

    Always ends with a non-binding reminder.
    """
    parts: list[str] = []

    if sample_status == "INSUFFICIENT":
        parts.append(f"Sample is too small ({trades_count} trade(s)) for reliable inference.")
    elif sample_status == "MINIMAL":
        parts.append(f"Sample is minimal ({trades_count} trades); treat observations as provisional.")
    else:
        parts.append(f"Adequate sample ({trades_count} trades).")

    if signal_obs == "POSITIVE_SIGNAL":
        parts.append("Win rate is above 60 % — positive signal pattern so far.")
    elif signal_obs == "NEGATIVE_SIGNAL":
        parts.append("Win rate is below 40 % — negative signal pattern so far.")
    elif signal_obs == "NEUTRAL_SIGNAL":
        parts.append("Win rate is near 50 % — no clear edge observed yet.")
    else:
        parts.append("No outcome data available.")

    if exec_status == "ATTENTION_NEEDED":
        parts.append("Some trades flagged for queen review — execution anomalies present.")
    else:
        parts.append("Execution quality is clean.")

    if last_regime not in ("UNKNOWN", ""):
        parts.append(f"Last observed regime: {last_regime}.")

    parts.append("This note is non-binding and does not affect execution or allocation.")
    return " ".join(parts)


def advise_group(group: dict[str, Any]) -> dict[str, Any]:
    """
    Generate advisory fields for a single learning-summary group dict.

    Returns the original group fields plus advisory fields. Does not modify
    the input dict. Never raises.
    """
    try:
        trades_count = int(group.get("trades_count") or 0)
        win_count = int(group.get("win_count") or 0)
        qar_count = int(group.get("queen_action_required_count") or 0)
        last_regime = str(group.get("last_market_regime") or "UNKNOWN")

        ss = _sample_size_status(trades_count)
        eq = _execution_quality_status(qar_count)
        so = _signal_observation(trades_count, win_count)
        note = _advisory_note(ss, eq, so, trades_count, last_regime)

        return {
            **group,
            "sample_size_status": ss,
            "execution_quality_status": eq,
            "signal_observation": so,
            "advisory_note": note,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            **group,
            "sample_size_status": "UNKNOWN",
            "execution_quality_status": "UNKNOWN",
            "signal_observation": "UNKNOWN",
            "advisory_note": f"Advisory generation error: {exc}. This note is non-binding.",
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_learning_summary(base_output_dir: str, lane: str) -> dict[str, Any] | None:
    """
    Read queen_learning_summary.json for the given lane.

    Returns the parsed dict, or None if the file is missing or unreadable.
    Never raises.
    """
    try:
        path = Path(base_output_dir) / lane / "queen_learning_summary.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def build_advisory(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build the complete advisory dict from the AC-179 learning summary.

    Returns the advisory dict regardless of whether writing succeeds.
    Never raises.
    """
    try:
        learning = read_learning_summary(base_output_dir, lane)
        if learning is None:
            groups_raw = []
            source_note = "queen_learning_summary.json not found or unreadable"
        else:
            groups_raw = learning.get("groups") or []
            source_note = "ok"

        advised_groups = [advise_group(g) for g in groups_raw if isinstance(g, dict)]

        return {
            "advisory_version": "1",
            "advisory_type": "queen_advisory_summary",
            "observational_only": True,
            "binding": False,
            "note": (
                "Non-binding advisory summary. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane": lane,
            "source_summary": str(Path(base_output_dir) / lane / "queen_learning_summary.json"),
            "source_status": source_note,
            "thresholds": {
                "min_trades_for_signal": MIN_TRADES_FOR_SIGNAL,
                "win_rate_high": WIN_RATE_HIGH,
                "win_rate_low": WIN_RATE_LOW,
            },
            "groups": advised_groups,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "advisory_version": "1",
            "advisory_type": "queen_advisory_summary",
            "observational_only": True,
            "binding": False,
            "note": (
                "Non-binding advisory summary. "
                "Does not affect execution, allocation, or live gates."
            ),
            "generated_ts_utc": _now_utc(),
            "source_lane": lane,
            "source_summary": str(Path(base_output_dir) / lane / "queen_learning_summary.json"),
            "source_status": f"error: {exc}",
            "thresholds": {
                "min_trades_for_signal": MIN_TRADES_FOR_SIGNAL,
                "win_rate_high": WIN_RATE_HIGH,
                "win_rate_low": WIN_RATE_LOW,
            },
            "groups": [],
        }


def run(base_output_dir: str, lane: str) -> dict[str, Any]:
    """
    Build and persist the Queen advisory summary for one lane.

    Writes:
        {base_output_dir}/{lane}/queen_advisory_summary.json

    Returns:
        {
            "ok": bool,
            "reason": str,
            "output_path": str | None,
            "advisory": dict
        }

    Never raises.
    """
    try:
        advisory = build_advisory(base_output_dir, lane)
        out_path = Path(base_output_dir) / lane / "queen_advisory_summary.json"
        _write_json_atomic(out_path, advisory)
        return {
            "ok": True,
            "reason": "ADVISORY_WRITTEN",
            "output_path": str(out_path),
            "advisory": advisory,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"unexpected advisory error: {exc}",
            "output_path": None,
            "advisory": {},
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m ant_colony.live.queen_advisory <base_output_dir> <lane>")
        sys.exit(1)

    result = run(sys.argv[1], sys.argv[2])
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
