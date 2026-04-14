"""
AC-167: Live Artifact Writer

Persists live execution artifacts to disk after each successful fill.

Directory layout:
    {base_output_dir}/{lane}/execution/  — entry execution results (AC-148)
    {base_output_dir}/{lane}/broker/     — raw broker responses (secrets stripped)
    {base_output_dir}/{lane}/feedback/   — closed-trade feedback records (AC-159)
    {base_output_dir}/{lane}/memory/     — queen memory entries (AC-161)

All writes are atomic: write to .tmp then os.replace() to the final path.
Fail-closed: any write failure returns ok=False with a descriptive reason.
No secrets are persisted (api_key, api_secret, etc. are stripped recursively).
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

_SECRET_KEYS: frozenset[str] = frozenset({
    "api_key", "api_secret", "APIKEY", "APISECRET", "apiKey", "apiSecret",
    "api_token", "access_token", "password", "secret",
})

_SAFE_NAME_RE = re.compile(r'[^a-zA-Z0-9_\-]')


def _strip_secrets(obj: Any) -> Any:
    """Recursively remove known secret keys from dicts. Lists are traversed."""
    if isinstance(obj, dict):
        return {k: _strip_secrets(v) for k, v in obj.items() if k not in _SECRET_KEYS}
    if isinstance(obj, list):
        return [_strip_secrets(item) for item in obj]
    return obj


def _safe_filename(name: str) -> str:
    """Return a filesystem-safe name: only alphanumeric, hyphens, underscores."""
    return _SAFE_NAME_RE.sub('_', name)


def _write_json_atomic(path: Path, obj: Any) -> None:
    """
    Write JSON atomically.

    Writes to a .tmp sibling first, then os.replace() to the target path.
    os.replace() is atomic on the same filesystem on both Windows and POSIX.
    Creates all parent directories with parents=True, exist_ok=True.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def write_entry_artifacts(
    base_output_dir: str,
    lane: str,
    execution_result: dict[str, Any],
    broker_response: dict[str, Any],
) -> dict[str, Any]:
    """
    Write execution and broker response artifacts for a live fill.

    Creates:
        {base_output_dir}/{lane}/execution/{trade_id}.json
        {base_output_dir}/{lane}/broker/{trade_id}.json

    The broker artifact has secrets stripped before writing.

    Returns:
        {"ok": True, "paths": {"execution": str, "broker": str}}
        {"ok": False, "reason": str}
    """
    try:
        trade_id = _safe_filename(str(execution_result.get("trade_id") or "UNKNOWN"))
        base = Path(base_output_dir) / lane

        exec_path = base / "execution" / f"{trade_id}.json"
        broker_path = base / "broker" / f"{trade_id}.json"

        _write_json_atomic(exec_path, execution_result)
        _write_json_atomic(broker_path, _strip_secrets(broker_response))

        return {
            "ok": True,
            "paths": {
                "execution": str(exec_path),
                "broker": str(broker_path),
            },
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"write_entry_artifacts failed: {exc}"}


def write_feedback_artifact(
    base_output_dir: str,
    lane: str,
    feedback_record: dict[str, Any],
) -> dict[str, Any]:
    """
    Write a closed-trade feedback artifact.

    Creates:
        {base_output_dir}/{lane}/feedback/{trade_id}.json

    Returns:
        {"ok": True, "paths": {"feedback": str}}
        {"ok": False, "reason": str}
    """
    try:
        trade_id = _safe_filename(str(feedback_record.get("trade_id") or "UNKNOWN"))
        path = Path(base_output_dir) / lane / "feedback" / f"{trade_id}.json"
        _write_json_atomic(path, feedback_record)
        return {"ok": True, "paths": {"feedback": str(path)}}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"write_feedback_artifact failed: {exc}"}


def write_exit_artifact(
    base_output_dir: str,
    lane: str,
    exit_artifact: dict[str, Any],
) -> dict[str, Any]:
    """
    AC-188: Write an exit artifact after a live position is closed.

    Creates:
        {base_output_dir}/{lane}/exit/{safe_entry_order_id}.json

    The filename is keyed on entry_order_id (which equals broker_order_id_entry
    in the corresponding execution artifact) so the open-position guard can
    cross-reference them.

    Returns:
        {"ok": True, "paths": {"exit": str}}
        {"ok": False, "reason": str}
    """
    try:
        entry_order_id = _safe_filename(
            str(exit_artifact.get("entry_order_id") or "UNKNOWN")
        )
        path = Path(base_output_dir) / lane / "exit" / f"{entry_order_id}.json"
        _write_json_atomic(path, exit_artifact)
        return {"ok": True, "paths": {"exit": str(path)}}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"write_exit_artifact failed: {exc}"}


def write_memory_artifact(
    base_output_dir: str,
    lane: str,
    memory_entry: dict[str, Any],
) -> dict[str, Any]:
    """
    Write a queen memory artifact.

    Creates:
        {base_output_dir}/{lane}/memory/{entry_id}.json

    entry_id is taken from memory_entry["entry_id"] if present, else ["trade_id"].

    Returns:
        {"ok": True, "paths": {"memory": str}}
        {"ok": False, "reason": str}
    """
    try:
        entry_id = _safe_filename(
            str(memory_entry.get("entry_id") or memory_entry.get("trade_id") or "UNKNOWN")
        )
        path = Path(base_output_dir) / lane / "memory" / f"{entry_id}.json"
        _write_json_atomic(path, memory_entry)
        return {"ok": True, "paths": {"memory": str(path)}}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"write_memory_artifact failed: {exc}"}
