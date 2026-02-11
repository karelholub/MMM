"""
Import run persistence for enterprise observability.

Every import creates a run with counts, validation summary, config snapshot.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

IMPORT_RUNS_FILE = Path(__file__).resolve().parent / "data" / "import_runs.json"
MAX_RUNS = 200


def _load() -> List[Dict[str, Any]]:
    if not IMPORT_RUNS_FILE.exists():
        return []
    try:
        raw = IMPORT_RUNS_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(runs: List[Dict[str, Any]]) -> None:
    IMPORT_RUNS_FILE.parent.mkdir(parents=True, exist_ok=True)
    IMPORT_RUNS_FILE.write_text(json.dumps(runs[:MAX_RUNS], indent=0, default=str), encoding="utf-8")


def create_run(
    *,
    source: str,
    status: str = "success",
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    total: int = 0,
    valid: int = 0,
    invalid: int = 0,
    converted: int = 0,
    channels_detected: Optional[List[str]] = None,
    validation_summary: Optional[Dict[str, Any]] = None,
    config_snapshot: Optional[Dict[str, Any]] = None,
    initiated_by: Optional[str] = None,
    import_note: Optional[str] = None,
    error: Optional[str] = None,
    preview_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    now = datetime.utcnow().isoformat() + "Z"
    run = {
        "id": str(uuid.uuid4())[:12],
        "source": source,
        "started_at": started_at or now,
        "finished_at": finished_at or now,
        "status": status,
        "total": total,
        "valid": valid,
        "invalid": invalid,
        "converted": converted,
        "channels_detected": channels_detected or [],
        "validation_summary": validation_summary or {},
        "config_snapshot": config_snapshot or {},
        "initiated_by": initiated_by,
        "import_note": import_note,
        "error": error,
        "preview_rows": preview_rows or [],
    }
    runs = _load()
    runs.insert(0, run)
    _save(runs)
    return run


def _run_with_at(r: Dict[str, Any]) -> Dict[str, Any]:
    """Add 'at' and 'count' for backward compat with legacy consumers."""
    out = dict(r)
    out["at"] = r.get("finished_at") or r.get("started_at") or r.get("at")
    out["count"] = r.get("valid") if r.get("valid") is not None else r.get("count", 0)
    return out


def get_runs(
    status: Optional[str] = None,
    source: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    runs = _load()
    if status:
        runs = [r for r in runs if r.get("status") == status]
    if source:
        runs = [r for r in runs if r.get("source") == source]
    if since:
        runs = [r for r in runs if (r.get("finished_at") or r.get("started_at") or "") >= since]
    if until:
        runs = [r for r in runs if (r.get("finished_at") or r.get("started_at") or "") <= until]
    return [_run_with_at(r) for r in runs[:limit]]


def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    for r in _load():
        if r.get("id") == run_id:
            return r
    return None


def get_last_successful_run() -> Optional[Dict[str, Any]]:
    for r in _load():
        if r.get("status") == "success":
            return r
    return None
