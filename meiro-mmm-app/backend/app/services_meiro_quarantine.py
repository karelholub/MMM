"""Persistence helpers for quarantined Meiro ingestion records."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


QUARANTINE_RUNS_FILE = Path(__file__).resolve().parent / "data" / "meiro_quarantine_runs.json"
MAX_RUNS = 100
MAX_RECORDS_PER_RUN = 200


def _load_runs() -> List[Dict[str, Any]]:
    if not QUARANTINE_RUNS_FILE.exists():
        return []
    try:
        raw = QUARANTINE_RUNS_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_runs(runs: List[Dict[str, Any]]) -> None:
    QUARANTINE_RUNS_FILE.parent.mkdir(parents=True, exist_ok=True)
    QUARANTINE_RUNS_FILE.write_text(
        json.dumps(runs[:MAX_RUNS], indent=2, default=str),
        encoding="utf-8",
    )


def create_quarantine_run(
    *,
    source: str,
    import_note: Optional[str] = None,
    parser_version: Optional[str] = None,
    summary: Optional[Dict[str, Any]] = None,
    records: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    now = datetime.utcnow().isoformat() + "Z"
    run = {
        "id": str(uuid.uuid4())[:12],
        "source": source,
        "created_at": now,
        "import_note": import_note,
        "parser_version": parser_version,
        "summary": summary or {},
        "records": (records or [])[:MAX_RECORDS_PER_RUN],
    }
    runs = _load_runs()
    runs.insert(0, run)
    _save_runs(runs)
    return run


def get_quarantine_runs(limit: int = 25, source: Optional[str] = None) -> List[Dict[str, Any]]:
    runs = _load_runs()
    if source:
        runs = [run for run in runs if run.get("source") == source]
    return runs[:limit]
