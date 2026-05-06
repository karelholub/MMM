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
_RUNS_CACHE: Dict[str, Any] = {"signature": None, "runs": []}


def _file_signature(path: Path) -> tuple[int, int]:
    try:
        stat = path.stat()
        return int(stat.st_mtime_ns), int(stat.st_size)
    except Exception:
        return 0, 0


def _load_runs() -> List[Dict[str, Any]]:
    if not QUARANTINE_RUNS_FILE.exists():
        return []
    signature = _file_signature(QUARANTINE_RUNS_FILE)
    if _RUNS_CACHE.get("signature") == signature:
        return list(_RUNS_CACHE.get("runs") or [])
    try:
        raw = QUARANTINE_RUNS_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        runs = data if isinstance(data, list) else []
        _RUNS_CACHE["signature"] = signature
        _RUNS_CACHE["runs"] = list(runs)
        return runs
    except Exception:
        return []


def _save_runs(runs: List[Dict[str, Any]]) -> None:
    QUARANTINE_RUNS_FILE.parent.mkdir(parents=True, exist_ok=True)
    QUARANTINE_RUNS_FILE.write_text(
        json.dumps(runs[:MAX_RUNS], indent=2, default=str),
        encoding="utf-8",
    )
    _RUNS_CACHE["signature"] = _file_signature(QUARANTINE_RUNS_FILE)
    _RUNS_CACHE["runs"] = list(runs[:MAX_RUNS])


def create_quarantine_run(
    *,
    source: str,
    import_note: Optional[str] = None,
    parser_version: Optional[str] = None,
    summary: Optional[Dict[str, Any]] = None,
    records: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    now = datetime.utcnow().isoformat() + "Z"
    normalized_records: List[Dict[str, Any]] = []
    for record in (records or [])[:MAX_RECORDS_PER_RUN]:
        if isinstance(record, dict):
            normalized_records.append(
                {
                    **record,
                    "remediation": record.get("remediation")
                    if isinstance(record.get("remediation"), dict)
                    else {
                        "status": "open",
                        "updated_at": now,
                        "history": [],
                    },
                }
            )
    run = {
        "id": str(uuid.uuid4())[:12],
        "source": source,
        "created_at": now,
        "import_note": import_note,
        "parser_version": parser_version,
        "summary": summary or {},
        "records": normalized_records,
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


def get_quarantine_run(run_id: str) -> Optional[Dict[str, Any]]:
    for run in _load_runs():
        if run.get("id") == run_id:
            return run
    return None


def update_quarantine_records(
    run_id: str,
    *,
    record_indices: List[int],
    status: str,
    note: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    runs = _load_runs()
    now = datetime.utcnow().isoformat() + "Z"
    normalized_indices = sorted({idx for idx in record_indices if isinstance(idx, int) and idx >= 0})
    for run in runs:
        if run.get("id") != run_id:
            continue
        records = run.get("records")
        if not isinstance(records, list):
            return run
        for idx in normalized_indices:
            if idx >= len(records):
                continue
            record = records[idx]
            if not isinstance(record, dict):
                continue
            remediation = record.get("remediation")
            if not isinstance(remediation, dict):
                remediation = {"status": "open", "updated_at": now, "history": []}
            history = remediation.get("history")
            if not isinstance(history, list):
                history = []
            history.append(
                {
                    "at": now,
                    "status": status,
                    "note": note,
                    "metadata": metadata or {},
                }
            )
            record["remediation"] = {
                **remediation,
                "status": status,
                "updated_at": now,
                "note": note,
                "metadata": metadata or {},
                "history": history[-20:],
            }
        _save_runs(runs)
        return run
    return None
