"""Optional Meiro Pipes CLI diagnostics.

The MMM API usually runs in Docker, while mpcli is often authenticated on the
host. This module therefore supports two modes:

- read a host-generated snapshot from app/data
- run mpcli directly when it is installed inside the API container
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.utils.meiro_config import get_target_instance_host, get_target_instance_url, instance_scope


DATA_DIR = Path(__file__).resolve().parent / "data"
SNAPSHOT_PATH = DATA_DIR / "meiro_pipes_cli_snapshot.json"
DEFAULT_TIMEOUT_SECONDS = 8


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: Dict[str, Any] = {}
        for key, item in value.items():
            lower = str(key).lower()
            if any(marker in lower for marker in ("token", "secret", "password", "api_key", "apikey")):
                redacted[key] = "[redacted]"
            else:
                redacted[key] = _redact(item)
        return redacted
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str) and value.startswith("mpat_"):
        return "[redacted]"
    return value


def _parse_json_objects(text: str) -> List[Dict[str, Any]]:
    decoder = json.JSONDecoder()
    pos = 0
    objects: List[Dict[str, Any]] = []
    while pos < len(text):
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos >= len(text):
            break
        try:
            parsed, next_pos = decoder.raw_decode(text, pos)
        except Exception:
            break
        if isinstance(parsed, dict):
            objects.append(_redact(parsed))
        pos = next_pos
    return objects


def _run_mpcli(args: List[str], *, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]:
    binary = os.getenv("MPCLI_BIN") or shutil.which("mpcli")
    if not binary:
        return {
            "ok": False,
            "available": False,
            "error": "mpcli_not_installed_in_api_container",
            "hint": "Generate a host snapshot with scripts/meiro_pipes_cli_snapshot.py or install mpcli in the API container.",
        }
    cmd = [binary, "--url", get_target_instance_url(), *args]
    env = {**os.environ}
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(1, min(30, int(timeout_seconds))),
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "available": True, "error": "timeout", "timeout_seconds": timeout_seconds}
    except Exception as exc:
        return {"ok": False, "available": True, "error": type(exc).__name__, "detail": str(exc)}
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    return {
        "ok": completed.returncode == 0,
        "available": True,
        "returncode": completed.returncode,
        "json": _parse_json_objects(stdout),
        "stdout_tail": stdout[-800:] if stdout and not _parse_json_objects(stdout) else None,
        "stderr_tail": stderr[-800:] if stderr else None,
    }


def read_snapshot() -> Optional[Dict[str, Any]]:
    try:
        parsed = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return _redact(parsed) if isinstance(parsed, dict) else None


def build_pipes_cli_status(*, live: bool = False) -> Dict[str, Any]:
    snapshot = read_snapshot()
    live_status = _run_mpcli(["status"]) if live else None
    source = "live" if live_status and live_status.get("available") else "snapshot" if snapshot else "none"
    status_objects = (live_status or {}).get("json") if isinstance(live_status, dict) else []
    snapshot_status = snapshot.get("status") if isinstance(snapshot, dict) else None
    instance_url = None
    auth_status = None
    api_info = None
    if status_objects:
        for item in status_objects:
            if item.get("instance"):
                instance_url = item.get("instance")
                api_info = item.get("api")
            if item.get("auth"):
                auth_status = item.get("auth")
    elif isinstance(snapshot_status, dict):
        instance_url = snapshot_status.get("instance")
        api_info = snapshot_status.get("api")
        auth_status = snapshot_status.get("auth")
    auth_text = str(auth_status or "").strip().lower()
    authenticated = bool(auth_text) and auth_text not in {
        "missing_or_invalid_token",
        "missing",
        "invalid",
        "unauthenticated",
        "not_authenticated",
    }
    scope = instance_scope(instance_url or get_target_instance_url())
    return {
        "generated_at": _now_iso(),
        "source": source,
        "target": {
            "instance_url": get_target_instance_url(),
            "instance_host": get_target_instance_host(),
        },
        "status": {
            "available": bool((live_status or {}).get("available") or snapshot),
            "live_checked": bool(live_status),
            "live_ok": bool((live_status or {}).get("ok")) if live_status else None,
            "instance_url": instance_url,
            "api": api_info,
            "auth": auth_status,
            "authenticated": authenticated,
            "instance_scope": scope,
            "error": (live_status or {}).get("error") if live_status and not live_status.get("ok") else None,
            "hint": (live_status or {}).get("hint") if live_status else None,
        },
        "snapshot": {
            "available": bool(snapshot),
            "generated_at": snapshot.get("generated_at") if isinstance(snapshot, dict) else None,
            "path": str(SNAPSHOT_PATH),
            "summary": snapshot.get("summary") if isinstance(snapshot, dict) else None,
        },
    }
