#!/usr/bin/env python3
"""Write a safe Meiro Pipes CLI snapshot for the Dockerized MMM app.

Run from the host where mpcli is installed and authenticated:

    python scripts/meiro_pipes_cli_snapshot.py

The output is written under backend/app/data, which is mounted into the API
container by docker-compose.yml. Secrets and token-like fields are redacted.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "backend" / "app" / "data" / "meiro_pipes_cli_snapshot.json"
DEFAULT_URL = "https://meiro-internal.eu.pipes.meiro.io"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            lower = str(key).lower()
            if any(marker in lower for marker in ("token", "secret", "password", "api_key", "apikey")):
                out[key] = "[redacted]"
            else:
                out[key] = redact(item)
        return out
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str) and value.startswith("mpat_"):
        return "[redacted]"
    return value


def parse_json_objects(text: str) -> List[Dict[str, Any]]:
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
            objects.append(redact(parsed))
        pos = next_pos
    return objects


def run_mpcli(binary: str, base_url: str, args: List[str]) -> Dict[str, Any]:
    cmd = [binary, "--url", base_url, *args]
    completed = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
    parsed = parse_json_objects(completed.stdout or "")
    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "json": parsed,
        "stderr_tail": (completed.stderr or "")[-800:] or None,
    }


def list_count(result: Dict[str, Any]) -> int | None:
    if not result.get("json"):
        return None
    value = result["json"][0]
    if isinstance(value.get("items"), list):
        return len(value["items"])
    if isinstance(value.get("data"), list):
        return len(value["data"])
    if isinstance(value.get("event_streams"), list):
        return len(value["event_streams"])
    return None


def main() -> int:
    binary = os.getenv("MPCLI_BIN") or shutil.which("mpcli")
    if not binary:
        print("mpcli not found on PATH", file=sys.stderr)
        return 1
    base_url = os.getenv("MEIRO_PRISM_BASE_URL") or os.getenv("MEIRO_PIPES_BASE_URL") or DEFAULT_URL
    commands = {
        "status": ["status"],
        "event_streams": ["api", "GET", "/api/event-streams"],
        "pipes": ["api", "GET", "/api/pipes"],
        "event_destinations": ["api", "GET", "/api/event-destinations"],
        "queues": ["api", "GET", "/api/health/queues"],
    }
    results = {name: run_mpcli(binary, base_url, args) for name, args in commands.items()}
    status_objects = results["status"].get("json") or []
    status = {}
    for item in status_objects:
        status.update(item)
    snapshot = {
        "generated_at": now_iso(),
        "instance_url": base_url,
        "status": status,
        "results": results,
        "summary": {
            "event_stream_count": list_count(results["event_streams"]),
            "pipe_count": list_count(results["pipes"]),
            "event_destination_count": list_count(results["event_destinations"]),
            "queues_ok": bool(results["queues"].get("ok")),
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(redact(snapshot), indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
