"""Store notification secrets (e.g. Slack webhook URLs) outside DB. Never expose in API."""

from pathlib import Path
from typing import Any, Dict, Optional

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SECRETS_PATH = DATA_DIR / "notification_secrets.json"


def _load() -> Dict[str, Any]:
    if not SECRETS_PATH.exists():
        return {}
    try:
        return __import__("json").loads(SECRETS_PATH.read_text())
    except Exception:
        return {}


def _save(data: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SECRETS_PATH.write_text(__import__("json").dumps(data, indent=2))


def get_webhook_url(channel_id: int) -> Optional[str]:
    """Return Slack webhook URL for channel_id if stored. Never log or return to client."""
    return _load().get("webhooks", {}).get(str(channel_id))


def set_webhook_url(channel_id: int, url: Optional[str]) -> None:
    """Store or remove Slack webhook URL for channel_id."""
    d = _load()
    webhooks = d.get("webhooks", {})
    if url is None or (isinstance(url, str) and not url.strip()):
        webhooks.pop(str(channel_id), None)
    else:
        webhooks[str(channel_id)] = url.strip()
    d["webhooks"] = webhooks
    _save(d)
