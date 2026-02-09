"""Stored OAuth/datasource credentials (admin-configured). Fallback when env vars are not set."""
import json
from pathlib import Path
from typing import Optional, Dict, Any

from .encrypt import encrypt, decrypt

CONFIG_PATH = Path(__file__).parent.parent / "data" / "datasource_config.json"

# Keys we store per platform (aligned with env: META_APP_ID -> app_id, etc.)
PLATFORM_KEYS = {
    "google": ["client_id", "client_secret"],
    "meta": ["app_id", "app_secret"],
    "linkedin": ["client_id", "client_secret"],
}

ENV_TO_STORED = {
    "google": {"GOOGLE_CLIENT_ID": "client_id", "GOOGLE_CLIENT_SECRET": "client_secret"},
    "meta": {"META_APP_ID": "app_id", "META_APP_SECRET": "app_secret"},
    "linkedin": {"LINKEDIN_CLIENT_ID": "client_id", "LINKEDIN_CLIENT_SECRET": "client_secret"},
}


def _ensure_dir() -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)


def _load_raw() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text())
    except Exception:
        return {}


def _save_raw(data: Dict[str, Any]) -> None:
    _ensure_dir()
    CONFIG_PATH.write_text(json.dumps(data, indent=2))


def get_stored(platform: str) -> Dict[str, str]:
    """Get decrypted stored config for a platform. Returns e.g. { 'client_id': '...', 'client_secret': '...' }."""
    data = _load_raw()
    platform_data = data.get(platform) or {}
    keys = PLATFORM_KEYS.get(platform, [])
    out = {}
    for k in keys:
        enc = platform_data.get(k)
        if enc:
            out[k] = decrypt(enc)
    return out


def set_stored(platform: str, **kwargs: str) -> None:
    """Store encrypted credentials for a platform. Only provided keys are updated."""
    if platform not in PLATFORM_KEYS:
        raise ValueError(f"Unknown platform: {platform}")
    data = _load_raw()
    platform_data = data.setdefault(platform, {})
    for k, v in kwargs.items():
        if k in PLATFORM_KEYS[platform] and v is not None:
            platform_data[k] = encrypt(v.strip()) if v else ""
    _save_raw(data)


def get_effective(platform: str, key: str) -> str:
    """Return effective value: env var first, then stored. key is stored key (client_id, app_id, etc.)."""
    import os
    env_map = {
        "google": {"client_id": "GOOGLE_CLIENT_ID", "client_secret": "GOOGLE_CLIENT_SECRET"},
        "meta": {"app_id": "META_APP_ID", "app_secret": "META_APP_SECRET"},
        "linkedin": {"client_id": "LINKEDIN_CLIENT_ID", "client_secret": "LINKEDIN_CLIENT_SECRET"},
    }
    env_name = (env_map.get(platform) or {}).get(key)
    if env_name:
        val = os.getenv(env_name, "").strip()
        if val:
            return val
    return (get_stored(platform) or {}).get(key, "")


def get_platform_configured(platform: str) -> bool:
    """True if we have at least the required credentials to start OAuth (e.g. client_id for Google)."""
    keys = PLATFORM_KEYS.get(platform, [])
    return all(get_effective(platform, k) for k in keys)


def get_status() -> Dict[str, Dict[str, bool]]:
    """Return { platform: { configured: bool } } for admin UI. No secret values."""
    return {
        "google": {"configured": get_platform_configured("google")},
        "meta": {"configured": get_platform_configured("meta")},
        "linkedin": {"configured": get_platform_configured("linkedin")},
    }
