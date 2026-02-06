"""Token storage system for OAuth tokens."""
import json
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
from .encrypt import encrypt, decrypt

TOKENS_DB_PATH = Path(__file__).parent.parent / "data" / "tokens.json"

def _ensure_dir():
    """Ensure data directory exists."""
    TOKENS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def save_token(platform: str, data: Dict[str, Any]) -> None:
    """Save encrypted OAuth token for a platform."""
    _ensure_dir()
    
    tokens = {}
    if TOKENS_DB_PATH.exists():
        try:
            tokens = json.loads(TOKENS_DB_PATH.read_text())
        except Exception:
            tokens = {}
    
    # Encrypt sensitive fields
    encrypted_data = {
        "access_token": encrypt(data.get("access_token", "")),
        "refresh_token": encrypt(data.get("refresh_token", "")) if data.get("refresh_token") else None,
        "expires_in": data.get("expires_in"),
        "token_type": data.get("token_type", "Bearer"),
        "updated_at": datetime.now().isoformat(),
    }
    
    # Store any additional fields (like ad_account_id for Meta)
    if "ad_account_id" in data:
        encrypted_data["ad_account_id"] = data["ad_account_id"]
    
    tokens[platform] = encrypted_data
    
    TOKENS_DB_PATH.write_text(json.dumps(tokens, indent=2))

def get_token(platform: str) -> Optional[Dict[str, Any]]:
    """Retrieve and decrypt token for a platform."""
    if not TOKENS_DB_PATH.exists():
        return None
    
    try:
        tokens = json.loads(TOKENS_DB_PATH.read_text())
        token_data = tokens.get(platform)
        if not token_data:
            return None
        
        # Decrypt sensitive fields
        decrypted = {
            "access_token": decrypt(token_data.get("access_token", "")),
            "refresh_token": decrypt(token_data.get("refresh_token", "")) if token_data.get("refresh_token") else None,
            "expires_in": token_data.get("expires_in"),
            "token_type": token_data.get("token_type", "Bearer"),
            "updated_at": token_data.get("updated_at"),
        }
        
        # Include additional fields
        if "ad_account_id" in token_data:
            decrypted["ad_account_id"] = token_data["ad_account_id"]
        
        return decrypted
    except Exception as e:
        print(f"Error reading token for {platform}: {e}")
        return None

def delete_token(platform: str) -> bool:
    """Delete token for a platform."""
    if not TOKENS_DB_PATH.exists():
        return False
    
    try:
        tokens = json.loads(TOKENS_DB_PATH.read_text())
        if platform in tokens:
            del tokens[platform]
            TOKENS_DB_PATH.write_text(json.dumps(tokens, indent=2))
            return True
    except Exception:
        pass
    
    return False

def get_all_connected_platforms() -> list[str]:
    """Get list of all connected platforms."""
    if not TOKENS_DB_PATH.exists():
        return []
    
    try:
        tokens = json.loads(TOKENS_DB_PATH.read_text())
        return [p for p in tokens.keys() if get_token(p) is not None]
    except Exception:
        return []

