"""Token encryption utilities for OAuth tokens."""
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64

# Use environment variable for encryption key, or generate a default (for dev only)
ENCRYPT_KEY = os.getenv("ENCRYPT_KEY", "dev-key-change-in-production-32-chars!")

def _get_fernet():
    """Get Fernet cipher instance from encryption key."""
    # Derive a key from the password using PBKDF2
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'meiro_mmm_salt_2024',
        iterations=100000,
        backend=default_backend()
    )
    key = base64.urlsafe_b64encode(kdf.derive(ENCRYPT_KEY.encode()))
    return Fernet(key)

def encrypt(text: str) -> str:
    """Encrypt a plaintext string."""
    if not text:
        return ""
    f = _get_fernet()
    return f.encrypt(text.encode()).decode()

def decrypt(encrypted_text: str) -> str:
    """Decrypt an encrypted string."""
    if not encrypted_text:
        return ""
    f = _get_fernet()
    try:
        return f.decrypt(encrypted_text.encode()).decode()
    except Exception:
        # If decryption fails, return empty string
        return ""

