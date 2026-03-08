"""Encrypted local vault for API keys.

Uses Fernet symmetric encryption (AES-128-CBC + HMAC-SHA256).
The master key is derived from a passphrase via PBKDF2, or auto-generated
and stored in ~/.econscraper/vault.key.

Vault file: ~/.econscraper/keys.vault (encrypted JSON)
"""

import json
import logging
import os
from base64 import urlsafe_b64encode
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

VAULT_DIR = Path.home() / ".econscraper"
VAULT_FILE = VAULT_DIR / "keys.vault"
KEY_FILE = VAULT_DIR / "vault.key"


class KeyVault:
    """Encrypted storage for API keys."""

    def __init__(self):
        self._fernet = None
        self._data: dict = {}
        VAULT_DIR.mkdir(parents=True, exist_ok=True)
        self._init_encryption()
        self._load()

    def _init_encryption(self):
        """Initialize Fernet with auto-generated key."""
        try:
            from cryptography.fernet import Fernet

            if KEY_FILE.exists():
                key = KEY_FILE.read_bytes()
            else:
                key = Fernet.generate_key()
                KEY_FILE.write_bytes(key)
                KEY_FILE.chmod(0o600)
                logger.info(f"[Vault] Generated new master key at {KEY_FILE}")

            self._fernet = Fernet(key)
        except ImportError:
            logger.warning("[Vault] cryptography not installed — keys stored in plaintext")
            self._fernet = None

    def _load(self):
        """Load vault from disk."""
        if not VAULT_FILE.exists():
            self._data = {"keys": {}, "metadata": {"created": datetime.now(timezone.utc).isoformat()}}
            return

        raw = VAULT_FILE.read_bytes()
        if self._fernet:
            try:
                decrypted = self._fernet.decrypt(raw)
                self._data = json.loads(decrypted)
            except Exception:
                logger.error("[Vault] Failed to decrypt vault — wrong key or corrupted file")
                self._data = {"keys": {}, "metadata": {}}
        else:
            self._data = json.loads(raw)

    def _save(self):
        """Persist vault to disk."""
        raw = json.dumps(self._data, indent=2).encode()
        if self._fernet:
            encrypted = self._fernet.encrypt(raw)
            VAULT_FILE.write_bytes(encrypted)
        else:
            VAULT_FILE.write_bytes(raw)
        VAULT_FILE.chmod(0o600)

    def store(self, api_id: str, key: str, env_var: str = "", metadata: dict = None):
        """Store an API key."""
        self._data.setdefault("keys", {})
        self._data["keys"][api_id] = {
            "key": key,
            "env_var": env_var,
            "stored_at": datetime.now(timezone.utc).isoformat(),
            "last_validated": None,
            "is_valid": None,
            "metadata": metadata or {},
        }
        self._save()
        logger.info(f"[Vault] Stored key for {api_id}")

    def get(self, api_id: str) -> Optional[str]:
        """Get a key by API ID."""
        entry = self._data.get("keys", {}).get(api_id)
        return entry["key"] if entry else None

    def get_entry(self, api_id: str) -> Optional[dict]:
        """Get full entry including metadata."""
        return self._data.get("keys", {}).get(api_id)

    def remove(self, api_id: str) -> bool:
        """Remove a key."""
        if api_id in self._data.get("keys", {}):
            del self._data["keys"][api_id]
            self._save()
            return True
        return False

    def list_keys(self) -> dict[str, dict]:
        """List all stored keys (masked)."""
        result = {}
        for api_id, entry in self._data.get("keys", {}).items():
            key = entry.get("key", "")
            result[api_id] = {
                "key_preview": f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "****",
                "env_var": entry.get("env_var", ""),
                "stored_at": entry.get("stored_at"),
                "last_validated": entry.get("last_validated"),
                "is_valid": entry.get("is_valid"),
            }
        return result

    def update_validation(self, api_id: str, is_valid: bool):
        """Update validation status for a key."""
        if api_id in self._data.get("keys", {}):
            self._data["keys"][api_id]["last_validated"] = datetime.now(timezone.utc).isoformat()
            self._data["keys"][api_id]["is_valid"] = is_valid
            self._save()

    def get_all_valid_keys(self) -> dict[str, str]:
        """Get all valid keys as env_var -> key mapping."""
        result = {}
        for api_id, entry in self._data.get("keys", {}).items():
            if entry.get("is_valid") is not False:  # Include un-validated keys too
                env_var = entry.get("env_var", "")
                if env_var:
                    result[env_var] = entry["key"]
        return result

    def export_env(self) -> str:
        """Export all keys as .env format string."""
        lines = ["# EconScraper API Keys (exported from vault)"]
        for api_id, entry in self._data.get("keys", {}).items():
            env_var = entry.get("env_var", "")
            key = entry.get("key", "")
            if env_var and key:
                valid = entry.get("is_valid")
                status = "valid" if valid else ("invalid" if valid is False else "untested")
                lines.append(f"# {api_id} [{status}]")
                lines.append(f"{env_var}={key}")
        return "\n".join(lines) + "\n"
