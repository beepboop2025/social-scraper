"""Injector — writes validated API keys into econscraper's .env and sources.yaml.

Three injection targets:
1. .env file — for docker-compose and direct use
2. sources.yaml — updates ${ENV_VAR} references
3. Runtime os.environ — for current process
"""

import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
SOURCES_YAML = PROJECT_ROOT / "config" / "sources.yaml"


class KeyInjector:
    """Inject API keys into econscraper configuration."""

    def inject_all(self, keys: dict[str, str], targets: list[str] = None) -> dict:
        """Inject keys into specified targets.

        Args:
            keys: {ENV_VAR_NAME: key_value} mapping
            targets: list of "env_file", "runtime", "sources_yaml" (default: all)

        Returns:
            Summary of what was injected where.
        """
        if targets is None:
            targets = ["env_file", "runtime"]

        results = {"injected": 0, "targets": {}}

        if "env_file" in targets:
            env_result = self._inject_env_file(keys)
            results["targets"]["env_file"] = env_result
            results["injected"] += env_result.get("updated", 0)

        if "runtime" in targets:
            rt_result = self._inject_runtime(keys)
            results["targets"]["runtime"] = rt_result
            results["injected"] += rt_result.get("updated", 0)

        return results

    def _inject_env_file(self, keys: dict[str, str]) -> dict:
        """Update .env file with API keys."""
        if not ENV_FILE.exists():
            # Copy from .env.example if available
            example = PROJECT_ROOT / ".env.example"
            if example.exists():
                ENV_FILE.write_text(example.read_text())
                logger.info("[Injector] Created .env from .env.example")
            else:
                ENV_FILE.write_text("")

        content = ENV_FILE.read_text()
        updated = 0

        for env_var, value in keys.items():
            if not value:
                continue

            # Pattern: VAR_NAME= or VAR_NAME=old_value
            pattern = rf'^({re.escape(env_var)})\s*=\s*(.*)$'
            match = re.search(pattern, content, re.MULTILINE)

            if match:
                old_val = match.group(2).strip()
                if old_val != value:
                    content = re.sub(
                        pattern,
                        f"{env_var}={value}",
                        content,
                        flags=re.MULTILINE,
                    )
                    updated += 1
                    logger.info(f"[Injector] Updated {env_var} in .env")
            else:
                # Add new entry
                content = content.rstrip() + f"\n{env_var}={value}\n"
                updated += 1
                logger.info(f"[Injector] Added {env_var} to .env")

        if updated:
            ENV_FILE.write_text(content)

        return {"file": str(ENV_FILE), "updated": updated}

    def _inject_runtime(self, keys: dict[str, str]) -> dict:
        """Set keys in current process environment."""
        updated = 0
        for env_var, value in keys.items():
            if value:
                os.environ[env_var] = value
                updated += 1
        return {"updated": updated}

    def get_missing_keys(self) -> list[dict]:
        """Find which API keys are configured but missing values."""
        from apikeys.catalog import CATALOG

        missing = []
        for api_id, info in CATALOG.items():
            env_vars = info.get("env_vars", {})
            if not env_vars:
                env_var = info.get("env_var", "")
                if env_var:
                    env_vars = {env_var: ""}

            for var in env_vars:
                current = os.getenv(var, "")
                if not current:
                    # Also check .env file
                    env_value = self._read_env_var(var)
                    if not env_value:
                        missing.append({
                            "api_id": api_id,
                            "api_name": info["name"],
                            "env_var": var,
                            "signup_url": info.get("signup_url", ""),
                            "signup_method": info.get("signup_method", ""),
                            "free_tier": info.get("free_tier", ""),
                            "priority": info.get("priority", "low"),
                        })
        return missing

    def get_configured_keys(self) -> list[dict]:
        """Find which API keys are already configured."""
        from apikeys.catalog import CATALOG

        configured = []
        for api_id, info in CATALOG.items():
            env_var = info.get("env_var", "")
            if env_var:
                value = os.getenv(env_var, "") or self._read_env_var(env_var)
                if value:
                    configured.append({
                        "api_id": api_id,
                        "api_name": info["name"],
                        "env_var": env_var,
                        "key_preview": f"{value[:4]}...{value[-4:]}" if len(value) > 8 else "****",
                    })
        return configured

    def _read_env_var(self, var_name: str) -> str:
        """Read a variable from the .env file."""
        if not ENV_FILE.exists():
            return ""

        content = ENV_FILE.read_text()
        match = re.search(rf'^{re.escape(var_name)}\s*=\s*(.+)$', content, re.MULTILINE)
        return match.group(1).strip() if match else ""

    def sync_from_vault(self) -> dict:
        """Pull all valid keys from vault and inject them."""
        from apikeys.vault import KeyVault

        vault = KeyVault()
        keys = vault.get_all_valid_keys()
        if not keys:
            return {"status": "no_keys", "injected": 0}

        return self.inject_all(keys)
