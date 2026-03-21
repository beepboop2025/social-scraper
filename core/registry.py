"""Auto-discovers and registers collectors from /collectors/ and sources.yaml.

Adding a new source = drop a .py file in collectors/ + add config to sources.yaml.
Zero changes to core code.
"""

import importlib
import logging
import os
import re
from pathlib import Path
from typing import Optional

import yaml

from core.base_collector import BaseCollector
from core.exceptions import EconScraperError

logger = logging.getLogger(__name__)


class ConfigurationError(EconScraperError):
    """Raised when a required environment variable is missing or empty."""

    def __init__(self, var_name: str, source_name: str = ""):
        self.var_name = var_name
        self.source_name = source_name
        msg = f"Required env var '{var_name}' is missing or empty"
        if source_name:
            msg += f" (needed by source '{source_name}')"
        super().__init__(msg)

_registry: dict[str, type[BaseCollector]] = {}


_unresolved_env_vars: list[tuple[str, str]] = []


def _substitute_env_vars(value, _source_name: str = ""):
    """Replace ${VAR_NAME} with environment variable values.

    Tracks unresolved variables in _unresolved_env_vars for later validation.
    Raises ConfigurationError if a required env var is missing.
    """
    if isinstance(value, str):
        pattern = re.compile(r"\$\{(\w+)\}")

        def replacer(match):
            var_name = match.group(1)
            env_val = os.getenv(var_name)
            if env_val is None or env_val == "":
                _unresolved_env_vars.append((var_name, _source_name))
                logger.warning(
                    f"[Registry] Env var '${{{var_name}}}' is not set"
                    + (f" (used by source '{_source_name}')" if _source_name else "")
                )
                return ""
            return env_val

        return pattern.sub(replacer, value)
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v, _source_name) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(v, _source_name) for v in value]
    return value


def load_sources_config(config_path: str = "config/sources.yaml") -> dict:
    """Load and parse sources.yaml with env var substitution."""
    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Sources config not found: {config_path}")
        return {}

    with open(path) as f:
        raw = yaml.safe_load(f)

    sources = raw.get("sources", {})
    resolved = {}
    for name, cfg in sources.items():
        resolved[name] = _substitute_env_vars(cfg, _source_name=name)
    return resolved


def _import_class(class_path: str) -> type:
    """Import a class from a dotted path like 'collectors.fred_api.FredCollector'."""
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def validate(config_path: str = "config/sources.yaml") -> dict:
    """Validate all sources configuration at startup.

    Checks that required environment variables are set.
    Returns a dict with 'valid', 'disabled_sources', and 'missing_vars'.
    """
    global _unresolved_env_vars
    _unresolved_env_vars = []

    sources = load_sources_config(config_path)
    disabled_sources = []
    missing_vars_by_source: dict[str, list[str]] = {}

    for var_name, source_name in _unresolved_env_vars:
        if source_name not in missing_vars_by_source:
            missing_vars_by_source[source_name] = []
        missing_vars_by_source[source_name].append(var_name)

    for source_name, vars_list in missing_vars_by_source.items():
        if source_name:
            disabled_sources.append(source_name)
            logger.warning(
                f"[Registry] Source '{source_name}' disabled — "
                f"missing env vars: {', '.join(vars_list)}"
            )

    result = {
        "valid": len(missing_vars_by_source) == 0,
        "disabled_sources": disabled_sources,
        "missing_vars": missing_vars_by_source,
        "total_sources": len(sources),
    }
    logger.info(
        f"[Registry] Validation complete: {len(sources)} sources, "
        f"{len(disabled_sources)} disabled due to missing credentials"
    )
    return result


def discover_collectors(config_path: str = "config/sources.yaml") -> dict[str, BaseCollector]:
    """Discover and instantiate all enabled collectors from sources.yaml.

    Returns a dict of {source_name: collector_instance}.
    Sources with missing required env vars are skipped with a warning.
    """
    global _unresolved_env_vars
    _unresolved_env_vars = []

    sources = load_sources_config(config_path)

    # Build set of sources with missing env vars
    sources_missing_creds = set()
    for var_name, source_name in _unresolved_env_vars:
        if source_name:
            sources_missing_creds.add(source_name)

    collectors = {}

    for name, source_config in sources.items():
        if not source_config.get("enabled", True):
            logger.info(f"[Registry] Skipping disabled source: {name}")
            continue

        if name in sources_missing_creds:
            missing = [v for v, s in _unresolved_env_vars if s == name]
            logger.warning(
                f"[Registry] Skipping source '{name}' — "
                f"missing required env vars: {', '.join(missing)}"
            )
            continue

        class_path = source_config.get("collector_class")
        if not class_path:
            logger.warning(f"[Registry] No collector_class for source: {name}")
            continue

        try:
            cls = _import_class(class_path)
            config = {
                "schedule": source_config.get("schedule", "0 * * * *"),
                **source_config.get("config", {}),
            }
            instance = cls(config)
            instance.name = name
            collectors[name] = instance
            _registry[name] = cls
            logger.info(f"[Registry] Registered collector: {name} ({class_path})")
        except Exception as e:
            logger.error(f"[Registry] Failed to register {name}: {e}")

    return collectors


def get_registered() -> dict[str, type[BaseCollector]]:
    """Return all registered collector classes."""
    return dict(_registry)


def get_schedules(config_path: str = "config/sources.yaml") -> dict[str, str]:
    """Return {source_name: cron_schedule} for all enabled sources."""
    sources = load_sources_config(config_path)
    return {
        name: cfg.get("schedule", "0 * * * *")
        for name, cfg in sources.items()
        if cfg.get("enabled", True)
    }
