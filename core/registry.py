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

logger = logging.getLogger(__name__)

_registry: dict[str, type[BaseCollector]] = {}


def _substitute_env_vars(value):
    """Replace ${VAR_NAME} with environment variable values."""
    if isinstance(value, str):
        pattern = re.compile(r"\$\{(\w+)\}")
        def replacer(match):
            return os.getenv(match.group(1), "")
        return pattern.sub(replacer, value)
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(v) for v in value]
    return value


def load_sources_config(config_path: str = "config/sources.yaml") -> dict:
    """Load and parse sources.yaml with env var substitution."""
    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Sources config not found: {config_path}")
        return {}

    with open(path) as f:
        raw = yaml.safe_load(f)

    return _substitute_env_vars(raw.get("sources", {}))


def _import_class(class_path: str) -> type:
    """Import a class from a dotted path like 'collectors.fred_api.FredCollector'."""
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def discover_collectors(config_path: str = "config/sources.yaml") -> dict[str, BaseCollector]:
    """Discover and instantiate all enabled collectors from sources.yaml.

    Returns a dict of {source_name: collector_instance}.
    """
    sources = load_sources_config(config_path)
    collectors = {}

    for name, source_config in sources.items():
        if not source_config.get("enabled", True):
            logger.info(f"[Registry] Skipping disabled source: {name}")
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
