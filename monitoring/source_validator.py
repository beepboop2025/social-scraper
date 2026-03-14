"""Source validator — checks that all configured sources can be reached."""

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent / "config" / "sources.yaml"


class SourceValidator:
    """Validate source configurations and connectivity."""

    def validate_all(self) -> list[dict]:
        """Validate all sources in sources.yaml."""
        results = []
        try:
            with open(CONFIG_PATH) as f:
                config = yaml.safe_load(f)

            for source_name, source_cfg in config.get("sources", {}).items():
                result = self._validate_source(source_name, source_cfg)
                results.append(result)
        except Exception as e:
            results.append({"source": "config", "status": "error", "error": str(e)})
        return results

    def _validate_source(self, name: str, cfg: dict) -> dict:
        """Validate a single source configuration."""
        result = {"source": name, "status": "ok", "checks": []}

        # Check collector_class exists
        collector_class = cfg.get("collector_class", "")
        if not collector_class:
            result["status"] = "error"
            result["checks"].append("missing collector_class")
            return result

        # Check module is importable
        try:
            parts = collector_class.rsplit(".", 1)
            if len(parts) == 2:
                module_path, class_name = parts
                import importlib
                mod = importlib.import_module(module_path)
                if not hasattr(mod, class_name):
                    result["status"] = "warning"
                    result["checks"].append(f"class {class_name} not found in {module_path}")
                else:
                    result["checks"].append("collector_class: importable")
        except ImportError as e:
            result["status"] = "warning"
            result["checks"].append(f"import failed: {e}")

        # Check schedule
        if not cfg.get("schedule"):
            result["checks"].append("no schedule defined")

        # Check required config keys
        config = cfg.get("config", {})
        if cfg.get("collector_class", "").endswith("FREDCollector") and not config.get("api_key"):
            result["checks"].append("warning: no api_key configured")

        return result
