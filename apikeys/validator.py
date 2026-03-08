"""API key validator — tests keys against their provider endpoints.

Makes a lightweight test API call for each key to verify it works.
Returns validation results with rate limit info when available.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from apikeys.catalog import CATALOG

logger = logging.getLogger(__name__)


class KeyValidator:
    """Validate API keys by making test calls."""

    def __init__(self, timeout: int = 15):
        self.timeout = timeout

    def validate(self, api_id: str, key: str) -> dict:
        """Validate a single API key.

        Returns:
            {
                "api_id": str,
                "is_valid": bool,
                "status_code": int | None,
                "message": str,
                "rate_limit": dict | None,
                "tested_at": str,
            }
        """
        catalog_entry = CATALOG.get(api_id)
        if not catalog_entry:
            return self._result(api_id, False, message=f"Unknown API: {api_id}")

        test_url = catalog_entry.get("test_endpoint")
        if not test_url:
            return self._result(api_id, None, message="No test endpoint configured")

        url = test_url.replace("{key}", key)

        headers = {}
        if catalog_entry.get("test_headers"):
            headers = {
                k: v.replace("{key}", key)
                for k, v in catalog_entry["test_headers"].items()
            }

        try:
            resp = httpx.get(url, headers=headers, timeout=self.timeout, follow_redirects=True)

            expected_status = catalog_entry.get("test_status", 200)
            is_valid = resp.status_code == expected_status

            # Additional JSON validation if configured
            if is_valid and catalog_entry.get("test_json_key"):
                try:
                    data = resp.json()
                    if catalog_entry["test_json_key"] not in data:
                        is_valid = False
                except Exception:
                    pass

            # Extract rate limit info
            rate_limit = self._extract_rate_limit(resp.headers)

            # Check for explicit error messages
            message = "Valid" if is_valid else f"HTTP {resp.status_code}"
            if not is_valid:
                try:
                    error_data = resp.json()
                    if isinstance(error_data, dict):
                        message = error_data.get("error", {}).get("message", "") or \
                                  error_data.get("message", "") or \
                                  error_data.get("error", message)
                except Exception:
                    pass

            return self._result(api_id, is_valid, resp.status_code, message, rate_limit)

        except httpx.TimeoutException:
            return self._result(api_id, None, message="Timeout — endpoint didn't respond")
        except httpx.ConnectError:
            return self._result(api_id, None, message="Connection failed — check network")
        except Exception as e:
            return self._result(api_id, False, message=f"Error: {str(e)[:200]}")

    def validate_all(self, keys: dict[str, str]) -> list[dict]:
        """Validate multiple keys. keys = {api_id: key_value}."""
        results = []
        for api_id, key in keys.items():
            result = self.validate(api_id, key)
            results.append(result)
        return results

    def validate_from_env(self) -> list[dict]:
        """Validate all keys found in environment variables."""
        import os
        from apikeys.catalog import get_all_env_vars

        results = []
        env_map = get_all_env_vars()

        for env_var, api_id in env_map.items():
            key = os.getenv(env_var, "")
            if key:
                result = self.validate(api_id, key)
                result["env_var"] = env_var
                results.append(result)

        return results

    def _extract_rate_limit(self, headers: dict) -> Optional[dict]:
        """Extract rate limit info from response headers."""
        rate_info = {}

        # Standard headers
        for key in ("x-ratelimit-limit", "x-rate-limit-limit", "ratelimit-limit"):
            if key in headers:
                rate_info["limit"] = headers[key]
                break

        for key in ("x-ratelimit-remaining", "x-rate-limit-remaining", "ratelimit-remaining"):
            if key in headers:
                rate_info["remaining"] = headers[key]
                break

        for key in ("x-ratelimit-reset", "x-rate-limit-reset", "ratelimit-reset"):
            if key in headers:
                rate_info["reset"] = headers[key]
                break

        return rate_info if rate_info else None

    def _result(
        self,
        api_id: str,
        is_valid: Optional[bool],
        status_code: int = None,
        message: str = "",
        rate_limit: dict = None,
    ) -> dict:
        return {
            "api_id": api_id,
            "is_valid": is_valid,
            "status_code": status_code,
            "message": message,
            "rate_limit": rate_limit,
            "tested_at": datetime.now(timezone.utc).isoformat(),
        }
