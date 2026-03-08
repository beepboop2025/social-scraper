"""API key provisioner — guides users through getting API keys.

For APIs with programmatic signup (rare), handles auto-registration.
For most APIs, provides step-by-step instructions with direct links.
"""

import logging
import webbrowser
from typing import Optional

from apikeys.catalog import CATALOG, CATEGORIES

logger = logging.getLogger(__name__)


class KeyProvisioner:
    """Guide users through API key acquisition."""

    def get_setup_plan(self, priority: str = None) -> list[dict]:
        """Generate a prioritized plan for which keys to get.

        Returns a list of APIs sorted by priority and ease of signup.
        """
        plan = []
        for api_id, info in CATALOG.items():
            if priority and info.get("priority") != priority:
                continue

            plan.append({
                "api_id": api_id,
                "name": info["name"],
                "priority": info["priority"],
                "free_tier": info["free_tier"],
                "signup_method": info["signup_method"],
                "signup_url": info["signup_url"],
                "time_estimate": "1 min" if info["signup_method"] == "instant" else "5 min",
                "env_var": info.get("env_var", ""),
                "category": info.get("category", ""),
            })

        # Sort: high priority first, then instant signup first
        priority_order = {"high": 0, "medium": 1, "low": 2}
        method_order = {"instant": 0, "manual": 1}
        plan.sort(key=lambda x: (
            priority_order.get(x["priority"], 3),
            method_order.get(x["signup_method"], 2),
        ))

        return plan

    def get_instructions(self, api_id: str) -> Optional[dict]:
        """Get detailed signup instructions for a specific API."""
        info = CATALOG.get(api_id)
        if not info:
            return None

        return {
            "api_id": api_id,
            "name": info["name"],
            "provider": info.get("provider", ""),
            "signup_url": info["signup_url"],
            "free_tier": info["free_tier"],
            "steps": info.get("signup_steps", []),
            "env_var": info.get("env_var", ""),
            "env_vars": info.get("env_vars"),
            "used_by": info.get("used_by", []),
            "tip": self._get_tip(api_id),
        }

    def open_signup(self, api_id: str) -> bool:
        """Open the signup URL in the user's browser."""
        info = CATALOG.get(api_id)
        if not info:
            return False

        url = info.get("signup_url", "")
        if url:
            webbrowser.open(url)
            return True
        return False

    def get_quick_start_apis(self) -> list[dict]:
        """Get APIs that can be set up in under 2 minutes (instant signup)."""
        return [
            {
                "api_id": api_id,
                "name": info["name"],
                "free_tier": info["free_tier"],
                "signup_url": info["signup_url"],
                "env_var": info.get("env_var", ""),
            }
            for api_id, info in CATALOG.items()
            if info.get("signup_method") == "instant" and info.get("priority") in ("high", "medium")
        ]

    def get_no_key_apis(self) -> list[str]:
        """List APIs/sources that work without any API key."""
        return [
            "Hacker News (Firebase public API)",
            "RSS feeds (public feeds)",
            "SEC EDGAR (public, rate limited)",
            "RBI DBIE (public data portal)",
            "RBI Circulars (web scraping)",
            "SEBI Circulars (web scraping)",
            "BSE API (public)",
            "NSE Bhavcopy (public)",
            "CCIL/FBIL Rates (public)",
            "World Bank API (no key needed)",
            "IMF SDMX API (no key needed)",
            "Reddit (.json endpoints, no auth needed)",
            "CoinGecko (basic endpoints, no key)",
            "arXiv RSS (public)",
        ]

    def _get_tip(self, api_id: str) -> str:
        """Return a helpful tip for specific APIs."""
        tips = {
            "fred": "FRED is essential for US economic data — the key activates instantly.",
            "alpha_vantage": "Only 25 requests/day on free tier. Good for daily snapshots, not real-time.",
            "finnhub": "Best free source for real-time US stock quotes. WebSocket also included.",
            "polygon": "Free tier has delayed data but unlimited history — great for backtesting.",
            "newsapi": "Free tier only returns articles from last month. Good for current events.",
            "reddit": "Works without auth at 10 req/min. Auth bumps to 60 req/min.",
            "youtube": "Each search costs 100 units of 10,000 daily quota = ~100 searches/day.",
            "github": "Without token: 60 req/hr. With token: 5000 req/hr. Big difference.",
            "telegram": "You need BOTH a Bot Token (for alerts) AND API ID/Hash (for channel scraping).",
            "data_gov_in": "Some datasets work without key, but key removes rate limits.",
            "coingecko": "Basic endpoints work without key. Demo key doubles rate limits.",
            "anthropic": "Used for daily digest generation and RAG Q&A. Ollama is the free fallback.",
        }
        return tips.get(api_id, "")

    def estimate_setup_time(self) -> dict:
        """Estimate total time to set up all API keys."""
        instant = sum(1 for v in CATALOG.values() if v.get("signup_method") == "instant")
        manual = sum(1 for v in CATALOG.values() if v.get("signup_method") == "manual")

        return {
            "total_apis": len(CATALOG),
            "instant_signup": instant,
            "manual_signup": manual,
            "estimated_time": f"{instant * 1 + manual * 5} minutes",
            "recommended_first": [
                api_id for api_id, v in CATALOG.items()
                if v.get("priority") == "high" and v.get("signup_method") == "instant"
            ],
        }
