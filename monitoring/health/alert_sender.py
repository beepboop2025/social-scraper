"""Send health alerts via Telegram."""

from __future__ import annotations

import os
from pathlib import Path

import httpx
import yaml

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "alerts.yaml"
TELEGRAM_API = "https://api.telegram.org"
TELEGRAM_MAX_LENGTH = 4096


def _load_config() -> dict:
    """Load alert configuration from YAML, expanding env vars."""
    if not CONFIG_PATH.exists():
        return {}
    text = CONFIG_PATH.read_text()
    # Expand ${ENV_VAR} references
    import re
    def _expand(m: re.Match) -> str:
        return os.environ.get(m.group(1), m.group(0))
    text = re.sub(r"\$\{(\w+)\}", _expand, text)
    return yaml.safe_load(text) or {}


def _split_message(text: str, max_len: int = TELEGRAM_MAX_LENGTH) -> list[str]:
    """Split a long message into parts that fit Telegram's limit."""
    if len(text) <= max_len:
        return [text]

    parts: list[str] = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        # Find a good split point (newline near the limit)
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1 or split_at < max_len // 2:
            split_at = max_len
        parts.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return parts


async def send_telegram_alert(message: str, chat_id: str, bot_token: str) -> bool:
    """Send a message to Telegram. Returns True on success."""
    parts = _split_message(message)

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        for part in parts:
            url = f"{TELEGRAM_API}/bot{bot_token}/sendMessage"
            try:
                resp = await client.post(url, json={
                    "chat_id": chat_id,
                    "text": part,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                })
                if resp.status_code == 429:
                    # Rate limited — wait and retry once
                    import asyncio
                    try:
                        retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
                    except Exception:
                        retry_after = 5
                    await asyncio.sleep(retry_after)
                    retry_resp = await client.post(url, json={
                        "chat_id": chat_id,
                        "text": part,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    })
                    if retry_resp.status_code >= 400:
                        print(f"  Telegram API error after retry: {retry_resp.status_code} {retry_resp.text[:200]}")
                        return False
                elif resp.status_code >= 400:
                    print(f"  Telegram API error: {resp.status_code} {resp.text[:200]}")
                    return False
            except Exception as e:
                print(f"  Telegram send failed: {e}")
                return False
    return True


async def send_daily_report(report: str, broken_only: bool = False) -> bool:
    """
    Send the health report via configured channels.
    If broken_only=True, only send if there are BROKEN sources.
    """
    config = _load_config()
    telegram_cfg = config.get("telegram", {})

    if not telegram_cfg.get("enabled", False):
        print("  Telegram alerts disabled in config.")
        return False

    bot_token = telegram_cfg.get("bot_token", "")
    chat_id = telegram_cfg.get("chat_id", "")

    if not bot_token or bot_token.startswith("${"):
        print("  TELEGRAM_BOT_TOKEN not set. Skipping Telegram alert.")
        return False
    if not chat_id or chat_id.startswith("${"):
        print("  TELEGRAM_CHAT_ID not set. Skipping Telegram alert.")
        return False

    # Check alert conditions
    alert_cfg = config.get("alerts", {})
    if broken_only and not alert_cfg.get("on_broken", True):
        return False

    return await send_telegram_alert(report, chat_id, bot_token)
