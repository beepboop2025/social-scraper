"""Telegram alerting bot — sends collection failures and digest notifications."""

import logging
import os

logger = logging.getLogger(__name__)


class TelegramAlertBot:
    """Sends alerts to a Telegram chat via the Bot API."""

    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_ALERT_CHAT_ID", "")
        self.enabled = bool(self.bot_token and self.chat_id)

    def send(self, message: str, parse_mode: str = "Markdown") -> bool:
        if not self.enabled:
            logger.debug("[TelegramBot] Not configured, skipping")
            return False

        try:
            import httpx

            resp = httpx.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": message[:4096],
                    "parse_mode": parse_mode,
                },
                timeout=10,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"[TelegramBot] Send failed: {e}")
            return False

    @staticmethod
    def _escape_markdown_v1(text: str) -> str:
        """Escape Markdown v1 special characters for Telegram."""
        for ch in r"_*`[":
            text = text.replace(ch, f"\\{ch}")
        return text

    def send_alert(self, source: str, error: str, consecutive_failures: int = 1):
        severity = "WARNING" if consecutive_failures < 5 else "CRITICAL"
        safe_error = self._escape_markdown_v1(error[:500])
        msg = (
            f"*{severity}* — EconScraper\n\n"
            f"Source: `{source}`\n"
            f"Error: {safe_error}\n"
            f"Consecutive failures: {consecutive_failures}"
        )
        self.send(msg)

    def send_digest(self, summary: str):
        self.send(f"*EconScraper Daily Digest*\n\n{summary[:3500]}")

    def send_recovery(self, source: str):
        self.send(f"*RECOVERED* — `{source}` is collecting data again.")
