"""
Gmail SMTP mailer — sends the daily PDF report as an email attachment.

Setup:
1. Enable 2FA on your Google account
2. Generate an App Password: Google Account → Security → App passwords
3. Set GMAIL_USER and GMAIL_APP_PASSWORD in .env

The email includes a styled HTML body with key stats inline,
plus the full PDF report attached.
"""

import logging
import os
import smtplib
from datetime import date
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

logger = logging.getLogger(__name__)

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


def _build_html_body(stats: dict = None) -> str:
    """Build a styled HTML email body with inline stats."""
    today = date.today().strftime("%A, %B %d, %Y")
    stats = stats or {}

    articles = stats.get("total_articles", "—")
    sources = stats.get("total_sources", "—")
    sentiment = stats.get("avg_sentiment", 0)
    sent_label = "Bullish" if sentiment > 0.1 else "Bearish" if sentiment < -0.1 else "Neutral"
    sent_color = "#2E7D32" if sentiment > 0.1 else "#C62828" if sentiment < -0.1 else "#757575"

    return f"""
    <html>
    <body style="margin:0; padding:0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: #F5F5F5;">
        <div style="max-width: 600px; margin: 0 auto; background: white;">
            <!-- Header -->
            <div style="background: linear-gradient(135deg, #0D47A1, #1565C0); padding: 30px 20px; text-align: center;">
                <h1 style="color: white; margin: 0; font-size: 24px; letter-spacing: 1px;">WORLD INTELLIGENCE</h1>
                <p style="color: #BBDEFB; margin: 6px 0 0; font-size: 13px;">Daily Briefing Report</p>
                <p style="color: white; margin: 10px 0 0; font-size: 15px; font-weight: bold;">{today}</p>
            </div>

            <!-- Stats Row -->
            <div style="display: flex; justify-content: space-around; padding: 20px 10px; background: #FAFAFA; border-bottom: 1px solid #E0E0E0;">
                <div style="text-align: center; flex: 1;">
                    <div style="font-size: 28px; font-weight: bold; color: #1565C0;">{articles}</div>
                    <div style="font-size: 11px; color: #757575; margin-top: 2px;">Articles</div>
                </div>
                <div style="text-align: center; flex: 1;">
                    <div style="font-size: 28px; font-weight: bold; color: #FF6F00;">{sources}</div>
                    <div style="font-size: 11px; color: #757575; margin-top: 2px;">Sources</div>
                </div>
                <div style="text-align: center; flex: 1;">
                    <div style="font-size: 28px; font-weight: bold; color: {sent_color};">{sent_label}</div>
                    <div style="font-size: 11px; color: #757575; margin-top: 2px;">Sentiment</div>
                </div>
            </div>

            <!-- Body -->
            <div style="padding: 25px 20px;">
                <p style="color: #424242; font-size: 14px; line-height: 1.6;">
                    Good morning! Your daily world intelligence report is attached as a PDF.
                </p>
                <p style="color: #424242; font-size: 14px; line-height: 1.6;">
                    The report covers market sentiment, economic indicators, top headlines,
                    social media pulse, and regulatory updates collected from {sources} data sources
                    over the last 24 hours.
                </p>
                <div style="background: #E3F2FD; border-left: 4px solid #1565C0; padding: 12px 16px; margin: 20px 0; border-radius: 0 6px 6px 0;">
                    <p style="margin: 0; color: #0D47A1; font-size: 13px;">
                        Open the attached PDF for detailed charts, graphs, and analysis.
                    </p>
                </div>
            </div>

            <!-- Footer -->
            <div style="background: #0D47A1; padding: 15px 20px; text-align: center;">
                <p style="color: #BBDEFB; font-size: 11px; margin: 0;">
                    Social Scraper Intelligence Platform | Automated Daily Report
                </p>
            </div>
        </div>
    </body>
    </html>
    """


def send_report_email(
    pdf_path: str,
    recipient: str = None,
    stats: dict = None,
) -> bool:
    """Send the PDF report via Gmail SMTP.

    Args:
        pdf_path: Path to the generated PDF file
        recipient: Email address to send to (defaults to GMAIL_USER)
        stats: Optional dict of stats to include in the email body

    Returns:
        True if sent successfully, False otherwise
    """
    gmail_user = os.getenv("GMAIL_USER", "")
    gmail_password = os.getenv("GMAIL_APP_PASSWORD", "")
    recipient = recipient or os.getenv("REPORT_RECIPIENT", gmail_user)

    if not gmail_user or not gmail_password:
        logger.error("GMAIL_USER or GMAIL_APP_PASSWORD not set in .env")
        return False

    if not recipient:
        logger.error("No recipient email address configured")
        return False

    if not Path(pdf_path).exists():
        logger.error(f"PDF file not found: {pdf_path}")
        return False

    today = date.today().strftime("%d %b %Y")
    subject = f"World Intelligence Report — {today}"

    # Build message
    msg = MIMEMultipart("mixed")
    msg["From"] = f"Social Scraper <{gmail_user}>"
    msg["To"] = recipient
    msg["Subject"] = subject

    # HTML body
    html_body = _build_html_body(stats)
    msg.attach(MIMEText(html_body, "html"))

    # PDF attachment
    pdf_filename = Path(pdf_path).name
    with open(pdf_path, "rb") as f:
        pdf_part = MIMEBase("application", "pdf")
        pdf_part.set_payload(f.read())
    encoders.encode_base64(pdf_part)
    pdf_part.add_header("Content-Disposition", f"attachment; filename={pdf_filename}")
    msg.attach(pdf_part)

    # Send
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(gmail_user, gmail_password)
            server.send_message(msg)
        logger.info(f"Report emailed to {recipient}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(
            "Gmail authentication failed. Make sure you're using an App Password, "
            "not your regular password. Generate one at: "
            "Google Account → Security → 2-Step Verification → App passwords"
        )
        return False
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False
