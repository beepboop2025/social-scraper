"""
CLI entry point — Generate the daily PDF report and email it.

Usage:
    python -m reports.generate_and_send              # Generate + email
    python -m reports.generate_and_send --pdf-only   # Generate PDF only (no email)
    python -m reports.generate_and_send --test       # Send a test email with sample data
"""

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from reports.pdf_generator import generate_report
from reports.mailer import send_report_email

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("report")


def main():
    parser = argparse.ArgumentParser(description="Daily World Intelligence Report")
    parser.add_argument("--pdf-only", action="store_true",
                        help="Generate PDF only, don't email")
    parser.add_argument("--test", action="store_true",
                        help="Send a test email with the generated report")
    parser.add_argument("--output", type=str, default=None,
                        help="Custom output path for the PDF")
    parser.add_argument("--recipient", type=str, default=None,
                        help="Override recipient email address")
    args = parser.parse_args()

    logger.info("Generating World Intelligence Report...")

    # Generate PDF
    try:
        pdf_path = generate_report(output_path=args.output)
        logger.info(f"PDF generated: {pdf_path}")
    except Exception as e:
        logger.error(f"PDF generation failed: {e}")
        sys.exit(1)

    if args.pdf_only:
        print(f"\nReport saved to: {pdf_path}")
        return

    # Email it
    logger.info("Sending report via Gmail...")
    stats = {
        "total_articles": "—",
        "total_sources": "—",
        "avg_sentiment": 0,
    }

    # Try to get real stats from the PDF generator's data
    try:
        from reports.pdf_generator import _fetch_report_data
        data = _fetch_report_data()
        stats = {
            "total_articles": data.get("total_articles", "—"),
            "total_sources": data.get("total_sources", "—"),
            "avg_sentiment": data.get("avg_sentiment", 0),
        }
    except Exception:
        pass

    success = send_report_email(
        pdf_path=pdf_path,
        recipient=args.recipient,
        stats=stats,
    )

    if success:
        print(f"\nReport sent successfully!")
        print(f"PDF: {pdf_path}")
    else:
        print(f"\nReport generated but email failed. Check your Gmail credentials.")
        print(f"PDF saved at: {pdf_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
