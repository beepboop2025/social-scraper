"""Scheduling logic for health checks — runs independently or via Celery Beat."""

from __future__ import annotations

import asyncio
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from monitoring.health.source_health_checker import (
    IST,
    HealthStatus,
    check_all_sources,
    format_results_table,
    quick_check_all_sources,
)

REPORTS_DIR = Path(__file__).parent / "reports"


async def run_quick_check() -> None:
    """Quick HTTP-only check. Alert only on BROKEN sources."""
    print(f"\n[{datetime.now(IST).strftime('%H:%M IST')}] Running quick health check...")
    results = await quick_check_all_sources()
    print(format_results_table(results))

    broken = [r for r in results if r.status == HealthStatus.BROKEN]
    if broken:
        from monitoring.health.alert_sender import send_daily_report
        msg = "🔴 EconScraper ALERT — Sources down:\n\n"
        for r in broken:
            msg += f"• {r.source_name}: {r.notes}\n"
        await send_daily_report(msg, broken_only=True)


async def run_deep_check() -> None:
    """Full check with structure validation + AI analysis."""
    from monitoring.health.ai_change_detector import analyze_changes
    from monitoring.health.structure_validator import validate_all

    print(f"\n[{datetime.now(IST).strftime('%H:%M IST')}] Running deep health check...")
    results = await check_all_sources()
    print(format_results_table(results))

    print("  Running structure validation...")
    validation = await validate_all()
    for name, match, diffs in validation:
        status = "OK" if match else "CHANGED"
        print(f"    {status:>7}  {name}")
        for d in diffs:
            print(f"             {d}")

    print("  Generating AI analysis report...")
    report = await analyze_changes(results, validation)

    # Save report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(IST).strftime("%Y-%m-%d")
    report_path = REPORTS_DIR / f"{date_str}.md"
    report_path.write_text(report)
    print(f"  Report saved: {report_path}")

    # Send via Telegram
    from monitoring.health.alert_sender import send_daily_report
    await send_daily_report(report)


async def scheduler_loop() -> None:
    """
    Main scheduler loop. Runs:
    - Quick check every 6 hours
    - Deep check once daily at 7 AM IST
    """
    print("EconScraper Health Scheduler started.")
    print("  Quick check: every 6 hours")
    print("  Deep check:  daily at 07:00 IST")
    print("  Press Ctrl+C to stop.\n")

    # Track last run times
    last_quick = datetime.min.replace(tzinfo=IST)
    last_deep_date: str = ""

    while True:
        now = datetime.now(IST)

        # Deep check at 7 AM IST (once per day)
        today_str = now.strftime("%Y-%m-%d")
        if now.hour >= 7 and last_deep_date != today_str:
            try:
                await run_deep_check()
            except Exception as e:
                print(f"  Deep check error: {e}")
            last_deep_date = today_str
            last_quick = now  # Deep check subsumes quick check

        # Quick check every 6 hours
        elif (now - last_quick).total_seconds() >= 6 * 3600:
            try:
                await run_quick_check()
            except Exception as e:
                print(f"  Quick check error: {e}")
            last_quick = now

        # Sleep 5 minutes between loop iterations
        await asyncio.sleep(300)


def start_scheduler() -> None:
    """Start the scheduler daemon."""
    loop = asyncio.new_event_loop()

    def _shutdown(sig, frame):
        print("\nScheduler shutting down...")
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(scheduler_loop())
    except KeyboardInterrupt:
        print("\nScheduler stopped.")


# --- Celery Beat integration (optional) ---

def _get_celery_app():
    """Try to import the main econscraper Celery app."""
    try:
        from econscraper.celery_app import app
        return app
    except ImportError:
        return None


celery_app = _get_celery_app()

if celery_app:
    @celery_app.task(name="monitoring.quick_health_check")
    def celery_quick_check():
        asyncio.run(run_quick_check())

    @celery_app.task(name="monitoring.deep_health_check")
    def celery_deep_check():
        asyncio.run(run_deep_check())
