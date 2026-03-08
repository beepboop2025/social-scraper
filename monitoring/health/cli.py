"""Command-line interface for the EconScraper health monitor."""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from monitoring.health.source_health_checker import (
    IST,
    check_all_sources,
    format_results_table,
    quick_check_all_sources,
)

REPORTS_DIR = Path(__file__).parent / "reports"


def cmd_check(args: argparse.Namespace) -> None:
    """Run health check."""
    if args.quick:
        print("Running quick health check (HTTP reachability only)...\n")
        results = asyncio.run(quick_check_all_sources())
    else:
        print("Running full health check...\n")
        results = asyncio.run(check_all_sources())

    print(format_results_table(results))

    # If full check, also run structure validation
    if not args.quick:
        from monitoring.health.structure_validator import validate_all

        print("\nStructure validation against baselines:")
        validation = asyncio.run(validate_all())
        for name, match, diffs in validation:
            status = "\033[92mOK\033[0m" if match else "\033[93mCHANGED\033[0m"
            print(f"  {status:>16}  {name}")
            for d in diffs:
                print(f"              {d}")
        print()


def cmd_report(args: argparse.Namespace) -> None:
    """Generate AI analysis report."""
    from monitoring.health.ai_change_detector import analyze_changes
    from monitoring.health.structure_validator import validate_all

    print("Running full health check...")
    results = asyncio.run(check_all_sources())
    print(format_results_table(results))

    print("Running structure validation...")
    validation = asyncio.run(validate_all())

    print("Generating AI analysis report (this may take a minute)...\n")
    report = asyncio.run(analyze_changes(results, validation))

    # Save report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(IST).strftime("%Y-%m-%d")
    report_path = REPORTS_DIR / f"{date_str}.md"
    report_path.write_text(report)

    print(report)
    print(f"\nReport saved to: {report_path}")


def cmd_baseline(args: argparse.Namespace) -> None:
    """Update baselines."""
    from monitoring.health.structure_validator import update_baseline

    source = args.source if hasattr(args, "source") and args.source else None
    if source:
        print(f"Updating baseline for: {source}")
    else:
        print("Updating all baselines...")

    results = asyncio.run(update_baseline(source))
    for line in results:
        print(line)
    print("\nBaselines updated.")


def cmd_history(args: argparse.Namespace) -> None:
    """Show recent reports."""
    if not REPORTS_DIR.exists():
        print("No reports directory found. Run a deep check first.")
        return

    reports = sorted(REPORTS_DIR.glob("*.md"), reverse=True)
    days = args.days if hasattr(args, "days") else 7
    reports = reports[:days]

    if not reports:
        print("No reports found.")
        return

    print(f"Last {len(reports)} reports:\n")
    for rp in reports:
        print(f"  {rp.stem}")
        # Print first few lines as summary
        lines = rp.read_text().split("\n")
        for line in lines[1:6]:
            if line.strip():
                print(f"    {line.strip()}")
        print()

    if args.full and reports:
        latest = reports[0]
        print(f"\n{'='*60}")
        print(f"Latest report: {latest.stem}")
        print(f"{'='*60}\n")
        print(latest.read_text())


def cmd_schedule(args: argparse.Namespace) -> None:
    """Start the scheduler daemon."""
    from monitoring.health.health_scheduler import start_scheduler
    start_scheduler()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="monitoring",
        description="EconScraper Source Health Monitor",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # check
    p_check = subparsers.add_parser("check", help="Run health check")
    p_check.add_argument("--quick", action="store_true", help="Quick HTTP-only check")
    p_check.set_defaults(func=cmd_check)

    # report
    p_report = subparsers.add_parser("report", help="Generate AI analysis report")
    p_report.set_defaults(func=cmd_report)

    # baseline
    p_baseline = subparsers.add_parser("baseline", help="Update baselines")
    p_baseline.add_argument("source", nargs="?", default=None, help="Specific source to update")
    p_baseline.set_defaults(func=cmd_baseline)

    # history
    p_history = subparsers.add_parser("history", help="Show recent reports")
    p_history.add_argument("--days", type=int, default=7, help="Number of days")
    p_history.add_argument("--full", action="store_true", help="Show full latest report")
    p_history.set_defaults(func=cmd_history)

    # schedule
    p_schedule = subparsers.add_parser("schedule", help="Start scheduler daemon")
    p_schedule.set_defaults(func=cmd_schedule)

    args = parser.parse_args()

    if not args.command:
        # Default: run full check
        args.quick = False
        cmd_check(args)
        return

    args.func(args)


if __name__ == "__main__":
    main()
