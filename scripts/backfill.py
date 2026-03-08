#!/usr/bin/env python3
"""Backfill historical data for configured collectors.

Usage:
    python scripts/backfill.py --source fred_api --days 365
    python scripts/backfill.py --all --days 30
"""

import argparse
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def backfill_source(source_name: str, days: int):
    """Run a single collector for backfill."""
    from core.registry import SourceRegistry

    registry = SourceRegistry()
    collector = registry.get_collector(source_name)
    if not collector:
        print(f"[backfill] Source '{source_name}' not found in registry")
        return

    print(f"[backfill] Running {source_name}...")
    result = await asyncio.to_thread(collector.run)
    print(f"[backfill] {source_name}: {result}")


async def backfill_all(days: int):
    """Run all collectors."""
    from core.registry import SourceRegistry

    registry = SourceRegistry()
    for name in registry.list_sources():
        await backfill_source(name, days)


def main():
    parser = argparse.ArgumentParser(description="Backfill historical data")
    parser.add_argument("--source", type=str, help="Source name to backfill")
    parser.add_argument("--all", action="store_true", help="Backfill all sources")
    parser.add_argument("--days", type=int, default=30, help="Days of history")
    args = parser.parse_args()

    if args.source:
        asyncio.run(backfill_source(args.source, args.days))
    elif args.all:
        asyncio.run(backfill_all(args.days))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
