#!/usr/bin/env python3
"""Initialize the econscraper database.

Creates all tables, TimescaleDB hypertables, pgvector extension,
and sets up compression/retention policies.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.database import engine, Base, init_db


def main():
    print("[init_db] Creating tables...")
    # Import models so they register with Base
    import storage.models  # noqa: F401

    init_db()
    print("[init_db] Tables created.")

    # TimescaleDB setup
    print("[init_db] Setting up TimescaleDB...")
    try:
        from storage.timescale import TimescaleManager
        ts = TimescaleManager()
        ts.create_hypertable()
        ts.setup_compression()
        ts.setup_retention()
        print("[init_db] TimescaleDB hypertable configured.")
    except Exception as e:
        print(f"[init_db] TimescaleDB setup skipped: {e}")

    # pgvector setup
    print("[init_db] Setting up pgvector...")
    try:
        from storage.vectors import VectorStore
        vs = VectorStore()
        vs.init_pgvector()
        print("[init_db] pgvector extension and index created.")
    except Exception as e:
        print(f"[init_db] pgvector setup skipped: {e}")

    print("[init_db] Done.")


if __name__ == "__main__":
    main()
