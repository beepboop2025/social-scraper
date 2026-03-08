"""TimescaleDB setup — convert tables to hypertables for time-series performance."""

import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)


def init_timescaledb():
    """Initialize TimescaleDB extensions and hypertables."""
    from api.database import engine

    with engine.connect() as conn:
        # Enable TimescaleDB
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"))
            logger.info("TimescaleDB extension enabled")
        except Exception as e:
            logger.warning(f"TimescaleDB not available (using regular PostgreSQL): {e}")
            conn.rollback()
            return

        # Convert economic_data to hypertable
        try:
            conn.execute(text("""
                SELECT create_hypertable('economic_data', 'date',
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                )
            """))
            logger.info("economic_data hypertable created")
        except Exception as e:
            logger.warning(f"economic_data hypertable: {e}")

        # Add compression policy (compress chunks older than 30 days)
        try:
            conn.execute(text("""
                ALTER TABLE economic_data SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'source,indicator'
                )
            """))
            conn.execute(text("""
                SELECT add_compression_policy('economic_data', INTERVAL '30 days',
                    if_not_exists => TRUE)
            """))
        except Exception:
            pass

        # Retention policy (keep 2 years of raw data)
        try:
            conn.execute(text("""
                SELECT add_retention_policy('economic_data', INTERVAL '2 years',
                    if_not_exists => TRUE)
            """))
        except Exception:
            pass

        conn.commit()
        logger.info("TimescaleDB initialization complete")


def create_continuous_aggregates():
    """Create materialized views for common queries."""
    from api.database import engine

    with engine.connect() as conn:
        # Daily aggregates for economic data
        try:
            conn.execute(text("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS economic_data_daily
                WITH (timescaledb.continuous) AS
                SELECT
                    time_bucket('1 day', date) AS bucket,
                    source,
                    indicator,
                    AVG(value) AS avg_value,
                    MIN(value) AS min_value,
                    MAX(value) AS max_value,
                    COUNT(*) AS sample_count
                FROM economic_data
                GROUP BY bucket, source, indicator
                WITH NO DATA;
            """))
            conn.execute(text("""
                SELECT add_continuous_aggregate_policy('economic_data_daily',
                    start_offset => INTERVAL '7 days',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 day',
                    if_not_exists => TRUE
                )
            """))
        except Exception as e:
            logger.warning(f"Continuous aggregate: {e}")

        conn.commit()
