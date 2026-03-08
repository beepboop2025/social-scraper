#!/usr/bin/env bash
# EconScraper database backup script.
# Usage: ./scripts/backup.sh [output_dir]
#
# Backs up PostgreSQL (pg_dump) and MinIO raw data.
# Designed for cron: 0 2 * * * /path/to/backup.sh /backups

set -euo pipefail

BACKUP_DIR="${1:-./backups}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DB_URL="${DATABASE_URL:-postgresql://localhost:5432/social_scraper}"

mkdir -p "$BACKUP_DIR"

echo "[backup] Starting backup at $TIMESTAMP"

# PostgreSQL backup
DB_FILE="$BACKUP_DIR/econscraper_db_${TIMESTAMP}.sql.gz"
echo "[backup] Dumping PostgreSQL..."
pg_dump "$DB_URL" | gzip > "$DB_FILE"
echo "[backup] DB backup: $DB_FILE ($(du -h "$DB_FILE" | cut -f1))"

# MinIO raw data backup (if mc is available)
if command -v mc &> /dev/null; then
    RAW_DIR="$BACKUP_DIR/raw_data_${TIMESTAMP}"
    echo "[backup] Syncing MinIO raw data..."
    mc mirror econscraper/econscraper-raw "$RAW_DIR" --quiet 2>/dev/null || echo "[backup] MinIO sync skipped"
fi

# Clean up backups older than 7 days
find "$BACKUP_DIR" -name "econscraper_db_*.sql.gz" -mtime +7 -delete 2>/dev/null || true
find "$BACKUP_DIR" -name "raw_data_*" -type d -mtime +7 -exec rm -rf {} + 2>/dev/null || true

echo "[backup] Done."
