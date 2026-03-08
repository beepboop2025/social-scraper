"""Raw data storage — save/retrieve immutable raw responses.

Supports filesystem (default) and MinIO (S3-compatible) backends.
Raw data is NEVER modified after storage — enables full reprocessing.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "./data/raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "econscraper-raw")
USE_MINIO = os.getenv("USE_MINIO", "false").lower() == "true"


class RawStore:
    """Store and retrieve raw data."""

    def __init__(self):
        self._minio_client = None

    def _get_minio(self):
        if self._minio_client is None and USE_MINIO:
            try:
                from minio import Minio
                self._minio_client = Minio(
                    MINIO_ENDPOINT,
                    access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY,
                    secure=False,
                )
                if not self._minio_client.bucket_exists(MINIO_BUCKET):
                    self._minio_client.make_bucket(MINIO_BUCKET)
            except Exception as e:
                logger.warning(f"MinIO init failed, falling back to filesystem: {e}")
                self._minio_client = None
        return self._minio_client

    def save(self, data: list[dict], source: str) -> str:
        """Save raw data and return the storage path."""
        now = datetime.now(timezone.utc)
        relative_path = f"{source}/{now.strftime('%Y-%m-%d')}/{source}_{now.strftime('%H%M%S')}.json"
        content = json.dumps(data, default=str, ensure_ascii=False)

        # Try MinIO first
        minio = self._get_minio()
        if minio:
            try:
                import io
                data_bytes = content.encode("utf-8")
                minio.put_object(
                    MINIO_BUCKET,
                    relative_path,
                    io.BytesIO(data_bytes),
                    len(data_bytes),
                    content_type="application/json",
                )
                return f"minio://{MINIO_BUCKET}/{relative_path}"
            except Exception as e:
                logger.warning(f"MinIO save failed, falling back to filesystem: {e}")

        # Filesystem fallback
        filepath = Path(RAW_DATA_DIR) / relative_path
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(content, encoding="utf-8")
        return str(filepath)

    def load(self, path: str) -> list[dict]:
        """Load raw data from path."""
        if path.startswith("minio://"):
            return self._load_minio(path)
        return self._load_filesystem(path)

    def _load_filesystem(self, path: str) -> list[dict]:
        filepath = Path(path)
        if not filepath.exists():
            raise FileNotFoundError(f"Raw data not found: {path}")
        return json.loads(filepath.read_text(encoding="utf-8"))

    def _load_minio(self, path: str) -> list[dict]:
        minio = self._get_minio()
        if not minio:
            raise RuntimeError("MinIO not available")

        # Parse minio://bucket/key
        parts = path.replace("minio://", "").split("/", 1)
        bucket, key = parts[0], parts[1]

        resp = minio.get_object(bucket, key)
        data = json.loads(resp.read().decode("utf-8"))
        resp.close()
        resp.release_conn()
        return data

    def list_raw_files(self, source: str, date: Optional[str] = None) -> list[str]:
        """List raw data files for a source."""
        if USE_MINIO:
            minio = self._get_minio()
            if minio:
                prefix = f"{source}/{date}" if date else source
                objects = minio.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
                return [f"minio://{MINIO_BUCKET}/{obj.object_name}" for obj in objects]

        base = Path(RAW_DATA_DIR) / source
        if date:
            base = base / date
        if not base.exists():
            return []
        return [str(f) for f in sorted(base.rglob("*.json"))]
