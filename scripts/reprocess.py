#!/usr/bin/env python3
"""Reprocess articles through the NLP pipeline.

Re-runs processors on articles that need reprocessing (e.g., after
model upgrades or pipeline bug fixes). Leverages immutable raw storage.

Usage:
    python scripts/reprocess.py --processor sentiment --batch 100
    python scripts/reprocess.py --all --reset
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROCESSORS = {
    "article_extractor": "processors.article_extractor:ArticleExtractor",
    "pdf_extractor": "processors.pdf_extractor:PDFExtractor",
    "deduplicator": "processors.deduplicator:Deduplicator",
    "embedder": "processors.embedder:Embedder",
    "sentiment": "processors.sentiment:SentimentAnalyzer",
    "entity_extractor": "processors.entity_extractor:EntityExtractor",
    "topic_classifier": "processors.topic_classifier:TopicClassifier",
    "daily_digest": "processors.daily_digest:DailyDigestGenerator",
}


def reset_processed_flag(batch_size: int = 1000):
    """Reset is_processed flag on all articles so they get re-processed."""
    from api.database import SessionLocal
    from storage.models import Article

    db = SessionLocal()
    try:
        updated = (
            db.query(Article)
            .filter(Article.is_processed == True)
            .limit(batch_size)
            .update({"is_processed": False}, synchronize_session=False)
        )
        db.commit()
        print(f"[reprocess] Reset {updated} articles to unprocessed")
    finally:
        db.close()


def run_processor(name: str, batch_size: int = 50):
    """Run a specific processor."""
    if name not in PROCESSORS:
        print(f"[reprocess] Unknown processor: {name}")
        print(f"[reprocess] Available: {', '.join(PROCESSORS.keys())}")
        return

    module_path, class_name = PROCESSORS[name].split(":")
    import importlib
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)

    processor = cls({"batch_size": batch_size})
    result = processor.run()
    print(f"[reprocess] {name}: {result}")


def main():
    parser = argparse.ArgumentParser(description="Reprocess articles")
    parser.add_argument("--processor", type=str, help="Processor name")
    parser.add_argument("--all", action="store_true", help="Run all processors")
    parser.add_argument("--reset", action="store_true", help="Reset processed flags first")
    parser.add_argument("--batch", type=int, default=50, help="Batch size")
    args = parser.parse_args()

    if args.reset:
        reset_processed_flag(args.batch * 10)

    if args.processor:
        run_processor(args.processor, args.batch)
    elif args.all:
        for name in PROCESSORS:
            if name != "daily_digest":
                run_processor(name, args.batch)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
