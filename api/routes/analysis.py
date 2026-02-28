"""Analysis API routes — trigger and view NLP analysis results."""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional

from api.database import get_db
from api.models import ScrapedPost, AnalysisResult

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post("/run/{batch_id}")
async def run_analysis(
    batch_id: str,
    analysis_type: str = "all",
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """Trigger NLP analysis on all posts in a batch."""
    count = db.query(ScrapedPost).filter(ScrapedPost.batch_id == batch_id).count()
    if count == 0:
        raise HTTPException(404, f"No posts found for batch {batch_id}")

    background_tasks.add_task(_run_batch_analysis, batch_id, analysis_type)

    return {"message": f"Analysis triggered for {count} posts", "batch_id": batch_id, "analysis_type": analysis_type}


@router.get("/results/{batch_id}")
def get_analysis_results(batch_id: str, analysis_type: Optional[str] = None, db: Session = Depends(get_db)):
    """Get analysis results for a batch."""
    q = (
        db.query(AnalysisResult)
        .join(ScrapedPost)
        .filter(ScrapedPost.batch_id == batch_id)
    )
    if analysis_type:
        q = q.filter(AnalysisResult.analysis_type == analysis_type)

    results = q.all()
    return [
        {
            "id": r.id,
            "post_id": r.post_id,
            "analysis_type": r.analysis_type,
            "result": r.result,
            "confidence": r.confidence,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in results
    ]


@router.get("/summary/{batch_id}")
def get_batch_summary(batch_id: str, db: Session = Depends(get_db)):
    """Get aggregated analysis summary for a batch."""
    # Sentiment distribution
    sentiments = (
        db.query(
            func.json_extract_path_text(AnalysisResult.result, "label").label("label"),
            func.count().label("count"),
        )
        .join(ScrapedPost)
        .filter(ScrapedPost.batch_id == batch_id, AnalysisResult.analysis_type == "sentiment")
        .group_by("label")
        .all()
    )

    # Top topics
    topics = (
        db.query(AnalysisResult.result)
        .join(ScrapedPost)
        .filter(ScrapedPost.batch_id == batch_id, AnalysisResult.analysis_type == "topic")
        .all()
    )

    # Top entities
    entities = (
        db.query(AnalysisResult.result)
        .join(ScrapedPost)
        .filter(ScrapedPost.batch_id == batch_id, AnalysisResult.analysis_type == "entity")
        .all()
    )

    total = db.query(ScrapedPost).filter(ScrapedPost.batch_id == batch_id).count()
    analyzed = (
        db.query(func.count(func.distinct(AnalysisResult.post_id)))
        .join(ScrapedPost)
        .filter(ScrapedPost.batch_id == batch_id)
        .scalar()
    )

    return {
        "batch_id": batch_id,
        "total_posts": total,
        "analyzed_posts": analyzed,
        "sentiment_distribution": {s.label: s.count for s in sentiments} if sentiments else {},
        "topics_count": len(topics),
        "entities_count": len(entities),
    }


async def _run_batch_analysis(batch_id: str, analysis_type: str):
    """Background task to run NLP analysis on a batch."""
    from api.database import SessionLocal
    from analysis.sentiment import analyze_sentiment
    from analysis.topics import extract_topics
    from analysis.entities import extract_entities
    from datetime import datetime

    db = SessionLocal()
    try:
        posts = db.query(ScrapedPost).filter(ScrapedPost.batch_id == batch_id).all()

        for post in posts:
            if not post.text:
                continue

            if analysis_type in ("all", "sentiment"):
                result = analyze_sentiment(post.text)
                db.add(AnalysisResult(
                    post_id=post.id,
                    analysis_type="sentiment",
                    result=result,
                    confidence=result.get("confidence"),
                    model_version="vader-1.0",
                    created_at=datetime.utcnow(),
                ))

            if analysis_type in ("all", "entity"):
                result = extract_entities(post.text)
                db.add(AnalysisResult(
                    post_id=post.id,
                    analysis_type="entity",
                    result=result,
                    model_version="spacy-sm-3.7",
                    created_at=datetime.utcnow(),
                ))

            if analysis_type in ("all", "topic"):
                # Topic modeling runs on the full batch, not per-post
                pass

        # Run topic modeling on the full batch
        if analysis_type in ("all", "topic"):
            texts = [p.text for p in posts if p.text]
            if texts:
                topic_results = extract_topics(texts)
                for i, post in enumerate(posts):
                    if post.text and i < len(topic_results):
                        db.add(AnalysisResult(
                            post_id=post.id,
                            analysis_type="topic",
                            result=topic_results[i],
                            model_version="lda-sklearn-1.0",
                            created_at=datetime.utcnow(),
                        ))

        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[Analysis] Batch {batch_id} failed: {e}")
    finally:
        db.close()
