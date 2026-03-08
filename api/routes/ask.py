"""RAG-powered Q&A endpoint — retrieval-augmented generation.

Flow: Embed query → pgvector similarity search → LLM with retrieved context → answer with citations.
"""

import logging
import os
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.database import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ask", tags=["rag"])


@router.get("")
async def ask_question(
    q: str = Query(..., min_length=5, description="Your economic/financial question"),
    top_k: int = Query(5, ge=1, le=20, description="Number of context documents"),
    db: Session = Depends(get_db),
):
    """Answer economic questions using RAG.

    1. Embed the question
    2. Retrieve top-k similar articles via pgvector
    3. Pass context + question to LLM
    4. Return answer with source citations
    """
    from api.routes.deps import get_embedder, get_vector_store

    embedder = get_embedder()
    vector_store = get_vector_store()

    query_embedding = embedder._embed_text(q)
    if not query_embedding:
        return {"answer": "Unable to process query — embedding failed.", "sources": []}

    context_docs = vector_store.search_similar(
        query_embedding=query_embedding,
        limit=top_k,
    )

    if not context_docs:
        return {"answer": "No relevant data found in the knowledge base.", "sources": []}

    context = _build_context(context_docs)
    answer = _generate_answer(q, context)

    return {
        "question": q,
        "answer": answer,
        "sources": [
            {
                "title": d.get("title"),
                "url": d.get("url"),
                "source": d.get("source"),
                "similarity": d.get("similarity"),
            }
            for d in context_docs
        ],
        "context_count": len(context_docs),
    }


def _build_context(docs: list[dict]) -> str:
    """Build LLM context from retrieved documents."""
    parts = []
    for i, doc in enumerate(docs, 1):
        parts.append(
            f"[{i}] {doc.get('title', 'Untitled')} ({doc.get('source', 'unknown')})\n"
            f"{doc.get('snippet', '')}\n"
        )
    return "\n---\n".join(parts)


def _generate_answer(question: str, context: str) -> str:
    """Generate answer using Claude API or Ollama fallback."""
    prompt = (
        "You are an economic analyst assistant. Answer the following question "
        "based ONLY on the provided context. Cite sources using [1], [2], etc. "
        "If the context doesn't contain enough information, say so.\n\n"
        f"Context:\n{context}\n\n"
        f"Question: {question}\n\n"
        "Answer:"
    )

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if api_key:
        try:
            import anthropic

            client = anthropic.Anthropic(api_key=api_key)
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            return message.content[0].text
        except Exception as e:
            logger.warning(f"[RAG] Claude API failed: {e}")

    ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
    try:
        import httpx

        resp = httpx.post(
            f"{ollama_url}/api/generate",
            json={"model": "llama3", "prompt": prompt, "stream": False},
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.json().get("response", "")
    except Exception as e:
        logger.warning(f"[RAG] Ollama failed: {e}")

    return (
        "LLM unavailable. Based on the retrieved documents, "
        f"there are {len(context.split('---'))} relevant sources for your query. "
        "Please check the sources list for details."
    )
