"""Vector embedding generator for semantic search and RAG.

Uses sentence-transformers (all-MiniLM-L6-v2, 384 dim) with
Ollama nomic-embed-text as fallback. Stores embeddings via VectorStore.
"""

import logging
from typing import Optional

from core.base_processor import BaseProcessor

logger = logging.getLogger(__name__)


class Embedder(BaseProcessor):
    name = "embedder"
    batch_size = 32

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.model_name = self.config.get("model", "all-MiniLM-L6-v2")
        self.ollama_model = self.config.get("ollama_model", "nomic-embed-text")
        self.ollama_url = self.config.get("ollama_url", "http://localhost:11434")
        self.dimension = self.config.get("dimension", 384)
        self._model = None

    def _get_model(self):
        """Lazy-load sentence-transformers model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer

                self._model = SentenceTransformer(self.model_name)
                logger.info(f"[Embedder] Loaded {self.model_name}")
            except ImportError:
                logger.warning("[Embedder] sentence-transformers not available, using Ollama fallback")
                self._model = "ollama"
        return self._model

    def process_one(self, article: dict) -> dict:
        text = article.get("full_text", "") or article.get("title", "")
        article_id = article.get("id")

        if not text or len(text.strip()) < 10:
            return {"article_id": article_id, "status": "skipped", "reason": "no_text"}

        # Truncate to ~512 tokens (~2000 chars) for embedding models
        text = text[:2000]

        embedding = self._embed_text(text)
        if embedding:
            return {
                "article_id": article_id,
                "status": "embedded",
                "embedding": embedding,
                "model": self.model_name,
            }

        return {"article_id": article_id, "status": "failed"}

    def process_batch(self, articles: list[dict]) -> list[dict]:
        """Override for GPU-batched embedding generation."""
        model = self._get_model()

        if model == "ollama":
            return super().process_batch(articles)

        texts = []
        valid_articles = []
        for a in articles:
            text = a.get("full_text", "") or a.get("title", "")
            if text and len(text.strip()) >= 10:
                texts.append(text[:2000])
                valid_articles.append(a)

        if not texts:
            return [{"article_id": a.get("id"), "status": "skipped"} for a in articles]

        try:
            embeddings = model.encode(texts, show_progress_bar=False, batch_size=self.batch_size)
            results = []
            valid_idx = 0
            for a in articles:
                if a in valid_articles:
                    results.append({
                        "article_id": a.get("id"),
                        "status": "embedded",
                        "embedding": embeddings[valid_idx].tolist(),
                        "model": self.model_name,
                    })
                    valid_idx += 1
                else:
                    results.append({"article_id": a.get("id"), "status": "skipped"})
            return results
        except Exception as e:
            logger.error(f"[Embedder] Batch encode failed: {e}")
            return super().process_batch(articles)

    def _embed_text(self, text: str) -> Optional[list[float]]:
        """Embed a single text string."""
        model = self._get_model()

        if model != "ollama":
            try:
                embedding = model.encode(text, show_progress_bar=False)
                return embedding.tolist()
            except Exception as e:
                logger.warning(f"[Embedder] sentence-transformers failed: {e}")

        return self._embed_ollama(text)

    def _embed_ollama(self, text: str) -> Optional[list[float]]:
        """Fallback: use Ollama for embeddings."""
        try:
            import httpx

            resp = httpx.post(
                f"{self.ollama_url}/api/embeddings",
                json={"model": self.ollama_model, "prompt": text},
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json().get("embedding")
        except Exception as e:
            logger.debug(f"[Embedder] Ollama fallback failed: {e}")
        return None

    def _store_results(self, results: list[dict], db):
        from storage.models import ArticleEmbedding

        for r in results:
            if r.get("status") == "embedded" and r.get("embedding"):
                emb = ArticleEmbedding(
                    article_id=r["article_id"],
                    embedding_json=r["embedding"],
                    model_name=r.get("model", self.model_name),
                )
                db.add(emb)
        db.commit()
