"""Text summarization using extractive methods (no heavy ML models needed)."""

import re
from collections import Counter


def extractive_summary(text: str, num_sentences: int = 3) -> str:
    """Create an extractive summary by selecting the most important sentences.

    Uses a simple TF-based scoring approach suitable for social media text.
    """
    if not text:
        return ""

    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 20]

    if len(sentences) <= num_sentences:
        return text

    # Score sentences by word frequency
    words = re.findall(r'\w+', text.lower())
    word_freq = Counter(words)
    # Remove very common words
    stopwords = {"the", "a", "an", "is", "are", "was", "were", "in", "on", "at", "to", "for", "of", "and", "or", "but", "with", "this", "that", "it", "be", "has", "have", "had"}
    for sw in stopwords:
        word_freq.pop(sw, None)

    sentence_scores = []
    for i, sent in enumerate(sentences):
        words_in_sent = re.findall(r'\w+', sent.lower())
        score = sum(word_freq.get(w, 0) for w in words_in_sent)
        # Boost earlier sentences slightly
        score *= (1.0 - i * 0.05)
        sentence_scores.append((score, i, sent))

    sentence_scores.sort(reverse=True)
    top = sorted(sentence_scores[:num_sentences], key=lambda x: x[1])

    return ". ".join(s[2] for s in top) + "."


def batch_summarize(texts: list[str], num_sentences: int = 2) -> list[str]:
    """Summarize a batch of texts."""
    return [extractive_summary(t, num_sentences) for t in texts]


def collection_summary(texts: list[str], top_n: int = 5) -> dict:
    """Summarize a collection of posts into key themes and highlights.

    Returns overview statistics and the most representative posts.
    """
    if not texts:
        return {"highlights": [], "word_cloud": {}, "total": 0}

    all_words = []
    for t in texts:
        all_words.extend(re.findall(r'\w+', t.lower()))

    stopwords = {"the", "a", "an", "is", "are", "was", "were", "in", "on", "at", "to", "for", "of", "and", "or", "but", "with", "this", "that", "it", "be", "has", "have", "had", "https", "http", "co", "rt"}
    word_freq = Counter(w for w in all_words if w not in stopwords and len(w) > 2)

    # Score each text by keyword density
    scored = []
    for i, text in enumerate(texts):
        words = re.findall(r'\w+', text.lower())
        score = sum(word_freq.get(w, 0) for w in words) / max(len(words), 1)
        scored.append((score, i, text))

    scored.sort(reverse=True)
    highlights = [s[2][:300] for s in scored[:top_n]]

    return {
        "highlights": highlights,
        "word_cloud": dict(word_freq.most_common(30)),
        "total": len(texts),
        "avg_length": round(sum(len(t) for t in texts) / max(len(texts), 1)),
    }
