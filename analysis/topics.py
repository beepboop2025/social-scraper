"""Topic modeling using Latent Dirichlet Allocation (LDA) via scikit-learn."""

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import numpy as np


def extract_topics(texts: list[str], n_topics: int = 5, n_words: int = 8) -> list[dict]:
    """Run LDA topic modeling on a collection of texts.

    Args:
        texts: List of text documents.
        n_topics: Number of topics to extract.
        n_words: Number of top words per topic.

    Returns:
        List of dicts, one per input text, with keys:
        - dominant_topic: int (0-indexed topic number)
        - topic_distribution: list of floats (probability per topic)
        - topic_words: list of strings (top words for the dominant topic)
    """
    if not texts or len(texts) < 2:
        return [{"dominant_topic": 0, "topic_distribution": [1.0], "topic_words": []}]

    # Vectorize
    vectorizer = CountVectorizer(
        max_df=0.95,
        min_df=2,
        max_features=5000,
        stop_words="english",
    )

    try:
        dtm = vectorizer.fit_transform(texts)
    except ValueError:
        # Not enough vocabulary
        return [{"dominant_topic": 0, "topic_distribution": [1.0], "topic_words": []} for _ in texts]

    actual_topics = min(n_topics, dtm.shape[0], dtm.shape[1])
    if actual_topics < 2:
        return [{"dominant_topic": 0, "topic_distribution": [1.0], "topic_words": []} for _ in texts]

    # Fit LDA
    lda = LatentDirichletAllocation(
        n_components=actual_topics,
        random_state=42,
        max_iter=20,
        learning_method="online",
    )
    doc_topics = lda.fit_transform(dtm)

    # Extract topic words
    feature_names = vectorizer.get_feature_names_out()
    topic_words = []
    for topic_idx in range(actual_topics):
        top_indices = lda.components_[topic_idx].argsort()[-n_words:][::-1]
        topic_words.append([feature_names[i] for i in top_indices])

    # Build per-document results
    results = []
    for i in range(len(texts)):
        dominant = int(np.argmax(doc_topics[i]))
        results.append({
            "dominant_topic": dominant,
            "topic_distribution": [round(float(p), 4) for p in doc_topics[i]],
            "topic_words": topic_words[dominant] if dominant < len(topic_words) else [],
        })

    return results
