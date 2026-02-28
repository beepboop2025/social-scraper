"""Named Entity Recognition using spaCy."""

import spacy

# Load small English model (fast, good enough for social media)
try:
    _nlp = spacy.load("en_core_web_sm")
except OSError:
    print("[NER] Model not found. Run: python -m spacy download en_core_web_sm")
    _nlp = None


def extract_entities(text: str) -> dict:
    """Extract named entities from text using spaCy.

    Returns:
        dict with keys: entities (list of {text, label, start, end}),
        entity_counts (dict of label -> count).
    """
    if not _nlp or not text:
        return {"entities": [], "entity_counts": {}}

    doc = _nlp(text[:10000])  # Limit text length for performance
    entities = []
    counts: dict[str, int] = {}

    for ent in doc.ents:
        entities.append({
            "text": ent.text,
            "label": ent.label_,
            "start": ent.start_char,
            "end": ent.end_char,
        })
        counts[ent.label_] = counts.get(ent.label_, 0) + 1

    return {
        "entities": entities,
        "entity_counts": counts,
    }


def batch_entities(texts: list[str]) -> list[dict]:
    """Extract entities from a batch of texts (uses spaCy pipe for speed)."""
    if not _nlp:
        return [{"entities": [], "entity_counts": {}} for _ in texts]

    results = []
    for doc in _nlp.pipe(texts, batch_size=50, n_process=1):
        entities = []
        counts: dict[str, int] = {}
        for ent in doc.ents:
            entities.append({"text": ent.text, "label": ent.label_})
            counts[ent.label_] = counts.get(ent.label_, 0) + 1
        results.append({"entities": entities, "entity_counts": counts})
    return results
