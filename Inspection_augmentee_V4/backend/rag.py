import os
import numpy as np
import faiss

# Forcer l'utilisation du cache local — évite les appels HuggingFace bloqués par le proxy SSL Capgemini
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

from sentence_transformers import SentenceTransformer

from database import get_connection, init_db

_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"

_model: SentenceTransformer | None = None
_index: faiss.IndexFlatIP | None = None
_clauses: list[dict] | None = None


def _load() -> None:
    global _model, _index, _clauses
    if _index is not None:
        return

    init_db()
    conn = get_connection()
    rows = conn.execute(
        "SELECT clause, titre, exigence FROM clauses_iso ORDER BY id"
    ).fetchall()
    conn.close()

    _clauses = [dict(r) for r in rows]

    _model = SentenceTransformer(_MODEL_NAME)

    # Concaténer titre + exigence pour un embedding riche
    texts = [f"{c['titre']} : {c['exigence']}" for c in _clauses]
    embeddings = _model.encode(
        texts, normalize_embeddings=True, show_progress_bar=False
    ).astype("float32")

    dim = embeddings.shape[1]
    _index = faiss.IndexFlatIP(dim)  # cosine similarity sur vecteurs normalisés
    _index.add(embeddings)


def trouver_clause(observation_text: str) -> dict:
    """Retourne la clause ISO 9001 la plus pertinente pour une observation terrain."""
    _load()
    query = _model.encode(
        [observation_text], normalize_embeddings=True
    ).astype("float32")
    scores, indices = _index.search(query, k=1)
    idx = int(indices[0][0])
    return {
        "clause": _clauses[idx]["clause"],
        "titre": _clauses[idx]["titre"],
        "exigence": _clauses[idx]["exigence"],
        "score": round(float(scores[0][0]), 4),
    }
