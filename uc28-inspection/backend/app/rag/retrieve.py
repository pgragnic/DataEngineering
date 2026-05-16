"""Retrieve normative context chunks from ChromaDB."""

from __future__ import annotations

import chromadb
from sentence_transformers import SentenceTransformer

from app.config import settings

_model = SentenceTransformer("all-MiniLM-L6-v2")
_chroma: chromadb.PersistentClient | None = None
_collection = None


def _get_collection():
    global _chroma, _collection
    if _collection is None:
        _chroma = chromadb.PersistentClient(path=settings.chroma_persist_dir)
        _collection = _chroma.get_or_create_collection("iso_norms")
    return _collection


def retrieve_norm_context(query: str, k: int = 3) -> list[dict]:
    """Return top-k normative chunks relevant to the query."""
    collection = _get_collection()

    if collection.count() == 0:
        return []

    embedding = _model.encode(query).tolist()
    results = collection.query(query_embeddings=[embedding], n_results=min(k, collection.count()))

    chunks = []
    for doc, meta, dist in zip(
        results["documents"][0], results["metadatas"][0], results["distances"][0]
    ):
        score = round(1 - dist, 4) if dist <= 1 else round(1 / (1 + dist), 4)
        chunks.append({"section": meta.get("section", ""), "excerpt": doc, "score": score})

    return chunks
