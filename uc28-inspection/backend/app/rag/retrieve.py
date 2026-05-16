"""Retrieve normative context — fallback to keyword search if ChromaDB unavailable."""

from __future__ import annotations

from pathlib import Path

CORPUS_DIR = Path(__file__).parent.parent.parent / "corpus"

# Try ChromaDB + sentence-transformers; fall back to simple keyword search
_USE_CHROMA = False
try:
    import chromadb
    from sentence_transformers import SentenceTransformer
    _model = SentenceTransformer("all-MiniLM-L6-v2")
    _USE_CHROMA = True
except Exception:
    pass

_chroma_collection = None


def _get_collection():
    global _chroma_collection
    if _chroma_collection is None:
        from app.config import settings
        client = chromadb.PersistentClient(path=settings.chroma_persist_dir)
        _chroma_collection = client.get_or_create_collection("iso_norms")
    return _chroma_collection


def _keyword_search(query: str, k: int = 3) -> list[dict]:
    """Simple keyword search across corpus markdown files."""
    query_words = set(query.lower().split())
    scored: list[tuple[float, str, str]] = []

    for md_file in sorted(CORPUS_DIR.rglob("*.md")):
        text = md_file.read_text(encoding="utf-8")
        text_lower = text.lower()
        hits = sum(1 for w in query_words if w in text_lower and len(w) > 3)
        if hits > 0:
            section = md_file.stem.split("_")[0].lstrip("0")
            scored.append((hits, section, text[:600]))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [
        {"section": s, "excerpt": excerpt, "score": round(hits / max(len(query_words), 1), 2)}
        for hits, s, excerpt in scored[:k]
    ]


def retrieve_norm_context(query: str, k: int = 3) -> list[dict]:
    """Return top-k normative chunks. Uses ChromaDB if available, keyword search otherwise."""
    if _USE_CHROMA:
        try:
            collection = _get_collection()
            if collection.count() == 0:
                return _keyword_search(query, k)
            embedding = _model.encode(query).tolist()
            results = collection.query(
                query_embeddings=[embedding], n_results=min(k, collection.count())
            )
            return [
                {
                    "section": meta.get("section", ""),
                    "excerpt": doc,
                    "score": round(1 - dist, 4) if dist <= 1 else round(1 / (1 + dist), 4),
                }
                for doc, meta, dist in zip(
                    results["documents"][0],
                    results["metadatas"][0],
                    results["distances"][0],
                )
            ]
        except Exception:
            pass

    return _keyword_search(query, k)
