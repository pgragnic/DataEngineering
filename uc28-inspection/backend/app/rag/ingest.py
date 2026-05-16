"""Ingest ISO 9001 corpus into ChromaDB (optional — requires chromadb + sentence-transformers)."""

from __future__ import annotations

import re
from pathlib import Path

CORPUS_DIR = Path(__file__).parent.parent.parent / "corpus"


def ingest():
    try:
        import chromadb
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("chromadb / sentence-transformers non installés.")
        print("Le RAG utilisera la recherche par mots-clés (fallback automatique).")
        return

    from app.config import settings

    model = SentenceTransformer("all-MiniLM-L6-v2")
    client = chromadb.PersistentClient(path=settings.chroma_persist_dir)
    collection = client.get_or_create_collection("iso_norms")

    docs, embeddings_list, metadatas, ids = [], [], [], []

    for md_file in sorted(CORPUS_DIR.rglob("*.md")):
        text = md_file.read_text(encoding="utf-8")
        norm = "ISO 9001" if "iso9001" in str(md_file) else "ISO 19011"
        match = re.match(r"(\d+)", md_file.stem)
        section = match.group(1) if match else md_file.stem

        words = text.split()
        for i in range(0, len(words), 400):
            chunk = " ".join(words[i: i + 400])
            if not chunk.strip():
                continue
            doc_id = f"{md_file.stem}_{i}"
            docs.append(chunk)
            embeddings_list.append(model.encode(chunk).tolist())
            metadatas.append({"norm": norm, "section": section, "source": md_file.name})
            ids.append(doc_id)

    if docs:
        collection.upsert(documents=docs, embeddings=embeddings_list, metadatas=metadatas, ids=ids)
        print(f"Ingested {len(docs)} chunks from {CORPUS_DIR}")


if __name__ == "__main__":
    ingest()
