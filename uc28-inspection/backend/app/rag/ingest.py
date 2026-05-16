"""Ingest the ISO 9001 / 19011 corpus into ChromaDB."""

from __future__ import annotations

import re
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

from app.config import settings

CORPUS_DIR = Path(__file__).parent.parent.parent / "corpus"
CHUNK_SIZE = 400  # tokens (approximate — split by words)

_model = SentenceTransformer("all-MiniLM-L6-v2")


def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE) -> list[str]:
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunks.append(" ".join(words[i : i + chunk_size]))
    return [c for c in chunks if c.strip()]


def _extract_section(filepath: Path) -> str:
    """Extract section identifier from filename, e.g. '07_support' → '7'."""
    match = re.match(r"(\d+)", filepath.stem)
    return match.group(1) if match else filepath.stem


def ingest():
    client = chromadb.PersistentClient(path=settings.chroma_persist_dir)
    collection = client.get_or_create_collection("iso_norms")

    docs, embeddings_list, metadatas, ids = [], [], [], []

    for md_file in sorted(CORPUS_DIR.rglob("*.md")):
        text = md_file.read_text(encoding="utf-8")
        norm = "ISO 9001" if "iso9001" in str(md_file) else "ISO 19011"
        section = _extract_section(md_file)

        for i, chunk in enumerate(_chunk_text(text)):
            doc_id = f"{md_file.stem}_{i}"
            embedding = _model.encode(chunk).tolist()
            docs.append(chunk)
            embeddings_list.append(embedding)
            metadatas.append({"norm": norm, "section": section, "source": md_file.name})
            ids.append(doc_id)

    if docs:
        collection.upsert(documents=docs, embeddings=embeddings_list, metadatas=metadatas, ids=ids)
        print(f"Ingested {len(docs)} chunks from {CORPUS_DIR}")
    else:
        print(f"No markdown files found in {CORPUS_DIR}")


if __name__ == "__main__":
    ingest()
