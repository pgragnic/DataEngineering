"""Save and load uploaded files (photos, audio) to local storage."""

from __future__ import annotations

import uuid
from pathlib import Path

from app.config import settings


def save_file(data: bytes, filename: str) -> tuple[str, str]:
    """Save file bytes to storage; return (file_id, relative_path)."""
    storage = Path(settings.storage_dir)
    storage.mkdir(parents=True, exist_ok=True)

    file_id = str(uuid.uuid4())
    suffix = Path(filename).suffix or ".bin"
    dest = storage / f"{file_id}{suffix}"
    dest.write_bytes(data)

    return file_id, str(dest.relative_to(Path(".")))


def load_file(path: str) -> bytes:
    return Path(path).read_bytes()


def kind_from_content_type(content_type: str) -> str:
    if content_type.startswith("image/"):
        return "photo"
    if content_type.startswith("audio/"):
        return "audio"
    return "file"
