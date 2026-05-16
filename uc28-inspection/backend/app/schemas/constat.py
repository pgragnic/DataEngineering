from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.models.constat import NCLevel


class ConstatCreate(BaseModel):
    raw_text: str
    checklist_point_id: str | None = None
    photo_id: str | None = None
    audio_id: str | None = None


class ConstatOut(BaseModel):
    id: UUID
    inspection_id: UUID
    checklist_point_id: str | None
    raw_text: str
    reformulated_text: str
    classification: NCLevel
    norm_reference: str | None
    norm_excerpt: str | None
    suggested_evidence: str | None
    suggested_action: str | None
    rag_chunks: list | None
    photo_path: str | None
    audio_path: str | None
    created_at: datetime

    model_config = {"from_attributes": True}
