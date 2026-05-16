from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base

if TYPE_CHECKING:
    from app.models.inspection import Inspection


class NCLevel(str, Enum):
    conforme = "conforme"
    observation = "observation"
    nc_mineure = "nc_mineure"
    nc_majeure = "nc_majeure"


class Constat(Base):
    __tablename__ = "constats"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    inspection_id: Mapped[UUID] = mapped_column(ForeignKey("inspections.id", ondelete="CASCADE"))
    checklist_point_id: Mapped[str | None] = mapped_column(String(50), nullable=True)

    raw_text: Mapped[str] = mapped_column(Text)
    reformulated_text: Mapped[str] = mapped_column(Text)
    classification: Mapped[NCLevel]
    norm_reference: Mapped[str | None] = mapped_column(String(100), nullable=True)
    norm_excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    suggested_evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    suggested_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    rag_chunks: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    photo_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    audio_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    inspection: Mapped[Inspection] = relationship(back_populates="constats")
