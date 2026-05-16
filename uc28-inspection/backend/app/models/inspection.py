from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base

if TYPE_CHECKING:
    from app.models.constat import Constat


class InspectionStatus(str, Enum):
    prepared = "prepared"
    ongoing = "ongoing"
    completed = "completed"


class Inspection(Base):
    __tablename__ = "inspections"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    client_name: Mapped[str] = mapped_column(String(255))
    client_siret: Mapped[str | None] = mapped_column(String(20))
    site_name: Mapped[str] = mapped_column(String(255))
    site_address: Mapped[str | None] = mapped_column(String(500))
    auditor_name: Mapped[str] = mapped_column(String(255))
    referential: Mapped[str] = mapped_column(String(100), default="ISO 9001:2015")
    scope: Mapped[str] = mapped_column(String(2000))
    status: Mapped[InspectionStatus] = mapped_column(default=InspectionStatus.prepared)
    checklist_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    constats: Mapped[list[Constat]] = relationship(
        back_populates="inspection", cascade="all, delete-orphan"
    )
