from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from app.models.inspection import InspectionStatus
from app.schemas.constat import ConstatOut


class InspectionCreate(BaseModel):
    client_name: str
    client_siret: str | None = None
    site_name: str
    site_address: str | None = None
    auditor_name: str
    referential: str = "ISO 9001:2015"
    scope: str


class InspectionSummary(BaseModel):
    id: UUID
    client_name: str
    site_name: str
    auditor_name: str
    referential: str
    status: InspectionStatus
    created_at: datetime
    started_at: datetime | None

    model_config = {"from_attributes": True}


class InspectionOut(InspectionSummary):
    client_siret: str | None
    site_address: str | None
    scope: str
    checklist_json: dict | None
    constats: list[ConstatOut] = []
