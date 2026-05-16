from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.agents.capture import classify_constat
from app.db import get_db
from app.models.constat import Constat, NCLevel
from app.models.inspection import Inspection
from app.schemas.constat import ConstatCreate, ConstatOut

router = APIRouter(tags=["constats"])


@router.post("/inspections/{inspection_id}/constats", response_model=ConstatOut, status_code=201)
def create_constat(
    inspection_id: UUID,
    body: ConstatCreate,
    db: Session = Depends(get_db),
):
    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    photo_bytes = None
    if body.photo_id:
        from pathlib import Path
        from app.config import settings
        storage = Path(settings.storage_dir)
        matches = list(storage.glob(f"{body.photo_id}*"))
        if matches:
            photo_bytes = matches[0].read_bytes()

    checklist_point = None
    if body.checklist_point_id and inspection.checklist_json:
        for section in inspection.checklist_json.get("sections", []):
            for point in section.get("points", []):
                if point["id"] == body.checklist_point_id:
                    checklist_point = point
                    break

    structured, rag_chunks = classify_constat(
        raw_text=body.raw_text,
        referential=inspection.referential,
        scope=inspection.scope,
        checklist_point=checklist_point,
        photo_bytes=photo_bytes,
    )

    photo_path = None
    if body.photo_id:
        from pathlib import Path
        from app.config import settings
        storage = Path(settings.storage_dir)
        matches = list(storage.glob(f"{body.photo_id}*"))
        if matches:
            photo_path = str(matches[0])

    constat = Constat(
        inspection_id=inspection_id,
        checklist_point_id=body.checklist_point_id,
        raw_text=body.raw_text,
        reformulated_text=structured.get("reformulated_text", body.raw_text),
        classification=NCLevel(structured.get("classification", "observation")),
        norm_reference=structured.get("norm_reference"),
        norm_excerpt=structured.get("norm_excerpt"),
        suggested_evidence=structured.get("suggested_evidence"),
        suggested_action=structured.get("suggested_action"),
        rag_chunks=rag_chunks,
        photo_path=photo_path,
    )
    db.add(constat)
    db.commit()
    db.refresh(constat)
    return constat


@router.get("/inspections/{inspection_id}/constats", response_model=list[ConstatOut])
def list_constats(inspection_id: UUID, db: Session = Depends(get_db)):
    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    return inspection.constats


@router.delete("/constats/{constat_id}", status_code=204)
def delete_constat(constat_id: UUID, db: Session = Depends(get_db)):
    constat = db.get(Constat, constat_id)
    if not constat:
        raise HTTPException(status_code=404, detail="Constat not found")
    db.delete(constat)
    db.commit()
