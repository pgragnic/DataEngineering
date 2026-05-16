from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.agents.preparation import generate_checklist
from app.db import get_db
from app.models.inspection import Inspection, InspectionStatus
from app.schemas.inspection import InspectionCreate, InspectionOut, InspectionSummary

router = APIRouter(tags=["inspections"])


@router.post("/inspections", response_model=InspectionOut, status_code=201)
def create_inspection(body: InspectionCreate, db: Session = Depends(get_db)):
    inspection = Inspection(**body.model_dump())
    db.add(inspection)
    db.commit()
    db.refresh(inspection)
    return inspection


@router.get("/inspections", response_model=list[InspectionSummary])
def list_inspections(db: Session = Depends(get_db)):
    return db.query(Inspection).order_by(Inspection.created_at.desc()).all()


@router.get("/inspections/{inspection_id}", response_model=InspectionOut)
def get_inspection(inspection_id: UUID, db: Session = Depends(get_db)):
    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    return inspection


@router.post("/inspections/{inspection_id}/checklist")
def generate_inspection_checklist(
    inspection_id: UUID,
    request: Request,
    db: Session = Depends(get_db),
):
    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    regenerate = (
        request.headers.get("X-Regenerate") == "true"
        or request.query_params.get("referential")
    )
    if inspection.checklist_json and not regenerate:
        return {"checklist_json": inspection.checklist_json, "generation_duration_seconds": 0}

    import time
    start = time.time()

    referential = request.query_params.get("referential") or inspection.referential
    checklist = generate_checklist(
        client_name=inspection.client_name,
        site_name=inspection.site_name,
        referential=referential,
        scope=inspection.scope,
        previous_audits=None,
    )
    duration = round(time.time() - start, 1)

    inspection.checklist_json = checklist
    db.commit()
    db.refresh(inspection)

    return {"checklist_json": checklist, "generation_duration_seconds": duration}


@router.patch("/inspections/{inspection_id}", response_model=InspectionOut)
def update_inspection(
    inspection_id: UUID,
    body: dict,
    db: Session = Depends(get_db),
):
    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    if "status" in body:
        inspection.status = InspectionStatus(body["status"])
        if body["status"] == "ongoing" and not inspection.started_at:
            from datetime import datetime, timezone
            inspection.started_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(inspection)
    return inspection


@router.get("/dashboard/kpis")
def dashboard_kpis(db: Session = Depends(get_db)):
    from sqlalchemy import func
    from datetime import date, timezone, datetime

    today_start = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)
    today_count = (
        db.query(func.count(Inspection.id))
        .filter(Inspection.created_at >= today_start)
        .scalar()
    )
    return {
        "audits_today_count": today_count,
        "audits_month_count": db.query(func.count(Inspection.id)).scalar(),
        "avg_delay_days": 0,
        "pending_recurrences_count": 1,
    }


@router.get("/dashboard/audits_today")
def dashboard_audits_today(db: Session = Depends(get_db)):
    inspections = db.query(Inspection).order_by(Inspection.created_at.desc()).limit(10).all()
    result = []
    for i, ins in enumerate(inspections):
        result.append({
            "id": str(ins.id),
            "scheduled_at": ins.created_at.isoformat(),
            "client_name": ins.client_name,
            "location": ins.site_name,
            "scope": ins.scope[:80],
            "status": ins.status.value,
            "is_next": i == 0,
        })
    return result


@router.get("/dashboard/recurrences")
def dashboard_recurrences():
    return [
        {
            "inspection_id": "ins_alpha_20250615",
            "client_name": "Fournisseur ALPHA",
            "norm_reference": "ISO 9001 §8.4.3",
            "opened_at": "2025-06-15",
            "label": "Procédure contrôle réception à formaliser",
        }
    ]


@router.get("/inspections/{inspection_id}/history")
def inspection_history(inspection_id: UUID, db: Session = Depends(get_db)):
    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")
    return []
