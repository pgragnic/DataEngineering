import time
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app.agents.restitution import generate_report_structure
from app.db import get_db
from app.docx_gen.report import generate_docx
from app.models.inspection import Inspection

router = APIRouter(tags=["reports"])


@router.post("/inspections/{inspection_id}/report")
def build_report(inspection_id: UUID, db: Session = Depends(get_db)):
    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    constats_data = [
        {
            "id": str(c.id),
            "classification": c.classification.value,
            "reformulated_text": c.reformulated_text,
            "norm_reference": c.norm_reference,
            "suggested_action": c.suggested_action,
            "checklist_point_id": c.checklist_point_id,
        }
        for c in inspection.constats
    ]

    start = time.time()
    report_structure = generate_report_structure(
        inspection={
            "client_name": inspection.client_name,
            "site_name": inspection.site_name,
            "auditor_name": inspection.auditor_name,
            "referential": inspection.referential,
            "scope": inspection.scope,
            "started_at": inspection.started_at,
            "checklist_json": inspection.checklist_json,
        },
        constats=constats_data,
    )
    duration = round(time.time() - start, 1)

    docx_url = f"/api/inspections/{inspection_id}/report.docx"
    inspection.checklist_json = inspection.checklist_json or {}
    inspection.checklist_json["_report_structure"] = report_structure
    db.commit()

    return {
        "report_structure": report_structure,
        "generation_duration_seconds": duration,
        "docx_url": docx_url,
    }


@router.get("/inspections/{inspection_id}/report.docx")
def download_report(inspection_id: UUID, db: Session = Depends(get_db)):
    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    if not inspection.checklist_json or "_report_structure" not in inspection.checklist_json:
        raise HTTPException(status_code=404, detail="Report not generated yet")

    report_structure = inspection.checklist_json["_report_structure"]
    constats_data = [
        {
            "id": str(c.id),
            "classification": c.classification.value,
            "reformulated_text": c.reformulated_text,
            "norm_reference": c.norm_reference,
            "suggested_action": c.suggested_action,
        }
        for c in inspection.constats
    ]

    docx_bytes = generate_docx(
        inspection={
            "client_name": inspection.client_name,
            "site_name": inspection.site_name,
            "auditor_name": inspection.auditor_name,
            "referential": inspection.referential,
        },
        report_structure=report_structure,
    )

    filename = f"rapport-{inspection.client_name.lower().replace(' ', '-')}.docx"
    return Response(
        content=docx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/inspections/{inspection_id}/send")
def send_report(inspection_id: UUID, body: dict, db: Session = Depends(get_db)):
    from datetime import datetime, timezone
    from app.models.inspection import InspectionStatus

    inspection = db.get(Inspection, inspection_id)
    if not inspection:
        raise HTTPException(status_code=404, detail="Inspection not found")

    inspection.status = InspectionStatus.completed
    db.commit()

    sent_at = datetime.now(timezone.utc).isoformat()
    return {"sent_at": sent_at, "recipient_count": len(body.get("to", []))}
