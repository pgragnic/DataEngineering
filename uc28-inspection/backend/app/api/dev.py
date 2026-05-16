"""Dev/demo helper routes. Never expose in production."""

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.constat import Constat, NCLevel
from app.models.inspection import Inspection, InspectionStatus

router = APIRouter(tags=["dev"])

FIXTURES_PATH = Path(__file__).parent.parent.parent / "data" / "fixtures" / "alpha.json"

_DEMO_INSPECTION_ID: str | None = None


@router.post("/reset-demo")
def reset_demo(db: Session = Depends(get_db)):
    global _DEMO_INSPECTION_ID

    if not FIXTURES_PATH.exists():
        return {"error": "Fixtures file not found", "path": str(FIXTURES_PATH)}

    fixtures = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))

    # Delete existing demo inspection
    existing = (
        db.query(Inspection)
        .filter(Inspection.client_name == "Fournisseur ALPHA")
        .first()
    )
    if existing:
        db.delete(existing)
        db.commit()

    insp_data = fixtures["inspection"]
    inspection = Inspection(
        client_name=insp_data["client_name"],
        client_siret=insp_data.get("client_siret"),
        site_name=insp_data["site_name"],
        site_address=insp_data.get("site_address"),
        auditor_name=insp_data["auditor_name"],
        referential=insp_data["referential"],
        scope=insp_data["scope"],
        status=InspectionStatus.ongoing,
        checklist_json=fixtures.get("checklist"),
        started_at=datetime.now(timezone.utc),
    )
    db.add(inspection)
    db.commit()
    db.refresh(inspection)

    _DEMO_INSPECTION_ID = str(inspection.id)
    reset_at = datetime.now(timezone.utc).isoformat()
    return {"reset_at": reset_at, "inspection_id": _DEMO_INSPECTION_ID}


@router.post("/replay/{constat_index}")
def replay_constat(constat_index: int, db: Session = Depends(get_db)):
    if not FIXTURES_PATH.exists():
        return {"error": "Fixtures file not found"}

    fixtures = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))
    constats = fixtures.get("constats", [])

    if constat_index < 1 or constat_index > len(constats):
        return {"error": f"Constat index must be 1-{len(constats)}"}

    inspection = (
        db.query(Inspection)
        .filter(Inspection.client_name == "Fournisseur ALPHA")
        .first()
    )
    if not inspection:
        return {"error": "Run /api/dev/reset-demo first"}

    c_data = constats[constat_index - 1]
    constat = Constat(
        inspection_id=inspection.id,
        checklist_point_id=c_data.get("checklist_point_id"),
        raw_text=c_data.get("raw_text", ""),
        reformulated_text=c_data.get("reformulated_text", ""),
        classification=NCLevel(c_data.get("classification", "observation")),
        norm_reference=c_data.get("norm_reference"),
        norm_excerpt=c_data.get("norm_excerpt"),
        suggested_evidence=c_data.get("suggested_evidence"),
        suggested_action=c_data.get("suggested_action"),
        rag_chunks=c_data.get("rag_chunks"),
    )
    db.add(constat)
    db.commit()
    db.refresh(constat)
    return {"constat": {"id": str(constat.id), "classification": constat.classification.value}}
