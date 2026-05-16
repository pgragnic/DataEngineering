import json
import uuid
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS

from app.agents.preparation import generate_checklist
from app.agents.capture import classify_constat
from app.agents.restitution import generate_report_structure
from app.docx_gen.report import generate_docx

app = Flask(__name__)
CORS(app)

# --- JSON datastore ---

DB_PATH = Path("data/db.json")
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./storage/files"))
FIXTURES_PATH = Path("data/fixtures/alpha.json")

def load_db() -> dict:
    if not DB_PATH.exists():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _save_db({"inspections": {}, "constats": []})
    return json.loads(DB_PATH.read_text(encoding="utf-8"))

def _save_db(db: dict):
    DB_PATH.write_text(json.dumps(db, indent=2, ensure_ascii=False, default=str))

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# --- Health ---

@app.get("/health")
def health():
    return jsonify({"status": "ok"})

# --- Dashboard ---

@app.get("/api/dashboard/kpis")
def dashboard_kpis():
    db = load_db()
    return jsonify({
        "audits_today_count": len(db["inspections"]),
        "audits_month_count": len(db["inspections"]),
        "avg_delay_days": 0,
        "pending_recurrences_count": 1,
    })

@app.get("/api/dashboard/audits_today")
def dashboard_audits_today():
    db = load_db()
    result = []
    for i, (iid, ins) in enumerate(db["inspections"].items()):
        result.append({
            "id": iid,
            "scheduled_at": ins.get("created_at", now_iso()),
            "client_name": ins["client_name"],
            "location": ins["site_name"],
            "scope": ins["scope"][:80],
            "status": ins["status"],
            "is_next": i == 0,
        })
    return jsonify(result)

@app.get("/api/dashboard/recurrences")
def dashboard_recurrences():
    return jsonify([{
        "inspection_id": "ins_alpha_20250615",
        "client_name": "Fournisseur ALPHA",
        "norm_reference": "ISO 9001 §8.4.3",
        "opened_at": "2025-06-15",
        "label": "Procédure contrôle réception à formaliser",
    }])

# --- Inspections ---

@app.post("/api/inspections")
def create_inspection():
    db = load_db()
    body = request.json
    iid = str(uuid.uuid4())
    inspection = {
        "id": iid,
        "client_name": body["client_name"],
        "client_siret": body.get("client_siret"),
        "site_name": body["site_name"],
        "site_address": body.get("site_address"),
        "auditor_name": body["auditor_name"],
        "referential": body.get("referential", "ISO 9001:2015"),
        "scope": body["scope"],
        "status": "prepared",
        "checklist_json": None,
        "report_structure": None,
        "started_at": None,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    db["inspections"][iid] = inspection
    _save_db(db)
    return jsonify({**inspection, "constats": []}), 201

@app.get("/api/inspections")
def list_inspections():
    db = load_db()
    return jsonify(list(db["inspections"].values()))

@app.get("/api/inspections/<iid>")
def get_inspection(iid):
    db = load_db()
    ins = db["inspections"].get(iid)
    if not ins:
        return jsonify({"error": "Not found"}), 404
    constats = [c for c in db["constats"] if c["inspection_id"] == iid]
    return jsonify({**ins, "constats": constats})

@app.post("/api/inspections/<iid>/checklist")
def generate_checklist_route(iid):
    db = load_db()
    ins = db["inspections"].get(iid)
    if not ins:
        return jsonify({"error": "Not found"}), 404

    referential = request.args.get("referential") or ins["referential"]
    regenerate = request.headers.get("X-Regenerate") == "true" or request.args.get("referential")

    if ins.get("checklist_json") and not regenerate:
        return jsonify({"checklist_json": ins["checklist_json"], "generation_duration_seconds": 0})

    start = time.time()
    checklist = generate_checklist(
        client_name=ins["client_name"],
        site_name=ins["site_name"],
        referential=referential,
        scope=ins["scope"],
    )
    duration = round(time.time() - start, 1)

    ins["checklist_json"] = checklist
    ins["updated_at"] = now_iso()
    _save_db(db)
    return jsonify({"checklist_json": checklist, "generation_duration_seconds": duration})

@app.patch("/api/inspections/<iid>")
def update_inspection(iid):
    db = load_db()
    ins = db["inspections"].get(iid)
    if not ins:
        return jsonify({"error": "Not found"}), 404
    body = request.json or {}
    if "status" in body:
        ins["status"] = body["status"]
        if body["status"] == "ongoing" and not ins.get("started_at"):
            ins["started_at"] = now_iso()
    ins["updated_at"] = now_iso()
    _save_db(db)
    constats = [c for c in db["constats"] if c["inspection_id"] == iid]
    return jsonify({**ins, "constats": constats})

@app.get("/api/inspections/<iid>/history")
def inspection_history(iid):
    return jsonify([])

# --- Constats ---

@app.post("/api/inspections/<iid>/constats")
def create_constat(iid):
    db = load_db()
    ins = db["inspections"].get(iid)
    if not ins:
        return jsonify({"error": "Not found"}), 404

    body = request.json
    raw_text = body["raw_text"]
    point_id = body.get("checklist_point_id")
    photo_id = body.get("photo_id")

    # Find checklist point for context
    checklist_point = None
    if point_id and ins.get("checklist_json"):
        for section in ins["checklist_json"].get("sections", []):
            for pt in section.get("points", []):
                if pt["id"] == point_id:
                    checklist_point = pt
                    break

    # Load photo if provided
    photo_bytes = None
    if photo_id:
        STORAGE_DIR.mkdir(parents=True, exist_ok=True)
        matches = list(STORAGE_DIR.glob(f"{photo_id}*"))
        if matches:
            photo_bytes = matches[0].read_bytes()

    structured, rag_chunks = classify_constat(
        raw_text=raw_text,
        referential=ins["referential"],
        scope=ins["scope"],
        checklist_point=checklist_point,
        photo_bytes=photo_bytes,
    )

    photo_path = None
    if photo_id:
        matches = list(STORAGE_DIR.glob(f"{photo_id}*"))
        if matches:
            photo_path = str(matches[0])

    constat = {
        "id": str(uuid.uuid4()),
        "inspection_id": iid,
        "checklist_point_id": point_id,
        "raw_text": raw_text,
        "reformulated_text": structured.get("reformulated_text", raw_text),
        "classification": structured.get("classification", "observation"),
        "norm_reference": structured.get("norm_reference"),
        "norm_excerpt": structured.get("norm_excerpt"),
        "suggested_evidence": structured.get("suggested_evidence"),
        "suggested_action": structured.get("suggested_action"),
        "rag_chunks": rag_chunks,
        "photo_path": photo_path,
        "audio_path": None,
        "created_at": now_iso(),
    }
    db["constats"].append(constat)
    _save_db(db)
    return jsonify(constat), 201

@app.get("/api/inspections/<iid>/constats")
def list_constats(iid):
    db = load_db()
    return jsonify([c for c in db["constats"] if c["inspection_id"] == iid])

@app.delete("/api/constats/<cid>")
def delete_constat(cid):
    db = load_db()
    db["constats"] = [c for c in db["constats"] if c["id"] != cid]
    _save_db(db)
    return "", 204

# --- Uploads ---

_uploads: dict[str, str] = {}

@app.post("/api/uploads")
def upload_file():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file"}), 400
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    file_id = str(uuid.uuid4())
    suffix = Path(file.filename or "upload").suffix or ".bin"
    dest = STORAGE_DIR / f"{file_id}{suffix}"
    file.save(dest)
    _uploads[file_id] = str(dest)
    kind = "photo" if file.content_type.startswith("image/") else "audio"
    return jsonify({"id": file_id, "path": str(dest), "kind": kind}), 201

@app.get("/api/uploads/<file_id>")
def get_upload(file_id):
    path = _uploads.get(file_id)
    if not path or not Path(path).exists():
        return jsonify({"error": "Not found"}), 404
    return send_file(path)

# --- Reports ---

@app.post("/api/inspections/<iid>/report")
def build_report(iid):
    db = load_db()
    ins = db["inspections"].get(iid)
    if not ins:
        return jsonify({"error": "Not found"}), 404

    constats = [c for c in db["constats"] if c["inspection_id"] == iid]
    start = time.time()
    report_structure = generate_report_structure(
        inspection=ins,
        constats=constats,
    )
    duration = round(time.time() - start, 1)

    ins["report_structure"] = report_structure
    ins["updated_at"] = now_iso()
    _save_db(db)

    return jsonify({
        "report_structure": report_structure,
        "generation_duration_seconds": duration,
        "docx_url": f"/api/inspections/{iid}/report.docx",
    })

@app.get("/api/inspections/<iid>/report.docx")
def download_report(iid):
    db = load_db()
    ins = db["inspections"].get(iid)
    if not ins or not ins.get("report_structure"):
        return jsonify({"error": "Report not generated"}), 404

    constats = [c for c in db["constats"] if c["inspection_id"] == iid]
    docx_bytes = generate_docx(inspection=ins, report_structure=ins["report_structure"])

    out = Path(f"/tmp/rapport-{iid}.docx")
    out.write_bytes(docx_bytes)
    return send_file(
        out,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        as_attachment=True,
        download_name=f"rapport-{ins['client_name'].lower().replace(' ', '-')}.docx",
    )

@app.post("/api/inspections/<iid>/send")
def send_report(iid):
    db = load_db()
    ins = db["inspections"].get(iid)
    if not ins:
        return jsonify({"error": "Not found"}), 404
    ins["status"] = "completed"
    ins["updated_at"] = now_iso()
    _save_db(db)
    body = request.json or {}
    return jsonify({"sent_at": now_iso(), "recipient_count": len(body.get("to", []))})

# --- Dev / demo helpers ---

@app.post("/api/dev/reset-demo")
def reset_demo():
    if not FIXTURES_PATH.exists():
        return jsonify({"error": "Fixtures not found"}), 404

    fixtures = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))
    db = load_db()

    # Remove existing ALPHA inspections
    to_remove = [iid for iid, ins in db["inspections"].items()
                 if ins["client_name"] == "Fournisseur ALPHA"]
    for iid in to_remove:
        del db["inspections"][iid]
        db["constats"] = [c for c in db["constats"] if c["inspection_id"] != iid]

    iid = str(uuid.uuid4())
    insp = fixtures["inspection"]
    db["inspections"][iid] = {
        "id": iid,
        "client_name": insp["client_name"],
        "client_siret": insp.get("client_siret"),
        "site_name": insp["site_name"],
        "site_address": insp.get("site_address"),
        "auditor_name": insp["auditor_name"],
        "referential": insp["referential"],
        "scope": insp["scope"],
        "status": "ongoing",
        "checklist_json": fixtures.get("checklist"),
        "report_structure": None,
        "started_at": now_iso(),
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    _save_db(db)
    return jsonify({"reset_at": now_iso(), "inspection_id": iid})

@app.post("/api/dev/replay/<int:index>")
def replay_constat(index):
    if not FIXTURES_PATH.exists():
        return jsonify({"error": "Fixtures not found"}), 404

    fixtures = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))
    constats_fixtures = fixtures.get("constats", [])
    if index < 1 or index > len(constats_fixtures):
        return jsonify({"error": f"Index 1-{len(constats_fixtures)} attendu"}), 400

    db = load_db()
    ins_entry = next(
        ((iid, ins) for iid, ins in db["inspections"].items()
         if ins["client_name"] == "Fournisseur ALPHA"),
        None,
    )
    if not ins_entry:
        return jsonify({"error": "Lancer /api/dev/reset-demo d'abord"}), 400

    iid, _ = ins_entry
    c_data = constats_fixtures[index - 1]
    constat = {
        "id": str(uuid.uuid4()),
        "inspection_id": iid,
        "checklist_point_id": c_data.get("checklist_point_id"),
        "raw_text": c_data.get("raw_text", ""),
        "reformulated_text": c_data.get("reformulated_text", ""),
        "classification": c_data.get("classification", "observation"),
        "norm_reference": c_data.get("norm_reference"),
        "norm_excerpt": c_data.get("norm_excerpt"),
        "suggested_evidence": c_data.get("suggested_evidence"),
        "suggested_action": c_data.get("suggested_action"),
        "rag_chunks": c_data.get("rag_chunks"),
        "photo_path": None,
        "audio_path": None,
        "created_at": now_iso(),
    }
    db["constats"].append(constat)
    _save_db(db)
    return jsonify({"constat": {"id": constat["id"], "classification": constat["classification"]}})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
