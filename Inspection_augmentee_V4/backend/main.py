import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from database import get_connection, init_db
from agent import analyser_observation, synthetiser_observation, generer_suggestions, generer_questions_oui_non, analyser_document_fournisseur, transcrire_manuscrit
from rag import trouver_clause
from rapport import generer_rapport


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Préchargement du modèle sentence-transformers au démarrage
    # (évite un délai de 30-60s sur le premier appel /analyser)
    trouver_clause("warmup")
    yield


app = FastAPI(
    title="Inspection Augmentée — UC 28",
    description="API backend — Hackathon Capgemini × Anthropic 2026",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:8000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/sites", summary="Liste tous les sites")
def list_sites():
    conn = get_connection()
    rows = conn.execute(
        """SELECT site_id, nom, localisation, perimetre, effectif,
                  responsable_qualite, derniere_certification_iso, prochaine_recertification
           FROM sites
           ORDER BY nom"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


class ObservationRequest(BaseModel):
    observation: str
    site_id: str | None = None


@app.post("/analyser", summary="Analyse une observation terrain via Claude")
def analyser(req: ObservationRequest):
    if not req.observation.strip():
        raise HTTPException(status_code=422, detail="L'observation ne peut pas être vide")
    return analyser_observation(req.observation, req.site_id)


class SyntheseRequest(BaseModel):
    observation: str


class SuggestionsRequest(BaseModel):
    item_texte: str
    clause: str
    section_titre: str


@app.post("/suggestions", summary="Génère 3 exemples d'observations terrain via Claude Opus")
def suggestions(req: SuggestionsRequest):
    if not req.item_texte.strip():
        raise HTTPException(status_code=422, detail="Le point de checklist ne peut pas être vide")
    exemples = generer_suggestions(req.item_texte, req.clause, req.section_titre)
    return {"suggestions": exemples}


class QuestionsOuiNonRequest(BaseModel):
    item_texte: str
    clause: str
    section_titre: str


@app.post("/questions_oui_non", summary="Génère 3 questions oui/non pour un point de checklist")
def questions_oui_non(req: QuestionsOuiNonRequest):
    if not req.item_texte.strip():
        raise HTTPException(status_code=422, detail="Le point de checklist ne peut pas être vide")
    questions = generer_questions_oui_non(req.item_texte, req.clause, req.section_titre)
    return {"questions": questions}


@app.post("/synthetiser", summary="Reformule une observation brute en constat d'audit professionnel")
def synthetiser(req: SyntheseRequest):
    if not req.observation.strip():
        raise HTTPException(status_code=422, detail="L'observation ne peut pas être vide")
    return {"synthese": synthetiser_observation(req.observation)}


@app.get("/sites/{site_id}", summary="Détail d'un site avec historique d'audits")
def get_site(site_id: str):
    conn = get_connection()

    site = conn.execute(
        "SELECT * FROM sites WHERE site_id = ?", (site_id,)
    ).fetchone()
    if site is None:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Site '{site_id}' introuvable")

    audits = conn.execute(
        """SELECT date, auditeur,
                  non_conformites_majeures, non_conformites_mineures,
                  themes_recurrents
           FROM audits_historiques
           WHERE site_id = ?
           ORDER BY date""",
        (site_id,),
    ).fetchall()
    conn.close()

    result = dict(site)
    result["historique_audits"] = [
        {
            "date": a["date"],
            "auditeur": a["auditeur"],
            "non_conformites_majeures": a["non_conformites_majeures"],
            "non_conformites_mineures": a["non_conformites_mineures"],
            "themes_recurrents": json.loads(a["themes_recurrents"] or "[]"),
        }
        for a in audits
    ]
    return result


class ConstatItem(BaseModel):
    observation: str = ""
    criticite: str
    clause: str | None = None
    titre_clause: str | None = None
    constat: str
    action: str | None = None


class RapportRequest(BaseModel):
    site_nom: str
    site_localisation: str
    site_id: str
    referentiel: str
    auditeur: str
    duree: str
    date: str
    constats: list[ConstatItem]


@app.post("/rapport/docx", summary="Génère le rapport d'audit en .docx")
def export_rapport_docx(req: RapportRequest):
    docx_bytes = generer_rapport(req.model_dump())
    filename = f"Rapport_RATP_{req.site_id}_{req.date.replace('/', '-')}.docx"
    return Response(
        content=docx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


class TranscrireManuscritRequest(BaseModel):
    image_base64: str
    item_texte: str = ""


@app.post("/transcrire_manuscrit", summary="Transcrit une image manuscrite en texte d'observation")
def transcrire_manuscrit_route(req: TranscrireManuscritRequest):
    if not req.image_base64.strip():
        raise HTTPException(status_code=422, detail="L'image ne peut pas être vide")
    texte = transcrire_manuscrit(req.image_base64, req.item_texte)
    return {"texte": texte}


class DocumentFournisseurRequest(BaseModel):
    nom: str
    contenu: str


@app.post("/analyser_document_fournisseur", summary="Analyse un document déposé par le fournisseur")
def analyser_doc_fournisseur(req: DocumentFournisseurRequest):
    if not req.contenu.strip():
        raise HTTPException(status_code=422, detail="Le contenu du document ne peut pas être vide")
    result = analyser_document_fournisseur(req.nom, req.contenu)
    return result


# --- Serve React SPA (production build) ---
# Active uniquement si frontend/dist/ existe (après npm run build)
_FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"

if _FRONTEND_DIST.is_dir():
    app.mount("/assets", StaticFiles(directory=str(_FRONTEND_DIST / "assets")), name="assets")

    @app.get("/", include_in_schema=False)
    def serve_index():
        return FileResponse(str(_FRONTEND_DIST / "index.html"))

    @app.get("/{full_path:path}", include_in_schema=False)
    def catch_all(full_path: str):
        # Retourne index.html pour toutes les routes React (SPA routing)
        return FileResponse(str(_FRONTEND_DIST / "index.html"))
