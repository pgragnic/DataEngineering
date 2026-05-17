# UC 28 - Inspection Augmentee - Bootstrap Script
# Cree le projet complet depuis zero sur un PC sans acces GitHub.
#
# UTILISATION :
#   1. Enregistrer ce fichier setup-uc28.ps1 sur le Bureau
#   2. Clic droit > "Executer avec PowerShell"
#      (ou : Set-ExecutionPolicy Bypass -Scope Process ; .\setup-uc28.ps1)
#
# PREREQUIS : Python 3.11+, Node.js 20+, acces internet (pip/npm)

$ErrorActionPreference = "Stop"
$ROOT = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "uc28-inspection"

Write-Host ""
Write-Host "=== UC 28 - Inspection Augmentee ===" -ForegroundColor Cyan
Write-Host "Dossier cible : $ROOT"
Write-Host ""

foreach ($d in @(
    "backend\app\agents",
    "backend\app\rag",
    "backend\app\docx_gen",
    "backend\data\fixtures",
    "backend\corpus\iso9001",
    "backend\storage\files",
    "frontend\src\pages",
    "frontend\components",
    "frontend\lib",
    "frontend\styles",
    "frontend\public"
)) { New-Item -ItemType Directory -Force -Path (Join-Path $ROOT $d) | Out-Null }

Write-Host "[1/4] Repertoires crees"

function wf($rel, $content) {
    $p = Join-Path $ROOT $rel
    [System.IO.File]::WriteAllText($p, $content, [System.Text.UTF8Encoding]::new($false))
}

# Empty __init__.py
wf "backend\app\__init__.py" ""
wf "backend\app\agents\__init__.py" ""
wf "backend\app\rag\__init__.py" ""
wf "backend\app\docx_gen\__init__.py" ""


wf ".env.example" @'
# --- Anthropic ---
# Clé fournie par les organisateurs Capgemini (hackathon)
# ou votre clé personnelle (dev)
ANTHROPIC_API_KEY=sk-ant-VOTRE_CLE_ICI

# URL API — décommenter selon le contexte :
# Dev perso (défaut)
ANTHROPIC_BASE_URL=https://api.anthropic.com
# Hackathon sur PC Capgemini (remplacer la ligne ci-dessus)
# ANTHROPIC_BASE_URL=https://anthropic.generative.engine.capgemini.com

CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS=1
CLAUDE_MODEL=claude-sonnet-4-6

# --- PostgreSQL ---
DATABASE_URL=postgresql+psycopg://uc28:uc28@localhost:5432/uc28

# --- RAG & Storage ---
CHROMA_PERSIST_DIR=./storage/chroma
STORAGE_DIR=./storage/files

'@

wf "start.bat" @'
@echo off
chcp 65001 >nul
title UC 28 — Inspection Augmentée

echo.
echo ╔══════════════════════════════════════════╗
echo ║  UC 28 — Inspection Augmentée           ║
echo ╚══════════════════════════════════════════╝
echo.

set ROOT=%~dp0
set BACKEND_DIR=%ROOT%backend
set FRONTEND_DIR=%ROOT%frontend

:: ── Vérifier .env ────────────────────────────────────────────────────────────
if not exist "%BACKEND_DIR%\.env" (
    echo [!] backend\.env introuvable — copie depuis .env.example...
    copy "%ROOT%.env.example" "%BACKEND_DIR%\.env" >nul
    echo [!] IMPORTANT : editez backend\.env et renseignez ANTHROPIC_API_KEY
    pause
    exit /b 1
)
echo [OK] .env present

:: ── Vérifier Python ──────────────────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo [!] Python introuvable. Installez Python 3.11+ depuis python.org
    pause
    exit /b 1
)
echo [OK] Python detecte

:: ── Installer deps Python ────────────────────────────────────────────────────
if not exist "%BACKEND_DIR%\.venv" (
    echo [>>] Creation du venv Python...
    python -m venv "%BACKEND_DIR%\.venv"
    echo [>>] Installation des dependances Python...
    "%BACKEND_DIR%\.venv\Scripts\pip" install -r "%BACKEND_DIR%\requirements.txt"
    echo [OK] Dependances Python installees
)

:: ── Vérifier Node ────────────────────────────────────────────────────────────
node --version >nul 2>&1
if errorlevel 1 (
    echo [!] Node.js introuvable. Installez Node.js 20+ depuis nodejs.org
    pause
    exit /b 1
)
echo [OK] Node.js detecte

:: ── Installer deps npm ───────────────────────────────────────────────────────
if not exist "%FRONTEND_DIR%\node_modules\.bin\vite" (
    echo [>>] Installation des dependances npm...
    pushd "%FRONTEND_DIR%"
    call npm install
    popd
    echo [OK] npm install termine
)

:: ── Démarrer le backend ──────────────────────────────────────────────────────
echo [>>] Demarrage du backend Flask sur http://localhost:8000
start "UC28-Backend" cmd /k "cd /d %BACKEND_DIR% && .venv\Scripts\python flask_app.py"

:: Attendre que Flask soit prêt
echo [>>] Attente du backend...
timeout /t 4 /nobreak >nul

:: ── Démarrer le frontend ─────────────────────────────────────────────────────
echo [>>] Demarrage du frontend Vite sur http://localhost:3000
start "UC28-Frontend" cmd /k "cd /d %FRONTEND_DIR% && npm run dev"

echo.
echo [OK] Services demarres !
echo.
echo   Backend  : http://localhost:8000/health
echo   Frontend : http://localhost:3000
echo.
echo Fermez les deux fenetres de terminal pour arreter les services.
echo.
pause

'@

wf "backend\requirements.txt" @'
flask==3.0.*
flask-cors==4.0.*
anthropic==0.40.*
python-docx==1.1.*
python-dotenv==1.0.*
pydantic-settings==2.*

'@

wf "backend\app\config.py" @'
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    anthropic_api_key: str
    anthropic_base_url: str = "https://api.anthropic.com"  # remplacer par l'URL Capgemini si hackathon
    database_url: str = "postgresql+psycopg://uc28:uc28@localhost:5432/uc28"
    chroma_persist_dir: str = "./storage/chroma"
    storage_dir: str = "./storage/files"
    claude_model: str = "claude-sonnet-4-6"


settings = Settings()

'@

wf "backend\app\agents\client.py" @'
import json

from anthropic import Anthropic

from app.config import settings

_client = Anthropic(
    api_key=settings.anthropic_api_key,
    base_url=settings.anthropic_base_url,
)


def call_claude_json(
    system: str,
    user_content: list[dict],
    max_tokens: int = 2000,
) -> dict:
    """Call Claude Sonnet 4.6, parse JSON from the response robustly."""
    resp = _client.messages.create(
        model=settings.claude_model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_content}],
    )
    text = resp.content[0].text
    # tolerate ```json fences
    text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(text)

'@

wf "backend\app\agents\preparation.py" @'
"""Agent 1 — Préparation : génère la check-list dynamique d'inspection."""

from __future__ import annotations

import json

from app.agents.client import call_claude_json
from app.rag.retrieve import retrieve_norm_context

SYSTEM_PROMPT = """Tu es un auditeur senior certifié IRCA spécialisé en audits qualité ISO 9001.
Ta mission est de préparer une check-list d'inspection structurée et opérationnelle pour un auditeur qui va sur le terrain.

Contraintes :
- Tu travailles sur le référentiel fourni (ISO 9001:2015).
- Tu adaptes systématiquement la check-list au périmètre demandé (scope).
- Chaque point de la check-list doit être : (1) actionnable sur le terrain, (2) lié à un article précis de la norme, (3) accompagné d'une indication des preuves attendues.
- Tu produis 3 à 6 sections, chaque section contenant 3 à 6 points. Pas plus.
- Tu ne fais pas de blabla introductif. Tu renvoies UNIQUEMENT le JSON demandé.

Format de sortie strict (JSON):
{
  "referential": "...",
  "scope_summary": "...",
  "sections": [
    { "id": "S1", "title": "...", "points": [
        { "id": "S1.P1", "question": "...", "expected_evidence": "...", "norm_reference": "ISO 9001 §..." }
    ]}
  ]
}

Si l'historique des audits passés mentionne des NC non clôturées chez ce client, inclus un point de check-list dédié à leur vérification."""


def generate_checklist(
    client_name: str,
    site_name: str,
    referential: str,
    scope: str,
    previous_audits: list[dict] | None = None,
) -> dict:
    rag_chunks = retrieve_norm_context(scope, k=5)
    rag_text = "\n\n".join(
        f"[{c['section']}] {c['excerpt']}" for c in rag_chunks
    )

    history_text = ""
    if previous_audits:
        history_text = "\n\nHistorique des audits précédents :\n" + json.dumps(
            previous_audits, ensure_ascii=False, indent=2
        )

    user_content = [
        {
            "type": "text",
            "text": (
                f"Client : {client_name}\n"
                f"Site : {site_name}\n"
                f"Référentiel : {referential}\n"
                f"Périmètre (scope) : {scope}"
                f"{history_text}\n\n"
                f"Extraits normatifs pertinents (RAG) :\n{rag_text}"
            ),
        }
    ]

    return call_claude_json(SYSTEM_PROMPT, user_content, max_tokens=3000)


if __name__ == "__main__":
    result = generate_checklist(
        client_name="Fournisseur ALPHA",
        site_name="Site de production — Tours, bâtiment B",
        referential="ISO 9001:2015",
        scope="Audit qualité — Processus achats et contrôle réception",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))

'@

wf "backend\app\agents\capture.py" @'
"""Agent 2 — Capture : classifie et structure un constat brut."""

from __future__ import annotations

import base64
import json

from app.agents.client import call_claude_json
from app.rag.retrieve import retrieve_norm_context

SYSTEM_PROMPT = """Tu es un assistant d'audit. Tu aides un inspecteur sur le terrain à formaliser et classifier ses constats en temps réel.

Ton travail, pour chaque constat brut :
1. Reformuler le constat en français professionnel et factuel, sans interprétation.
2. Le classifier : "conforme" | "observation" | "nc_mineure" | "nc_majeure".
   - conforme : tout va bien.
   - observation : point d'amélioration sans écart à la norme.
   - nc_mineure : écart ponctuel, isolé, qui n'affecte pas le système qualité.
   - nc_majeure : écart systémique, ou qui touche à la sécurité des personnes, ou qui empêche le SMQ de fonctionner.
3. Identifier l'article du référentiel le plus pertinent. Utilise UNIQUEMENT les extraits normatifs fournis dans le contexte ; ne cite jamais un article que tu n'as pas vu.
4. Indiquer la preuve à collecter (photo, document, mesure).
5. Proposer une action corrective concrète et proportionnée.

Tu renvoies UNIQUEMENT le JSON :
{
  "reformulated_text": "...",
  "classification": "conforme|observation|nc_mineure|nc_majeure",
  "norm_reference": "ISO 9001 §...",
  "norm_excerpt": "...",
  "suggested_evidence": "...",
  "suggested_action": "..."
}

Si tu ne peux pas sourcer à un article précis avec les extraits fournis, mets norm_reference: null et explique-le brièvement dans suggested_action."""


def classify_constat(
    raw_text: str,
    referential: str = "ISO 9001:2015",
    scope: str = "",
    checklist_point: dict | None = None,
    photo_bytes: bytes | None = None,
    photo_media_type: str = "image/jpeg",
) -> tuple[dict, list[dict]]:
    """Returns (structured_constat, rag_chunks)."""
    query = raw_text + (" " + scope if scope else "")
    rag_chunks = retrieve_norm_context(query, k=3)
    rag_text = "\n\n".join(
        f"[{c['section']}] {c['excerpt']}" for c in rag_chunks
    )

    context_parts = [
        f"Référentiel : {referential}",
        f"Périmètre : {scope}" if scope else "",
    ]
    if checklist_point:
        context_parts.append(
            f"Point de check-list en cours : {checklist_point.get('question', '')} "
            f"({checklist_point.get('norm_reference', '')})"
        )

    context_text = "\n".join(p for p in context_parts if p)

    user_content: list[dict] = [
        {
            "type": "text",
            "text": (
                f"{context_text}\n\n"
                f"Constat brut de l'inspecteur :\n{raw_text}\n\n"
                f"Extraits normatifs pertinents (RAG) :\n{rag_text}"
            ),
        }
    ]

    if photo_bytes:
        user_content.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": photo_media_type,
                    "data": base64.b64encode(photo_bytes).decode(),
                },
            }
        )

    result = call_claude_json(SYSTEM_PROMPT, user_content, max_tokens=1500)
    return result, rag_chunks


if __name__ == "__main__":
    result, chunks = classify_constat(
        raw_text="la sortie de secours du bâtiment B est obstruée par un chariot de stockage",
        scope="Processus achats et contrôle réception",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("\nRAG chunks:", [c["section"] for c in chunks])

'@

wf "backend\app\agents\restitution.py" @'
"""Agent 3 — Restitution : génère la structure du pré-rapport."""

from __future__ import annotations

import json

from app.agents.client import call_claude_json

SYSTEM_PROMPT = """Tu es un auditeur senior qui rédige le pré-rapport d'un audit ISO 9001 à destination du client audité.

Style :
- Français professionnel, factuel, sans jargon inutile.
- Phrases courtes. Pas d'opinion. Que des constats sourcés.

Tu reçois en entrée le contexte complet de l'audit : métadonnées, check-list utilisée, tous les constats avec leur classification.

Tu produis UNIQUEMENT le JSON suivant :
{
  "executive_summary": "2-3 phrases factuelles résumant l'audit, le nombre de NC par niveau, et l'appréciation globale.",
  "conformity_summary": { "conforme": N, "observation": N, "nc_mineure": N, "nc_majeure": N },
  "sections": [ { "title": "...", "findings": [ ... ] } ],
  "action_plan": [
    { "priority": 1|2|3, "finding_ref": "Constat #X", "action": "...", "responsible": "...", "deadline": "..." }
  ],
  "next_audit_recommendation": "..."
}

Règles :
- Regroupe les constats par thème (section de la check-list).
- Dans le plan d'action, priorise : priorité 1 = NC majeure, 2 = NC mineure, 3 = observation.
- Les responsables et délais sont des suggestions par défaut (à confirmer par le client).
- Recommande systématiquement un audit de suivi s'il y a au moins une NC majeure."""


def generate_report_structure(inspection: dict, constats: list[dict]) -> dict:
    """Takes inspection metadata dict and list of constat dicts."""
    payload = {
        "inspection": {
            "client_name": inspection["client_name"],
            "site_name": inspection["site_name"],
            "auditor_name": inspection["auditor_name"],
            "referential": inspection["referential"],
            "scope": inspection["scope"],
            "started_at": str(inspection.get("started_at", "")),
            "checklist": inspection.get("checklist_json"),
        },
        "constats": [
            {
                "id": str(c.get("id", i + 1)),
                "classification": c["classification"],
                "reformulated_text": c["reformulated_text"],
                "norm_reference": c.get("norm_reference"),
                "suggested_action": c.get("suggested_action"),
                "checklist_point_id": c.get("checklist_point_id"),
            }
            for i, c in enumerate(constats)
        ],
    }

    user_content = [
        {
            "type": "text",
            "text": json.dumps(payload, ensure_ascii=False, indent=2),
        }
    ]

    return call_claude_json(SYSTEM_PROMPT, user_content, max_tokens=3000)

'@

wf "backend\app\rag\retrieve.py" @'
"""Retrieve normative context — fallback to keyword search if ChromaDB unavailable."""

from __future__ import annotations

from pathlib import Path

CORPUS_DIR = Path(__file__).parent.parent.parent / "corpus"

# Try ChromaDB + sentence-transformers; fall back to simple keyword search
_USE_CHROMA = False
try:
    import chromadb
    from sentence_transformers import SentenceTransformer
    _model = SentenceTransformer("all-MiniLM-L6-v2")
    _USE_CHROMA = True
except Exception:
    pass

_chroma_collection = None


def _get_collection():
    global _chroma_collection
    if _chroma_collection is None:
        from app.config import settings
        client = chromadb.PersistentClient(path=settings.chroma_persist_dir)
        _chroma_collection = client.get_or_create_collection("iso_norms")
    return _chroma_collection


def _keyword_search(query: str, k: int = 3) -> list[dict]:
    """Simple keyword search across corpus markdown files."""
    query_words = set(query.lower().split())
    scored: list[tuple[float, str, str]] = []

    for md_file in sorted(CORPUS_DIR.rglob("*.md")):
        text = md_file.read_text(encoding="utf-8")
        text_lower = text.lower()
        hits = sum(1 for w in query_words if w in text_lower and len(w) > 3)
        if hits > 0:
            section = md_file.stem.split("_")[0].lstrip("0")
            scored.append((hits, section, text[:600]))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [
        {"section": s, "excerpt": excerpt, "score": round(hits / max(len(query_words), 1), 2)}
        for hits, s, excerpt in scored[:k]
    ]


def retrieve_norm_context(query: str, k: int = 3) -> list[dict]:
    """Return top-k normative chunks. Uses ChromaDB if available, keyword search otherwise."""
    if _USE_CHROMA:
        try:
            collection = _get_collection()
            if collection.count() == 0:
                return _keyword_search(query, k)
            embedding = _model.encode(query).tolist()
            results = collection.query(
                query_embeddings=[embedding], n_results=min(k, collection.count())
            )
            return [
                {
                    "section": meta.get("section", ""),
                    "excerpt": doc,
                    "score": round(1 - dist, 4) if dist <= 1 else round(1 / (1 + dist), 4),
                }
                for doc, meta, dist in zip(
                    results["documents"][0],
                    results["metadatas"][0],
                    results["distances"][0],
                )
            ]
        except Exception:
            pass

    return _keyword_search(query, k)

'@

wf "backend\app\docx_gen\report.py" @'
"""Generate the pre-report DOCX from the Agent 3 report structure."""

from __future__ import annotations

import io
from datetime import datetime

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Pt, RGBColor


_NC_COLORS = {
    "nc_majeure": RGBColor(0xDC, 0x26, 0x26),
    "nc_mineure": RGBColor(0xF5, 0x9E, 0x0B),
    "observation": RGBColor(0x2C, 0x7A, 0x7B),
    "conforme": RGBColor(0x10, 0xB9, 0x81),
}

_NC_LABELS = {
    "nc_majeure": "NC MAJEURE",
    "nc_mineure": "NC MINEURE",
    "observation": "OBSERVATION",
    "conforme": "CONFORME",
}


def _add_heading(doc: Document, text: str, level: int = 1) -> None:
    doc.add_heading(text, level=level)


def _add_kv(doc: Document, key: str, value: str) -> None:
    p = doc.add_paragraph()
    p.add_run(f"{key} : ").bold = True
    p.add_run(value)


def generate_docx(
    inspection: dict,
    report_structure: dict,
) -> bytes:
    """Return the DOCX file as bytes."""
    doc = Document()

    # --- Page de garde ---
    title = doc.add_heading("Pré-rapport d'audit qualité", 0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER

    doc.add_paragraph()
    _add_kv(doc, "Client", inspection.get("client_name", ""))
    _add_kv(doc, "Site", inspection.get("site_name", ""))
    _add_kv(doc, "Auditeur", inspection.get("auditor_name", ""))
    _add_kv(doc, "Référentiel", inspection.get("referential", "ISO 9001:2015"))
    _add_kv(doc, "Date", datetime.now().strftime("%d/%m/%Y"))
    doc.add_page_break()

    # --- Synthèse exécutive ---
    _add_heading(doc, "Synthèse exécutive")
    doc.add_paragraph(report_structure.get("executive_summary", ""))

    # --- Tableau de conformité ---
    _add_heading(doc, "Résumé de conformité", level=2)
    summary = report_structure.get("conformity_summary", {})
    table = doc.add_table(rows=1, cols=4)
    table.style = "Table Grid"
    hdr = table.rows[0].cells
    for i, label in enumerate(["Conforme", "Observation", "NC Mineure", "NC Majeure"]):
        hdr[i].text = label

    row = table.add_row().cells
    row[0].text = str(summary.get("conforme", 0))
    row[1].text = str(summary.get("observation", 0))
    row[2].text = str(summary.get("nc_mineure", 0))
    row[3].text = str(summary.get("nc_majeure", 0))
    doc.add_paragraph()

    # --- Constats par section ---
    _add_heading(doc, "Constats détaillés")
    for section in report_structure.get("sections", []):
        _add_heading(doc, section.get("title", ""), level=2)
        for finding in section.get("findings", []):
            classification = finding.get("classification", "")
            label = _NC_LABELS.get(classification, classification.upper())
            p = doc.add_paragraph()
            run = p.add_run(f"[{label}] ")
            run.bold = True
            color = _NC_COLORS.get(classification)
            if color:
                run.font.color.rgb = color
            if finding.get("norm_reference"):
                ref_run = p.add_run(f"{finding['norm_reference']} — ")
                ref_run.font.size = Pt(9)
            p.add_run(finding.get("reformulated_text", ""))
            if finding.get("suggested_action"):
                action_p = doc.add_paragraph(style="List Bullet")
                action_p.add_run("Action : ").italic = True
                action_p.add_run(finding["suggested_action"])
        doc.add_paragraph()

    # --- Plan d'action ---
    _add_heading(doc, "Plan d'action")
    actions = report_structure.get("action_plan", [])
    if actions:
        action_table = doc.add_table(rows=1, cols=5)
        action_table.style = "Table Grid"
        headers = action_table.rows[0].cells
        for i, h in enumerate(["Priorité", "Réf. constat", "Action", "Responsable", "Délai"]):
            headers[i].text = h
        for action in actions:
            row = action_table.add_row().cells
            row[0].text = f"P{action.get('priority', '')}"
            row[1].text = action.get("finding_ref", "")
            row[2].text = action.get("action", "")
            row[3].text = action.get("responsible", "")
            row[4].text = action.get("deadline", "")
    doc.add_paragraph()

    # --- Recommandation ---
    if report_structure.get("next_audit_recommendation"):
        _add_heading(doc, "Recommandation")
        doc.add_paragraph(report_structure["next_audit_recommendation"])

    # --- Footer (pied de page) ---
    section_obj = doc.sections[0]
    footer = section_obj.footer
    footer_para = footer.paragraphs[0]
    footer_para.text = (
        f"UC 28 — Inspection Augmentée · "
        f"{inspection.get('auditor_name', '')} · "
        f"{datetime.now().strftime('%d/%m/%Y')}"
    )
    footer_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()

'@

wf "backend\flask_app.py" @'
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

_BASE = Path(__file__).parent
DB_PATH = _BASE / "data" / "db.json"
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", str(_BASE / "storage" / "files")))
FIXTURES_PATH = _BASE / "data" / "fixtures" / "alpha.json"

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
    return jsonify({
        "audits_today_count": 4,
        "audits_month_count": 18,
        "avg_delay_days": 2,
        "pending_recurrences_count": 3,
    })

@app.get("/api/dashboard/audits_today")
def dashboard_audits_today():
    db = load_db()
    today = datetime.now().strftime("%Y-%m-%d")
    # Real inspection ID from DB (first found), fallback to fixture ID
    alpha_id = next(iter(db["inspections"]), "ins_alpha_20260512")
    alpha_status = db["inspections"].get(alpha_id, {}).get("status", "prepared")

    return jsonify([
        {
            "id": "demo_bureau_0800",
            "scheduled_at": f"{today}T08:00:00+02:00",
            "client_name": "Vérif. matinale · planning",
            "location": "Bureau Lyon",
            "scope": "Préparation des audits du jour · revue des briefs",
            "status": "completed",
            "is_next": False,
        },
        {
            "id": "demo_omega_1000",
            "scheduled_at": f"{today}T10:00:00+02:00",
            "client_name": "Fournisseur OMEGA Industries",
            "location": "Site Vénissieux",
            "scope": "Audit ISO 9001 · Processus production",
            "status": "completed",
            "is_next": False,
        },
        {
            "id": alpha_id,
            "scheduled_at": f"{today}T14:30:00+02:00",
            "client_name": "Fournisseur ALPHA",
            "location": "Tours · Bâtiment B",
            "scope": "Audit ISO 9001 · Processus achats et contrôle réception",
            "status": alpha_status,
            "is_next": alpha_status != "completed",
        },
        {
            "id": "demo_delta_1700",
            "scheduled_at": f"{today}T17:00:00+02:00",
            "client_name": "Sous-traitant DELTA Logistics",
            "location": "Visioconférence",
            "scope": "Audit ISO 9001 · Suivi NC mineures 2025",
            "status": "prepared",
            "is_next": False,
        },
    ])

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

'@

wf "backend\data\fixtures\alpha.json" @'
{
  "_meta": {
    "name": "Fixture scénario démo — Fournisseur ALPHA",
    "purpose": "Seed la DB en sprint 1 ET alimente le replay mode du sprint 4",
    "usage": [
      "backend/data/fixtures/alpha.json (référence à la racine de la DB)",
      "À charger via POST /api/dev/reset-demo (route admin S4)",
      "À injecter via raccourcis Cmd+Shift+1..4 pour le replay mode"
    ],
    "version": "1.0",
    "last_updated": "2026-05-12"
  },

  "inspection": {
    "id": "ins_alpha_20260512",
    "client_name": "Fournisseur ALPHA",
    "client_siret": "12345678900012",
    "site_name": "Site de production — Tours, bâtiment B",
    "site_address": "12 rue de l'Industrie, 37000 Tours",
    "auditor_name": "Antoine Mercier",
    "auditor_id": "BV-FR-AM-042",
    "referential": "ISO 9001:2015",
    "scope": "Audit qualité fournisseur — Processus achats et contrôle réception. Vérification de la conformité au système de management de la qualité ISO 9001, focus sur la maîtrise documentaire (§7.5), l'environnement de production (§7.1.4) et les processus de réalisation externalisés (§8.4).",
    "status": "ongoing",
    "started_at": "2026-05-12T14:32:00+02:00",
    "created_at": "2026-05-12T08:00:00+02:00",
    "updated_at": "2026-05-12T14:32:00+02:00",
    "previous_audits": [
      {
        "id": "ins_alpha_20250615",
        "date": "2025-06-15",
        "result": "1 NC mineure non clôturée — procédure de contrôle réception à formaliser",
        "follow_up_needed": true
      }
    ]
  },

  "checklist": {
    "generated_by": "agent_preparation",
    "generated_at": "2026-05-12T08:15:00+02:00",
    "referential": "ISO 9001:2015",
    "scope_summary": "Audit du processus achats et du contrôle réception du fournisseur ALPHA. Focus sur la maîtrise documentaire, l'environnement de production et la gestion des prestataires externes. Vérification de la clôture de la NC mineure ouverte lors de l'audit 2025.",
    "sections": [

      {
        "id": "S1",
        "title": "Maîtrise des informations documentées (§7.5)",
        "points": [
          {
            "id": "S1.P1",
            "question": "La procédure achats est-elle documentée, accessible aux opérationnels et à jour ?",
            "expected_evidence": "Procédure achats version courante, date de dernière revue, accusé de réception par les acheteurs",
            "norm_reference": "ISO 9001 §7.5.3"
          },
          {
            "id": "S1.P2",
            "question": "Les enregistrements de contrôle réception sont-ils tracés et conservés ?",
            "expected_evidence": "Fichier de contrôles des 3 derniers mois, durée de conservation définie",
            "norm_reference": "ISO 9001 §7.5.3.2"
          },
          {
            "id": "S1.P3",
            "question": "Les revues de direction sont-elles formalisées et planifiées ?",
            "expected_evidence": "Compte-rendu de la dernière revue, planning des revues à venir",
            "norm_reference": "ISO 9001 §9.3"
          }
        ]
      },

      {
        "id": "S2",
        "title": "Processus externalisés et achats (§8.4)",
        "points": [
          {
            "id": "S2.P1",
            "question": "Les fournisseurs critiques sont-ils évalués et suivis ?",
            "expected_evidence": "Liste des fournisseurs critiques, grille d'évaluation, fréquence de revue",
            "norm_reference": "ISO 9001 §8.4.1"
          },
          {
            "id": "S2.P2",
            "question": "Les critères de contrôle à réception sont-ils définis par référence ?",
            "expected_evidence": "Document de spécification des contrôles par famille de produits",
            "norm_reference": "ISO 9001 §8.4.3"
          },
          {
            "id": "S2.P3",
            "question": "Les non-conformités fournisseurs sont-elles tracées et traitées ?",
            "expected_evidence": "Registre des NC fournisseurs des 6 derniers mois, actions correctives associées",
            "norm_reference": "ISO 9001 §10.2"
          }
        ]
      },

      {
        "id": "S3",
        "title": "Compétences et ressources humaines (§7.2)",
        "points": [
          {
            "id": "S3.P1",
            "question": "Les compétences requises pour les fonctions critiques sont-elles définies ?",
            "expected_evidence": "Fiches de poste, matrice de compétences, plans de formation",
            "norm_reference": "ISO 9001 §7.2"
          },
          {
            "id": "S3.P2",
            "question": "Les formations sont-elles suivies et leur efficacité évaluée ?",
            "expected_evidence": "Plan de formation annuel, attestations, évaluations à froid",
            "norm_reference": "ISO 9001 §7.2 c)"
          }
        ]
      },

      {
        "id": "S4",
        "title": "Environnement de production et sécurité (§7.1.4)",
        "points": [
          {
            "id": "S4.P1",
            "question": "L'environnement de travail est-il maîtrisé (température, propreté, sécurité) ?",
            "expected_evidence": "Visite des ateliers, relevés environnementaux récents, plans d'évacuation à jour",
            "norm_reference": "ISO 9001 §7.1.4"
          },
          {
            "id": "S4.P2",
            "question": "Les voies d'évacuation et issues de secours sont-elles dégagées en permanence ?",
            "expected_evidence": "Visite visuelle des issues, registre de sécurité",
            "norm_reference": "ISO 9001 §7.1.4 + Code du travail R4227-4"
          }
        ]
      }
    ]
  },

  "constats": [

    {
      "id": "constat_001",
      "inspection_id": "ins_alpha_20260512",
      "checklist_point_id": "S1.P1",
      "sequence": 1,
      "captured_at": "2026-05-12T14:32:18+02:00",
      "raw_text": "la procédure achats existe version 3 datée de mars 2026 elle est accessible sur l'intranet",
      "reformulated_text": "La procédure achats version 3, datée de mars 2026, est documentée et accessible aux opérationnels via l'intranet.",
      "classification": "conforme",
      "norm_reference": "ISO 9001 §7.5.3",
      "norm_excerpt": "Pour la maîtrise des informations documentées, l'organisme doit traiter, le cas échéant, les activités suivantes : distribution, accès, recherche et utilisation ; stockage et préservation, y compris la préservation de la lisibilité ; maîtrise des modifications.",
      "suggested_evidence": "Capture d'écran de la procédure sur l'intranet ; accusé de lecture des acheteurs depuis la dernière mise à jour",
      "suggested_action": "Pratique à pérenniser. Recommandation : revue documentaire annuelle.",
      "photo_path": null,
      "audio_path": "audio/constat_001.webm",
      "rag_chunks": [
        {
          "section": "§7.5.3",
          "title": "Maîtrise des informations documentées",
          "excerpt": "Pour la maîtrise des informations documentées, l'organisme doit traiter, le cas échéant : distribution, accès, recherche et utilisation ; stockage et préservation, y compris la préservation de la lisibilité ; maîtrise des modifications…",
          "similarity": 0.94
        },
        {
          "section": "§7.5.2",
          "title": "Création et mise à jour",
          "excerpt": "Lors de la création et de la mise à jour des informations documentées, l'organisme doit veiller à : l'identification et la description (titre, date, auteur, numéro de référence)…",
          "similarity": 0.81
        },
        {
          "section": "§4.4",
          "title": "Système de management de la qualité et ses processus",
          "excerpt": "L'organisme doit conserver les informations documentées nécessaires au fonctionnement de ses processus…",
          "similarity": 0.65
        }
      ]
    },

    {
      "id": "constat_002",
      "inspection_id": "ins_alpha_20260512",
      "checklist_point_id": "S4.P2",
      "sequence": 2,
      "captured_at": "2026-05-12T14:33:42+02:00",
      "raw_text": "la sortie de secours du bâtiment B est obstruée par un chariot de stockage rempli de cartons",
      "reformulated_text": "La sortie de secours du bâtiment B est obstruée par un chariot de stockage chargé de cartons, empêchant l'évacuation rapide en cas d'urgence.",
      "classification": "nc_majeure",
      "norm_reference": "ISO 9001 §7.1.4",
      "norm_excerpt": "L'organisme doit déterminer, fournir et maintenir l'environnement nécessaire au fonctionnement de ses processus et à l'obtention de la conformité des produits et services. Un environnement adapté peut être une combinaison d'aspects humains et physiques, tels que la sécurité (par exemple, la prévention du stress, la prévention des accidents)…",
      "suggested_evidence": "Photo de la sortie obstruée avec le chariot et son chargement ; relevé de l'étiquette d'identification du chariot ; numéro de bâtiment et de l'issue",
      "suggested_action": "Action immédiate : libérer la sortie de secours et reculer le chariot. Action à 7 jours : rappel formel aux magasiniers des consignes de sécurité. Action à 30 jours : ajout au plan d'audit sécurité annuel et marquage au sol des zones de stockage interdites.",
      "photo_path": "photos/constat_002_sortie_secours.jpg",
      "audio_path": "audio/constat_002.webm",
      "rag_chunks": [
        {
          "section": "§7.1.4",
          "title": "Environnement pour la mise en œuvre des processus",
          "excerpt": "L'organisme doit déterminer, fournir et maintenir l'environnement nécessaire au fonctionnement de ses processus et à l'obtention de la conformité des produits et services…",
          "similarity": 0.93
        },
        {
          "section": "§7.1.3",
          "title": "Infrastructure",
          "excerpt": "L'organisme doit déterminer, fournir et maintenir l'infrastructure nécessaire au fonctionnement de ses processus, incluant locaux et services associés…",
          "similarity": 0.78
        },
        {
          "section": "§8.5.1",
          "title": "Maîtrise de la production et de la prestation de service",
          "excerpt": "L'organisme doit mettre en œuvre la production et la prestation de service dans des conditions maîtrisées, incluant la disponibilité d'informations documentées…",
          "similarity": 0.62
        }
      ]
    },

    {
      "id": "constat_003",
      "inspection_id": "ins_alpha_20260512",
      "checklist_point_id": "S2.P2",
      "sequence": 3,
      "captured_at": "2026-05-12T14:35:11+02:00",
      "raw_text": "pas de procédure documentée de contrôle réception mais le magasinier a un cahier manuscrit où il note les écarts",
      "reformulated_text": "Aucune procédure formelle de contrôle à réception n'est documentée ; le suivi est effectué sur un cahier manuscrit tenu par le magasinier, qui n'est ni standardisé ni intégré au système qualité.",
      "classification": "nc_mineure",
      "norm_reference": "ISO 9001 §8.4.3",
      "norm_excerpt": "L'organisme doit s'assurer que les processus, produits et services fournis par des prestataires externes ne compromettent pas l'aptitude de l'organisme à toujours fournir à ses clients des produits et services conformes…",
      "suggested_evidence": "Photo du cahier manuscrit (entrées des 7 derniers jours) ; entretien avec le magasinier ; vérification de l'absence de procédure dans la GED",
      "suggested_action": "Action à 60 jours : formaliser la procédure de contrôle réception en s'appuyant sur la pratique existante du magasinier. Définir critères, fréquences, format d'enregistrement. Intégrer au système qualité (GED). NB : cette NC était déjà mentionnée lors de l'audit 2025-06-15 et n'a pas été clôturée.",
      "photo_path": "photos/constat_003_cahier.jpg",
      "audio_path": "audio/constat_003.webm",
      "rag_chunks": [
        {
          "section": "§8.4.3",
          "title": "Informations à l'attention des prestataires externes",
          "excerpt": "L'organisme doit communiquer aux prestataires externes ses exigences concernant les processus, produits et services à fournir ; l'approbation des produits et services ; les méthodes, processus et équipements ; la libération des produits et services…",
          "similarity": 0.88
        },
        {
          "section": "§8.4.2",
          "title": "Type et étendue de la maîtrise",
          "excerpt": "L'organisme doit définir la maîtrise à appliquer aux processus, produits et services fournis par des prestataires externes…",
          "similarity": 0.82
        },
        {
          "section": "§7.5.1",
          "title": "Généralités sur les informations documentées",
          "excerpt": "Le système de management de la qualité de l'organisme doit inclure les informations documentées exigées par la présente Norme internationale…",
          "similarity": 0.71
        }
      ]
    },

    {
      "id": "constat_004",
      "inspection_id": "ins_alpha_20260512",
      "checklist_point_id": "S3.P1",
      "sequence": 4,
      "captured_at": "2026-05-12T14:36:48+02:00",
      "raw_text": "le magasinier semble très bien formé sur le geste et les contrôles malgré l'absence de procédure écrite c'est impressionnant",
      "reformulated_text": "Le magasinier démontre une maîtrise opérationnelle du contrôle à réception malgré l'absence de procédure documentée. La compétence est portée par l'expérience individuelle et n'est pas formellement transmissible.",
      "classification": "observation",
      "norm_reference": null,
      "norm_excerpt": null,
      "suggested_evidence": "Entretien avec le magasinier ; observation du geste sur 2-3 contrôles consécutifs ; absence de doublon ou de procédure de relais en cas d'absence",
      "suggested_action": "Pérenniser la pratique en la formalisant. Recommandation : transcrire le savoir-faire du magasinier en procédure (cf. constat #3), former un second opérateur en relais. Risque actuel : perte du savoir en cas de départ ou d'absence.",
      "photo_path": null,
      "audio_path": "audio/constat_004.webm",
      "rag_chunks": [
        {
          "section": "§7.2",
          "title": "Compétences",
          "excerpt": "L'organisme doit déterminer les compétences nécessaires de la ou des personne(s) effectuant, sous son contrôle, un travail qui a une incidence sur les performances et l'efficacité du système de management de la qualité…",
          "similarity": 0.74
        },
        {
          "section": "§7.1.6",
          "title": "Connaissances organisationnelles",
          "excerpt": "L'organisme doit déterminer les connaissances nécessaires au fonctionnement de ses processus et à l'obtention de la conformité…",
          "similarity": 0.69
        },
        {
          "section": "§7.5.1",
          "title": "Informations documentées",
          "excerpt": "Le système de management de la qualité de l'organisme doit inclure les informations documentées jugées nécessaires par l'organisme pour l'efficacité du système…",
          "similarity": 0.58
        }
      ]
    }

  ],

  "expected_report_structure": {
    "_purpose": "Sortie attendue de l'Agent 3 (Restitution) quand on clique sur 'Générer le pré-rapport'",
    "executive_summary": "L'audit qualité du fournisseur ALPHA, mené le 12 mai 2026 sur le site de production de Tours, a relevé 1 non-conformité majeure, 1 non-conformité mineure (récurrente depuis l'audit 2025), 1 observation et 1 point conforme sur le périmètre achats, contrôle réception et environnement de production. La NC majeure concerne la sécurité physique des locaux et requiert une action immédiate. La NC mineure récurrente impose une formalisation documentaire sous 60 jours. Un audit de suivi est recommandé sous 3 mois.",
    "conformity_summary": {
      "conforme": 1,
      "observation": 1,
      "nc_mineure": 1,
      "nc_majeure": 1
    },
    "sections": [
      {
        "title": "Maîtrise des informations documentées (§7.5)",
        "findings": ["constat_001"]
      },
      {
        "title": "Processus externalisés et achats (§8.4)",
        "findings": ["constat_003"]
      },
      {
        "title": "Compétences (§7.2)",
        "findings": ["constat_004"]
      },
      {
        "title": "Environnement et sécurité (§7.1.4)",
        "findings": ["constat_002"]
      }
    ],
    "action_plan": [
      {
        "priority": 1,
        "finding_ref": "constat_002",
        "action": "Libérer la sortie de secours du bâtiment B",
        "responsible": "Responsable site / Magasinier",
        "deadline": "Sous 24h"
      },
      {
        "priority": 1,
        "finding_ref": "constat_002",
        "action": "Rappel formel aux magasiniers des consignes de sécurité incendie",
        "responsible": "Responsable HSE",
        "deadline": "Sous 7 jours"
      },
      {
        "priority": 2,
        "finding_ref": "constat_003",
        "action": "Formaliser la procédure de contrôle réception (clôture de la NC 2025)",
        "responsible": "Responsable qualité",
        "deadline": "Sous 60 jours"
      },
      {
        "priority": 3,
        "finding_ref": "constat_004",
        "action": "Former un second opérateur en relais sur le contrôle réception",
        "responsible": "Responsable production",
        "deadline": "Sous 90 jours"
      }
    ],
    "next_audit_recommendation": "Audit de suivi recommandé sous 3 mois pour vérifier la clôture des NC majeure et mineure. Vérification systématique des sorties de secours et de la procédure de contrôle réception formalisée."
  }
}

'@

wf "backend\corpus\iso9001\04_contexte_organisme.md" @'
# ISO 9001 §4 — Contexte de l'organisme

**Référence** : ISO 9001:2015, clauses 4.1 à 4.4
**Mots-clés** : contexte, parties intéressées, domaine d'application, SMQ, processus

## §4.1 — Compréhension de l'organisme et de son contexte

L'organisme doit déterminer les enjeux externes et internes pertinents pour sa mission et qui influent sur son aptitude à obtenir le(s) résultat(s) attendu(s) de son SMQ.

**Exemples de NC observées**
- Absence d'analyse des enjeux internes/externes documentée
- Revue non actualisée suite à changement de périmètre

## §4.2 — Compréhension des besoins et attentes des parties intéressées

L'organisme doit déterminer les parties intéressées pertinentes et leurs exigences.

**Exemples de NC observées**
- Liste des parties intéressées inexistante ou non mise à jour
- Exigences réglementaires non identifiées

## §4.3 — Détermination du domaine d'application

L'organisme doit déterminer les limites et l'applicabilité du SMQ.

## §4.4 — Système de management de la qualité et ses processus

L'organisme doit établir, mettre en œuvre, tenir à jour et améliorer continuellement le SMQ, y compris les processus nécessaires et leurs interactions.

**Exemples de NC observées**
- Cartographie des processus absente
- Interactions entre processus non documentées
- Responsables de processus non désignés

'@

wf "backend\corpus\iso9001\05_leadership.md" @'
# ISO 9001 §5 — Leadership

**Référence** : ISO 9001:2015, clauses 5.1 à 5.3
**Mots-clés** : direction, engagement, politique qualité, rôles, responsabilités

## §5.1 — Leadership et engagement

La direction doit démontrer son leadership et son engagement à l'égard du SMQ en assumant la responsabilité de l'efficacité du SMQ, en veillant à ce que la politique et les objectifs qualité soient établis.

**Exemples de NC observées**
- Revues de direction non tenues ou non formalisées
- Absence de compte rendu de revue de direction

## §5.2 — Politique qualité

La direction doit établir, mettre en œuvre et maintenir une politique qualité appropriée au contexte de l'organisme, fournissant un cadre pour les objectifs qualité.

**Exemples de NC observées**
- Politique qualité non communiquée au personnel
- Politique non affichée ni accessible
- Politique non révisée depuis plus de 3 ans

## §5.3 — Rôles, responsabilités et autorités au sein de l'organisme

La direction doit s'assurer que les responsabilités et autorités correspondant aux rôles pertinents sont attribuées, communiquées et comprises.

**Exemples de NC observées**
- Organigramme non à jour
- Fiches de poste absentes ou non signées
- Pas de suppléant désigné pour les rôles critiques

'@

wf "backend\corpus\iso9001\06_planification.md" @'
# ISO 9001 §6 — Planification

**Référence** : ISO 9001:2015, clauses 6.1 à 6.3
**Mots-clés** : risques, opportunités, objectifs qualité, planification des modifications

## §6.1 — Actions à mettre en œuvre face aux risques et opportunités

L'organisme doit planifier des actions pour faire face aux risques et opportunités afin d'assurer que le SMQ peut atteindre ses résultats attendus.

**Exemples de NC observées**
- Registre des risques absent ou non mis à jour
- Actions préventives non planifiées ni suivies
- Opportunités identifiées mais non exploitées

## §6.2 — Objectifs qualité et planification des actions pour les atteindre

Les objectifs qualité doivent être cohérents avec la politique qualité, mesurables, surveillés, communiqués et mis à jour si nécessaire.

**Exemples de NC observées**
- Objectifs non chiffrés ou non mesurables
- Aucune revue des objectifs qualité dans l'année
- Objectifs non déclinés à chaque niveau opérationnel

## §6.3 — Planification des modifications

Les modifications du SMQ doivent être réalisées de manière planifiée.

**Exemples de NC observées**
- Modification de processus sans analyse d'impact préalable
- Pas de plan de transition documenté

'@

wf "backend\corpus\iso9001\07_support.md" @'
# ISO 9001 §7 — Support

**Référence** : ISO 9001:2015, clauses 7.1 à 7.5
**Mots-clés** : ressources, compétences, sensibilisation, communication, informations documentées

## §7.1.1 — Généralités sur les ressources

L'organisme doit déterminer et fournir les ressources nécessaires à l'établissement, la mise en œuvre, la tenue à jour et l'amélioration continue du SMQ.

## §7.1.2 — Ressources humaines

L'organisme doit déterminer et fournir les personnes nécessaires à la mise en œuvre efficace du SMQ et à la maîtrise et au fonctionnement de ses processus.

## §7.1.3 — Infrastructure

L'organisme doit déterminer, fournir et maintenir l'infrastructure nécessaire au fonctionnement de ses processus et à l'obtention de la conformité des produits et services. L'infrastructure peut comprendre les bâtiments, équipements, transports, technologies de l'information et de la communication.

**Exemples de NC observées**
- Plan de maintenance préventive absent
- Équipements critiques sans gamme de maintenance
- Infrastructure informatique sans plan de sauvegarde

## §7.1.4 — Environnement pour la mise en œuvre des processus

L'organisme doit déterminer, fournir et maintenir l'environnement nécessaire au fonctionnement de ses processus et à l'obtention de la conformité des produits et services. L'environnement peut comprendre des facteurs humains et physiques tels que le social (calme, non conflictuel), psychologique (réducteur de stress, préventif en matière d'épuisement), et physique (température, chaleur, humidité, lumière, circulation d'air, hygiène, bruit).

**Exemples de NC observées**
- Sortie de secours obstruée par du matériel ou des chariots de stockage
- Locaux mal aérés, températures inadaptées pour le personnel ou les produits
- Bruit excessif sans protection auditive fournie
- Postes de travail non ergonomiques, éclairage insuffisant
- Allées de circulation encombrées, risque de chute
- Conditions de stockage non conformes (température, humidité) pour les produits sensibles

## §7.1.5 — Ressources pour la surveillance et la mesure

L'organisme doit déterminer et fournir les ressources nécessaires à la surveillance et à la mesure (étalonnage des équipements).

**Exemples de NC observées**
- Équipements de mesure non étalonnés
- Certificats d'étalonnage expirés
- Pas de traçabilité des étalonnages

## §7.1.6 — Connaissances organisationnelles

L'organisme doit déterminer les connaissances nécessaires au fonctionnement de ses processus et à l'obtention de la conformité des produits et services.

## §7.2 — Compétences

L'organisme doit déterminer les compétences nécessaires des personnes accomplissant, sous son autorité, un travail qui a une incidence sur les performances et l'efficacité du SMQ.

**Exemples de NC observées**
- Pas de plan de formation formalisé
- Habilitations non vérifiées (électrique, conduite d'engins, etc.)
- Compétences requises non définies pour les postes clés
- Absence d'évaluation des formations (efficacité non vérifiée)

## §7.3 — Sensibilisation

Le personnel doit être sensibilisé à la politique qualité, aux objectifs qualité, à sa contribution à l'efficacité du SMQ et aux répercussions d'un non-respect des exigences du SMQ.

**Exemples de NC observées**
- Personnel non informé de la politique qualité
- Pas de communication des objectifs qualité aux opérateurs

## §7.4 — Communication

L'organisme doit déterminer les communications internes et externes pertinentes pour le SMQ.

## §7.5 — Informations documentées

L'organisme doit inclure les informations documentées requises par la norme ISO 9001 et celles que l'organisme estime nécessaires à l'efficacité du SMQ.

**§7.5.1 — Généralités** : Le SMQ doit inclure les informations documentées requises par la présente norme.

**§7.5.2 — Création et mise à jour** : L'organisme doit s'assurer que les informations documentées sont correctement identifiées, décrites, revues et approuvées.

**§7.5.3 — Maîtrise des informations documentées** : Les informations documentées requises par le SMQ doivent être maîtrisées pour s'assurer qu'elles sont disponibles, appropriées à l'utilisation et convenablement protégées.

**Exemples de NC observées**
- Procédures non référencées, non versionnées ou sans date de révision
- Documents périmés encore utilisés sur le terrain
- Procédure achats non accessible au point d'utilisation
- Pas de liste maîtresse des documents du SMQ
- Enregistrements non conservés ou non retrouvables
- Durée de conservation des enregistrements non définie

'@

wf "backend\corpus\iso9001\08_realisation.md" @'
# ISO 9001 §8 — Réalisation des activités opérationnelles

**Référence** : ISO 9001:2015, clauses 8.1 à 8.7
**Mots-clés** : planification opérationnelle, exigences client, conception, achats, production, non-conformité

## §8.1 — Planification et maîtrise opérationnelles

L'organisme doit planifier, mettre en œuvre, maîtriser, tenir à jour et améliorer les processus nécessaires à la réalisation des produits et services.

## §8.2 — Exigences relatives aux produits et services

L'organisme doit s'assurer de bien comprendre et déterminer les exigences relatives aux produits et services avant de s'engager à les fournir.

**Exemples de NC observées**
- Cahiers des charges clients non formalisés
- Revues de contrat non documentées

## §8.4 — Maîtrise des processus, produits et services fournis par des prestataires externes

L'organisme doit s'assurer que les processus, produits et services fournis par des prestataires externes sont conformes aux exigences spécifiées.

**§8.4.1 — Généralités** : L'organisme doit maîtriser les produits et services fournis de l'extérieur.

**§8.4.2 — Type et étendue de la maîtrise** : L'organisme doit définir les contrôles à appliquer aux prestataires externes.

**§8.4.3 — Informations à l'attention des prestataires externes** : L'organisme doit communiquer aux prestataires les exigences relatives aux processus, produits et services à fournir, y compris les exigences de qualification du personnel et les interactions avec l'organisme.

**Exemples de NC observées**
- Pas de procédure documentée de contrôle réception
- Cahier manuscrit du magasinier non validé comme document qualité
- Aucune spécification transmise aux fournisseurs
- Liste des fournisseurs approuvés absente ou non mise à jour
- Pas de critères d'évaluation des prestataires définis
- Bons de livraison non vérifiés à réception

## §8.5 — Production et prestation de service

L'organisme doit mettre en œuvre la production et la prestation de service dans des conditions maîtrisées.

**Exemples de NC observées**
- Gammes opératoires non à jour
- Paramètres de process non définis ou non surveillés

## §8.6 — Libération des produits et services

L'organisme doit mettre en œuvre les dispositions planifiées pour vérifier que les exigences ont été satisfaites avant la libération des produits et services.

**Exemples de NC observées**
- Contrôles finaux non documentés
- Signature de libération manquante sur les enregistrements

## §8.7 — Maîtrise des éléments de sortie non conformes

L'organisme doit s'assurer que les éléments de sortie qui ne sont pas conformes aux exigences sont identifiés et maîtrisés.

**Exemples de NC observées**
- Zone de quarantaine non délimitée ou non respectée
- Produits non conformes mélangés aux produits conformes
- Pas d'étiquetage des pièces non conformes

'@

wf "backend\corpus\iso9001\09_evaluation.md" @'
# ISO 9001 §9 — Évaluation des performances

**Référence** : ISO 9001:2015, clauses 9.1 à 9.3
**Mots-clés** : surveillance, mesure, audit interne, revue de direction, satisfaction client

## §9.1 — Surveillance, mesure, analyse et évaluation

L'organisme doit surveiller, mesurer, analyser et évaluer les performances et l'efficacité du SMQ.

**§9.1.2 — Satisfaction du client** : L'organisme doit surveiller la perception des clients sur le niveau de satisfaction de leurs besoins et attentes.

**§9.1.3 — Analyse et évaluation** : L'organisme doit analyser et évaluer les données et informations appropriées issues de la surveillance et de la mesure.

**Exemples de NC observées**
- Aucune enquête de satisfaction client réalisée
- Indicateurs qualité non définis ou non suivis
- Tableau de bord qualité non présenté en revue de direction

## §9.2 — Audit interne

L'organisme doit réaliser des audits internes à des intervalles planifiés pour obtenir des informations permettant de déterminer si le SMQ est conforme aux exigences.

**Exemples de NC observées**
- Programme d'audit interne absent ou non suivi
- Auditeurs internes non formés
- Rapports d'audit non archivés
- Actions correctives issues des audits non clôturées dans les délais

## §9.3 — Revue de direction

La direction doit procéder à la revue du SMQ de l'organisme à des intervalles planifiés.

**Exemples de NC observées**
- Revue de direction non tenue depuis plus de 12 mois
- Compte rendu de revue de direction absent
- Points obligatoires de la revue non traités (NC récurrentes, risques, objectifs)

'@

wf "backend\corpus\iso9001\10_amelioration.md" @'
# ISO 9001 §10 — Amélioration

**Référence** : ISO 9001:2015, clauses 10.1 à 10.3
**Mots-clés** : non-conformité, actions correctives, amélioration continue

## §10.1 — Généralités

L'organisme doit déterminer et sélectionner les opportunités d'amélioration et mettre en œuvre toutes les actions nécessaires pour satisfaire aux exigences du client et accroître la satisfaction du client.

## §10.2 — Non-conformité et action corrective

Lorsqu'une non-conformité se produit, l'organisme doit réagir, évaluer le besoin d'actions pour éliminer les causes afin qu'elle ne se reproduise pas, et mettre en œuvre les actions nécessaires.

L'organisme doit conserver des informations documentées comme preuves de la nature des non-conformités, des actions entreprises et des résultats de toute action corrective.

**Exemples de NC observées**
- Fiche de NC sans analyse des causes (pas de méthode 5M, 5 Pourquoi, etc.)
- Actions correctives non vérifiées dans les délais définis
- NC récurrentes non analysées systémiquement
- Pas de registre des NC maintenu
- Efficacité des actions correctives non vérifiée

## §10.3 — Amélioration continue

L'organisme doit améliorer en continu la pertinence, l'adéquation et l'efficacité du SMQ.

**Exemples de NC observées**
- Pas de démarche d'amélioration formalisée (Kaizen, PDCA, etc.)
- Suggestions du personnel non recueillies ni traitées
- Résultats des audits non exploités pour définir des axes d'amélioration

'@

wf "frontend\package.json" @'
{
  "name": "uc28-frontend",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build",
    "preview": "vite preview --host 0.0.0.0 --port 3000",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "react": "18.3.1",
    "react-dom": "18.3.1",
    "react-router-dom": "^6.26.0",
    "@tanstack/react-query": "^5.56.2",
    "framer-motion": "^11.5.4",
    "axios": "^1.7.7"
  },
  "devDependencies": {
    "@types/node": "^20",
    "@types/react": "^18",
    "@types/react-dom": "^18",
    "@vitejs/plugin-react": "^4.3.1",
    "typescript": "5.4.5",
    "tailwindcss": "3.4.13",
    "autoprefixer": "^10",
    "postcss": "^8",
    "vite": "^5.4.8"
  }
}

'@

wf "frontend\vite.config.ts" @'
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, ".") },
  },
  server: {
    host: "0.0.0.0",
    port: 3000,
    env: {
      VITE_API_URL: process.env.VITE_API_URL ?? "http://localhost:8000",
    },
  },
});

'@

wf "frontend\tsconfig.json" @'
{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "paths": { "@/*": ["./*"] }
  },
  "include": ["src", "vite.config.ts", "components", "lib", "styles"],
  "exclude": ["node_modules"]
}

'@

wf "frontend\tailwind.config.ts" @'
import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./index.html",
    "./src/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        uc: {
          "bg-dark": "#0F2027",
          "bg-panel": "#164E5E",
          panel: "#F8FAFC",
          primary: "#134E5E",
          "primary-2": "#2C7A7B",
          accent: "#10B981",
          "accent-lt": "#6EE7B7",
          "accent-50": "#ECFDF5",
          alert: "#F59E0B",
          "alert-50": "#FEF3C7",
          danger: "#DC2626",
          "danger-50": "#FEE2E2",
          "text-dark": "#0F172A",
          "text-body": "#334155",
          "text-mute": "#64748B",
          border: "#CBD5E1",
        },
      },
      fontFamily: {
        sans: ["Inter", "sans-serif"],
        mono: ["JetBrains Mono", "monospace"],
      },
    },
  },
  plugins: [],
};

export default config;

'@

wf "frontend\postcss.config.mjs" @'
const config = {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
};

export default config;

'@

wf "frontend\index.html" @'
<!DOCTYPE html>
<html lang="fr">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>BV·Inspect — Inspection Augmentée</title>
    <link rel="manifest" href="/manifest.webmanifest" />
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>

'@

wf "frontend\public\manifest.webmanifest" @'
{
  "name": "BV·Inspect — Inspection Augmentée",
  "short_name": "BV·Inspect",
  "description": "Copilote IA pour les inspecteurs Bureau Veritas",
  "start_url": "/",
  "display": "fullscreen",
  "background_color": "#0F2027",
  "theme_color": "#10B981",
  "icons": [
    { "src": "/icon-192.png", "sizes": "192x192", "type": "image/png" },
    { "src": "/icon-512.png", "sizes": "512x512", "type": "image/png" }
  ]
}

'@

wf "frontend\styles\globals.css" @'
@import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap");

@tailwind base;
@tailwind components;
@tailwind utilities;

html,
body {
  height: 100%;
  overflow: hidden;
  @apply font-sans bg-uc-panel text-uc-text-dark antialiased;
}

@layer utilities {
  .line-clamp-2 {
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }
}

'@

wf "frontend\src\main.tsx" @'
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { QueryProvider } from "@/lib/query-provider";
import "@/styles/globals.css";
import Home from "./pages/Home";
import Brief from "./pages/Brief";
import Capture from "./pages/Capture";
import Report from "./pages/Report";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/inspection/:id/brief" element={<Brief />} />
          <Route path="/inspection/:id/capture" element={<Capture />} />
          <Route path="/inspection/:id/report" element={<Report />} />
        </Routes>
      </BrowserRouter>
    </QueryProvider>
  </React.StrictMode>
);

'@

wf "frontend\src\pages\Home.tsx" @'
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { getDashboardKpis, getAuditsToday, type AuditToday } from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";

const RECURRENCES = [
  { client: "Fournisseur ALPHA", level: "NC mineure", ref: "§8.4.3", date: "2025-06-15", label: "Procédure contrôle réception", borderColor: "border-uc-alert", textColor: "text-uc-alert" },
  { client: "Fournisseur OMEGA", level: "Observation", ref: "§7.2", date: "2025-11-08", label: "Polyvalence opérateurs", borderColor: "border-uc-primary-2", textColor: "text-uc-primary-2" },
  { client: "Sous-traitant DELTA", level: "NC majeure", ref: "§7.1.4", date: "2026-02-21", label: "Sécurité quai chargement", borderColor: "border-uc-danger", textColor: "text-uc-danger" },
];

function KpiCard({ label, value, color }: { label: string; value: number | string; color: string }) {
  return (
    <div className={`bg-white rounded-lg shadow-sm border-l-4 ${color} p-4 flex flex-col gap-1`}>
      <span className="text-3xl font-mono font-bold text-uc-text-dark">{value}</span>
      <span className="text-xs text-uc-text-mute uppercase tracking-wide">{label}</span>
    </div>
  );
}

function auditBadge(audit: AuditToday) {
  if (audit.is_next)
    return { label: "PROCHAIN", cls: "bg-uc-alert-50 text-uc-alert border border-uc-alert" };
  if (audit.status === "completed")
    return { label: "TERMINÉ", cls: "bg-uc-text-mute/20 text-uc-text-mute" };
  if (audit.status === "ongoing")
    return { label: "EN COURS", cls: "bg-uc-accent text-white" };
  return { label: "PLANIFIÉ", cls: "bg-uc-panel text-uc-text-mute border border-uc-border" };
}

function AuditCard({ audit, onStart }: { audit: AuditToday; onStart: () => void }) {
  const highlight = audit.is_next;
  const badge = auditBadge(audit);
  const time = new Date(audit.scheduled_at)
    .toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })
    .replace(":", "h");

  return (
    <div
      className={`bg-white rounded-xl p-4 flex items-center gap-4 ${
        highlight
          ? "border-2 border-uc-accent shadow-md"
          : "border border-uc-border shadow-sm"
      }`}
    >
      {/* Heure + lieu + badge */}
      <div className="w-20 shrink-0 flex flex-col items-center gap-1.5 text-center">
        <p className="font-mono text-xl font-bold text-uc-primary leading-none">{time}</p>
        <p className="text-[10px] text-uc-text-mute leading-tight">{audit.location}</p>
        <span className={`text-[10px] px-2 py-0.5 rounded-full font-bold uppercase tracking-wide ${badge.cls}`}>
          {badge.label}
        </span>
      </div>

      {/* Contenu */}
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-uc-text-dark truncate">{audit.client_name}</p>
        <p className="text-xs text-uc-text-mute truncate">{audit.location}</p>
        <p className="text-xs text-uc-text-body mt-0.5 line-clamp-2">{audit.scope}</p>
      </div>

      {/* Action */}
      {highlight && (
        <button
          onClick={onStart}
          className="px-4 py-2 bg-uc-accent text-white text-sm font-bold rounded-lg hover:bg-emerald-600 transition-colors whitespace-nowrap shrink-0"
        >
          Démarrer →
        </button>
      )}
      {!highlight && audit.status === "completed" && (
        <span className="text-xs text-uc-primary-2 cursor-pointer hover:underline whitespace-nowrap shrink-0">
          Voir le rapport ↗
        </span>
      )}
      {!highlight && audit.status === "prepared" && (
        <span className="text-xs text-uc-text-mute whitespace-nowrap shrink-0">À {time}</span>
      )}
    </div>
  );
}

export default function Home() {
  const navigate = useNavigate();
  const { data: kpis } = useQuery({ queryKey: ["kpis"], queryFn: getDashboardKpis });
  const { data: audits = [] } = useQuery({ queryKey: ["audits_today"], queryFn: getAuditsToday });

  return (
    <div className="h-dvh w-dvw flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="dashboard"
        title={`Mes audits — ${new Date().toLocaleDateString("fr-FR", { weekday: "long", day: "numeric", month: "long", year: "numeric" })}`}
        subtitle="Bureau Veritas Certification · Antoine Mercier · Lyon"
      />

      <main className="flex-1 overflow-y-auto p-6 flex flex-col gap-6">
        {/* KPIs */}
        <div className="grid grid-cols-4 gap-4">
          <KpiCard label="Audits planifiés aujourd'hui" value={kpis?.audits_today_count ?? "—"} color="border-uc-accent" />
          <KpiCard label="Audits ce mois" value={kpis?.audits_month_count ?? "—"} color="border-uc-primary" />
          <KpiCard label="Délai moyen audit → rapport (j)" value={kpis?.avg_delay_days ?? "—"} color="border-uc-primary-2" />
          <KpiCard label="Récurrences à vérifier" value={kpis?.pending_recurrences_count ?? "—"} color="border-uc-alert" />
        </div>

        {/* Corps : audits + récurrences */}
        <div className="flex gap-6 flex-1 min-h-0">
          {/* Audits du jour */}
          <div className="flex-1 flex flex-col gap-3 min-w-0">
            <h2 className="text-xs font-bold text-uc-text-mute uppercase tracking-wider">
              Audits du jour
            </h2>
            {audits.length === 0 ? (
              <div className="bg-white rounded-xl p-8 text-center text-uc-text-mute border border-uc-border">
                Aucun audit planifié aujourd'hui
              </div>
            ) : (
              audits.map((a) => (
                <AuditCard
                  key={a.id}
                  audit={a}
                  onStart={() => navigate(`/inspection/${a.id}/brief`)}
                />
              ))
            )}
          </div>

          {/* Récurrences à vérifier */}
          <div className="w-72 shrink-0 flex flex-col gap-3">
            <div>
              <h2 className="text-xs font-bold text-uc-alert uppercase tracking-wider">
                Récurrences à vérifier
              </h2>
              <p className="text-[10px] text-uc-text-mute mt-0.5">
                NC non clôturées chez les fournisseurs du jour
              </p>
            </div>
            {RECURRENCES.map((r) => (
              <div
                key={r.client}
                className={`bg-white rounded-xl border border-uc-border border-l-4 ${r.borderColor} p-3 shadow-sm`}
              >
                <p className="font-semibold text-sm text-uc-text-dark">{r.client}</p>
                <p className={`text-xs font-bold ${r.textColor}`}>
                  {r.level} {r.ref}
                </p>
                <p className="text-[10px] text-uc-text-mute mt-0.5">ouverte le {r.date}</p>
                <p className="text-xs text-uc-text-body mt-1">{r.label}</p>
              </div>
            ))}
          </div>
        </div>
      </main>

      <footer className="bg-uc-bg-dark text-uc-text-mute text-xs p-4 flex items-center justify-between shrink-0">
        <span className="font-bold text-uc-accent-lt uppercase tracking-wider">
          Antoine Mercier · Lead Auditor
        </span>
        <span>
          Lyon · {new Date().toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })} · UC 28 — Inspection Augmentée
        </span>
      </footer>
    </div>
  );
}

'@

wf "frontend\src\pages\Brief.tsx" @'
import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion } from "framer-motion";
import { getInspection, generateChecklist, updateInspection } from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";

const PREV_AUDITS = [
  { date: "2025-06-15", label: "Audit ok", nc: "NC mineure → à vérifier", alert: true },
  { date: "2024-07-22", label: "Audit ok", nc: "3 observations", alert: false },
  { date: "2023-09-04", label: "NC mineure", nc: "1 observation", alert: false },
];

export default function BriefPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const { data: inspection } = useQuery({
    queryKey: ["inspection", id],
    queryFn: () => getInspection(id!),
    enabled: !!id,
  });

  const [generationSeconds, setGenerationSeconds] = useState<number | null>(null);
  const [generating, setGenerating] = useState(false);

  const checklistMutation = useMutation({
    mutationFn: () => generateChecklist(id!),
    onSuccess: (data) => {
      setGenerating(false);
      setGenerationSeconds(data.generation_duration_seconds);
      qc.invalidateQueries({ queryKey: ["inspection", id] });
    },
  });

  const startMutation = useMutation({
    mutationFn: () => updateInspection(id!, { status: "ongoing" }),
    onSuccess: () => navigate(`/inspection/${id}/capture`),
  });

  useEffect(() => {
    if (inspection && !inspection.checklist_json && !generating) {
      setGenerating(true);
      checklistMutation.mutate();
    }
  }, [inspection]); // eslint-disable-line react-hooks/exhaustive-deps

  const hasChecklist = Boolean(inspection?.checklist_json);
  const sections = inspection?.checklist_json?.sections ?? [];
  const totalPoints = sections.reduce((acc, s) => acc + s.points.length, 0);
  const scopeBullets = inspection?.scope
    ? inspection.scope.split(/[.·]/).map((s) => s.trim()).filter((s) => s.length > 6).slice(0, 4)
    : [];

  return (
    <div className="h-dvh w-dvw flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="brief"
        title={`Audit ${inspection?.referential ?? ""} · ${inspection?.client_name ?? ""}`}
        subtitle={`${inspection?.site_name ?? ""} · auditeur · ${inspection?.auditor_name ?? ""}`}
      />

      <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0 overflow-hidden">

        {/* ── BRIEF CLIENT ── 3/12 */}
        <div className="col-span-3 bg-white rounded-xl shadow-sm p-4 overflow-y-auto flex flex-col gap-3">
          <h2 className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
            Brief client
            <span className="ml-1 font-normal normal-case">données saisies ou pré-enregistrées</span>
          </h2>
          {inspection && (
            <>
              {[
                { label: "Client", value: inspection.client_name },
                { label: "SIRET", value: (inspection as any).client_siret ?? "—" },
                { label: "Site", value: `${(inspection as any).site_address ?? inspection.site_name}` },
                { label: "Référentiel", value: inspection.referential },
              ].map(({ label, value }) => (
                <div key={label}>
                  <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">{label}</p>
                  <p className="text-xs text-uc-text-body mt-0.5">{value}</p>
                </div>
              ))}

              {scopeBullets.length > 0 && (
                <div>
                  <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">Scope</p>
                  <ul className="mt-0.5 flex flex-col gap-0.5">
                    {scopeBullets.map((b, i) => (
                      <li key={i} className="text-xs text-uc-text-body flex gap-1">
                        <span className="text-uc-accent shrink-0">•</span>
                        <span>{b}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              <div>
                <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">Contact</p>
                <p className="text-xs text-uc-text-body mt-0.5">Marie Lemaitre</p>
                <p className="text-[10px] text-uc-text-mute">Responsable Qualité</p>
              </div>

              <div>
                <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">Durée prévue</p>
                <p className="text-xs text-uc-text-body mt-0.5">2 h sur site</p>
              </div>
            </>
          )}
        </div>

        {/* ── CHECK-LIST AGENT 1 ── 6/12 */}
        <div className="col-span-6 flex flex-col gap-3 overflow-hidden">
          <h2 className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
            Check-list générée par l&apos;Agent 1
            <span className="ml-1 font-normal normal-case">RAG ISO 9001 + scope client + historique fournisseur</span>
          </h2>

          {generating && (
            <div className="bg-uc-accent-50 border border-uc-accent rounded-lg p-4 flex items-center gap-3">
              <div className="w-5 h-5 border-2 border-uc-accent border-t-transparent rounded-full animate-spin shrink-0" />
              <div>
                <p className="text-sm font-semibold text-uc-primary">Préparation par l&apos;Agent 1…</p>
                <p className="text-xs text-uc-text-mute">Croisement scope · normes ISO 9001 · historique fournisseur</p>
              </div>
            </div>
          )}

          {hasChecklist && generationSeconds !== null && (
            <motion.div
              initial={{ opacity: 0, y: -4 }}
              animate={{ opacity: 1, y: 0 }}
              className="bg-uc-accent-50 border border-uc-accent rounded-lg p-3 flex items-center justify-between"
            >
              <div className="flex items-center gap-2">
                <span className="text-uc-accent font-bold text-lg">✓</span>
                <span className="text-sm text-uc-primary font-semibold">
                  Préparation terminée — {generationSeconds} secondes
                </span>
              </div>
              <span className="text-xs text-uc-text-mute">
                {sections.length} sections · {totalPoints} points · {sections.length} actions ISO sources
              </span>
            </motion.div>
          )}

          <div className="flex-1 overflow-y-auto flex flex-col gap-2">
            {hasChecklist && sections.map((section) => (
              <motion.div
                key={section.id}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                className="bg-white rounded-lg border border-uc-border p-3 flex items-start gap-3"
              >
                <span className="font-mono text-xs font-bold text-uc-primary bg-uc-panel px-2 py-1 rounded shrink-0">
                  {section.id}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-semibold text-uc-text-dark">{section.title}</p>
                  <p className="text-[10px] text-uc-text-mute mt-0.5">
                    {section.points.length} point{section.points.length > 1 ? "s" : ""} · sources ISO 9001
                  </p>
                </div>
              </motion.div>
            ))}

            {!hasChecklist && !generating && (
              <div className="flex-1 flex items-center justify-center text-uc-text-mute text-sm">
                En attente de génération…
              </div>
            )}
          </div>
        </div>

        {/* ── AUDITS PRÉCÉDENTS ── 3/12 */}
        <div className="col-span-3 flex flex-col gap-3 overflow-y-auto">
          <h2 className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
            Audits précédents
            <span className="ml-1 font-normal normal-case">Fournisseur · 3 derniers audits</span>
          </h2>

          {PREV_AUDITS.map((a) => (
            <div
              key={a.date}
              className={`rounded-lg border p-3 ${
                a.alert ? "border-uc-alert bg-uc-alert-50" : "border-uc-border bg-white"
              }`}
            >
              <p className="font-mono text-xs font-bold text-uc-text-mute">{a.date}</p>
              <p className="text-xs font-semibold text-uc-text-dark mt-0.5">{a.label}</p>
              <p className={`text-xs mt-0.5 ${a.alert ? "text-uc-alert font-medium" : "text-uc-text-mute"}`}>
                {a.nc}
              </p>
            </div>
          ))}

          <div className="bg-uc-bg-dark rounded-lg p-3 border-l-4 border-uc-alert mt-1">
            <p className="text-xs font-bold text-uc-alert mb-1">⚠ Point d&apos;attention</p>
            <p className="text-xs text-uc-accent-lt leading-snug">
              La NC mineure de 2025 n&apos;est pas clôturée → point S2 inclus automatiquement par l&apos;Agent 1 — ne pas l&apos;ignorer
            </p>
          </div>
        </div>
      </main>

      <footer className="bg-uc-bg-dark p-4 flex items-center justify-between shrink-0">
        <div>
          <p className="text-xs font-bold text-uc-accent-lt uppercase tracking-wider">
            {hasChecklist ? "PRÉPARATION TERMINÉE" : "GÉNÉRATION EN COURS…"}
          </p>
          {hasChecklist && (
            <p className="text-[10px] text-uc-text-mute mt-0.5">
              Check-list générée · {generationSeconds ?? "—"} sec · {sections.length} sections · {totalPoints} points · 1 point récurrent
            </p>
          )}
        </div>
        <button
          disabled={!hasChecklist || startMutation.isPending}
          onClick={() => startMutation.mutate()}
          className="px-6 py-3 bg-uc-accent text-white font-bold rounded-xl disabled:opacity-50 disabled:cursor-not-allowed hover:bg-emerald-600 transition-colors"
        >
          Démarrer l&apos;inspection →
        </button>
      </footer>
    </div>
  );
}

'@

wf "frontend\src\pages\Capture.tsx" @'
import { useCallback, useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import {
  getInspection,
  createConstat,
  generateReport,
  type Constat,
  type RagChunk,
} from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";
import ChecklistView from "@/components/ChecklistView";
import VoiceCapture from "@/components/VoiceCapture";
import PhotoCapture from "@/components/PhotoCapture";
import RagTransparency from "@/components/RagTransparency";
import NCBadge from "@/components/NCBadge";

type CaptureState = "idle" | "processing" | "result" | "validated";

const BORDER: Record<string, string> = {
  nc_majeure: "border-uc-danger",
  nc_mineure: "border-uc-alert",
  observation: "border-uc-primary-2",
  conforme: "border-uc-accent",
};

function useElapsed(startedAt: string | null | undefined) {
  const [label, setLabel] = useState("00:00");
  useEffect(() => {
    if (!startedAt) return;
    const start = new Date(startedAt).getTime();
    const tick = () => {
      const s = Math.max(0, Math.floor((Date.now() - start) / 1000));
      setLabel(`${String(Math.floor(s / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`);
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [startedAt]);
  return label;
}

export default function CapturePage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const { data: inspection, refetch } = useQuery({
    queryKey: ["inspection", id],
    queryFn: () => getInspection(id!),
    enabled: !!id,
    refetchInterval: 0,
  });

  const elapsed = useElapsed(inspection?.started_at);

  const [captureState, setCaptureState] = useState<CaptureState>("idle");
  const [rawText, setRawText] = useState("");
  const [currentConstat, setCurrentConstat] = useState<Constat | null>(null);
  const [ragChunks, setRagChunks] = useState<RagChunk[]>([]);
  const [activePointId, setActivePointId] = useState<string | null>(null);
  const [photoId, setPhotoId] = useState<string | null>(null);
  const [photoPreview, setPhotoPreview] = useState<string | null>(null);
  const [showGeneratingModal, setShowGeneratingModal] = useState(false);
  const [generatingMessages] = useState([
    "Synthèse exécutive…",
    "Regroupement des constats par thème…",
    "Construction du plan d'action priorisé…",
    "Mise en forme du document…",
    "Génération du DOCX…",
  ]);
  const [genMsgIdx, setGenMsgIdx] = useState(0);
  const [textFallback, setTextFallback] = useState(false);

  const constatMutation = useMutation({
    mutationFn: (text: string) =>
      createConstat(id!, {
        raw_text: text,
        checklist_point_id: activePointId ?? undefined,
        photo_id: photoId ?? undefined,
      }),
    onSuccess: (constat) => {
      setCurrentConstat(constat);
      setRagChunks(constat.rag_chunks ?? []);
      setCaptureState("result");
    },
    onError: () => setCaptureState("idle"),
  });

  const reportMutation = useMutation({
    mutationFn: () => generateReport(id!),
    onSuccess: () => {
      setShowGeneratingModal(false);
      navigate(`/inspection/${id}/report`);
    },
  });

  const handleTranscript = useCallback(
    (text: string) => {
      setRawText(text);
      setCaptureState("processing");
      setRagChunks([]);
      constatMutation.mutate(text);
    },
    [constatMutation]
  );

  const handleValidate = () => {
    if (!currentConstat) return;
    setCaptureState("validated");
    setTimeout(() => {
      refetch();
      qc.invalidateQueries({ queryKey: ["inspection", id] });
      setCaptureState("idle");
      setRawText("");
      setCurrentConstat(null);
      setPhotoId(null);
      setPhotoPreview(null);
    }, 300);
  };

  const handleRedo = () => {
    setCaptureState("idle");
    setRawText("");
    setCurrentConstat(null);
    setPhotoId(null);
    setPhotoPreview(null);
  };

  const handleGenerateReport = () => {
    setShowGeneratingModal(true);
    setGenMsgIdx(0);
    reportMutation.mutate();
  };

  useEffect(() => {
    if (!showGeneratingModal) return;
    const timer = setInterval(() => setGenMsgIdx((i) => (i + 1) % generatingMessages.length), 2000);
    return () => clearInterval(timer);
  }, [showGeneratingModal, generatingMessages.length]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "t" || e.key === "T") setTextFallback((v) => !v);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, []);

  const constats = inspection?.constats ?? [];
  const validatedPointIds = new Set(
    constats.filter((c) => c.checklist_point_id).map((c) => c.checklist_point_id!)
  );
  const stats = {
    total: constats.length,
    nc_majeure: constats.filter((c) => c.classification === "nc_majeure").length,
    nc_mineure: constats.filter((c) => c.classification === "nc_mineure").length,
    observation: constats.filter((c) => c.classification === "observation").length,
    conforme: constats.filter((c) => c.classification === "conforme").length,
  };

  const startTime = inspection?.started_at
    ? new Date(inspection.started_at).toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })
    : null;

  return (
    <div className="h-dvh w-dvw flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="capture"
        title={`Audit ${inspection?.referential ?? ""} · ${inspection?.client_name ?? ""}`}
        subtitle={`${inspection?.site_name ?? ""} · Auditeur : ${inspection?.auditor_name ?? ""}`}
        startedAt={inspection?.started_at ?? undefined}
      />

      <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0 overflow-hidden">

        {/* ── CHECK-LIST ── 3/12 */}
        <div className="col-span-3 overflow-hidden flex flex-col">
          <ChecklistView
            checklist={inspection?.checklist_json ?? null}
            activePointId={activePointId}
            validatedPointIds={validatedPointIds}
            onSelectPoint={setActivePointId}
          />
        </div>

        {/* ── CAPTURE EN COURS ── 6/12 */}
        <div className="col-span-6 flex flex-col gap-3 overflow-hidden">
          <div>
            <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
              Capture en cours
              <span className="ml-1 font-normal normal-case">voix → classification temps réel</span>
            </p>
          </div>

          {/* Zone micro / saisie */}
          {textFallback ? (
            <div className="bg-uc-bg-dark rounded-lg p-4 flex flex-col gap-2">
              <textarea
                className="bg-uc-bg-panel text-white rounded p-2 text-sm resize-none h-20 focus:outline-none focus:ring-1 focus:ring-uc-accent"
                placeholder="Tapez votre constat ici… (Ctrl+Enter pour valider)"
                value={rawText}
                onChange={(e) => setRawText(e.target.value)}
                onKeyDown={(e) => {
                  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
                    setCaptureState("processing");
                    setRagChunks([]);
                    constatMutation.mutate(rawText);
                  }
                }}
              />
              <p className="text-xs text-uc-text-mute">Ctrl+Enter pour envoyer · <kbd className="bg-uc-bg-panel px-1 rounded">T</kbd> pour revenir au micro</p>
            </div>
          ) : (
            <VoiceCapture onTranscript={handleTranscript} />
          )}

          {/* Transcript intermédiaire */}
          {rawText && captureState !== "idle" && (
            <p className="text-xs text-uc-text-mute italic px-1">
              → {rawText.slice(0, 100)}{rawText.length > 100 ? "…" : ""}
            </p>
          )}

          {/* Résultat de la classification */}
          <AnimatePresence mode="wait">
            {captureState === "processing" && (
              <motion.div
                key="loading"
                initial={{ opacity: 0, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0 }}
                className="bg-uc-bg-dark rounded-lg border border-uc-accent p-4 flex flex-col gap-2 flex-1"
              >
                <div className="flex gap-3 items-center mb-2">
                  <div className="w-4 h-4 border-2 border-uc-accent border-t-transparent rounded-full animate-spin shrink-0" />
                  <span className="text-sm text-uc-accent-lt font-medium">Classification par l&apos;Agent 2…</span>
                </div>
                {[2, 3, 2].map((w, i) => (
                  <div key={i} className={`h-3 bg-uc-bg-panel rounded animate-pulse w-${w}/3`} />
                ))}
              </motion.div>
            )}

            {captureState === "result" && currentConstat && (
              <motion.div
                key="result"
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.2 }}
                className={`rounded-lg border-l-4 ${BORDER[currentConstat.classification] ?? "border-uc-border"} border border-uc-border bg-white p-4 flex flex-col gap-3`}
              >
                {/* Badges */}
                <div className="flex items-center gap-2 flex-wrap">
                  <NCBadge level={currentConstat.classification} />
                  {currentConstat.norm_reference && (
                    <span className="font-mono text-xs bg-uc-primary text-white px-2 py-0.5 rounded font-bold">
                      {currentConstat.norm_reference}
                    </span>
                  )}
                  {currentConstat.norm_reference && (
                    <span className="text-xs text-uc-accent border border-uc-accent px-2 py-0.5 rounded font-medium">
                      ✓ sourcé
                    </span>
                  )}
                  {photoPreview && (
                    <img src={photoPreview} alt="" className="w-8 h-6 object-cover rounded ml-auto" />
                  )}
                </div>

                {/* Constat reformulé */}
                <div>
                  <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider mb-1">Constat reformulé :</p>
                  <p className="text-sm text-uc-text-dark font-medium leading-snug">
                    {currentConstat.reformulated_text}
                  </p>
                </div>

                {/* Action corrective */}
                {currentConstat.suggested_action && (
                  <div>
                    <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider mb-1">Action corrective suggérée :</p>
                    <p className="text-xs text-uc-text-body leading-snug">
                      {currentConstat.suggested_action}
                    </p>
                  </div>
                )}

                {/* Actions */}
                <div className="flex gap-2 mt-1">
                  <PhotoCapture
                    onUploaded={(pid, preview) => { setPhotoId(pid); setPhotoPreview(preview); }}
                  />
                  <button
                    onClick={handleValidate}
                    className="flex-1 py-2 bg-uc-primary text-white text-sm font-bold rounded-lg hover:bg-uc-primary-2 transition-colors"
                  >
                    Valider →
                  </button>
                  <button
                    onClick={handleRedo}
                    className="px-4 py-2 bg-white border border-uc-border text-uc-text-body text-sm rounded-lg hover:bg-uc-panel transition-colors"
                  >
                    Refaire
                  </button>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* ── CONSTATS ── 3/12 */}
        <div className="col-span-3 flex flex-col gap-2 overflow-y-auto">
          <p className="text-[10px] font-bold text-uc-text-mute uppercase tracking-wider">
            Constats ({stats.total})
          </p>
          {constats.slice().reverse().map((c) => (
            <motion.div
              key={c.id}
              initial={{ x: 40, opacity: 0 }}
              animate={{ x: 0, opacity: 1 }}
              transition={{ duration: 0.3, type: "spring", bounce: 0.3 }}
              className={`bg-white rounded-lg border-l-4 ${BORDER[c.classification] ?? "border-uc-border"} border border-uc-border p-3`}
            >
              <div className="flex items-center gap-1.5 flex-wrap">
                <NCBadge level={c.classification} />
                {c.norm_reference && (
                  <span className="font-mono text-[10px] text-uc-text-mute">{c.norm_reference}</span>
                )}
                {c.photo_path && <span className="text-uc-text-mute text-xs ml-auto">📷</span>}
              </div>
              <p className="text-xs text-uc-text-body mt-1.5 line-clamp-2 leading-snug">
                {c.reformulated_text}
              </p>
            </motion.div>
          ))}
        </div>
      </main>

      {/* ── RAG ── */}
      <RagTransparency chunks={ragChunks} loading={captureState === "processing"} />

      {/* ── FOOTER ── */}
      <footer className="bg-uc-bg-dark px-6 py-3 flex items-center justify-between shrink-0">
        <div>
          <p className="text-xs font-bold text-uc-accent-lt uppercase tracking-wider">INSPECTION EN COURS</p>
          <p className="text-xs text-white mt-0.5">
            {stats.total} constat{stats.total > 1 ? "s" : ""} · {stats.nc_majeure} NC majeure · {stats.nc_mineure} NC mineure · {stats.observation} observation · {stats.conforme} conforme
          </p>
          {startTime && (
            <p className="text-[10px] text-uc-text-mute mt-0.5">
              Démarré à {startTime} · Durée : {elapsed}
            </p>
          )}
        </div>
        <button
          onClick={handleGenerateReport}
          disabled={stats.total === 0 || reportMutation.isPending}
          className="px-8 py-4 bg-uc-accent text-white text-base font-bold rounded-xl disabled:opacity-50 disabled:cursor-not-allowed hover:bg-emerald-600 hover:scale-[1.02] transition-all"
        >
          Générer le pré-rapport →
        </button>
      </footer>

      {/* ── MODAL GÉNÉRATION ── */}
      <AnimatePresence>
        {showGeneratingModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="fixed inset-0 bg-uc-bg-dark/90 flex flex-col items-center justify-center gap-6 z-50"
          >
            <div className="w-16 h-16 border-4 border-uc-accent border-t-transparent rounded-full animate-spin" />
            <p className="text-uc-accent-lt text-lg font-semibold">{generatingMessages[genMsgIdx]}</p>
            <p className="text-uc-text-mute text-sm">Agent 3 — Restitution en cours…</p>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

'@

wf "frontend\src\pages\Report.tsx" @'
import { useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useQuery, useMutation } from "@tanstack/react-query";
import { getInspection } from "@/lib/api";
import HeaderBar from "@/components/HeaderBar";
import NCBadge from "@/components/NCBadge";
import type { NCLevel } from "@/lib/api";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

export default function ReportPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [sent, setSent] = useState(false);

  const { data: inspection } = useQuery({
    queryKey: ["inspection", id],
    queryFn: () => getInspection(id!),
    enabled: !!id,
  });

  const sendMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch(`${API_URL}/api/inspections/${id}/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          to: ["marie.lemaitre@alpha.fr"],
          cc: ["audit.bv@bureauveritas.com"],
          subject: `Pré-rapport audit ${inspection?.client_name} — ${new Date().toLocaleDateString("fr-FR")}`,
          message: "Veuillez trouver ci-joint le pré-rapport de votre audit ISO 9001.",
        }),
      });
      return res.json();
    },
    onSuccess: () => {
      setSent(true);
      setTimeout(() => navigate("/"), 2000);
    },
  });

  const constats = inspection?.constats ?? [];
  const stats = {
    nc_majeure: constats.filter((c) => c.classification === "nc_majeure").length,
    nc_mineure: constats.filter((c) => c.classification === "nc_mineure").length,
    observation: constats.filter((c) => c.classification === "observation").length,
    conforme: constats.filter((c) => c.classification === "conforme").length,
  };

  const STAT_CONFIG: { key: NCLevel; label: string; color: string; textColor: string }[] = [
    { key: "nc_majeure", label: "NC Majeure", color: "border-uc-danger", textColor: "text-uc-danger" },
    { key: "nc_mineure", label: "NC Mineure", color: "border-uc-alert", textColor: "text-uc-alert" },
    { key: "observation", label: "Observation", color: "border-uc-primary-2", textColor: "text-uc-primary-2" },
    { key: "conforme", label: "Conforme", color: "border-uc-accent", textColor: "text-uc-accent" },
  ];

  return (
    <div className="h-dvh w-dvw flex flex-col overflow-hidden bg-uc-panel">
      <HeaderBar
        variant="report"
        title={`Audit ${inspection?.referential ?? ""} · ${inspection?.client_name ?? ""}`}
        subtitle={`${inspection?.site_name ?? ""} · audit terminé`}
      />

      <main className="flex-1 grid grid-cols-12 gap-4 p-4 min-h-0 overflow-hidden">
        {/* Synthèse — 3/12 */}
        <div className="col-span-3 flex flex-col gap-3 overflow-y-auto">
          <h2 className="text-xs font-bold text-uc-text-mute uppercase tracking-wider">Synthèse</h2>
          {STAT_CONFIG.map(({ key, label, color, textColor }) => (
            <div key={key} className={`bg-white rounded-lg shadow-sm border-l-4 ${color} p-3`}>
              <span className={`text-4xl font-mono font-bold ${textColor}`}>{stats[key]}</span>
              <p className="text-xs text-uc-text-mute mt-1">{label}</p>
            </div>
          ))}
          <div className="bg-uc-bg-dark rounded-lg p-3 mt-2">
            <p className="text-xs font-bold text-uc-accent-lt mb-2">Plan d&apos;action</p>
            {constats
              .filter((c) => c.classification === "nc_majeure" || c.classification === "nc_mineure")
              .slice(0, 3)
              .map((c, i) => (
                <div key={c.id} className="flex gap-2 items-start mb-2">
                  <span className={`text-xs font-bold px-1.5 py-0.5 rounded ${i === 0 ? "bg-uc-danger text-white" : "bg-uc-alert text-white"}`}>
                    P{i + 1}
                  </span>
                  <p className="text-xs text-uc-accent-lt leading-snug">{c.suggested_action ?? c.reformulated_text}</p>
                </div>
              ))}
          </div>
        </div>

        {/* DOCX Preview — 6/12 */}
        <div className="col-span-6 bg-white rounded-xl shadow-sm overflow-y-auto p-6">
          <div className="text-center border-b border-uc-border pb-6 mb-6">
            <h1 className="text-xl font-bold text-uc-text-dark">Pré-rapport d&apos;audit qualité</h1>
            <p className="text-sm text-uc-text-mute mt-1">{inspection?.client_name} · {new Date().toLocaleDateString("fr-FR")}</p>
            <p className="text-xs text-uc-text-mute">{inspection?.site_name} · {inspection?.auditor_name}</p>
          </div>
          <div className="flex flex-col gap-4">
            {constats.map((c) => (
              <div key={c.id} className="border-l-4 border-uc-border pl-3">
                <div className="flex items-center gap-2 mb-1">
                  <NCBadge level={c.classification} />
                  {c.norm_reference && (
                    <span className="font-mono text-xs text-uc-text-mute">{c.norm_reference}</span>
                  )}
                </div>
                <p className="text-sm text-uc-text-body">{c.reformulated_text}</p>
                {c.suggested_action && (
                  <p className="text-xs text-uc-text-mute mt-1">→ {c.suggested_action}</p>
                )}
              </div>
            ))}
          </div>
          <a
            href={`${API_URL}/api/inspections/${id}/report.docx`}
            download
            className="mt-6 inline-block px-4 py-2 border border-uc-border text-uc-text-body text-sm rounded-lg hover:bg-uc-panel transition-colors"
          >
            ⬇ Télécharger DOCX
          </a>
        </div>

        {/* Send form — 3/12 */}
        <div className="col-span-3 bg-white rounded-xl shadow-sm p-4 flex flex-col gap-3 overflow-y-auto">
          <h2 className="text-xs font-bold text-uc-text-mute uppercase tracking-wider">Envoi au client</h2>
          <div className="flex flex-col gap-2 text-sm">
            {[
              ["À", "marie.lemaitre@alpha.fr"],
              ["CC", "audit.bv@bureauveritas.com"],
              ["Objet", `Pré-rapport audit ${inspection?.client_name} — ${new Date().toLocaleDateString("fr-FR")}`],
            ].map(([label, value]) => (
              <div key={label}>
                <p className="text-xs text-uc-text-mute">{label}</p>
                <p className="text-sm text-uc-text-body">{value}</p>
              </div>
            ))}
            <div>
              <p className="text-xs text-uc-text-mute">Message</p>
              <textarea
                className="w-full text-sm text-uc-text-body border border-uc-border rounded p-2 h-24 resize-none mt-1"
                defaultValue="Madame, Monsieur,\n\nVeuillez trouver ci-joint le pré-rapport de votre audit ISO 9001.\n\nCordialement,\nAntoine Mercier"
              />
            </div>
            <div>
              <p className="text-xs text-uc-text-mute">Pièces jointes</p>
              <p className="text-xs text-uc-text-body">Rapport DOCX + {constats.filter((c) => c.photo_path).length} photos</p>
            </div>
          </div>
        </div>
      </main>

      <footer className="bg-uc-bg-dark px-6 py-4 flex items-center justify-between shrink-0">
        <div>
          <p className="text-xs text-uc-accent-lt font-bold uppercase tracking-wider">AUDIT TERMINÉ</p>
          <p className="text-sm text-white">
            {constats.length} constats · {stats.nc_majeure} NC majeure · {stats.nc_mineure} NC mineure
          </p>
        </div>
        <button
          onClick={() => sendMutation.mutate()}
          disabled={sendMutation.isPending || sent}
          className="px-8 py-4 bg-uc-accent text-white text-lg font-bold rounded-xl disabled:opacity-50 hover:bg-emerald-600 transition-colors"
        >
          {sent ? "✓ Envoyé" : "Envoyer au client →"}
        </button>
      </footer>

      {sent && (
        <div className="fixed inset-0 bg-uc-bg-dark/80 flex items-center justify-center z-50">
          <div className="bg-white rounded-2xl p-8 text-center shadow-2xl">
            <p className="text-4xl mb-3">✅</p>
            <p className="text-lg font-bold text-uc-text-dark">Rapport envoyé</p>
            <p className="text-sm text-uc-text-mute">à Marie Lemaitre · {new Date().toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })}</p>
          </div>
        </div>
      )}
    </div>
  );
}

'@

wf "frontend\lib\api.ts" @'
import axios from "axios";

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || "http://localhost:8000",
  headers: { "Content-Type": "application/json" },
});

export default api;

// --- Types ---

export type InspectionStatus = "prepared" | "ongoing" | "completed";
export type NCLevel = "conforme" | "observation" | "nc_mineure" | "nc_majeure";

export interface Constat {
  id: string;
  inspection_id: string;
  checklist_point_id: string | null;
  raw_text: string;
  reformulated_text: string;
  classification: NCLevel;
  norm_reference: string | null;
  norm_excerpt: string | null;
  suggested_evidence: string | null;
  suggested_action: string | null;
  rag_chunks: RagChunk[] | null;
  photo_path: string | null;
  created_at: string;
}

export interface RagChunk {
  section: string;
  excerpt: string;
  score: number;
}

export interface Inspection {
  id: string;
  client_name: string;
  site_name: string;
  auditor_name: string;
  referential: string;
  scope: string;
  status: InspectionStatus;
  started_at: string | null;
  created_at: string;
  checklist_json: Checklist | null;
  constats: Constat[];
}

export interface Checklist {
  referential: string;
  scope_summary: string;
  sections: ChecklistSection[];
}

export interface ChecklistSection {
  id: string;
  title: string;
  points: ChecklistPoint[];
}

export interface ChecklistPoint {
  id: string;
  question: string;
  expected_evidence: string;
  norm_reference: string;
}

export interface DashboardKpis {
  audits_today_count: number;
  audits_month_count: number;
  avg_delay_days: number;
  pending_recurrences_count: number;
}

export interface AuditToday {
  id: string;
  scheduled_at: string;
  client_name: string;
  location: string;
  scope: string;
  status: InspectionStatus;
  is_next: boolean;
}

// --- API functions ---

export const getInspection = (id: string) =>
  api.get<Inspection>(`/api/inspections/${id}`).then((r) => r.data);

export const listInspections = () =>
  api.get<Inspection[]>("/api/inspections").then((r) => r.data);

export const createInspection = (body: Partial<Inspection>) =>
  api.post<Inspection>("/api/inspections", body).then((r) => r.data);

export const updateInspection = (id: string, body: Partial<Inspection>) =>
  api.patch<Inspection>(`/api/inspections/${id}`, body).then((r) => r.data);

export const generateChecklist = (id: string, referential?: string) => {
  const params = referential ? { referential } : {};
  return api.post<{ checklist_json: Checklist; generation_duration_seconds: number }>(
    `/api/inspections/${id}/checklist`,
    {},
    { params }
  ).then((r) => r.data);
};

export const createConstat = (
  inspectionId: string,
  body: { raw_text: string; checklist_point_id?: string; photo_id?: string }
) => api.post<Constat>(`/api/inspections/${inspectionId}/constats`, body).then((r) => r.data);

export const uploadPhoto = (file: File) => {
  const form = new FormData();
  form.append("file", file);
  return api
    .post<{ id: string; path: string; kind: string }>("/api/uploads", form, {
      headers: { "Content-Type": "multipart/form-data" },
    })
    .then((r) => r.data);
};

export const generateReport = (id: string) =>
  api
    .post<{ report_structure: object; generation_duration_seconds: number; docx_url: string }>(
      `/api/inspections/${id}/report`
    )
    .then((r) => r.data);

export const getDashboardKpis = () =>
  api.get<DashboardKpis>("/api/dashboard/kpis").then((r) => r.data);

export const getAuditsToday = () =>
  api.get<AuditToday[]>("/api/dashboard/audits_today").then((r) => r.data);

export const resetDemo = () =>
  api.post<{ reset_at: string; inspection_id: string }>("/api/dev/reset-demo").then((r) => r.data);

export const replayConstat = (index: number) =>
  api.post<{ constat: { id: string; classification: string } }>(`/api/dev/replay/${index}`).then((r) => r.data);

'@

wf "frontend\lib\query-provider.tsx" @'
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

export function QueryProvider({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(() => new QueryClient({
    defaultOptions: { queries: { staleTime: 10_000 } },
  }));
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

'@

wf "frontend\components\HeaderBar.tsx" @'

import { useEffect, useState } from "react";

type Variant = "dashboard" | "brief" | "capture" | "report";

interface Props {
  variant: Variant;
  title?: string;
  subtitle?: string;
  startedAt?: string | null;
  referential?: string;
  onReferentialChange?: (ref: string) => void;
}

function ElapsedTimer({ startedAt }: { startedAt: string }) {
  const [elapsed, setElapsed] = useState("00:00");

  useEffect(() => {
    const start = new Date(startedAt).getTime();
    const update = () => {
      const diff = Math.max(0, Math.floor((Date.now() - start) / 1000));
      const m = String(Math.floor(diff / 60)).padStart(2, "0");
      const s = String(diff % 60).padStart(2, "0");
      setElapsed(`${m}:${s}`);
    };
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [startedAt]);

  return <span className="font-mono text-uc-accent-lt text-sm font-bold">{elapsed}</span>;
}

const STATUS_PILL: Partial<Record<Variant, { label: string; className: string }>> = {
  brief: { label: "EN PRÉPARATION", className: "bg-uc-alert text-white" },
  report: { label: "AUDIT TERMINÉ", className: "bg-uc-accent text-white" },
};

export default function HeaderBar({
  variant,
  title = "BV·Inspect",
  subtitle,
  startedAt,
  referential = "ISO 9001",
  onReferentialChange,
}: Props) {
  const pill = STATUS_PILL[variant];

  return (
    <header className="h-[72px] bg-uc-bg-dark text-white flex items-center px-6 gap-4 shrink-0">
      <span className="font-bold text-lg tracking-wide text-uc-accent-lt">BV·INSPECT</span>
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-sm truncate">{title}</p>
        {subtitle && <p className="text-xs text-uc-text-mute truncate">{subtitle}</p>}
      </div>
      {pill && (
        <span className={`px-3 py-1 rounded-full text-xs font-bold tracking-wider ${pill.className}`}>
          {pill.label}
        </span>
      )}
      {variant === "capture" && (
        <select
          className="bg-uc-bg-panel border border-uc-accent text-white text-sm rounded px-2 py-1"
          value={referential}
          onChange={(e) => {
            if (
              window.confirm(`Régénérer la check-list pour ${e.target.value} ?`)
            ) {
              onReferentialChange?.(e.target.value);
            }
          }}
        >
          <option>ISO 9001</option>
          <option>NFC 15-100</option>
          <option>ATEX</option>
        </select>
      )}
      {variant === "capture" && startedAt && <ElapsedTimer startedAt={startedAt} />}
      <div className="w-8 h-8 rounded-full bg-uc-primary-2 flex items-center justify-center text-xs font-bold">
        AM
      </div>
    </header>
  );
}

'@

wf "frontend\components\ChecklistView.tsx" @'

import { useState } from "react";
import type { Checklist } from "@/lib/api";

interface Props {
  checklist: Checklist | null;
  activePointId?: string | null;
  validatedPointIds?: Set<string>;
  onSelectPoint?: (pointId: string) => void;
}

function PointMarker({ validated, active }: { validated: boolean; active: boolean }) {
  if (validated) return <span className="text-uc-accent font-bold">✓</span>;
  if (active) return <span className="text-uc-alert font-bold">◐</span>;
  return <span className="text-uc-text-mute">○</span>;
}

export default function ChecklistView({
  checklist,
  activePointId,
  validatedPointIds = new Set(),
  onSelectPoint,
}: Props) {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(["S1"]));

  if (!checklist) {
    return (
      <div className="flex items-center justify-center h-full text-uc-text-mute text-sm">
        Check-list non générée
      </div>
    );
  }

  const toggle = (id: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  return (
    <div className="flex flex-col gap-2 overflow-y-auto">
      <p className="text-xs font-bold text-uc-text-mute uppercase tracking-wider mb-1">
        Check-list dynamique
      </p>
      {checklist.sections.map((section) => {
        const isExpanded = expandedSections.has(section.id);
        return (
          <div key={section.id} className="rounded-lg border border-uc-border bg-white overflow-hidden">
            <button
              onClick={() => toggle(section.id)}
              className="w-full flex items-center gap-2 p-2 text-left hover:bg-uc-accent-50 transition-colors"
            >
              <span className="font-mono text-xs font-bold text-uc-primary">{section.id}</span>
              <span className="text-xs font-medium text-uc-text-body flex-1 truncate">
                {section.title}
              </span>
              <span className="text-uc-text-mute text-xs">{isExpanded ? "▾" : "▸"}</span>
            </button>
            {isExpanded && (
              <div className="border-t border-uc-border divide-y divide-uc-border">
                {section.points.map((point) => (
                  <button
                    key={point.id}
                    onClick={() => onSelectPoint?.(point.id)}
                    className={`w-full flex items-start gap-2 p-2 text-left hover:bg-uc-accent-50 transition-colors ${
                      activePointId === point.id ? "bg-uc-alert-50" : ""
                    }`}
                  >
                    <span className="mt-0.5 text-xs">
                      <PointMarker
                        validated={validatedPointIds.has(point.id)}
                        active={activePointId === point.id}
                      />
                    </span>
                    <span className="text-xs text-uc-text-body leading-snug">{point.question}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
        );
      })}
      <div className="mt-1 flex gap-3 text-xs text-uc-text-mute">
        <span><span className="text-uc-accent">✓</span> Validé</span>
        <span><span className="text-uc-alert">◐</span> Actif</span>
        <span>○ À faire</span>
      </div>
    </div>
  );
}

'@

wf "frontend\components\VoiceCapture.tsx" @'

import { useCallback, useEffect, useRef, useState } from "react";

interface Props {
  onTranscript: (text: string) => void;
}

export default function VoiceCapture({ onTranscript }: Props) {
  const [listening, setListening] = useState(false);
  const [interim, setInterim] = useState("");
  const recognitionRef = useRef<SpeechRecognition | null>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animFrameRef = useRef<number>(0);
  const analyserRef = useRef<AnalyserNode | null>(null);

  const drawWaveform = useCallback(() => {
    const canvas = canvasRef.current;
    const analyser = analyserRef.current;
    if (!canvas || !analyser) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const data = new Uint8Array(analyser.frequencyBinCount);
    analyser.getByteFrequencyData(data);

    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const barCount = 35;
    const barW = canvas.width / barCount - 2;

    for (let i = 0; i < barCount; i++) {
      const value = data[Math.floor((i / barCount) * data.length)];
      const barH = (value / 255) * canvas.height;
      ctx.fillStyle = i % 2 === 0 ? "#10B981" : "#6EE7B7";
      ctx.fillRect(i * (barW + 2), canvas.height - barH, barW, barH);
    }

    animFrameRef.current = requestAnimationFrame(drawWaveform);
  }, []);

  const startMic = useCallback(async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const ctx = new AudioContext();
      const source = ctx.createMediaStreamSource(stream);
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 256;
      source.connect(analyser);
      analyserRef.current = analyser;
      animFrameRef.current = requestAnimationFrame(drawWaveform);
    } catch {
      // mic permission denied — waveform stays flat
    }
  }, [drawWaveform]);

  const stopMic = useCallback(() => {
    cancelAnimationFrame(animFrameRef.current);
    analyserRef.current = null;
    const canvas = canvasRef.current;
    if (canvas) {
      const ctx = canvas.getContext("2d");
      ctx?.clearRect(0, 0, canvas.width, canvas.height);
    }
  }, []);

  const toggleListening = useCallback(() => {
    const SpeechRecognition =
      (window as Window & typeof globalThis & { SpeechRecognition?: typeof window.SpeechRecognition; webkitSpeechRecognition?: typeof window.SpeechRecognition }).SpeechRecognition ||
      (window as Window & typeof globalThis & { webkitSpeechRecognition?: typeof window.SpeechRecognition }).webkitSpeechRecognition;

    if (!SpeechRecognition) {
      alert("Web Speech API non disponible. Utilisez la touche T pour saisir au clavier.");
      return;
    }

    if (listening) {
      recognitionRef.current?.stop();
      setListening(false);
      stopMic();
      return;
    }

    const recognition = new SpeechRecognition();
    recognition.lang = "fr-FR";
    recognition.continuous = true;
    recognition.interimResults = true;

    recognition.onresult = (e: SpeechRecognitionEvent) => {
      let finalText = "";
      let interimText = "";
      for (let i = e.resultIndex; i < e.results.length; i++) {
        if (e.results[i].isFinal) finalText += e.results[i][0].transcript;
        else interimText += e.results[i][0].transcript;
      }
      setInterim(interimText);
      if (finalText) {
        onTranscript(finalText.trim());
        setInterim("");
      }
    };

    recognition.onend = () => {
      setListening(false);
      stopMic();
    };

    recognitionRef.current = recognition;
    recognition.start();
    setListening(true);
    startMic();
  }, [listening, onTranscript, startMic, stopMic]);

  useEffect(() => {
    return () => {
      recognitionRef.current?.stop();
      cancelAnimationFrame(animFrameRef.current);
    };
  }, []);

  return (
    <div className="bg-uc-bg-dark rounded-lg p-4 flex flex-col gap-3">
      <div className="flex items-center gap-4">
        <button
          onClick={toggleListening}
          aria-label={listening ? "Arrêter l'enregistrement" : "Démarrer l'enregistrement"}
          className={`w-14 h-14 rounded-full flex items-center justify-center shrink-0 transition-colors ${
            listening ? "bg-uc-danger animate-pulse" : "bg-uc-text-mute"
          }`}
        >
          <span className={`w-4 h-4 rounded-full ${listening ? "bg-uc-alert" : "bg-white"}`} />
        </button>
        <canvas ref={canvasRef} width={300} height={56} className="flex-1 rounded" />
      </div>
      {interim && (
        <p className="text-sm italic text-uc-accent-lt">« {interim} »</p>
      )}
      {!listening && !interim && (
        <p className="text-xs text-uc-text-mute text-center">
          Cliquez le micro ou tapez <kbd className="bg-uc-bg-panel px-1 rounded">T</kbd> pour parler
        </p>
      )}
    </div>
  );
}

'@

wf "frontend\components\PhotoCapture.tsx" @'
"use client";

import { useRef } from "react";
import { uploadPhoto } from "@/lib/api";

interface Props {
  onUploaded: (photoId: string, previewUrl: string) => void;
}

export default function PhotoCapture({ onUploaded }: Props) {
  const inputRef = useRef<HTMLInputElement>(null);

  const handleChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const preview = URL.createObjectURL(file);
    const { id } = await uploadPhoto(file);
    onUploaded(id, preview);
    e.target.value = "";
  };

  return (
    <>
      <input
        ref={inputRef}
        type="file"
        accept="image/*"
        capture="environment"
        className="hidden"
        onChange={handleChange}
      />
      <button
        onClick={() => inputRef.current?.click()}
        className="flex items-center gap-2 px-4 py-2 bg-uc-accent text-white text-sm font-medium rounded-lg hover:bg-emerald-600 transition-colors"
        aria-label="Ajouter une photo"
      >
        📷 Ajouter photo
      </button>
    </>
  );
}

'@

wf "frontend\components\RagTransparency.tsx" @'

import { motion } from "framer-motion";
import type { RagChunk } from "@/lib/api";

interface Props {
  chunks: RagChunk[];
  loading?: boolean;
}

function ScoreChip({ score }: { score: number }) {
  const color =
    score >= 0.85
      ? "bg-uc-accent text-white"
      : score >= 0.7
      ? "bg-uc-alert text-white"
      : "bg-uc-text-mute text-white";
  return (
    <motion.span
      initial={{ scale: 0.8 }}
      animate={{ scale: 1 }}
      transition={{ duration: 0.15 }}
      className={`text-xs font-mono px-2 py-0.5 rounded-full ${color}`}
    >
      {score.toFixed(2)}
    </motion.span>
  );
}

export default function RagTransparency({ chunks, loading }: Props) {
  return (
    <div className="bg-uc-accent-50 border-t border-uc-accent px-6 py-3 shrink-0">
      <p className="text-center text-xs font-bold text-uc-primary mb-1">
        RAG · Articles normatifs remontés en temps réel
      </p>
      {loading ? (
        <div className="grid grid-cols-3 gap-4">
          {[0, 1, 2].map((i) => (
            <div key={i} className="bg-white rounded-lg p-3 animate-pulse h-20" />
          ))}
        </div>
      ) : chunks.length > 0 ? (
        <div className="grid grid-cols-3 gap-4">
          {chunks.map((chunk, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: i * 0.1 }}
              className="bg-white rounded-lg p-3 flex flex-col gap-1"
            >
              <p className="text-xs font-bold text-uc-primary font-mono">
                §{chunk.section}
              </p>
              <p className="text-xs text-uc-text-body line-clamp-2 flex-1">
                {chunk.excerpt.slice(0, 120)}…
              </p>
              <div className="flex justify-end">
                <ScoreChip score={chunk.score} />
              </div>
            </motion.div>
          ))}
        </div>
      ) : (
        <p className="text-center text-xs italic text-uc-text-mute">
          Les chunks normatifs s&apos;affichent pendant la classification
        </p>
      )}
    </div>
  );
}

'@

wf "frontend\components\NCBadge.tsx" @'
import type { NCLevel } from "@/lib/api";

const CONFIG: Record<NCLevel, { label: string; className: string }> = {
  nc_majeure:  { label: "NC MAJEURE",  className: "bg-uc-danger text-white" },
  nc_mineure:  { label: "NC MINEURE",  className: "bg-uc-alert text-white" },
  observation: { label: "OBSERVATION", className: "bg-uc-primary-2 text-white" },
  conforme:    { label: "CONFORME",    className: "bg-uc-accent text-white" },
};

export default function NCBadge({ level }: { level: NCLevel }) {
  const { label, className } = CONFIG[level];
  return (
    <span className={`inline-block px-2 py-0.5 text-xs font-bold tracking-wider rounded ${className}`}>
      {label}
    </span>
  );
}

'@

wf "frontend\components\ConstatCard.tsx" @'

import { motion } from "framer-motion";
import type { Constat } from "@/lib/api";
import NCBadge from "./NCBadge";

const BORDER_COLORS: Record<string, string> = {
  nc_majeure:  "border-uc-danger",
  nc_mineure:  "border-uc-alert",
  observation: "border-uc-primary-2",
  conforme:    "border-uc-accent",
};

export default function ConstatCard({ constat }: { constat: Constat }) {
  const borderColor = BORDER_COLORS[constat.classification] ?? "border-uc-border";

  return (
    <motion.div
      initial={{ x: 40, opacity: 0 }}
      animate={{ x: 0, opacity: 1 }}
      transition={{ duration: 0.3, type: "spring", bounce: 0.3 }}
      className={`bg-white rounded-lg shadow-sm border-l-4 ${borderColor} p-3 cursor-pointer hover:shadow-md transition-shadow`}
    >
      <div className="flex items-start justify-between gap-2 mb-1">
        <NCBadge level={constat.classification} />
        <div className="flex items-center gap-2">
          {constat.photo_path && (
            <span className="text-uc-text-mute text-xs">📷</span>
          )}
          {constat.norm_reference && (
            <span className="font-mono text-xs text-uc-text-mute truncate max-w-[120px]">
              {constat.norm_reference}
            </span>
          )}
        </div>
      </div>
      <p className="text-sm text-uc-text-body line-clamp-2">
        {constat.reformulated_text}
      </p>
    </motion.div>
  );
}

'@


Write-Host "[2/4] Fichiers sources ecrits (45 fichiers)"

Write-Host "[3/4] Verification..."
$missing = @()
foreach ($f in @("backend\flask_app.py","backend\requirements.txt","frontend\package.json","frontend\index.html")) {
    if (-not (Test-Path (Join-Path $ROOT $f))) { $missing += $f }
}
if ($missing.Count -gt 0) {
    Write-Host "ATTENTION : fichiers manquants : $($missing -join ', ')" -ForegroundColor Yellow
} else {
    Write-Host "[4/4] Tous les fichiers presents" -ForegroundColor Green
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  PROJET UC 28 CREE AVEC SUCCES !" -ForegroundColor Green
Write-Host "============================================"
Write-Host ""
Write-Host "ETAPES SUIVANTES :" -ForegroundColor Yellow
Write-Host ""
Write-Host "  1. Editez le fichier  backend\.env"
Write-Host "     Renseignez votre ANTHROPIC_API_KEY"
Write-Host "     (URL Capgemini : https://anthropic.generative.engine.capgemini.com)"
Write-Host ""
Write-Host "  2. Double-cliquez sur  start.bat"
Write-Host ""
Write-Host "  3. Ouvrez http://localhost:3000 dans Chrome"
Write-Host ""
Write-Host "Dossier du projet : $ROOT" -ForegroundColor Cyan
Write-Host ""
