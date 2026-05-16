# UC 28 — Inspection Augmentée

Copilote IA pour les inspecteurs Bureau Veritas / APAVE.
Pipeline : **Agent 1** (check-list) → **Agent 2** (classification NC) → **Agent 3** (pré-rapport DOCX).
Hackathon Vibe Coding Capgemini × Anthropic · démo cible 3 min.

## Démarrage rapide (< 10 min)

### Pré-requis
- Docker & Docker Compose
- Node.js 20+
- Python 3.11+
- Une clé API Anthropic

### Installation

```bash
# 1. Cloner et se placer dans le projet
cd uc28-inspection

# 2. Copier les variables d'environnement
cp .env.example .env
# → Renseigner ANTHROPIC_API_KEY dans .env

# 3. Lancer l'infra (postgres + chromadb)
docker-compose up -d

# 4. Backend
cd backend
pip install -e ".[dev]"
alembic upgrade head
python -m app.rag.ingest          # ingestion du corpus normatif
uvicorn app.main:app --reload     # → http://localhost:8000

# 5. Frontend (dans un autre terminal)
cd frontend
npm install
npm run dev                       # → http://localhost:3000
```

## Structure du projet

```
uc28-inspection/
├── docs/            # Blueprint, UI spec, wireframes
├── demo/            # Script de démo Teams, decks
├── backend/         # FastAPI + 3 agents Claude
│   ├── app/
│   │   ├── agents/  # preparation, capture, restitution
│   │   ├── api/     # routes FastAPI
│   │   ├── models/  # SQLAlchemy models
│   │   ├── rag/     # ChromaDB RAG
│   │   └── docx_gen/
│   └── corpus/      # Reformulations ISO 9001 / 19011
└── frontend/        # Next.js 14 PWA
    ├── app/         # Routes App Router
    └── components/
```

## Commandes utiles

```bash
# Tests backend
cd backend && pytest -q

# Typecheck + lint frontend
cd frontend && npm run typecheck && npm run lint

# Migrations
cd backend && alembic revision --autogenerate -m "message"

# Smoke test agents
cd backend && python -m app.agents.preparation
cd backend && python -m app.agents.capture

# Reset démo
curl -X POST http://localhost:8000/api/dev/reset-demo
```

## Documentation

- **Blueprint complet** : `docs/blueprint.md`
- **Spec UI** : `docs/ui-spec.md`
- **Wireframes** : `docs/wireframes/`
- **Script de démo** : `demo/script-teams.md`
- **Fixture scénario ALPHA** : `backend/data/fixtures/alpha.json`
