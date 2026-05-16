# UC 28 — Inspection Augmentée

Copilote IA pour les inspecteurs Bureau Veritas / APAVE.
Pipeline : **Agent 1** (check-list) → **Agent 2** (classification NC) → **Agent 3** (pré-rapport DOCX).
Hackathon Vibe Coding Capgemini × Anthropic · démo cible 3 min.

## Stack technique

| Couche | Technologie |
|---|---|
| Frontend | Next.js 14 (App Router) + TypeScript + Tailwind |
| Backend | FastAPI (Python 3.11+) |
| LLM | Claude Sonnet 4.6 via SDK `anthropic` |
| Base de données | PostgreSQL 16 |
| RAG | ChromaDB embedded + sentence-transformers |
| Génération DOCX | python-docx |
| Packages Python | **uv** |

## Démarrage rapide (< 10 min)

### Pré-requis
- [uv](https://docs.astral.sh/uv/getting-started/installation/) — `pip install uv` ou `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Node.js 20+
- PostgreSQL 16 (local ou Docker)
- Clé API Anthropic (organisateurs Capgemini ou compte perso)

### 1. Variables d'environnement

```bash
cd uc28-inspection
cp .env.example backend/.env
# Éditer backend/.env : renseigner ANTHROPIC_API_KEY
# Sur PC Capgemini : décommenter ANTHROPIC_BASE_URL Capgemini
```

### 2. Base de données PostgreSQL

```bash
# Avec Docker (recommandé)
docker-compose up -d postgres

# Ou PostgreSQL local (déjà installé)
createuser -s uc28
createdb -O uc28 uc28
psql -c "ALTER USER uc28 WITH PASSWORD 'uc28';" uc28
```

### 3. Backend FastAPI

```bash
cd backend

# Installer les dépendances avec uv (crée .venv automatiquement)
uv sync

# Migrations PostgreSQL
uv run alembic upgrade head

# Ingérer le corpus normatif ISO 9001 (une seule fois, ~2 min)
uv run python -m app.rag.ingest

# Lancer le backend
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 4. Frontend Next.js

```bash
cd frontend
npm install
npm run dev
# → http://localhost:3000
```

### 5. Charger le scénario de démo

```bash
curl -X POST http://localhost:8000/api/dev/reset-demo
```

Ouvrez Chrome → `http://localhost:3000`

## Commandes quotidiennes

```bash
# Backend
cd backend
uv run uvicorn app.main:app --reload        # serveur dev
uv run pytest -q                            # tests
uv run ruff check .                         # lint

# Migrations
uv run alembic upgrade head
uv run alembic revision --autogenerate -m "description"

# Agents (smoke test CLI)
uv run python -m app.agents.preparation
uv run python -m app.agents.capture

# Corpus RAG
uv run python -m app.rag.ingest

# Frontend
cd frontend
npm run dev
npm run typecheck
npm run lint
```

## Structure du projet

```
uc28-inspection/
├── .claude/
│   └── settings.json        # Claude Code — config Capgemini
├── .env.example             # Template variables d'environnement
├── CLAUDE.md                # Instructions pour Claude Code
├── docker-compose.yml       # PostgreSQL local
├── docs/                    # Blueprint, UI spec, wireframes
├── demo/                    # Script de démo
└── backend/
    ├── pyproject.toml       # Dépendances Python (uv sync)
    ├── alembic/             # Migrations PostgreSQL
    ├── app/
    │   ├── agents/          # 3 agents Claude (préparation, capture, restitution)
    │   ├── api/             # Routes FastAPI
    │   ├── models/          # SQLAlchemy models
    │   ├── rag/             # ChromaDB + retrieval
    │   └── docx_gen/        # Génération DOCX
    ├── corpus/iso9001/      # Corpus normatif ISO 9001 §4–10
    ├── data/fixtures/       # Fixture scénario ALPHA
    └── tests/
└── frontend/
    ├── app/                 # Pages Next.js (App Router)
    └── components/          # Composants React
```

## Configuration Claude Code (Capgemini)

Voir `.claude/settings.json` — à renseigner avec la clé fournie par les organisateurs.

## Documentation

- **Blueprint complet** : `docs/blueprint.md`
- **Spec UI** : `docs/ui-spec.md`
- **Script de démo** : `demo/script-teams.md`
- **Fixture ALPHA** : `backend/data/fixtures/alpha.json`
