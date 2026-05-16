# UC 28 — Inspection Augmentée

## Quoi
PWA de copilote IA pour les inspecteurs Bureau Veritas / APAVE.
Pipeline en 3 agents Claude : Préparation (check-list dynamique) → Capture (classification NC + sourcing norme) → Restitution (pré-rapport DOCX).
Démonstrateur hackathon, démo cible 3 min. Spec complète dans `docs/blueprint.md`.

## Stack
- **Frontend** : Next.js 14 (App Router) + TypeScript + Tailwind. PWA mobile-first.
- **Backend** : FastAPI (Python 3.11+), SQLAlchemy 2 + Alembic, **PostgreSQL 16**.
- **LLM** : Claude Sonnet 4.6 (`claude-sonnet-4-6`) via SDK `anthropic`.
- **RAG** : ChromaDB embedded (`./storage/chroma/`). Corpus dans `backend/corpus/`.
- **DOCX** : `python-docx`.
- **Packages Python** : **uv** (`uv sync` dans `backend/`).

## Commandes
```bash
# dev environment
docker-compose up -d                                    # postgres
cd backend && uv run uvicorn app.main:app --reload      # backend :8000
cd frontend && npm run dev                              # frontend :3000

# tests & lint
cd backend && uv run pytest -q
cd backend && uv run ruff check .
cd frontend && npm run typecheck && npm run lint

# migrations
cd backend && uv run alembic upgrade head
cd backend && uv run alembic revision --autogenerate -m "message"

# ingestion corpus
cd backend && uv run python -m app.rag.ingest

# smoke agents (CLI)
cd backend && uv run python -m app.agents.preparation
cd backend && uv run python -m app.agents.capture
```

## Conventions
- **Langue** : code et commits en anglais ; messages métier (UI, prompts, DOCX) en français.
- **Commits** : Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`).
- **Tests** : Pytest pour le backend. Un test par agent avec mock de l'API Claude.
- **Pas d'API keys dans le repo.** Secrets via `backend/.env` (jamais commité).
- **Typage strict** : `ruff` côté backend, `strict: true` côté frontend.

## Spec source de vérité
- Modèle de données : `docs/blueprint.md` Section 4.
- Contrats d'API : `docs/blueprint.md` Section 5.
- Prompts agents : `docs/blueprint.md` Section 6. **Ne pas réécrire les prompts sans validation équipe.**
- Sprint en cours : voir `docs/blueprint.md` Section 8.

## Gotchas
- Le corpus `backend/corpus/` contient uniquement des **reformulations publiques** d'ISO 9001 (texte payant).
- La vision Claude (photo dans Agent 2) ne s'appelle que si une photo est attachée.
- Web Speech API ne fonctionne pas en HTTP sauf sur `localhost`.
- PostgreSQL doit tourner avant `uv run alembic upgrade head`.

## Démo
Scénario figé : `docs/blueprint.md` Section 9. Ne pas modifier à partir de S3.
