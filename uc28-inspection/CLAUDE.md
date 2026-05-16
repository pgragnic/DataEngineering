# UC 28 — Inspection Augmentée

## Quoi
PWA de copilote IA pour les inspecteurs Bureau Veritas / APAVE.
Pipeline en 3 agents Claude : Préparation (check-list dynamique) → Capture (classification NC + sourcing norme) → Restitution (pré-rapport DOCX).
Démonstrateur hackathon, démo cible 3 min. Spec complète dans `docs/blueprint.md`.

## Stack
- **Frontend** : Next.js 14 (App Router) + TypeScript + Tailwind. PWA mobile-first.
- **Backend** : FastAPI (Python 3.11+), SQLAlchemy 2 + Alembic, PostgreSQL 16.
- **LLM** : Claude Sonnet 4.6 (`claude-sonnet-4-6`) via SDK `anthropic`.
- **RAG** : ChromaDB embedded (`./storage/chroma/`). Corpus dans `backend/corpus/`.
- **DOCX** : `python-docx`.
- **Voix** : Web Speech API (browser natif).

## Commandes
```bash
# dev environment complet
docker-compose up -d           # postgres + chromadb
cd backend && uvicorn app.main:app --reload     # backend :8000
cd frontend && npm run dev                       # frontend :3000

# tests
cd backend && pytest -q
cd frontend && npm run typecheck && npm run lint

# migrations
cd backend && alembic upgrade head
cd backend && alembic revision --autogenerate -m "message"

# ingestion corpus
cd backend && python -m app.rag.ingest

# smoke agent (CLI)
cd backend && python -m app.agents.preparation
```

## Conventions
- **Langue** : code et commits en anglais ; messages métier (UI, prompts, DOCX) en français.
- **Commits** : Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`).
- **Tests** : Pytest pour le backend. Tests minimaux mais sur les agents (au moins un test par agent qui mocke l'API).
- **Pas d'API keys dans le repo.** Toutes les secrets via `.env` (jamais commité). `.env.example` à jour.
- **Typage strict** : `mypy` côté backend (best effort), `strict: true` côté frontend.

## Spec source de vérité
- Modèle de données : `docs/blueprint.md` Section 4.
- Contrats d'API : `docs/blueprint.md` Section 5.
- Prompts agents : `docs/blueprint.md` Section 6. **Ne pas réécrire les prompts sans validation équipe.**
- Sprint en cours : voir `docs/blueprint.md` Section 8.

## Gotchas
- Le texte intégral des normes ISO 9001 et 19011 est **payant**. Le corpus dans `backend/corpus/` ne contient que des **reformulations publiques**. Ne jamais committer du texte normatif brut.
- Le scope démo s'arrête à **ISO 9001**. Pas d'autres référentiels avant la fin de S3.
- La vision Claude (photo dans Agent 2) ne doit être appelée que si une photo est effectivement attachée — sinon coût inutile.
- Web Speech API ne fonctionne **pas** sur iOS Safari < 14.5 et pas en HTTP — toujours servir la PWA en HTTPS pour les tests mobile.

## Démo
Scénario figé : `docs/blueprint.md` Section 9. Ne pas modifier sans accord équipe à partir de S3.
