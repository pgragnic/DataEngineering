.PHONY: install migrate ingest backend frontend reset-demo test lint

install:
	cd backend && uv sync
	cd frontend && npm install

migrate:
	cd backend && uv run alembic upgrade head

ingest:
	cd backend && uv run python -m app.rag.ingest

backend:
	cd backend && uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

frontend:
	cd frontend && npm run dev

reset-demo:
	curl -s -X POST http://localhost:8000/api/dev/reset-demo | python3 -m json.tool

test:
	cd backend && uv run pytest -q

lint:
	cd backend && uv run ruff check .
	cd frontend && npm run typecheck
