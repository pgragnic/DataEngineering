from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import constats, dev, inspections, reports, uploads
from app.db import Base, engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="UC28 — Inspection Augmentée", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(inspections.router, prefix="/api")
app.include_router(constats.router, prefix="/api")
app.include_router(reports.router, prefix="/api")
app.include_router(uploads.router, prefix="/api")
app.include_router(dev.router, prefix="/api/dev")


@app.get("/health")
def health():
    return {"status": "ok"}
