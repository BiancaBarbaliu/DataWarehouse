"""FastAPI application entry point."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db.client import ensure_indexes
from api.routers import assets, sources, timeseries
from analytics.router import router as analytics_router

app = FastAPI(
    title="Acme Financial Data Warehouse API",
    description=(
        "A temporal data warehouse for financial market data. "
        "Supports stocks, crypto, commodities and more via Yahoo Finance."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    ensure_indexes()


# ── routers ───────────────────────────────────────────────────────────────────
app.include_router(assets.router)
app.include_router(sources.router)
app.include_router(timeseries.router)
app.include_router(analytics_router)


@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "service": "Acme Financial DW API"}


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}
