"""
Data source endpoints:
  GET /sources        → Q3: list all data sources (id + name)
  GET /sources/{id}   → Q4: full details of a data source
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from db import repository as repo

router = APIRouter(prefix="/sources", tags=["Data Sources"])


class SourceSummary(BaseModel):
    id: str
    name: str


class SourceDetail(BaseModel):
    id: str
    name: str
    api_endpoint: str
    description: str
    created_at: str


@router.get("", response_model=list[SourceSummary], summary="Q3 – List all data sources")
def list_sources():
    """Return identification data for all data sources in the warehouse."""
    return [SourceSummary(id=d["_id"], name=d["name"]) for d in repo.list_sources()]


@router.get("/{source_id}", response_model=SourceDetail, summary="Q4 – Data source details")
def get_source(source_id: str):
    """Return full details of a financial data source by its identifier."""
    doc = repo.get_source_by_id(source_id)
    if not doc:
        raise HTTPException(404, f"Source {source_id!r} not found")
    return SourceDetail(
        id=doc["_id"],
        name=doc["name"],
        api_endpoint=doc.get("api_endpoint", ""),
        description=doc.get("description", ""),
        created_at=str(doc.get("created_at", "")),
    )
