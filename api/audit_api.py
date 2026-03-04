"""
Audit API Endpoints
===================

RESTful API for accessing audit trail data.

Endpoints:
- GET /api/audit/{query_id} - Get specific audit record
- GET /api/audit - Search audit records
- GET /api/audit/stats - Get usage statistics
- GET /api/audit/session/{session_id} - Get session history
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

from services.audit_store import get_audit_store, AuditRecord

router = APIRouter(prefix="/api/audit", tags=["audit"])


class AuditResponse(BaseModel):
    """Response model for audit records."""
    query_id: str
    sql: str
    tables_queried: List[str]
    filters_applied: Dict[str, Any]
    row_count: int
    source_files: List[str]
    pipeline_run_id: Optional[str]
    executed_at: Optional[str]
    execution_ms: float
    user_id: Optional[str]
    session_id: Optional[str]
    endpoint: Optional[str]


class AuditSearchResponse(BaseModel):
    """Response model for audit search."""
    records: List[AuditResponse]
    total: int
    offset: int
    limit: int


class AuditStatsResponse(BaseModel):
    """Response model for audit statistics."""
    total_queries: int
    avg_execution_ms: float
    avg_row_count: float
    top_tables: List[Dict[str, Any]]


def _record_to_response(record: AuditRecord) -> AuditResponse:
    """Convert AuditRecord to API response."""
    return AuditResponse(
        query_id=record.query_id,
        sql=record.sql,
        tables_queried=record.tables_queried,
        filters_applied=record.filters_applied,
        row_count=record.row_count,
        source_files=record.source_files or [],
        pipeline_run_id=record.pipeline_run_id,
        executed_at=record.executed_at.isoformat() if record.executed_at else None,
        execution_ms=record.execution_ms,
        user_id=record.user_id,
        session_id=record.session_id,
        endpoint=record.endpoint,
    )


@router.get("/{query_id}", response_model=AuditResponse)
async def get_audit_record(query_id: str):
    """
    Get a specific audit record by query ID.
    
    This allows tracing any data point back to its source query.
    """
    store = get_audit_store()
    record = store.get(query_id)
    
    if not record:
        raise HTTPException(status_code=404, detail=f"Audit record not found: {query_id}")
    
    return _record_to_response(record)


@router.get("", response_model=AuditSearchResponse)
async def search_audit_records(
    start_date: Optional[str] = Query(None, description="Start date (ISO format)"),
    end_date: Optional[str] = Query(None, description="End date (ISO format)"),
    tables: Optional[str] = Query(None, description="Comma-separated table names"),
    user_id: Optional[str] = Query(None, description="User ID"),
    session_id: Optional[str] = Query(None, description="Session ID"),
    endpoint: Optional[str] = Query(None, description="API endpoint"),
    min_rows: Optional[int] = Query(None, description="Minimum row count"),
    max_rows: Optional[int] = Query(None, description="Maximum row count"),
    limit: int = Query(100, ge=1, le=1000, description="Max records to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Search audit records with various filters.
    
    Returns matching records in reverse chronological order.
    """
    store = get_audit_store()
    
    parsed_start = datetime.fromisoformat(start_date) if start_date else None
    parsed_end = datetime.fromisoformat(end_date) if end_date else None
    parsed_tables = tables.split(',') if tables else None
    
    records = store.search(
        start_date=parsed_start,
        end_date=parsed_end,
        tables=parsed_tables,
        user_id=user_id,
        session_id=session_id,
        endpoint=endpoint,
        min_rows=min_rows,
        max_rows=max_rows,
        limit=limit,
        offset=offset,
    )
    
    return AuditSearchResponse(
        records=[_record_to_response(r) for r in records],
        total=len(records),
        offset=offset,
        limit=limit,
    )


@router.get("/recent/{hours}", response_model=AuditSearchResponse)
async def get_recent_audit_records(
    hours: int = 24,
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Get audit records from the last N hours.
    
    Default is last 24 hours.
    """
    store = get_audit_store()
    records = store.get_recent(hours=hours, limit=limit)
    
    return AuditSearchResponse(
        records=[_record_to_response(r) for r in records],
        total=len(records),
        offset=0,
        limit=limit,
    )


@router.get("/session/{session_id}", response_model=AuditSearchResponse)
async def get_session_audit_records(session_id: str):
    """
    Get all audit records for a specific session.
    
    Useful for tracing all queries made during a user session.
    """
    store = get_audit_store()
    records = store.get_by_session(session_id)
    
    return AuditSearchResponse(
        records=[_record_to_response(r) for r in records],
        total=len(records),
        offset=0,
        limit=len(records),
    )


@router.get("/stats", response_model=AuditStatsResponse)
async def get_audit_stats(
    start_date: Optional[str] = Query(None, description="Start date (ISO format)"),
    end_date: Optional[str] = Query(None, description="End date (ISO format)"),
):
    """
    Get usage statistics for audit records.
    
    Returns:
    - Total query count
    - Average execution time
    - Average row count
    - Most queried tables
    """
    store = get_audit_store()
    
    parsed_start = datetime.fromisoformat(start_date) if start_date else None
    parsed_end = datetime.fromisoformat(end_date) if end_date else None
    
    stats = store.get_stats(start_date=parsed_start, end_date=parsed_end)
    
    return AuditStatsResponse(**stats)


@router.post("/replay/{query_id}")
async def replay_query(query_id: str):
    """
    Replay a query from the audit log.
    
    Executes the same SQL and returns fresh results.
    Useful for debugging and verification.
    """
    store = get_audit_store()
    record = store.get(query_id)
    
    if not record:
        raise HTTPException(status_code=404, detail=f"Audit record not found: {query_id}")
    
    from services.data_service import get_data_service
    
    service = get_data_service()
    
    try:
        result = service._execute_query(
            record.sql,
            record.tables_queried,
            record.filters_applied
        )
        
        return {
            "original_query_id": query_id,
            "new_query_id": result.audit.query_id,
            "original_row_count": record.row_count,
            "new_row_count": result.audit.row_count,
            "match": record.row_count == result.audit.row_count,
            "data": result.data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query replay failed: {str(e)}")


def register_routes(app):
    """Register audit routes with the main FastAPI app."""
    app.include_router(router)
