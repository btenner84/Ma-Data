"""
Agent V2 API Routes
===================

Exposes the multi-step agent with full audit trail.

Endpoints:
- POST /api/v2/agent/ask - Ask a question (returns answer + audit)
- GET /api/v2/agent/audit/{run_id} - Get full audit for a run
- GET /api/v2/agent/metrics - Get aggregate usage metrics
- GET /api/v2/agent/history - Get recent query history
"""

import os
import json
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import asyncio

# Import our agent
from api.services.agent_v2 import MAAgentV2, AgentAudit, get_agent_metrics


router = APIRouter(prefix="/api/v2/agent", tags=["Agent V2"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class AskRequest(BaseModel):
    """Request to ask the agent a question."""
    question: str = Field(..., description="The question to ask")
    user_id: str = Field(default="anonymous", description="User identifier")
    include_full_audit: bool = Field(default=False, description="Include full audit trail in response")
    stream: bool = Field(default=False, description="Stream the response")
    
    class Config:
        json_schema_extra = {
            "example": {
                "question": "Tell me about the 2027 advance notice",
                "user_id": "analyst-1",
                "include_full_audit": False,
            }
        }


class ThoughtStep(BaseModel):
    """A single thought in the reasoning chain."""
    step: str
    reasoning: str
    conclusion: str
    confidence: float = 0.0


class SQLQuery(BaseModel):
    """A SQL query that was executed."""
    sql: str
    description: str
    rows_returned: int
    success: bool
    error: Optional[str] = None


class AskResponse(BaseModel):
    """Response from asking the agent."""
    answer: str = Field(..., description="The agent's response")
    run_id: str = Field(..., description="Unique ID for this run")
    
    # Metrics
    llm_calls: int = Field(..., description="Number of LLM calls made")
    tool_calls: int = Field(..., description="Number of tool calls made")
    total_tokens: int = Field(..., description="Total tokens used")
    cost_usd: float = Field(..., description="Estimated cost in USD")
    latency_ms: int = Field(..., description="Total latency in milliseconds")
    confidence: float = Field(..., description="Agent confidence in answer")
    
    # Structured outputs
    data_tables: List[dict] = Field(default=[], description="Data tables from analysis")
    charts: List[dict] = Field(default=[], description="Chart specifications")
    sources: List[str] = Field(default=[], description="Sources used")
    
    # Thought process - WHY the agent made decisions
    thought_process: List[dict] = Field(default=[], description="Agent's reasoning chain")
    sql_queries: List[dict] = Field(default=[], description="SQL queries executed")
    
    # Optional full audit
    audit: Optional[dict] = Field(default=None, description="Full audit trail if requested")


class MetricsResponse(BaseModel):
    """Aggregate metrics response."""
    total_runs: int
    total_cost_usd: float
    total_tokens: int
    avg_latency_ms: float
    avg_llm_calls: float
    avg_tool_calls: float
    avg_confidence: float


# =============================================================================
# AUDIT STORAGE
# =============================================================================

# In-memory audit store (in production, use a database)
_audit_store: dict[str, dict] = {}

def store_audit(audit: AgentAudit):
    """Store an audit record."""
    _audit_store[audit.run_id] = audit.to_dict()
    
    # Keep only last 1000 audits in memory
    if len(_audit_store) > 1000:
        oldest = sorted(_audit_store.keys())[:-1000]
        for key in oldest:
            del _audit_store[key]

def get_audit(run_id: str) -> Optional[dict]:
    """Retrieve an audit record."""
    return _audit_store.get(run_id)


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.post("/ask", response_model=AskResponse)
async def ask_agent(request: AskRequest):
    """
    Ask the agent a question.
    
    The agent uses a multi-step process:
    1. **Planning**: Determines what data is needed
    2. **Executing**: Gathers data via tools/queries
    3. **Analyzing**: Extracts insights (may loop back for more data)
    4. **Validating**: Sanity checks the analysis
    5. **Synthesizing**: Creates a natural language response
    
    All steps are tracked in an audit trail with LLM usage and costs.
    """
    try:
        agent = MAAgentV2()
        answer, audit = await agent.answer(
            question=request.question,
            user_id=request.user_id,
        )
        
        # Store audit
        store_audit(audit)
        
        # Record in metrics
        get_agent_metrics().record_run(audit)
        
        # Extract thought process for response
        thought_process = [
            {
                "step": t.step,
                "reasoning": t.reasoning,
                "conclusion": t.conclusion,
                "confidence": t.confidence
            }
            for t in audit.thought_process
        ] if hasattr(audit, 'thought_process') else []
        
        # Extract SQL queries for response
        sql_queries = audit.sql_queries_executed if hasattr(audit, 'sql_queries_executed') else []
        
        return AskResponse(
            answer=answer,
            run_id=audit.run_id,
            llm_calls=audit.total_llm_calls,
            tool_calls=audit.total_tool_calls,
            total_tokens=audit.total_tokens,
            cost_usd=round(audit.total_cost_usd, 4),
            latency_ms=audit.total_latency_ms,
            confidence=audit.confidence,
            data_tables=audit.data_tables,
            charts=audit.charts,
            sources=audit.sources,
            thought_process=thought_process,
            sql_queries=sql_queries,
            audit=audit.to_dict() if request.include_full_audit else None,
        )
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"ERROR in /ask endpoint: {str(e)}")
        print(f"Full traceback:\n{error_trace}")
        raise HTTPException(status_code=500, detail=f"Request failed: {str(e)}")


@router.post("/ask/stream")
async def ask_agent_stream(request: AskRequest):
    """
    Ask the agent with streaming response.
    
    Streams server-sent events with progress updates and final answer.
    """
    async def generate():
        try:
            agent = MAAgentV2()
            
            # Send initial event
            yield f"data: {json.dumps({'event': 'start', 'question': request.question})}\n\n"
            
            # Run agent and track phases
            answer, audit = await agent.answer(
                question=request.question,
                user_id=request.user_id,
            )
            
            # Send step events
            for step in audit.steps:
                yield f"data: {json.dumps({'event': 'step', 'phase': step.phase, 'description': step.description})}\n\n"
                await asyncio.sleep(0.01)  # Small delay for streaming effect
            
            # Send final answer
            yield f"data: {json.dumps({'event': 'answer', 'answer': answer, 'run_id': audit.run_id})}\n\n"
            
            # Send metrics
            yield f"data: {json.dumps({'event': 'metrics', 'summary': audit.summary()})}\n\n"
            
            # Send done
            yield f"data: {json.dumps({'event': 'done'})}\n\n"
            
            # Store audit
            store_audit(audit)
            get_agent_metrics().record_run(audit)
            
        except Exception as e:
            yield f"data: {json.dumps({'event': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@router.get("/audit/{run_id}")
async def get_audit_trail(run_id: str):
    """
    Get the full audit trail for a specific run.
    
    Includes:
    - Every step the agent took
    - All LLM calls with tokens/costs
    - All tool calls with results
    - Decision points and reasoning
    """
    audit = get_audit(run_id)
    if not audit:
        raise HTTPException(status_code=404, detail=f"Audit not found: {run_id}")
    
    return audit


@router.get("/metrics", response_model=MetricsResponse)
async def get_metrics():
    """
    Get aggregate usage metrics.
    
    Useful for monitoring LLM costs and performance.
    """
    metrics = get_agent_metrics()
    summary = metrics.get_summary()
    
    return MetricsResponse(
        total_runs=summary.get("total_runs", 0),
        total_cost_usd=summary.get("total_cost_usd", 0.0),
        total_tokens=summary.get("total_tokens", 0),
        avg_latency_ms=summary.get("avg_latency_ms", 0.0),
        avg_llm_calls=summary.get("avg_llm_calls", 0.0),
        avg_tool_calls=summary.get("avg_tool_calls", 0.0),
        avg_confidence=summary.get("avg_confidence", 0.0),
    )


@router.get("/history")
async def get_history(
    limit: int = Query(default=50, le=100),
    user_id: Optional[str] = Query(default=None),
):
    """
    Get recent query history.
    
    Returns a list of recent runs with questions and summaries.
    """
    audits = list(_audit_store.values())
    
    # Filter by user if specified
    if user_id:
        audits = [a for a in audits if a.get("user_id") == user_id]
    
    # Sort by time descending
    audits.sort(key=lambda x: x.get("start_time", ""), reverse=True)
    
    # Return summaries
    return [
        {
            "run_id": a.get("run_id"),
            "question": a.get("question"),
            "user_id": a.get("user_id"),
            "start_time": a.get("start_time"),
            "status": a.get("status"),
            "llm_calls": a.get("total_llm_calls"),
            "cost_usd": a.get("total_cost_usd"),
            "confidence": a.get("confidence"),
        }
        for a in audits[:limit]
    ]


# =============================================================================
# COST TRACKING ENDPOINT
# =============================================================================

@router.get("/costs/daily")
async def get_daily_costs(days: int = Query(default=7, le=30)):
    """
    Get daily cost breakdown.
    
    Shows LLM costs by day for monitoring.
    """
    from datetime import timedelta
    
    audits = list(_audit_store.values())
    
    # Group by day
    daily = {}
    for a in audits:
        try:
            dt = datetime.fromisoformat(a.get("start_time", "").replace("Z", "+00:00"))
            day = dt.strftime("%Y-%m-%d")
            if day not in daily:
                daily[day] = {"cost": 0.0, "tokens": 0, "runs": 0}
            daily[day]["cost"] += a.get("total_cost_usd", 0)
            daily[day]["tokens"] += a.get("total_tokens", 0)
            daily[day]["runs"] += 1
        except:
            pass
    
    # Sort by date
    return dict(sorted(daily.items(), reverse=True)[:days])
