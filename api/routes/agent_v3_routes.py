"""
Agent V3 Routes - FastAPI endpoints for the transparent AI agent.

Endpoints:
- POST /api/v3/agent/ask - Ask a question (non-streaming)
- POST /api/v3/agent/ask/stream - Ask with SSE streaming (for thinking updates)
- GET /api/v3/agent/tools - List available tools
"""

import json
import asyncio
import traceback
from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.services.agent_v3 import get_agent_v3, MAAgentV3
from api.services.tool_definitions import get_tool_definitions


router = APIRouter(prefix="/api/v3/agent", tags=["agent-v3"])


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class AskRequest(BaseModel):
    """Request to ask the agent a question."""
    question: str
    user_id: str = "api"
    include_thinking: bool = True


class ThinkingStepResponse(BaseModel):
    """Single thinking step in response."""
    id: str
    phase: str
    title: str
    content: str
    status: str
    duration_ms: int = 0
    tool_name: Optional[str] = None
    tool_params: Optional[Dict] = None
    service_called: Optional[str] = None
    row_count: Optional[int] = None
    validations: Optional[List[Dict]] = None
    confidence: Optional[str] = None


class ThinkingResponse(BaseModel):
    """Full thinking process in response."""
    query_id: str
    question: str
    steps: List[ThinkingStepResponse]
    total_duration_ms: int
    total_tokens: int
    tools_called: int
    status: str


class ChartResponse(BaseModel):
    """Chart specification in response."""
    type: str
    title: str
    data: List[Dict]
    xKey: Optional[str] = None
    yKeys: Optional[List[str]] = None
    colors: Optional[List[str]] = None


class TableResponse(BaseModel):
    """Table specification in response."""
    type: str = "table"
    title: str
    data: List[Dict]
    columns: Optional[List[str]] = None


class AskResponse(BaseModel):
    """Full response from agent."""
    status: str
    response: str
    charts: List[Dict] = []
    tables: List[Dict] = []
    thinking: Optional[Dict] = None
    sources: List[str] = []
    confidence: str = "high"
    error: Optional[str] = None


class ToolInfo(BaseModel):
    """Information about an available tool."""
    name: str
    description: str
    parameters: Dict


class ToolsResponse(BaseModel):
    """List of available tools."""
    tools: List[ToolInfo]
    context: str


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.post("/ask", response_model=AskResponse)
async def ask_agent(request: AskRequest):
    """
    Ask the agent a question.
    
    Returns a complete response with:
    - Text response
    - Charts and tables
    - Full thinking process (if include_thinking=True)
    - Sources and confidence
    """
    try:
        agent = get_agent_v3()
        result = await agent.answer(request.question, request.user_id)
        
        response_dict = result.to_dict()
        
        # Remove thinking if not requested
        if not request.include_thinking:
            response_dict['thinking'] = None
        
        return AskResponse(**response_dict)
        
    except Exception as e:
        print(f"Agent V3 error: {e}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Agent error: {str(e)}"
        )


@router.post("/ask/stream")
async def ask_agent_stream(request: AskRequest):
    """
    Ask the agent with Server-Sent Events (SSE) streaming.
    
    Streams thinking updates in real-time, then final response.
    
    Event types:
    - thinking: Partial thinking updates
    - response: Final complete response
    - error: Error occurred
    """
    async def generate():
        try:
            agent = get_agent_v3()
            
            # For now, we don't have true streaming in the agent
            # but we can simulate progress updates
            # TODO: Implement true streaming in agent
            
            # Send initial event
            yield f"data: {json.dumps({'type': 'start', 'query_id': 'stream'})}\n\n"
            
            # Get the full response
            result = await agent.answer(request.question, request.user_id)
            
            # Send thinking steps as events
            if result.thinking:
                for step in result.thinking.steps:
                    yield f"data: {json.dumps({'type': 'thinking', 'step': step.to_dict()})}\n\n"
                    await asyncio.sleep(0.05)  # Small delay for UI effect
            
            # Send final response
            response_dict = result.to_dict()
            yield f"data: {json.dumps({'type': 'response', 'data': response_dict})}\n\n"
            
            # Send done event
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
            
        except Exception as e:
            print(f"Stream error: {e}")
            traceback.print_exc()
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/tools", response_model=ToolsResponse)
async def list_tools():
    """
    List all available tools the agent can use.
    
    Useful for understanding what queries are possible.
    """
    try:
        tool_defs = get_tool_definitions()
        tools = tool_defs.get_tool_definitions()
        context = tool_defs.get_system_prompt_context()
        
        tool_infos = [
            ToolInfo(
                name=t['name'],
                description=t['description'],
                parameters=t['input_schema']
            )
            for t in tools
        ]
        
        return ToolsResponse(
            tools=tool_infos,
            context=context
        )
        
    except Exception as e:
        print(f"Tools list error: {e}")
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Error listing tools: {str(e)}"
        )


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "3.0",
        "timestamp": datetime.now().isoformat()
    }
