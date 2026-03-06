"""
AI Chat API Endpoint
=====================

Provides natural language interface to query MA data using the MA Intelligence Agent.

Endpoints:
- POST /api/chat - Process a chat message
- POST /api/chat/stream - Stream a response
- POST /api/chat/feedback - Submit feedback on a response
- GET /api/chat/stats - Get usage and feedback statistics

The agent uses:
- Multi-LLM support (Claude, GPT-4)
- Tool-based data queries
- Self-learning from user feedback
- Confidence scores and source citations
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
import json
import asyncio
from datetime import datetime

# Import agent components
try:
    from api.services.ma_agent import MAIntelligenceAgent, get_ma_agent, AgentResponse
    from api.services.learning_store import get_learning_store
    AGENT_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Agent not available: {e}")
    AGENT_AVAILABLE = False

# Fallback to rule-based if agent not available
try:
    from services.data_service import get_data_service
    FALLBACK_AVAILABLE = True
except ImportError:
    FALLBACK_AVAILABLE = False

router = APIRouter(prefix="/api", tags=["chat"])


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: Optional[List[ChatMessage]] = None
    provider: Optional[str] = "anthropic"  # anthropic, openai
    user_id: Optional[str] = "anonymous"


class ChatResponse(BaseModel):
    response: str
    query_id: Optional[str] = None
    confidence: Optional[float] = None
    sources: Optional[List[str]] = None
    tools_used: Optional[List[Dict[str, Any]]] = None
    sql_executed: Optional[List[str]] = None
    warnings: Optional[List[str]] = None
    data: Optional[List[Dict[str, Any]]] = None
    chart: Optional[Dict[str, Any]] = None
    audit: Optional[Dict[str, Any]] = None
    reasoning: Optional[List[Dict[str, Any]]] = None  # Step-by-step thought process


class FeedbackRequest(BaseModel):
    query_id: str
    rating: str  # 'positive', 'negative', 'correction'
    original_question: str
    original_response: str
    correction: Optional[str] = None
    correct_answer: Optional[str] = None
    user_id: Optional[str] = "anonymous"


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Process a chat message and return a response with data.
    
    Uses the MA Intelligence Agent with:
    - Tool-based data queries
    - Knowledge base lookup
    - Self-learning from feedback
    """
    if not AGENT_AVAILABLE:
        return await _fallback_chat(request)
    
    try:
        agent = get_ma_agent(provider=request.provider or "anthropic")
        
        # Convert history to dict format
        history = None
        if request.history:
            history = [{"role": m.role, "content": m.content} for m in request.history]
        
        # Get response from agent
        response = await agent.answer(
            question=request.message,
            user_id=request.user_id or "anonymous",
            history=history,
        )
        
        # Extract any data from tool results for visualization
        data = None
        if response.tools_used:
            for tool in response.tools_used:
                if tool.get("tool") == "query_database" and tool.get("success"):
                    # We'd need to store the actual data - for now just note it was queried
                    pass
        
        # Build reasoning steps from tools used
        reasoning = []
        for i, tool in enumerate(response.tools_used):
            tool_name = tool.get("tool", "unknown")
            args = tool.get("arguments", {})
            success = tool.get("success", False)
            
            # Translate tool calls into human-readable reasoning steps
            if tool_name == "calculate_metric":
                metric = args.get("metric", "metric")
                filters = args.get("filters", {})
                payer = filters.get("parent_org", "industry")
                step = f"Calculating {metric} for {payer}"
            elif tool_name == "query_database":
                context = args.get("context", "data")
                step = f"Querying database: {context}"
            elif tool_name == "lookup_knowledge":
                query = args.get("query", "term")
                step = f"Looking up: {query}"
            elif tool_name == "get_payer_info":
                payer = args.get("payer_name", "payer")
                step = f"Getting info for {payer}"
            else:
                step = f"Using {tool_name}"
            
            reasoning.append({
                "step": i + 1,
                "action": step,
                "tool": tool_name,
                "success": success,
                "sql": response.sql_executed[i] if i < len(response.sql_executed) else None,
            })
        
        return ChatResponse(
            response=response.answer,
            query_id=response.query_id,
            confidence=response.confidence,
            sources=response.sources,
            tools_used=response.tools_used,
            sql_executed=response.sql_executed,
            warnings=response.warnings,
            data=data,
            reasoning=reasoning if reasoning else None,
            audit={
                "query_id": response.query_id,
                "provider": response.metadata.get("provider"),
                "model": response.metadata.get("model"),
                "latency_ms": response.metadata.get("latency_ms"),
                "tools_count": len(response.tools_used),
                "sql_count": len(response.sql_executed),
                "learning_context_used": response.metadata.get("learning_context_used", False),
            }
        )
        
    except Exception as e:
        return ChatResponse(
            response=f"I encountered an error: {str(e)}. Please try rephrasing your question.",
            warnings=[str(e)],
        )


@router.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """
    Stream a chat response.
    
    Returns server-sent events with text chunks.
    """
    if not AGENT_AVAILABLE:
        raise HTTPException(status_code=501, detail="Streaming requires agent module")
    
    async def generate():
        try:
            agent = get_ma_agent(provider=request.provider or "anthropic")
            
            history = None
            if request.history:
                history = [{"role": m.role, "content": m.content} for m in request.history]
            
            async for chunk in agent.answer_streaming(
                question=request.message,
                user_id=request.user_id or "anonymous",
                history=history,
            ):
                yield f"data: {json.dumps({'text': chunk})}\n\n"
            
            yield f"data: {json.dumps({'done': True})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@router.post("/chat/feedback")
async def submit_feedback(request: FeedbackRequest):
    """
    Submit feedback on a chat response.
    
    Supports:
    - positive: Thumbs up
    - negative: Thumbs down
    - correction: User provides the correct answer
    
    Feedback is used to improve future responses.
    """
    if not AGENT_AVAILABLE:
        raise HTTPException(status_code=501, detail="Feedback requires agent module")
    
    try:
        agent = get_ma_agent()
        
        await agent.submit_feedback(
            query_id=request.query_id,
            user_id=request.user_id or "anonymous",
            rating=request.rating,
            original_question=request.original_question,
            original_response=request.original_response,
            correction=request.correction,
            correct_answer=request.correct_answer,
        )
        
        return {
            "status": "success",
            "message": f"Thank you for your feedback! This helps improve future responses.",
            "feedback_type": request.rating,
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chat/stats")
async def get_chat_stats():
    """
    Get usage and feedback statistics.
    
    Returns:
    - Total feedback count
    - Satisfaction rate
    - Most used tools
    - Example count
    - Rule count
    """
    if not AGENT_AVAILABLE:
        return {"error": "Stats require agent module"}
    
    try:
        learning_store = get_learning_store()
        
        stats = learning_store.get_feedback_stats()
        
        return {
            "feedback": stats,
            "examples_count": len(learning_store._examples_cache),
            "rules_count": len(learning_store._rules_cache),
            "recent_feedback": [
                {
                    "rating": f.rating,
                    "question": f.original_question[:100],
                    "created_at": f.created_at,
                }
                for f in learning_store.get_recent_feedback(5)
            ],
        }
        
    except Exception as e:
        return {"error": str(e)}


@router.get("/chat/examples")
async def get_examples(domain: Optional[str] = None, limit: int = 10):
    """
    Get learned examples for a domain.
    
    Useful for understanding what the system has learned.
    """
    if not AGENT_AVAILABLE:
        return {"examples": []}
    
    try:
        learning_store = get_learning_store()
        examples = learning_store.get_best_examples(domain=domain, limit=limit)
        
        return {
            "domain": domain,
            "examples": [
                {
                    "question": ex.question,
                    "answer": ex.answer[:300],
                    "domain": ex.domain,
                    "rating": ex.average_rating,
                    "tools_used": ex.tools_used,
                }
                for ex in examples
            ]
        }
        
    except Exception as e:
        return {"error": str(e)}


@router.get("/chat/rules")
async def get_rules():
    """
    Get all correction rules.
    
    Rules are learned from user corrections.
    """
    if not AGENT_AVAILABLE:
        return {"rules": []}
    
    try:
        learning_store = get_learning_store()
        rules = learning_store.get_all_rules()
        
        return {
            "rules": [
                {
                    "trigger": rule.trigger_pattern,
                    "correct_behavior": rule.correct_behavior[:200],
                    "confidence": rule.confidence,
                    "times_applied": rule.times_applied,
                }
                for rule in rules
            ]
        }
        
    except Exception as e:
        return {"error": str(e)}


async def _fallback_chat(request: ChatRequest) -> ChatResponse:
    """
    Fallback to simple rule-based chat when agent is not available.
    """
    message = request.message.lower()
    
    # Simple keyword-based responses
    if "enrollment" in message:
        return ChatResponse(
            response="I can help with enrollment data. The MA Intelligence Agent is currently initializing. Please try again in a moment or check the Enrollment page for visual data.",
            warnings=["Agent module not fully initialized"],
        )
    elif "star" in message or "rating" in message:
        return ChatResponse(
            response="I can help with star ratings. The MA Intelligence Agent is currently initializing. Please check the Stars page for detailed ratings data.",
            warnings=["Agent module not fully initialized"],
        )
    elif "risk" in message:
        return ChatResponse(
            response="I can help with risk scores. The MA Intelligence Agent is currently initializing. Please check the Risk Scores page for detailed data.",
            warnings=["Agent module not fully initialized"],
        )
    else:
        return ChatResponse(
            response="I'm the MA Intelligence Assistant. I can help with Medicare Advantage enrollment, star ratings, risk scores, and policy questions. The full agent is initializing - please try again in a moment.",
            warnings=["Agent module not fully initialized"],
        )


def register_routes(app):
    """Register chat routes with the main FastAPI app."""
    app.include_router(router)
