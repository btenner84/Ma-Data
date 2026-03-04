"""
AI Chat API Endpoint
=====================

Provides natural language interface to query MA data.
Uses the semantic layer (metrics_catalog.yaml) and LLM to:
1. Parse user questions
2. Generate appropriate SQL queries
3. Execute queries via data_service
4. Format responses with audit metadata

Endpoints:
- POST /api/chat - Process a chat message
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
import yaml
import json
import re
from datetime import datetime

from services.data_service import get_data_service, UnifiedDataService

router = APIRouter(prefix="/api", tags=["chat"])

METRICS_CATALOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "metrics_catalog.yaml"
)


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: Optional[List[ChatMessage]] = None


class ChatResponse(BaseModel):
    response: str
    data: Optional[List[Dict[str, Any]]] = None
    chart: Optional[Dict[str, Any]] = None
    audit: Optional[Dict[str, Any]] = None


def load_metrics_catalog() -> dict:
    """Load the metrics catalog for context."""
    try:
        with open(METRICS_CATALOG_PATH, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        return {"error": str(e)}


def extract_query_intent(message: str, catalog: dict) -> dict:
    """
    Simple rule-based intent extraction.
    In production, this would use an LLM.
    
    Returns: {
        'metric': str,
        'filters': dict,
        'dimensions': list,
        'domain': str
    }
    """
    message_lower = message.lower()
    
    intent = {
        'metric': 'enrollment',
        'filters': {},
        'dimensions': [],
        'domain': 'enrollment',
    }
    
    if 'star' in message_lower or 'rating' in message_lower:
        intent['domain'] = 'stars'
        intent['metric'] = 'overall_rating'
        if '4+' in message_lower or 'four plus' in message_lower:
            intent['metric'] = 'four_plus_pct'
    elif 'risk' in message_lower:
        intent['domain'] = 'risk_scores'
        intent['metric'] = 'avg_risk_score'
    
    year_match = re.search(r'20\d{2}', message)
    if year_match:
        intent['filters']['year'] = int(year_match.group())
    else:
        intent['filters']['year'] = 2026
    
    payer_keywords = {
        'humana': 'Humana Inc.',
        'united': 'UnitedHealth Group, Inc.',
        'cvs': 'CVS Health Corporation',
        'aetna': 'CVS Health Corporation',
        'cigna': 'Cigna Healthcare',
        'elevance': 'Elevance Health, Inc.',
        'anthem': 'Elevance Health, Inc.',
        'kaiser': 'Kaiser Foundation Health Plan, Inc.',
        'centene': 'Centene Corporation',
    }
    
    for keyword, org_name in payer_keywords.items():
        if keyword in message_lower:
            intent['filters']['parent_org'] = org_name
            break
    
    state_patterns = [
        (r'\b(california|ca)\b', 'CA'),
        (r'\b(texas|tx)\b', 'TX'),
        (r'\b(florida|fl)\b', 'FL'),
        (r'\b(new york|ny)\b', 'NY'),
        (r'\b(pennsylvania|pa)\b', 'PA'),
        (r'\b(ohio|oh)\b', 'OH'),
        (r'\b(illinois|il)\b', 'IL'),
    ]
    
    for pattern, state_code in state_patterns:
        if re.search(pattern, message_lower):
            intent['filters']['state'] = state_code
            break
    
    if 'd-snp' in message_lower or 'dual' in message_lower:
        intent['filters']['snp_type'] = 'D-SNP'
    elif 'c-snp' in message_lower or 'chronic' in message_lower:
        intent['filters']['snp_type'] = 'C-SNP'
    elif 'i-snp' in message_lower or 'institutional' in message_lower:
        intent['filters']['snp_type'] = 'I-SNP'
    
    if 'over time' in message_lower or 'trend' in message_lower or 'changed' in message_lower:
        intent['dimensions'].append('year')
    
    if 'by state' in message_lower:
        intent['dimensions'].append('state')
    if 'by payer' in message_lower or 'by parent' in message_lower:
        intent['dimensions'].append('parent_org')
    if 'by plan type' in message_lower:
        intent['dimensions'].append('plan_type')
    
    return intent


def format_response(intent: dict, result, catalog: dict) -> ChatResponse:
    """Format the query result into a chat response."""
    
    if hasattr(result, 'data') and result.data.get('error'):
        return ChatResponse(
            response=f"I encountered an error: {result.data['error']}",
            audit=result.audit.__dict__ if result.audit else None
        )
    
    rows = result.data.get('rows', []) if hasattr(result, 'data') else []
    
    metric_name = intent.get('metric', 'data')
    year = intent['filters'].get('year', 2026)
    parent_org = intent['filters'].get('parent_org')
    
    if not rows:
        response = f"I couldn't find any data matching your query for {year}."
    elif len(rows) == 1 and not intent.get('dimensions'):
        row = rows[0]
        value = row.get(metric_name) or row.get('total_enrollment') or row.get('enrollment')
        if value:
            if isinstance(value, (int, float)) and value > 1000:
                formatted_value = f"{value:,.0f}"
            elif isinstance(value, float):
                formatted_value = f"{value:.2f}"
            else:
                formatted_value = str(value)
            
            context = f" for {parent_org}" if parent_org else ""
            response = f"The {metric_name.replace('_', ' ')}{context} in {year} is **{formatted_value}**."
        else:
            response = f"Here's what I found for {year}."
    else:
        response = f"Here are the results for your query:\n\n"
        if parent_org:
            response = f"Here are the results for {parent_org}:\n\n"
    
    audit_dict = None
    if hasattr(result, 'audit') and result.audit:
        audit_dict = {
            'query_id': result.audit.query_id,
            'sql': result.audit.sql,
            'tables_queried': result.audit.tables_queried,
            'filters_applied': result.audit.filters_applied,
            'row_count': result.audit.row_count,
            'source_files': result.audit.source_files,
            'executed_at': result.audit.executed_at.isoformat() if result.audit.executed_at else None,
            'execution_ms': result.audit.execution_ms,
        }
    
    return ChatResponse(
        response=response,
        data=rows if rows else None,
        audit=audit_dict
    )


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Process a chat message and return a response with data.
    
    The endpoint:
    1. Parses the user's question to extract intent
    2. Executes the appropriate query via data_service
    3. Formats a natural language response with data
    """
    catalog = load_metrics_catalog()
    intent = extract_query_intent(request.message, catalog)
    
    try:
        service = get_data_service()
        
        if intent.get('dimensions'):
            result = service.timeseries(
                metric='enrollment' if intent['domain'] == 'enrollment' else intent['metric'],
                filters=intent['filters'],
                source='national',
                group_by=intent['dimensions'][0] if intent['dimensions'] else None
            )
        elif intent['domain'] == 'stars':
            result = service.get_stars_distribution(
                year=intent['filters'].get('year', 2026),
                filters=intent['filters']
            )
        elif intent['domain'] == 'risk_scores':
            result = service.get_risk_scores_by_parent(
                year=intent['filters'].get('year', 2026),
                filters=intent['filters']
            )
        else:
            result = service.get_enrollment_summary(
                year=intent['filters'].get('year', 2026),
                month=intent['filters'].get('month', 1),
                filters=intent['filters']
            )
        
        return format_response(intent, result, catalog)
        
    except Exception as e:
        return ChatResponse(
            response=f"I encountered an error processing your question: {str(e)}",
            data=None,
            audit=None
        )


def register_routes(app):
    """Register chat routes with the main FastAPI app."""
    app.include_router(router)
