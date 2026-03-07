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

class DocumentContext(BaseModel):
    """A selected document or data source for context."""
    type: str  # rate_notice_advance, rate_notice_final, tech_notes_stars, cpsc, enrollment, stars, etc.
    year: int
    name: str
    isDataSource: bool = False  # True for raw data sources (cpsc, enrollment, stars, etc.)


class AskRequest(BaseModel):
    """Request to ask the agent a question."""
    question: str
    user_id: str = "api"
    include_thinking: bool = True
    document_context: Optional[List[DocumentContext]] = None


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


class DownloadFile(BaseModel):
    """A downloadable file."""
    filename: str
    display_name: str
    excel_base64: str
    row_count: int = 0

class DataLinkResult(BaseModel):
    """Result from data linking operation."""
    source_files: List[DownloadFile] = []
    combined_file: Optional[DownloadFile] = None
    join_logic: Optional[Dict] = None
    summary: Optional[Dict] = None

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
    data_link_result: Optional[DataLinkResult] = None  # Excel downloads when linking data


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

def _should_export_data(question: str) -> bool:
    """Check if user is asking to export/download data files."""
    export_keywords = ['excel', 'download', 'export', 'give me', 'get the data', 'output', 'combine', 'workbook']
    q_lower = question.lower()
    return any(kw in q_lower for kw in export_keywords)


async def _export_data_to_workbook(data_sources: List[Dict]) -> Optional[Dict]:
    """Export selected data sources to a single Excel workbook with multiple worksheets.
    
    Simple approach: each selected file becomes a worksheet in one workbook.
    User can then use Excel formulas (VLOOKUP, INDEX/MATCH) to link as needed.
    """
    import io
    import pandas as pd
    import base64
    
    print(f"[DATA EXPORT] Exporting {len(data_sources)} sources to workbook: {data_sources}")
    
    try:
        from db import get_engine
        engine = get_engine()
        
        # Table config
        table_config = {
            "cpsc": {"table": "fact_enrollment_all_years", "has_month": True, "display_name": "CPSC"},
            "enrollment": {"table": "gold_fact_enrollment_national", "has_month": True, "display_name": "Enrollment"},
            "stars": {"table": "summary_all_years", "has_month": False, "display_name": "Stars"},
            "risk_scores": {"table": "fact_risk_scores_unified", "has_month": False, "display_name": "RiskScores"},
            "snp": {"table": "fact_snp_combined", "has_month": False, "display_name": "SNP"},  # Virtual table
            # Crosswalks
            "crosswalk_contract": {"table": "gold_dim_entity", "has_month": False, "is_crosswalk": True, "display_name": "ContractXwalk"},
            "crosswalk_plan": {"table": "gold_dim_plan", "has_month": False, "is_crosswalk": True, "display_name": "PlanXwalk"},
            "crosswalk_geography": {"table": "gold_dim_geography", "has_month": False, "is_crosswalk": True, "display_name": "GeoXwalk"}
        }
        
        # Fetch each data source
        worksheets = []
        
        for ds in data_sources:
            src_id = ds["type"]
            year = ds.get("year")  # May be None for crosswalks
            
            if src_id not in table_config:
                print(f"[DATA EXPORT] Unknown source: {src_id}")
                continue
            
            config = table_config[src_id]
            table = config["table"]
            is_crosswalk = config.get("is_crosswalk", False)
            
            # Build query
            month_label = ""
            
            if table == "fact_snp_combined":
                # Special handling: union of fact_snp + fact_snp_historical
                sql = f"""
                    SELECT * FROM (
                        SELECT * FROM fact_snp WHERE year = {year}
                        UNION ALL
                        SELECT * FROM fact_snp_historical WHERE year = {year}
                    ) LIMIT 50000
                """
                df = engine.query(sql)
                sheet_name = f"{config['display_name']}_{year}"[:31]
                display_name = f"{config['display_name']} {year}"
            elif is_crosswalk:
                # Crosswalks: get all data (or filter by year if provided and table has year column)
                if year:
                    # Check if table has year column
                    try:
                        sql = f"SELECT * FROM {table} WHERE year = {year} LIMIT 50000"
                        df = engine.query(sql)
                    except:
                        # Table doesn't have year column, get all
                        sql = f"SELECT * FROM {table} LIMIT 50000"
                        df = engine.query(sql)
                else:
                    sql = f"SELECT * FROM {table} LIMIT 50000"
                    df = engine.query(sql)
                sheet_name = config['display_name'][:31]
                display_name = config['display_name']
            elif config.get("has_month"):
                # Monthly data: get latest month
                month_sql = f"SELECT MAX(month) as m FROM {table} WHERE year = {year}"
                month_result = engine.query(month_sql)
                latest_month = int(month_result.iloc[0]['m']) if not month_result.empty else 12
                month_filter = f" AND month = {latest_month}"
                month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                month_label = f"_{month_names[latest_month]}"
                sql = f"SELECT * FROM {table} WHERE year = {year}{month_filter} LIMIT 50000"
                df = engine.query(sql)
                sheet_name = f"{config['display_name']}_{year}{month_label}"[:31]
                display_name = f"{config['display_name']} {year}{month_label}"
            else:
                # Yearly data
                sql = f"SELECT * FROM {table} WHERE year = {year} LIMIT 50000"
                df = engine.query(sql)
                sheet_name = f"{config['display_name']}_{year}"[:31]
                display_name = f"{config['display_name']} {year}"
            
            print(f"[DATA EXPORT] Fetching {src_id}: {sql[:100]}...")
            print(f"[DATA EXPORT] Got {len(df)} rows for {src_id}")
            
            worksheets.append({
                "sheet_name": sheet_name,
                "source_id": src_id,
                "year": year,
                "display_name": display_name,
                "row_count": len(df),
                "columns": list(df.columns),
                "df": df
            })
        
        if not worksheets:
            return {"success": False, "error": "No data sources could be loaded"}
        
        # Create single Excel workbook with multiple sheets
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for ws in worksheets:
                ws["df"].to_excel(writer, sheet_name=ws["sheet_name"], index=False)
        output.seek(0)
        workbook_base64 = base64.b64encode(output.read()).decode('utf-8')
        
        # Build sheet info for response
        sheets_info = []
        for ws in worksheets:
            sheets_info.append({
                "name": ws["sheet_name"],
                "display_name": ws["display_name"],
                "row_count": ws["row_count"],
                "columns": ws["columns"][:10],  # First 10 columns for preview
                "key_column": "contract_id" if "contract_id" in ws["columns"] else ws["columns"][0]
            })
        
        print(f"[DATA EXPORT] Success! Created workbook with {len(worksheets)} sheets")
        
        return {
            "success": True,
            "workbook_file": {
                "filename": "cms_data_export.xlsx",
                "display_name": "CMS Data Export",
                "excel_base64": workbook_base64,
                "total_rows": sum(ws["row_count"] for ws in worksheets)
            },
            "sheets": sheets_info,
            "summary": {
                "sheet_count": len(worksheets),
                "total_rows": sum(ws["row_count"] for ws in worksheets),
                "linking_tip": "Use VLOOKUP or INDEX/MATCH on contract_id to link data between sheets"
            }
        }
        
    except Exception as e:
        print(f"[DATA EXPORT] Error: {e}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}


@router.post("/ask", response_model=AskResponse)
async def ask_agent(request: AskRequest):
    """
    Ask the agent a question.
    
    Returns a complete response with:
    - Text response
    - Charts and tables
    - Full thinking process (if include_thinking=True)
    - Sources and confidence
    - Data link result with Excel downloads (when linking data sources)
    """
    try:
        agent = get_agent_v3()
        
        # Convert document_context to dict format for agent
        doc_context = None
        data_sources = []
        if request.document_context:
            doc_context = [
                {"type": d.type, "year": d.year, "name": d.name, "isDataSource": d.isDataSource}
                for d in request.document_context
            ]
            # Extract just the data sources
            data_sources = [d for d in doc_context if d.get("isDataSource", False)]
        
        result = await agent.answer(
            request.question, 
            request.user_id,
            document_context=doc_context
        )
        
        response_dict = result.to_dict()
        
        # Check if user wants to export data files
        data_link_result = None
        
        print(f"[ASK] Data sources: {len(data_sources)}, Question contains export keywords: {_should_export_data(request.question)}")
        
        # Export if user asks OR if they have multiple data sources selected (convenience)
        if len(data_sources) >= 1 and _should_export_data(request.question):
            print(f"[ASK] Exporting {len(data_sources)} data sources to workbook...")
            try:
                export_result = await _export_data_to_workbook(data_sources)
                print(f"[ASK] Export result: success={export_result.get('success') if export_result else 'N/A'}")
                
                if export_result and export_result.get("success"):
                    # Convert to data_link_result format for frontend compatibility
                    workbook = export_result.get("workbook_file", {})
                    sheets = export_result.get("sheets", [])
                    summary = export_result.get("summary", {})
                    
                    # Build sheet list for response text
                    sheet_list = ", ".join([f"{s['display_name']} ({s['row_count']:,} rows)" for s in sheets])
                    
                    data_link_result = {
                        "source_files": [],  # No individual files in simple mode
                        "combined_file": {
                            "filename": workbook.get("filename", "cms_data_export.xlsx"),
                            "display_name": workbook.get("display_name", "CMS Data Export"),
                            "excel_base64": workbook.get("excel_base64", ""),
                            "row_count": workbook.get("total_rows", 0)
                        },
                        "join_logic": {
                            "sources_linked": [s["display_name"] for s in sheets],
                            "join_keys_used": ["contract_id"],
                            "join_type": "Separate worksheets (link with VLOOKUP/INDEX-MATCH)",
                            "explanation": summary.get("linking_tip", "Use contract_id to link between sheets")
                        },
                        "summary": {
                            "sources_count": len(sheets),
                            "total_source_rows": summary.get("total_rows", 0),
                            "combined_rows": summary.get("total_rows", 0)
                        }
                    }
                    
                    response_dict["response"] += f"\n\n**Excel Ready:** Your data has been exported to a single workbook with {len(sheets)} worksheets: {sheet_list}.\n\nTo link the data, use Excel formulas like `=VLOOKUP(A2, Stars!A:Z, 2, FALSE)` on the `contract_id` column."
                else:
                    error = export_result.get("error") if export_result else "Unknown error"
                    response_dict["response"] += f"\n\n**Note:** Could not export data: {error}"
            except Exception as e:
                print(f"[ASK] Export error: {e}")
                response_dict["response"] += f"\n\n**Note:** Could not generate Excel file: {str(e)}"
        
        response_dict["data_link_result"] = data_link_result
        
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
