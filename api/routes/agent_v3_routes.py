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

def _should_link_data(question: str) -> bool:
    """Check if user is asking to link/combine data."""
    link_keywords = ['link', 'join', 'combine', 'merge', 'connect', 'excel', 'download', 'export']
    q_lower = question.lower()
    return any(kw in q_lower for kw in link_keywords)


async def _perform_data_linking(data_sources: List[Dict]) -> Optional[Dict]:
    """Call the data linking endpoint and return Excel files."""
    import io
    import pandas as pd
    import base64
    
    print(f"[DATA LINK] Starting data linking for {len(data_sources)} sources: {data_sources}")
    
    try:
        from db import get_engine
        engine = get_engine()
        
        # Table config with ACTUAL join keys based on real schema
        table_config = {
            "cpsc": {
                "table": "fact_enrollment_all_years", 
                "has_month": True,
                "join_keys": ["contract_id", "year"],  # CPSC has contract_id
                "display_name": "CPSC Enrollment"
            },
            "enrollment": {
                "table": "gold_fact_enrollment_national", 
                "has_month": True,
                "join_keys": ["contract_id", "year"],  # HAS contract_id and plan_id!
                "display_name": "Monthly Enrollment by Plan"
            },
            "stars": {
                "table": "summary_all_years", 
                "has_month": False,
                "join_keys": ["contract_id", "year"],
                "display_name": "Star Ratings"
            },
            "risk_scores": {
                "table": "fact_risk_scores_unified", 
                "has_month": False,
                "join_keys": ["contract_id", "year"],
                "display_name": "Risk Scores"
            },
            "snp": {
                "table": "fact_snp_historical", 
                "has_month": False,
                "join_keys": ["contract_id", "year"],
                "display_name": "SNP Classification"
            }
        }
        
        # Fetch each data source
        dataframes = {}
        source_info = []
        
        for ds in data_sources:
            src_id = ds["type"]
            year = ds["year"]
            
            if src_id not in table_config:
                print(f"[DATA LINK] Unknown source: {src_id}")
                continue
            
            config = table_config[src_id]
            table = config["table"]
            
            # Build query with month filter if applicable
            month_filter = ""
            if config["has_month"]:
                month_sql = f"SELECT MAX(month) as m FROM {table} WHERE year = {year}"
                month_result = engine.query(month_sql)
                latest_month = int(month_result.iloc[0]['m']) if not month_result.empty else 12
                month_filter = f" AND month = {latest_month}"
            
            # Fetch data (limit to prevent memory issues - 10k rows is ~2-5MB Excel)
            sql = f"SELECT * FROM {table} WHERE year = {year}{month_filter} LIMIT 10000"
            print(f"[DATA LINK] Fetching {src_id}: {sql[:100]}...")
            df = engine.query(sql)
            print(f"[DATA LINK] Got {len(df)} rows for {src_id}")
            
            key = f"{src_id}_{year}"
            dataframes[key] = df
            source_info.append({
                "key": key,
                "source_id": src_id,
                "year": year,
                "display_name": config["display_name"],
                "join_keys": config["join_keys"],
                "row_count": len(df),
                "columns": list(df.columns)
            })
        
        if len(dataframes) < 2:
            print(f"[DATA LINK] Not enough data sources fetched: {len(dataframes)}")
            return None
        
        # Determine join keys (find common keys between sources)
        all_join_keys = [set(s["join_keys"]) for s in source_info]
        common_keys = all_join_keys[0]
        for keys in all_join_keys[1:]:
            common_keys = common_keys.intersection(keys)
        
        join_keys = list(common_keys) if common_keys else ["year"]
        print(f"[DATA LINK] Common join keys: {join_keys}")
        
        # Check if this will be a useful join or a cartesian disaster
        if join_keys == ["year"]:
            # Only joining on year - this will create a cartesian product
            # Return a warning instead of garbage data
            source_names = [s["display_name"] for s in source_info]
            return {
                "success": False,
                "error": f"Cannot meaningfully link {' + '.join(source_names)}. They only share 'year' as a common column, which would create a useless cartesian product. For contract-level linking with Stars, use CPSC instead of Enrollment National."
            }
        
        print(f"[DATA LINK] Using join keys: {join_keys}")
        
        # Smart linking strategy:
        # 1. Stars should be the BASE (one row per contract) 
        # 2. Enrollment-type data gets AGGREGATED and added as columns to Stars
        
        source_ids = [s["source_id"] for s in source_info]
        has_stars = "stars" in source_ids
        has_enrollment = "enrollment" in source_ids
        has_cpsc = "cpsc" in source_ids
        
        if has_stars and (has_enrollment or has_cpsc):
            # STAR-CENTRIC OUTPUT: Stars as base, add aggregated enrollment column
            print(f"[DATA LINK] Star-centric linking: Stars as base with enrollment aggregated")
            
            # Get stars dataframe (this is the base)
            stars_key = next(k for k in dataframes.keys() if k.startswith("stars"))
            stars_df = dataframes[stars_key].copy()
            stars_year = next(s["year"] for s in source_info if s["source_id"] == "stars")
            print(f"[DATA LINK] Stars base: {len(stars_df)} contracts")
            
            # Aggregate enrollment by contract
            if has_enrollment:
                enroll_key = next(k for k in dataframes.keys() if k.startswith("enrollment"))
                enroll_df = dataframes[enroll_key]
                
                # Aggregate to contract level - sum enrollment
                enroll_agg = enroll_df.groupby('contract_id').agg({
                    'enrollment': 'sum'
                }).reset_index()
                enroll_agg = enroll_agg.rename(columns={'enrollment': 'total_enrollment'})
                print(f"[DATA LINK] Enrollment aggregated: {len(enroll_agg)} contracts, total enrollment: {enroll_agg['total_enrollment'].sum():,.0f}")
                
                # Left join: Stars + enrollment column
                combined_df = pd.merge(
                    stars_df,
                    enroll_agg[['contract_id', 'total_enrollment']],
                    on='contract_id',
                    how='left'
                )
                # Fill NaN enrollment with 0
                combined_df['total_enrollment'] = combined_df['total_enrollment'].fillna(0).astype(int)
                
            elif has_cpsc:
                cpsc_key = next(k for k in dataframes.keys() if k.startswith("cpsc"))
                cpsc_df = dataframes[cpsc_key]
                
                # Aggregate CPSC to contract level
                cpsc_agg = cpsc_df.groupby('contract_id').agg({
                    'enrollment': 'sum'
                }).reset_index()
                cpsc_agg = cpsc_agg.rename(columns={'enrollment': 'total_enrollment'})
                print(f"[DATA LINK] CPSC aggregated: {len(cpsc_agg)} contracts, total enrollment: {cpsc_agg['total_enrollment'].sum():,.0f}")
                
                # Left join: Stars + enrollment column
                combined_df = pd.merge(
                    stars_df,
                    cpsc_agg[['contract_id', 'total_enrollment']],
                    on='contract_id',
                    how='left'
                )
                combined_df['total_enrollment'] = combined_df['total_enrollment'].fillna(0).astype(int)
            
            print(f"[DATA LINK] Combined: {len(combined_df)} rows (same as Stars base)")
            
        else:
            # Generic linking for other combinations
            keys = list(dataframes.keys())
            processed_dfs = {}
            
            for key, df in dataframes.items():
                processed_dfs[key] = df
            
            # Perform the join
            combined_df = processed_dfs[keys[0]].copy()
            
            for i, key in enumerate(keys[1:], 1):
                right_df = processed_dfs[key]
                suffix = f"_{source_info[i]['source_id']}"
                
                # Perform LEFT join
                combined_df = pd.merge(
                    combined_df, 
                    right_df, 
                    on=join_keys, 
                    how='left',
                    suffixes=('', suffix)
                )
            
            print(f"[DATA LINK] Combined: {len(combined_df)} rows")
        
        # Create Excel files as base64
        def df_to_excel_base64(df, sheet_name="Data"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            output.seek(0)
            return base64.b64encode(output.read()).decode('utf-8')
        
        # Generate individual source files
        source_files = []
        for key, df in dataframes.items():
            info = next(s for s in source_info if s["key"] == key)
            source_files.append({
                "filename": f"{info['source_id']}_{info['year']}.xlsx",
                "display_name": f"{info['display_name']} ({info['year']})",
                "row_count": len(df),
                "excel_base64": df_to_excel_base64(df, f"{info['source_id']}_{info['year']}")
            })
        
        # Generate combined file
        combined_file = {
            "filename": "combined_linked_data.xlsx",
            "display_name": "Combined Linked Data",
            "row_count": len(combined_df),
            "excel_base64": df_to_excel_base64(combined_df, "Combined")
        }
        
        # Build join logic explanation
        source_names = [s["display_name"] for s in source_info]
        
        if has_stars and (has_enrollment or has_cpsc):
            enroll_type = "Monthly Enrollment by Plan" if has_enrollment else "CPSC Enrollment"
            join_logic = {
                "sources_linked": source_names,
                "join_keys_used": ["contract_id"],
                "join_type": "LEFT JOIN (Stars as base + aggregated enrollment)",
                "explanation": f"Stars data with total_enrollment column added. Enrollment was aggregated (SUM) by contract_id from {enroll_type}.",
            }
        else:
            join_logic = {
                "sources_linked": source_names,
                "join_keys_used": join_keys,
                "join_type": "LEFT JOIN",
                "explanation": f"Linked {' + '.join(source_names)} using columns: {', '.join(join_keys)}",
            }
        
        print(f"[DATA LINK] Success! Generated {len(source_files)} source files + combined file")
        
        return {
            "success": True,
            "source_files": source_files,
            "combined_file": combined_file,
            "join_logic": join_logic,
            "summary": {
                "sources_count": len(data_sources),
                "total_source_rows": sum(s["row_count"] for s in source_info),
                "combined_rows": len(combined_df)
            }
        }
        
    except Exception as e:
        print(f"[DATA LINK] Failed: {e}")
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
        
        # Check if user wants to link data and has 2+ data sources selected
        data_link_result = None
        link_error = None
        
        print(f"[ASK] Data sources: {len(data_sources)}, Question contains link keywords: {_should_link_data(request.question)}")
        
        if len(data_sources) >= 2 and _should_link_data(request.question):
            print(f"[ASK] Attempting to link {len(data_sources)} data sources...")
            try:
                link_result = await _perform_data_linking(data_sources)
                print(f"[ASK] Link result: {link_result is not None}, success: {link_result.get('success') if link_result else 'N/A'}")
                
                if link_result and link_result.get("success"):
                    data_link_result = {
                        "source_files": link_result.get("source_files", []),
                        "combined_file": link_result.get("combined_file"),
                        "join_logic": link_result.get("join_logic"),
                        "summary": link_result.get("summary")
                    }
                    # Enhance response text to mention downloads
                    response_dict["response"] += f"\n\n**Downloads Ready:** I've linked your {len(data_sources)} data sources. You can download the individual files or the combined linked Excel file below."
                else:
                    link_error = link_result.get("error") if link_result else "Unknown error"
                    response_dict["response"] += f"\n\n**Note:** Data linking encountered an issue: {link_error}"
            except Exception as e:
                link_error = str(e)
                print(f"[ASK] Link error: {e}")
                response_dict["response"] += f"\n\n**Note:** Could not generate Excel files: {link_error}"
        elif len(data_sources) >= 2:
            print(f"[ASK] Data sources selected but question doesn't seem to ask for linking")
        elif _should_link_data(request.question):
            print(f"[ASK] Question asks for linking but not enough data sources selected ({len(data_sources)})")
        
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
