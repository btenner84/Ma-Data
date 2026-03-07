"""
MA Agent V3 - Transparent AI with Structured Tools

Key improvements over V2:
1. NO raw SQL - uses structured tools that wrap existing services
2. Full thinking transparency - streams plan/query/analyze/validate steps
3. Better validation - checks results against sanity bounds
4. Cleaner architecture - single-pass tool calling with Claude
"""

import os
import time
import json
import traceback
from typing import Dict, List, Optional, Any, AsyncGenerator
from dataclasses import dataclass, field, asdict
from datetime import datetime
from uuid import uuid4

import anthropic
import boto3

from api.services.structured_tools import get_structured_tools, ToolResult
from api.services.tool_definitions import get_tool_definitions
from api.services.visualization_service import VisualizationService, VizIntent


# =============================================================================
# DATA STRUCTURES FOR THINKING PROCESS
# =============================================================================

@dataclass
class ThinkingStep:
    """One step in the AI's thinking process."""
    id: str
    phase: str  # "plan", "query", "analyze", "validate", "synthesize"
    title: str
    content: str
    status: str  # "running", "complete", "error"
    duration_ms: int = 0
    
    # Phase-specific data
    tool_name: Optional[str] = None
    tool_params: Optional[Dict] = None
    service_called: Optional[str] = None
    row_count: Optional[int] = None
    
    validations: Optional[List[Dict]] = None
    confidence: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class ThinkingProcess:
    """Complete thinking process for a query."""
    query_id: str
    question: str
    steps: List[ThinkingStep] = field(default_factory=list)
    total_duration_ms: int = 0
    total_tokens: int = 0
    tools_called: int = 0
    status: str = "thinking"  # "thinking", "complete", "error"
    
    def add_step(self, step: ThinkingStep):
        self.steps.append(step)
    
    def to_dict(self) -> Dict:
        return {
            "query_id": self.query_id,
            "question": self.question,
            "steps": [s.to_dict() for s in self.steps],
            "total_duration_ms": self.total_duration_ms,
            "total_tokens": self.total_tokens,
            "tools_called": self.tools_called,
            "status": self.status
        }


@dataclass
class AgentResponseV3:
    """Final response from the agent."""
    status: str
    response: str
    charts: List[Dict] = field(default_factory=list)
    tables: List[Dict] = field(default_factory=list)
    thinking: Optional[ThinkingProcess] = None
    sources: List[str] = field(default_factory=list)
    confidence: str = "high"
    error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            "status": self.status,
            "response": self.response,
            "charts": self.charts,
            "tables": self.tables,
            "thinking": self.thinking.to_dict() if self.thinking else None,
            "sources": self.sources,
            "confidence": self.confidence,
            "error": self.error
        }


# =============================================================================
# VALIDATION RULES
# =============================================================================

SANITY_CHECKS = {
    "enrollment": {
        "max_single_payer": 15_000_000,  # No single payer has >15M
        "max_industry_total": 40_000_000,  # Total MA ~33M
        "min_reasonable": 1000  # Below this is suspiciously low for major payers
    },
    "fourplus_pct": {
        "max": 100.0,
        "min": 0.0
    },
    "risk_score": {
        "max": 2.5,  # Very high but possible for SNPs
        "min": 0.5   # Very low but possible
    }
}


def validate_results(data: Any, metric_type: str) -> List[Dict]:
    """
    Validate results against sanity bounds.
    Returns list of validation results.
    """
    validations = []
    checks = SANITY_CHECKS.get(metric_type, {})
    
    if not checks or not data:
        return [{"check": "data_exists", "passed": bool(data), "message": "Data retrieved" if data else "No data"}]
    
    if isinstance(data, list):
        # Check each row
        for row in data[:5]:  # Check first 5 rows
            if metric_type == "enrollment":
                enrollment = row.get('total_enrollment') or row.get('enrollment', 0)
                if enrollment > checks.get('max_single_payer', float('inf')):
                    validations.append({
                        "check": "max_enrollment",
                        "passed": False,
                        "message": f"Enrollment {enrollment:,} exceeds realistic max ({checks['max_single_payer']:,})"
                    })
            elif metric_type == "fourplus_pct":
                pct = row.get('fourplus_pct', 0)
                if pct > 100 or pct < 0:
                    validations.append({
                        "check": "pct_bounds",
                        "passed": False,
                        "message": f"Percentage {pct} is out of bounds (0-100)"
                    })
    
    if not validations:
        validations.append({"check": "sanity", "passed": True, "message": "Data within expected bounds"})
    
    return validations


# =============================================================================
# MAIN AGENT CLASS
# =============================================================================

class MAAgentV3:
    """
    Medicare Advantage AI Agent with transparent thinking.
    
    Uses structured tools (no raw SQL) and streams thinking process.
    """
    
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.tools = get_structured_tools()
        self.tool_defs = get_tool_definitions()
        self.viz_service = VisualizationService()
        self.model = "claude-sonnet-4-20250514"
    
    def _fetch_document_content(self, doc_type: str, year: int) -> Optional[str]:
        """Fetch document text content from S3."""
        type_to_prefix = {
            "rate_notice_advance": "documents/text/rate_notice_advance",
            "rate_notice_final": "documents/text/rate_notice_final",
            "tech_notes_stars": "documents/text/tech_notes",
        }
        
        if doc_type not in type_to_prefix:
            return None
            
        try:
            s3 = boto3.client('s3')
            bucket = 'ma-data123'
            key = f"{type_to_prefix[doc_type]}/{year}.txt"
            
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            # Limit content size to avoid context overflow
            max_chars = 30000
            if len(content) > max_chars:
                content = content[:max_chars] + "\n\n[Document truncated for context...]"
            
            return content
        except Exception as e:
            print(f"Failed to fetch document {doc_type}/{year}: {e}")
            return None
    
    def _fetch_data_schema(self, source_id: str, year: int) -> Optional[str]:
        """Fetch schema and sample data for a raw data source.
        Uses latest available month (Dec for past years, Feb for 2026)."""
        from db import get_engine
        
        table_config = {
            "cpsc": {"table": "fact_enrollment_all_years", "has_month": True},
            "enrollment": {"table": "fact_enrollment_national", "has_month": True},
            "stars": {"table": "summary_all_years", "has_month": False},
            "risk_scores": {"table": "fact_risk_scores_unified", "has_month": False},
            "snp": {"table": "fact_snp_historical", "has_month": False}
        }
        
        if source_id not in table_config:
            return None
        
        try:
            engine = get_engine()
            config = table_config[source_id]
            table = config["table"]
            has_month = config["has_month"]
            
            # Get schema
            schema_sql = f"DESCRIBE {table}"
            schema_result = engine.query(schema_sql)
            columns = [(row['column_name'], row['column_type']) for _, row in schema_result.iterrows()]
            
            # Build month filter for tables with monthly data
            month_filter = ""
            month_label = ""
            month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            
            if has_month:
                # Get latest month for this year
                month_sql = f"SELECT MAX(month) as m FROM {table} WHERE year = {year}"
                month_result = engine.query(month_sql)
                latest_month = int(month_result.iloc[0]['m']) if not month_result.empty else 12
                month_filter = f" AND month = {latest_month}"
                month_label = f" - {month_names[latest_month]}"
            
            # Get sample rows
            sample_sql = f"SELECT * FROM {table} WHERE year = {year}{month_filter} LIMIT 5"
            sample_result = engine.query(sample_sql)
            sample_rows = sample_result.to_dict('records')
            
            # Format as context
            schema_text = f"""
=== {source_id.upper()} DATA ({year}{month_label}) ===
Table: {table}

COLUMNS:
{chr(10).join(f'  - {col[0]}: {col[1]}' for col in columns)}

SAMPLE DATA (5 rows):
{json.dumps(sample_rows, indent=2, default=str)[:3000]}

KEY JOIN COLUMNS:
- contract_id: Links to other tables by contract
- plan_id: Links at plan level (where available)
- year: Filter by year
{f'- month: Filter by month (latest available: {month_names[latest_month]})' if has_month else ''}
"""
            return schema_text
            
        except Exception as e:
            print(f"Failed to fetch schema {source_id}/{year}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _get_system_prompt(self, document_context: Optional[List[Dict]] = None) -> str:
        """Build system prompt with context."""
        context = self.tool_defs.get_system_prompt_context()
        
        # Build document context section if provided
        doc_context_section = ""
        data_context_section = ""
        
        if document_context:
            doc_contents = []
            data_contents = []
            
            for doc in document_context:
                is_data_source = doc.get('isDataSource', False)
                
                if is_data_source:
                    # Fetch schema + sample for data source
                    content = self._fetch_data_schema(doc['type'], doc['year'])
                    if content:
                        data_contents.append(content)
                else:
                    # Fetch document text
                    content = self._fetch_document_content(doc['type'], doc['year'])
                    if content:
                        doc_contents.append(f"""
=== {doc['name']} ===
{content}
""")
            
            if doc_contents:
                doc_context_section = f"""
DOCUMENT CONTEXT:
The user has provided the following CMS documents as context for their question.
Use this information to answer document-specific questions accurately.
{''.join(doc_contents)}

DOCUMENT USAGE GUIDELINES:
- When answering questions about these documents, cite specific sections and page references when possible
- Compare information across documents if multiple years are provided
- Highlight any changes or differences between document versions
- Reference the document year/type in your answers
"""
            
            if data_contents:
                data_context_section = f"""
RAW DATA CONTEXT (DATA LINKING MODE):
The user has selected raw CMS data sources to link/combine. They have selected:
{''.join(data_contents)}

DATA LINKING CAPABILITIES:
When the user asks to "link", "join", "combine", or "merge" the selected data sources:
1. The system can generate downloadable Excel files with:
   - Individual source files (each data source as separate Excel)
   - Combined/linked file (all sources joined together)
2. Explain the join logic used (which columns link the tables)
3. Show what the combined data looks like

RESPONSE GUIDELINES:
- If user asks to link/combine the data, explain how the join works
- Mention they can download: individual files + combined linked file
- Explain the join keys used (contract_id, plan_id, year)
- Describe what columns are in the combined output
- If they ask for analysis on the linked data, explain how to calculate metrics
- Format any SQL examples in code blocks
"""
        
        return f"""You are an expert Medicare Advantage data analyst with access to comprehensive MA data.

{context}
{doc_context_section}
{data_context_section}

YOUR ROLE:
- Answer questions about MA enrollment, star ratings, and risk scores
- Use the provided tools to fetch data - NEVER make up numbers
- Provide clear, data-backed insights with visualizations when helpful
- When document context is provided, use it to answer document-specific questions
- When raw data context is provided (tutorial mode), explain how to work with the data step-by-step

RESPONSE FORMAT:
1. Start with a brief, direct answer to the question
2. Support with specific data points from tool results or document context
3. Suggest relevant visualizations (charts/tables) when appropriate
4. Note any caveats or data limitations
5. In tutorial mode, show SQL queries and explain the logic

TOOL USAGE:
- Call tools to get real data - don't guess
- For comparisons, use compare_payers or multiple tool calls
- For trends, use timeseries tools
- For rankings, use by_payer tools with appropriate year
- In tutorial mode, you can still call tools to demonstrate outputs

BE CONCISE BUT COMPLETE. Lead with insights, not methodology."""
    
    async def answer(self, question: str, user_id: str = "api", document_context: Optional[List[Dict]] = None) -> AgentResponseV3:
        """
        Answer a question with full thinking transparency.
        """
        start_time = time.time()
        query_id = str(uuid4())[:8]
        
        # Initialize thinking process
        thinking = ThinkingProcess(
            query_id=query_id,
            question=question
        )
        
        try:
            # PHASE 1: PLAN
            plan_step = ThinkingStep(
                id=f"{query_id}-plan",
                phase="plan",
                title="Understanding the question",
                content="Analyzing question to determine required data...",
                status="running"
            )
            thinking.add_step(plan_step)
            plan_start = time.time()
            
            # Call Claude with tools
            messages = [{"role": "user", "content": question}]
            tool_definitions = self.tool_defs.get_tool_definitions()
            
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=self._get_system_prompt(document_context),
                messages=messages,
                tools=tool_definitions
            )
            
            # Track tokens
            thinking.total_tokens += response.usage.input_tokens + response.usage.output_tokens
            
            plan_step.duration_ms = int((time.time() - plan_start) * 1000)
            plan_step.status = "complete"
            
            # Extract Claude's plan from initial response
            plan_content = []
            tool_calls = []
            
            for block in response.content:
                if block.type == "text":
                    plan_content.append(block.text)
                elif block.type == "tool_use":
                    tool_calls.append({
                        "id": block.id,
                        "name": block.name,
                        "input": block.input
                    })
            
            plan_step.content = "\n".join(plan_content) if plan_content else f"Planning to call {len(tool_calls)} tool(s)"
            
            # PHASE 2: QUERY (execute tool calls)
            all_results = []
            
            for i, tool_call in enumerate(tool_calls):
                query_step = ThinkingStep(
                    id=f"{query_id}-query-{i}",
                    phase="query",
                    title=f"Fetching data: {tool_call['name']}",
                    content=f"Calling {tool_call['name']}...",
                    status="running",
                    tool_name=tool_call['name'],
                    tool_params=tool_call['input']
                )
                thinking.add_step(query_step)
                query_start = time.time()
                
                # Execute the tool
                result = self._execute_tool(tool_call['name'], tool_call['input'])
                
                query_step.duration_ms = int((time.time() - query_start) * 1000)
                query_step.service_called = result.service_called if result else None
                query_step.row_count = result.row_count if result else 0
                query_step.status = "complete" if result and result.success else "error"
                query_step.content = f"Retrieved {result.row_count} rows from {result.service_called}" if result and result.success else f"Error: {result.error if result else 'Unknown'}"
                
                thinking.tools_called += 1
                all_results.append({
                    "tool": tool_call['name'],
                    "params": tool_call['input'],
                    "result": result
                })
            
            # PHASE 3: ANALYZE - Send results back to Claude
            if tool_calls and all_results:
                analyze_step = ThinkingStep(
                    id=f"{query_id}-analyze",
                    phase="analyze",
                    title="Analyzing results",
                    content="Processing data and generating insights...",
                    status="running"
                )
                thinking.add_step(analyze_step)
                analyze_start = time.time()
                
                # Build tool results for Claude
                tool_results = []
                for tc, res in zip(tool_calls, all_results):
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tc['id'],
                        "content": json.dumps(res['result'].data if res['result'] and res['result'].success else {"error": res['result'].error if res['result'] else "Failed"}, default=str)
                    })
                
                # Convert response.content to serializable format for message history
                assistant_content = self._serialize_content(response.content)
                
                # Continue conversation with tool results
                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})
                
                final_response = self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=self._get_system_prompt(document_context),
                    messages=messages,
                    tools=tool_definitions
                )
                
                thinking.total_tokens += final_response.usage.input_tokens + final_response.usage.output_tokens
                
                # Check if Claude wants more tools
                more_tool_calls = []
                final_text = []
                
                for block in final_response.content:
                    if block.type == "text":
                        final_text.append(block.text)
                    elif block.type == "tool_use":
                        more_tool_calls.append({
                            "id": block.id,
                            "name": block.name,
                            "input": block.input
                        })
                
                # Execute any additional tool calls
                if more_tool_calls:
                    for i, tool_call in enumerate(more_tool_calls):
                        query_step = ThinkingStep(
                            id=f"{query_id}-query2-{i}",
                            phase="query",
                            title=f"Additional data: {tool_call['name']}",
                            content=f"Calling {tool_call['name']}...",
                            status="running",
                            tool_name=tool_call['name'],
                            tool_params=tool_call['input']
                        )
                        thinking.add_step(query_step)
                        query_start = time.time()
                        
                        result = self._execute_tool(tool_call['name'], tool_call['input'])
                        
                        query_step.duration_ms = int((time.time() - query_start) * 1000)
                        query_step.service_called = result.service_called if result else None
                        query_step.row_count = result.row_count if result else 0
                        query_step.status = "complete" if result and result.success else "error"
                        query_step.content = f"Retrieved {result.row_count} rows" if result and result.success else f"Error: {result.error if result else 'Unknown'}"
                        
                        thinking.tools_called += 1
                        all_results.append({
                            "tool": tool_call['name'],
                            "params": tool_call['input'],
                            "result": result
                        })
                    
                    # Get final response with new results
                    tool_results2 = []
                    for tc, res in zip(more_tool_calls, all_results[-len(more_tool_calls):]):
                        tool_results2.append({
                            "type": "tool_result",
                            "tool_use_id": tc['id'],
                            "content": json.dumps(res['result'].data if res['result'] and res['result'].success else {"error": res['result'].error if res['result'] else "Failed"}, default=str)
                        })
                    
                    # Convert to serializable format
                    assistant_content2 = self._serialize_content(final_response.content)
                    messages.append({"role": "assistant", "content": assistant_content2})
                    messages.append({"role": "user", "content": tool_results2})
                    
                    final_response = self.client.messages.create(
                        model=self.model,
                        max_tokens=4096,
                        system=self._get_system_prompt(document_context),
                        messages=messages
                    )
                    
                    thinking.total_tokens += final_response.usage.input_tokens + final_response.usage.output_tokens
                    
                    final_text = [block.text for block in final_response.content if block.type == "text"]
                
                analyze_step.duration_ms = int((time.time() - analyze_start) * 1000)
                analyze_step.status = "complete"
                analyze_step.content = "Data analysis complete"
                
                response_text = "\n".join(final_text)
            else:
                # No tools called - just use initial response
                response_text = "\n".join(plan_content)
            
            # PHASE 4: VALIDATE
            validate_step = ThinkingStep(
                id=f"{query_id}-validate",
                phase="validate",
                title="Validating results",
                content="Checking data sanity...",
                status="running"
            )
            thinking.add_step(validate_step)
            validate_start = time.time()
            
            # Run sanity checks on results
            all_validations = []
            for res in all_results:
                if res['result'] and res['result'].success:
                    # Determine metric type from tool name
                    tool_name = res['tool']
                    metric_type = "enrollment" if "enrollment" in tool_name else "fourplus_pct" if "stars" in tool_name else "risk_score" if "risk" in tool_name else None
                    if metric_type:
                        validations = validate_results(res['result'].data, metric_type)
                        all_validations.extend(validations)
            
            validate_step.validations = all_validations
            validate_step.confidence = "high" if all(v.get('passed', True) for v in all_validations) else "medium"
            validate_step.duration_ms = int((time.time() - validate_start) * 1000)
            validate_step.status = "complete"
            validate_step.content = f"Ran {len(all_validations)} validation checks"
            
            # PHASE 5: BUILD VISUALIZATIONS
            charts = []
            tables = []
            
            for res in all_results:
                if res['result'] and res['result'].success:
                    data = res['result'].data
                    tool_name = res['tool']
                    
                    # Build appropriate visualization
                    viz = self._build_visualization(tool_name, data, res['params'])
                    if viz:
                        if viz.get('type') == 'table':
                            tables.append(viz)
                        else:
                            charts.append(viz)
            
            # Finalize
            thinking.total_duration_ms = int((time.time() - start_time) * 1000)
            thinking.status = "complete"
            
            # Build sources
            sources = list(set(
                res['result'].service_called
                for res in all_results
                if res['result'] and res['result'].service_called
            ))
            
            # Add document context sources
            if document_context:
                doc_sources = [doc['name'] for doc in document_context]
                sources.extend(doc_sources)
            
            return AgentResponseV3(
                status="success",
                response=response_text,
                charts=charts,
                tables=tables,
                thinking=thinking,
                sources=sources,
                confidence=validate_step.confidence or "high"
            )
            
        except Exception as e:
            # Error handling
            thinking.status = "error"
            thinking.total_duration_ms = int((time.time() - start_time) * 1000)
            
            error_step = ThinkingStep(
                id=f"{query_id}-error",
                phase="error",
                title="Error occurred",
                content=str(e),
                status="error"
            )
            thinking.add_step(error_step)
            
            print(f"Agent error: {e}")
            traceback.print_exc()
            
            return AgentResponseV3(
                status="error",
                response=f"I encountered an error processing your question: {str(e)}",
                thinking=thinking,
                error=str(e),
                confidence="low"
            )
    
    def _serialize_content(self, content: list) -> list:
        """
        Convert Anthropic ContentBlock objects to serializable dicts.
        This fixes the 'by_alias' error when passing content to messages.
        """
        serialized = []
        for block in content:
            if block.type == "text":
                serialized.append({
                    "type": "text",
                    "text": block.text
                })
            elif block.type == "tool_use":
                serialized.append({
                    "type": "tool_use",
                    "id": block.id,
                    "name": block.name,
                    "input": block.input
                })
        return serialized
    
    def _execute_tool(self, tool_name: str, params: Dict) -> Optional[ToolResult]:
        """Execute a tool by name with given parameters."""
        try:
            # Get the method from structured tools
            method = getattr(self.tools, tool_name, None)
            if not method:
                return ToolResult(
                    success=False,
                    data=None,
                    row_count=0,
                    service_called=tool_name,
                    error=f"Tool '{tool_name}' not found"
                )
            
            # Call the method
            return method(**params)
            
        except Exception as e:
            print(f"Tool execution error: {e}")
            traceback.print_exc()
            return ToolResult(
                success=False,
                data=None,
                row_count=0,
                service_called=tool_name,
                error=str(e)
            )
    
    def _build_visualization(self, tool_name: str, data: Any, params: Dict) -> Optional[Dict]:
        """Build appropriate visualization for tool result."""
        if not data:
            return None
        
        try:
            # Determine viz type based on tool
            if "timeseries" in tool_name:
                # Line chart for timeseries
                if isinstance(data, dict) and 'years' in data and 'series' in data:
                    chart_data = []
                    years = data['years']
                    series = data.get('series', {})
                    
                    for i, year in enumerate(years):
                        row = {'year': year}
                        for name, values in series.items():
                            if i < len(values) and values[i] is not None:
                                row[name] = values[i]
                        chart_data.append(row)
                    
                    return {
                        'type': 'line',
                        'title': self._get_chart_title(tool_name, params),
                        'data': chart_data,
                        'xKey': 'year',
                        'yKeys': list(series.keys()),
                        'colors': self._get_colors(len(series))
                    }
                elif isinstance(data, list) and len(data) > 0:
                    # Check if this is multi-payer data (has parent_org field)
                    if 'parent_org' in data[0]:
                        # Transform flat list to multi-series format
                        payers = list(set(row.get('parent_org') for row in data if row.get('parent_org')))
                        years = sorted(set(row.get('year') for row in data if row.get('year')))
                        
                        # Determine the value field
                        value_field = 'enrollment' if 'enrollment' in tool_name else 'fourplus_pct' if 'stars' in tool_name else 'wavg_risk_score'
                        if value_field not in data[0]:
                            value_field = 'total_enrollment' if 'total_enrollment' in data[0] else list(data[0].keys())[2]
                        
                        # Build pivoted data for chart
                        chart_data = []
                        for year in years:
                            row = {'year': year}
                            for payer in payers:
                                # Find the value for this payer/year
                                match = next((d for d in data if d.get('year') == year and d.get('parent_org') == payer), None)
                                if match:
                                    row[payer] = match.get(value_field) or match.get('enrollment') or match.get('total_enrollment')
                            chart_data.append(row)
                        
                        return {
                            'type': 'line',
                            'title': self._get_chart_title(tool_name, params),
                            'data': chart_data,
                            'xKey': 'year',
                            'yKeys': payers,
                            'colors': self._get_colors(len(payers))
                        }
                    else:
                        # Single series
                        return {
                            'type': 'line',
                            'title': self._get_chart_title(tool_name, params),
                            'data': data,
                            'xKey': 'year',
                            'yKeys': ['enrollment'] if 'enrollment' in tool_name else ['fourplus_pct'] if 'stars' in tool_name else ['wavg_risk_score'],
                            'colors': ['#2563eb']
                        }
            
            elif "by_payer" in tool_name or "by_parent" in tool_name:
                # Bar chart for rankings
                if isinstance(data, list) and len(data) > 0:
                    chart_data = data[:15]  # Top 15 for better visibility
                    metric = 'total_enrollment' if 'enrollment' in tool_name else 'fourplus_pct' if 'stars' in tool_name else 'wavg_risk_score'
                    
                    return {
                        'type': 'bar',
                        'title': self._get_chart_title(tool_name, params),
                        'data': chart_data,
                        'xKey': 'parent_org',
                        'yKeys': [metric],
                        'colors': ['#2563eb']
                    }
            
            elif "compare" in tool_name:
                # Multi-metric comparison - flatten for table display
                if isinstance(data, dict):
                    table_rows = []
                    payers = data.get('payers', [])
                    metrics = data.get('metrics', {})
                    
                    for payer in payers:
                        row = {'parent_org': payer}
                        for metric_name, payer_data in metrics.items():
                            if payer in payer_data:
                                val = payer_data[payer]
                                if isinstance(val, dict):
                                    for k, v in val.items():
                                        row[f"{metric_name}_{k}"] = self._format_value(v, metric_name)
                                else:
                                    row[metric_name] = self._format_value(val, metric_name)
                        table_rows.append(row)
                    
                    if table_rows:
                        return {
                            'type': 'table',
                            'title': 'Payer Comparison',
                            'data': table_rows,
                            'columns': list(table_rows[0].keys()) if table_rows else []
                        }
            
            elif "distribution" in tool_name and "stars" in tool_name:
                # Stars distribution - line chart
                if isinstance(data, dict) and 'years' in data:
                    chart_data = []
                    years = data['years']
                    series = data.get('series', {})
                    
                    for i, year in enumerate(years):
                        row = {'year': year}
                        for name, values in series.items():
                            if i < len(values) and values[i] is not None:
                                row[name] = values[i]
                        chart_data.append(row)
                    
                    return {
                        'type': 'line',
                        'title': '4+ Star Enrollment % Over Time',
                        'data': chart_data,
                        'xKey': 'year',
                        'yKeys': list(series.keys()),
                        'colors': self._get_colors(len(series))
                    }
            
            elif "analyze_star_drops" in tool_name:
                # Table of drop events with clean columns
                if isinstance(data, list):
                    clean_data = []
                    for d in data[:15]:
                        clean_data.append({
                            'Payer': d.get('parent_org', ''),
                            'Drop Year': d.get('drop_year', ''),
                            'Before': f"{d.get('pre_drop_pct', 0):.1f}%",
                            'After': f"{d.get('post_drop_pct', 0):.1f}%",
                            'Change': f"-{d.get('drop_magnitude', 0):.1f}pp",
                            'Recovered': d.get('recovery_status', 'Unknown')
                        })
                    
                    return {
                        'type': 'table',
                        'title': 'Major Star Rating Drops & Recovery',
                        'columns': ['Payer', 'Drop Year', 'Before', 'After', 'Change', 'Recovered'],
                        'data': clean_data
                    }
            
            elif "market_concentration" in tool_name:
                # Market concentration - bar chart
                if isinstance(data, dict) and 'top_payers' in data:
                    return {
                        'type': 'bar',
                        'title': f"Market Concentration ({params.get('year', 2026)})",
                        'data': data['top_payers'][:10],
                        'xKey': 'parent_org',
                        'yKeys': ['market_share_pct'],
                        'colors': ['#16a34a']
                    }
            
            # Default: return as table if list with flat data
            if isinstance(data, list) and len(data) > 0:
                # Clean the data - flatten any nested dicts and format numbers
                clean_data = []
                for row in data[:25]:
                    clean_row = {}
                    if isinstance(row, dict):
                        for k, v in row.items():
                            if isinstance(v, dict):
                                # Skip nested dicts or flatten one level
                                for nk, nv in v.items():
                                    clean_row[f"{k}_{nk}"] = self._format_value(nv, k)
                            elif isinstance(v, list):
                                clean_row[k] = f"[{len(v)} items]"
                            else:
                                clean_row[k] = self._format_value(v, k)
                        clean_data.append(clean_row)
                
                if clean_data:
                    return {
                        'type': 'table',
                        'title': self._get_chart_title(tool_name, params),
                        'data': clean_data,
                        'columns': list(clean_data[0].keys()) if clean_data else []
                    }
            
            return None
            
        except Exception as e:
            print(f"Visualization error: {e}")
            traceback.print_exc()
            return None
    
    def _format_value(self, val: Any, context: str = "") -> Any:
        """Format a value for display."""
        if val is None:
            return "-"
        if isinstance(val, float):
            if "pct" in context.lower() or "share" in context.lower() or "percent" in context.lower():
                return f"{val:.1f}%"
            if abs(val) >= 1000000:
                return f"{val/1000000:.1f}M"
            if abs(val) >= 1000:
                return f"{val/1000:.1f}K"
            return round(val, 2)
        if isinstance(val, int):
            if abs(val) >= 1000000:
                return f"{val/1000000:.1f}M"
            if abs(val) >= 1000:
                return f"{val:,}"
            return val
        return val
    
    def _get_chart_title(self, tool_name: str, params: Dict) -> str:
        """Generate appropriate chart title."""
        titles = {
            "get_enrollment_by_payer": f"Top MA Payers by Enrollment ({params.get('year', 2026)})",
            "get_enrollment_timeseries": "MA Enrollment Over Time",
            "get_enrollment_by_state": f"Enrollment by State ({params.get('year', 2026)})",
            "get_stars_distribution": "4+ Star Enrollment % Over Time",
            "get_stars_by_payer": f"Star Ratings by Payer ({params.get('star_year', 2026)})",
            "get_stars_timeseries": "Star Rating Trends",
            "get_risk_scores_by_payer": f"Risk Scores by Payer ({params.get('year', 2024)})",
            "get_risk_scores_timeseries": "Risk Score Trends",
            "compare_payers": "Payer Comparison",
            "analyze_star_drops": "Major Star Rating Drops"
        }
        return titles.get(tool_name, tool_name.replace('_', ' ').title())
    
    def _get_colors(self, count: int) -> List[str]:
        """Get color palette for charts."""
        palette = [
            '#2563eb',  # Blue
            '#dc2626',  # Red
            '#16a34a',  # Green
            '#ca8a04',  # Yellow
            '#9333ea',  # Purple
            '#0891b2',  # Cyan
            '#ea580c',  # Orange
            '#4f46e5',  # Indigo
            '#be185d',  # Pink
            '#65a30d',  # Lime
        ]
        return palette[:count]


# Singleton
_agent_instance = None

def get_agent_v3() -> MAAgentV3:
    """Get or create singleton agent instance."""
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = MAAgentV3()
    return _agent_instance
