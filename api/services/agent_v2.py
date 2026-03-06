"""
MA Intelligence Agent V2 - Robust Multi-Step Architecture
=========================================================

A production-grade agent with:
- Multi-step reasoning with iteration loops
- Full audit trail for every decision
- LLM usage tracking (tokens, cost, latency)
- Validation gates before responses
- Structured analysis outputs (tables, charts)

Architecture:
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  User Question                                                              │
│       │                                                                     │
│       ▼                                                                     │
│  ┌──────────────┐                                                           │
│  │   PLANNER    │  Determines what information is needed                    │
│  │              │  Output: List of data requirements                        │
│  └──────────────┘                                                           │
│       │                                                                     │
│       ▼                                                                     │
│  ┌──────────────┐     ┌──────────────┐                                      │
│  │   EXECUTOR   │────▶│   ANALYZER   │──┐                                   │
│  │  (tools)     │◀────│  (may need   │  │  Can loop for more data          │
│  │              │     │   more data) │◀─┘                                   │
│  └──────────────┘     └──────────────┘                                      │
│                              │                                              │
│                              ▼                                              │
│                       ┌──────────────┐                                      │
│                       │  VALIDATOR   │  Check results make sense            │
│                       └──────────────┘                                      │
│                              │                                              │
│                              ▼                                              │
│                       ┌──────────────┐                                      │
│                       │ SYNTHESIZER  │  Natural language + charts           │
│                       └──────────────┘                                      │
│                              │                                              │
│       ┌──────────────────────┼──────────────────────┐                       │
│       ▼                      ▼                      ▼                       │
│   Response              Data Tables              Charts                     │
│                                                                             │
│  ═══════════════════════════════════════════════════════════════════════    │
│  AUDIT LOG: Every step tracked with timestamp, tokens, cost, decisions      │
│  ═══════════════════════════════════════════════════════════════════════    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
"""

import os
import json
import time
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
import asyncio

# LLM costs (approximate, per 1M tokens)
LLM_COSTS = {
    "claude-sonnet-4": {"input": 3.00, "output": 15.00},
    "claude-3-5-sonnet": {"input": 3.00, "output": 15.00},
    "claude-3-opus": {"input": 15.00, "output": 75.00},
    "claude-3-haiku": {"input": 0.25, "output": 1.25},
    "gpt-4-turbo": {"input": 10.00, "output": 30.00},
    "gpt-4o": {"input": 5.00, "output": 15.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
}


class AgentPhase(Enum):
    """Phases of agent execution."""
    PLANNING = "planning"
    EXECUTING = "executing"
    ANALYZING = "analyzing"
    VALIDATING = "validating"
    SYNTHESIZING = "synthesizing"
    COMPLETE = "complete"
    ERROR = "error"


@dataclass
class LLMCall:
    """Record of a single LLM call."""
    call_id: str
    phase: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    cost_usd: float
    latency_ms: int
    timestamp: str
    prompt_preview: str  # First 200 chars
    response_preview: str  # First 200 chars
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class ToolCall:
    """Record of a tool execution."""
    call_id: str
    tool_name: str
    arguments: Dict
    result_preview: str  # First 500 chars
    success: bool
    error: Optional[str]
    latency_ms: int
    timestamp: str
    rows_returned: Optional[int] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class AgentStep:
    """A single step in the agent's reasoning."""
    step_id: str
    step_number: int
    phase: str
    description: str
    llm_calls: List[LLMCall] = field(default_factory=list)
    tool_calls: List[ToolCall] = field(default_factory=list)
    input_data: Optional[str] = None
    output_data: Optional[str] = None
    decision: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['llm_calls'] = [c.to_dict() if hasattr(c, 'to_dict') else c for c in self.llm_calls]
        d['tool_calls'] = [c.to_dict() if hasattr(c, 'to_dict') else c for c in self.tool_calls]
        return d


@dataclass
class AgentAudit:
    """Complete audit trail for an agent run."""
    run_id: str
    question: str
    user_id: str
    start_time: str
    end_time: Optional[str] = None
    status: str = "running"
    
    # Execution trace
    steps: List[AgentStep] = field(default_factory=list)
    
    # Aggregated metrics
    total_llm_calls: int = 0
    total_tool_calls: int = 0
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    total_latency_ms: int = 0
    
    # Results
    final_answer: Optional[str] = None
    confidence: float = 0.0
    data_tables: List[Dict] = field(default_factory=list)
    charts: List[Dict] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def add_step(self, step: AgentStep):
        """Add a step and update aggregates."""
        self.steps.append(step)
        for llm_call in step.llm_calls:
            self.total_llm_calls += 1
            self.total_tokens += llm_call.total_tokens
            self.total_cost_usd += llm_call.cost_usd
            self.total_latency_ms += llm_call.latency_ms
        for tool_call in step.tool_calls:
            self.total_tool_calls += 1
            self.total_latency_ms += tool_call.latency_ms
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['steps'] = [s.to_dict() if hasattr(s, 'to_dict') else s for s in self.steps]
        return d
    
    def summary(self) -> Dict:
        """Get summary metrics."""
        return {
            "run_id": self.run_id,
            "status": self.status,
            "llm_calls": self.total_llm_calls,
            "tool_calls": self.total_tool_calls,
            "total_tokens": self.total_tokens,
            "cost_usd": round(self.total_cost_usd, 4),
            "latency_ms": self.total_latency_ms,
            "steps": len(self.steps),
            "confidence": self.confidence,
        }


@dataclass
class DataRequirement:
    """A piece of data the agent needs to gather."""
    requirement_id: str
    description: str
    data_type: str  # "enrollment", "stars", "risk", "policy", "benchmark"
    query_approach: str  # "sql", "tool", "knowledge"
    specific_query: Optional[str] = None
    priority: int = 1  # 1=critical, 2=helpful, 3=nice-to-have
    satisfied: bool = False
    result: Optional[Any] = None


@dataclass 
class AnalysisResult:
    """Result of data analysis."""
    findings: List[str]
    data_tables: List[Dict]
    charts: List[Dict]
    needs_more_data: bool = False
    additional_requirements: List[DataRequirement] = field(default_factory=list)
    confidence: float = 0.8


@dataclass
class ChartSpec:
    """Specification for a chart to render."""
    chart_type: str  # "line", "bar", "pie", "area", "scatter"
    title: str
    data: List[Dict]
    x_axis: str
    y_axis: str
    series: List[str] = field(default_factory=list)
    insights: List[str] = field(default_factory=list)


# =============================================================================
# AGENT PROMPTS - Specialized for each phase
# =============================================================================

PLANNER_PROMPT = """You are a Medicare Advantage data analyst planning how to answer a question.

Given a user question, determine what data is needed to answer it completely.

Output a JSON list of data requirements:
```json
{
  "question_type": "policy|enrollment|stars|risk|comparison|trend",
  "requirements": [
    {
      "description": "What data is needed",
      "data_type": "enrollment|stars|risk|policy|benchmark",
      "query_approach": "sql|tool|knowledge",
      "specific_query": "SQL query or tool name with args",
      "priority": 1
    }
  ],
  "analysis_needed": "What analysis to perform on the data",
  "comparison_context": "What to compare against (if applicable)"
}
```

Be thorough - it's better to gather extra context than to miss something important.

For policy questions (rate notices, HCC models): Use tools like get_rate_notice_metrics, get_hcc_model_info
For data questions: Use SQL queries against available tables
For definitions: Use lookup_knowledge

Available SQL tables:
- Enrollment: fact_enrollment_all_years, enrollment_by_parent
- Stars: measures_all_years, measure_stars_all_years, summary_all_years
- Risk: fact_risk_scores_unified, hcc_coefficients_all, risk_adjustment_parameters
- Rates: county_benchmarks, ma_growth_rates, part_d_parameters"""

ANALYZER_PROMPT = """You are a Medicare Advantage analyst examining data to extract insights.

Given the gathered data, analyze it to answer the user's question.

Your analysis should:
1. Identify the key findings
2. Compare to relevant benchmarks or historical values
3. Note any surprising or noteworthy patterns
4. Flag if more data is needed for a complete answer
5. CREATE CHARTS and DATA TABLES when data supports visualization

Output JSON:
```json
{
  "findings": ["Key finding 1", "Key finding 2", ...],
  "comparisons": {"metric": {"current": X, "prior_year": Y, "industry": Z}},
  "trends": [{"metric": "name", "direction": "up|down|stable", "magnitude": "X%"}],
  "data_tables": [
    {
      "title": "Title for the table",
      "summary": "Brief description of what this shows",
      "columns": ["Column1", "Column2", "Column3"],
      "rows": [
        {"Column1": "val1", "Column2": 123, "Column3": 45.6},
        {"Column1": "val2", "Column2": 456, "Column3": 78.9}
      ]
    }
  ],
  "charts": [
    {
      "chart_type": "line|bar|area",
      "title": "Chart title",
      "x_axis": "field_for_x",
      "y_axis": "field_for_y",
      "data": [
        {"x_field": "2020", "y_field": 100},
        {"x_field": "2021", "y_field": 120}
      ],
      "series": [{"key": "y_field", "label": "Metric Name", "color": "#3B82F6"}]
    }
  ],
  "needs_more_data": false,
  "additional_queries": [],
  "confidence": 0.9,
  "caveats": ["Any data limitations"]
}
```

IMPORTANT VISUAL OUTPUTS:
- If data shows a TREND over time -> create a LINE or AREA chart
- If comparing CATEGORIES (payers, plans) -> create a BAR chart
- If showing RANKINGS or LISTS -> create a DATA TABLE
- If data has NUMBERS -> format them nicely with commas and percentages

Be analytical - look for the story in the data, not just the numbers."""

VALIDATOR_PROMPT = """You are a quality checker for Medicare Advantage analysis.

Review the analysis and check for:
1. Logical consistency - do the conclusions follow from the data?
2. Completeness - is anything important missing?
3. Accuracy - are the numbers plausible for MA context?
4. Clarity - will the user understand this?

MA sanity checks:
- Total MA enrollment should be ~30-35M (2024-2026)
- Star ratings are 1-5 scale
- Risk scores typically 0.8-1.5 range
- MA growth rates typically 2-5% annually
- Part D deductibles ~$500-600

Output JSON:
```json
{
  "passes_validation": true,
  "issues": [],
  "suggestions": [],
  "confidence_adjustment": 0
}
```"""

SYNTHESIZER_PROMPT = """You are an expert Medicare Advantage consultant explaining findings to a colleague.

Given the analyzed data, create a natural, conversational response that:
1. Leads with the key insight (not data dumps)
2. Explains implications, not just facts
3. Provides context and comparisons
4. Is conversational, not robotic
5. REFERENCES the charts and tables that will be shown below your response

IMPORTANT: Charts and data tables are displayed SEPARATELY below your text response. 
- Don't repeat all the numbers that are in the tables
- DO reference them: "As shown in the table below...", "The chart illustrates..."
- Your text should INTERPRET and ADD CONTEXT to the visuals

TONE: Like a senior consultant briefing a colleague - knowledgeable, direct, insightful.

DO NOT:
- List all the raw numbers that are already in the tables
- Use robotic phrasing like "Based on my analysis..."
- Structure response like a formal report
- Repeat data that's in the charts/tables

DO:
- Lead with the headline takeaway
- Explain what the numbers MEAN
- Reference the visualizations naturally
- Share your expert perspective
- Note any caveats naturally in the flow

Example good response (when a table of payer drops is shown below):
"Humana's 4-star drop is actually unprecedented for them - they've been the model of consistency for years. The table below shows the major drops since 2014, and you'll notice Humana had never appeared on this list until now. What's interesting is the recovery patterns: most large payers bounce back within 2-3 years. Centene is the outlier here - they've had repeated drops without sustained recovery, which suggests structural issues rather than one-off problems."

NOT:
"Here are all the drops:
- Humana: 96.9% to 40.8% in 2025
- CIGNA: 74.4% to 21.0% in 2017
- Healthfirst: 100% to 0% in 2018..."
"""


# =============================================================================
# MAIN AGENT CLASS
# =============================================================================

class MAAgentV2:
    """
    Production-grade MA Intelligence Agent.
    
    Features:
    - Multi-step reasoning with iteration
    - Full audit trail
    - LLM usage tracking
    - Validation gates
    - Structured outputs (tables, charts)
    """
    
    MAX_ITERATIONS = 5  # Prevent infinite loops
    
    def __init__(
        self,
        llm_provider: str = "anthropic",
        model: str = "claude-sonnet-4-20250514",
        api_key: Optional[str] = None,
    ):
        self.llm_provider = llm_provider
        self.model = model
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        
        # Import tools
        from api.services.agent_tools import MAAgentTools, get_agent_tools
        self.tools = get_agent_tools()
        
        # Initialize audit
        self.current_audit: Optional[AgentAudit] = None
    
    async def answer(
        self,
        question: str,
        user_id: str = "anonymous",
        history: List[Dict] = None,
    ) -> Tuple[str, AgentAudit]:
        """
        Answer a question with full audit trail.
        
        Returns: (answer_text, audit_record)
        """
        # Initialize audit
        run_id = str(uuid.uuid4())
        self.current_audit = AgentAudit(
            run_id=run_id,
            question=question,
            user_id=user_id,
            start_time=datetime.now(timezone.utc).isoformat(),
        )
        
        try:
            # Phase 1: Planning
            requirements = await self._plan(question)
            
            # Phase 2: Execute + Analyze loop
            analysis = await self._execute_and_analyze(question, requirements)
            
            # Phase 3: Validate
            validation = await self._validate(question, analysis)
            
            # Phase 4: Synthesize
            response = await self._synthesize(question, analysis, validation)
            
            # Finalize audit
            self.current_audit.status = "complete"
            self.current_audit.end_time = datetime.now(timezone.utc).isoformat()
            self.current_audit.final_answer = response
            self.current_audit.confidence = analysis.confidence
            self.current_audit.data_tables = analysis.data_tables
            self.current_audit.charts = analysis.charts
            
            return response, self.current_audit
            
        except Exception as e:
            self.current_audit.status = "error"
            self.current_audit.end_time = datetime.now(timezone.utc).isoformat()
            self.current_audit.warnings.append(f"Error: {str(e)}")
            raise
    
    async def _plan(self, question: str) -> List[DataRequirement]:
        """Plan what data is needed to answer the question."""
        step = AgentStep(
            step_id=str(uuid.uuid4()),
            step_number=len(self.current_audit.steps) + 1,
            phase=AgentPhase.PLANNING.value,
            description="Determining data requirements",
            input_data=question,
        )
        
        # Call LLM for planning
        prompt = f"{PLANNER_PROMPT}\n\nUser Question: {question}"
        
        start_time = time.time()
        response = await self._call_llm(prompt, step)
        latency = int((time.time() - start_time) * 1000)
        
        # Parse requirements from response
        requirements = self._parse_requirements(response)
        
        step.output_data = json.dumps([asdict(r) for r in requirements], indent=2)
        step.decision = f"Identified {len(requirements)} data requirements"
        
        self.current_audit.add_step(step)
        return requirements
    
    async def _execute_and_analyze(
        self, 
        question: str, 
        requirements: List[DataRequirement]
    ) -> AnalysisResult:
        """Execute tools and analyze results, with iteration if needed."""
        
        iteration = 0
        all_data = {}
        
        while iteration < self.MAX_ITERATIONS:
            iteration += 1
            
            # Execute pending requirements
            exec_step = AgentStep(
                step_id=str(uuid.uuid4()),
                step_number=len(self.current_audit.steps) + 1,
                phase=AgentPhase.EXECUTING.value,
                description=f"Gathering data (iteration {iteration})",
            )
            
            pending = [r for r in requirements if not r.satisfied]
            
            for req in pending:
                result = await self._execute_requirement(req, exec_step)
                if result is not None:
                    all_data[req.requirement_id] = {
                        "description": req.description,
                        "data": result,
                    }
                    req.satisfied = True
                    req.result = result
            
            exec_step.output_data = f"Gathered {len([r for r in requirements if r.satisfied])}/{len(requirements)} requirements"
            self.current_audit.add_step(exec_step)
            
            # Analyze gathered data
            analysis_step = AgentStep(
                step_id=str(uuid.uuid4()),
                step_number=len(self.current_audit.steps) + 1,
                phase=AgentPhase.ANALYZING.value,
                description=f"Analyzing data (iteration {iteration})",
                input_data=json.dumps(all_data, default=str)[:2000],
            )
            
            analysis = await self._analyze(question, all_data, analysis_step)
            
            analysis_step.output_data = json.dumps({
                "findings": analysis.findings,
                "needs_more_data": analysis.needs_more_data,
                "confidence": analysis.confidence,
            }, indent=2)
            
            self.current_audit.add_step(analysis_step)
            
            # Check if we need more data
            if not analysis.needs_more_data:
                return analysis
            
            # Add additional requirements
            for new_req in analysis.additional_requirements:
                if new_req.requirement_id not in [r.requirement_id for r in requirements]:
                    requirements.append(new_req)
        
        # Max iterations reached
        self.current_audit.warnings.append(f"Reached max iterations ({self.MAX_ITERATIONS})")
        return analysis
    
    def _extract_year_from_text(self, text: str) -> int:
        """Extract year from description text."""
        import re
        # Look for 4-digit years between 2013-2030
        years = re.findall(r'\b(20[12][0-9]|2030)\b', text)
        if years:
            return int(years[0])
        return 2027  # Default to current
    
    def _extract_model_version(self, text: str) -> str:
        """Extract HCC model version from text."""
        import re
        match = re.search(r'V?(\d+)', text, re.IGNORECASE)
        if match:
            num = match.group(1)
            return f"V{num}"
        return "V28"  # Default to current
    
    async def _execute_requirement(
        self, 
        req: DataRequirement, 
        step: AgentStep
    ) -> Optional[Any]:
        """Execute a single data requirement."""
        start_time = time.time()
        result = None
        error = None
        tool_name = req.query_approach
        
        try:
            desc_lower = req.description.lower()
            
            if req.query_approach == "sql" and req.specific_query:
                # Execute SQL query
                tool_name = "query_database"
                tool_result = self.tools.query_database(
                    sql=req.specific_query,
                    context=req.description
                )
                if tool_result.success:
                    result = tool_result.data
                else:
                    error = tool_result.error
                    
            elif req.query_approach == "tool":
                # Determine which tool based on description
                if "rate_notice" in desc_lower or "advance notice" in desc_lower or "final notice" in desc_lower:
                    tool_name = "get_rate_notice_metrics"
                    year = self._extract_year_from_text(req.description)
                    notice_type = "final" if "final" in desc_lower else "advance"
                    tool_result = self.tools.get_rate_notice_metrics(
                        year=year,
                        notice_type=notice_type
                    )
                    result = tool_result.data if tool_result.success else None
                    error = tool_result.error if not tool_result.success else None
                    
                elif "hcc" in desc_lower or "risk model" in desc_lower or "risk adjustment model" in desc_lower:
                    tool_name = "get_hcc_model_info"
                    version = self._extract_model_version(req.description)
                    year = self._extract_year_from_text(req.description)
                    tool_result = self.tools.get_hcc_model_info(
                        model_version=version,
                        year=year
                    )
                    result = tool_result.data if tool_result.success else None
                    error = tool_result.error if not tool_result.success else None
                    
                elif "policy" in desc_lower or "change" in desc_lower:
                    tool_name = "get_ma_policy_changes"
                    year = self._extract_year_from_text(req.description)
                    category = None
                    for cat in ["risk_adjustment", "star_ratings", "part_d", "network", "snp"]:
                        if cat.replace("_", " ") in desc_lower:
                            category = cat
                            break
                    tool_result = self.tools.get_ma_policy_changes(
                        year=year,
                        category=category
                    )
                    result = tool_result.data if tool_result.success else None
                    error = tool_result.error if not tool_result.success else None
                    
                elif "document" in desc_lower or "search" in desc_lower or "cms" in desc_lower:
                    tool_name = "search_documents"
                    tool_result = self.tools.search_documents(
                        query=req.description,
                        max_results=5
                    )
                    result = tool_result.data if tool_result.success else None
                    error = tool_result.error if not tool_result.success else None
                    
                elif "payer" in desc_lower or "organization" in desc_lower or "parent org" in desc_lower:
                    tool_name = "get_payer_info"
                    # Try to extract payer name from description
                    import re
                    match = re.search(r'(?:about|for|on)\s+([A-Z][a-zA-Z\s&]+)', req.description)
                    payer_name = match.group(1).strip() if match else req.description
                    year = self._extract_year_from_text(req.description)
                    tool_result = self.tools.get_payer_info(
                        payer_name=payer_name,
                        year=year
                    )
                    result = tool_result.data if tool_result.success else None
                    error = tool_result.error if not tool_result.success else None
                    
                else:
                    # Default: try knowledge lookup
                    tool_name = "lookup_knowledge"
                    tool_result = self.tools.lookup_knowledge(topic=req.description)
                    result = tool_result.data if tool_result.success else None
                    error = tool_result.error if not tool_result.success else None
                    
            elif req.query_approach == "knowledge":
                tool_name = "lookup_knowledge"
                tool_result = self.tools.lookup_knowledge(topic=req.description)
                result = tool_result.data if tool_result.success else None
                error = tool_result.error if not tool_result.success else None
                
        except Exception as e:
            error = str(e)
        
        latency = int((time.time() - start_time) * 1000)
        
        # Record tool call with proper tool name
        tool_call = ToolCall(
            call_id=str(uuid.uuid4()),
            tool_name=tool_name,
            arguments={"query": req.specific_query, "description": req.description},
            result_preview=str(result)[:500] if result else "",
            success=result is not None,
            error=error,
            latency_ms=latency,
            timestamp=datetime.now(timezone.utc).isoformat(),
            rows_returned=len(result) if isinstance(result, (list, dict)) and hasattr(result, '__len__') else None,
        )
        step.tool_calls.append(tool_call)
        
        return result
    
    async def _analyze(
        self, 
        question: str, 
        data: Dict, 
        step: AgentStep
    ) -> AnalysisResult:
        """Analyze gathered data."""
        # Determine what visualizations would be helpful
        question_lower = question.lower()
        viz_hints = []
        if any(w in question_lower for w in ["trend", "over time", "history", "year", "growth"]):
            viz_hints.append("Create a LINE chart showing the trend over time")
        if any(w in question_lower for w in ["compare", "vs", "versus", "rank", "top"]):
            viz_hints.append("Create a BAR chart or table comparing the entities")
        if any(w in question_lower for w in ["drop", "decrease", "increase", "change"]):
            viz_hints.append("Show the change magnitude in a table with before/after values")
        
        viz_instruction = "\n".join(viz_hints) if viz_hints else "Create appropriate visualizations"
        
        prompt = f"""{ANALYZER_PROMPT}

User Question: {question}

Visualization Hints: {viz_instruction}

Gathered Data:
{json.dumps(data, default=str, indent=2)[:6000]}

Analyze this data and provide your findings with structured charts and tables."""

        response = await self._call_llm(prompt, step)
        
        # Parse analysis response
        data_tables = []
        charts = []
        findings = []
        confidence = 0.7
        needs_more_data = False
        
        try:
            # Try to extract JSON from response
            json_match = response.find("{")
            if json_match >= 0:
                json_str = response[json_match:]
                # Find matching closing brace
                depth = 0
                end_idx = 0
                for i, c in enumerate(json_str):
                    if c == '{':
                        depth += 1
                    elif c == '}':
                        depth -= 1
                        if depth == 0:
                            end_idx = i + 1
                            break
                
                if end_idx > 0:
                    parsed = json.loads(json_str[:end_idx])
                    findings = parsed.get("findings", [])
                    data_tables = parsed.get("data_tables", [])
                    charts = parsed.get("charts", [])
                    needs_more_data = parsed.get("needs_more_data", False)
                    confidence = parsed.get("confidence", 0.7)
        except Exception as e:
            # Log but continue
            print(f"Error parsing analysis JSON: {e}")
        
        # If no structured output, try to build from raw data
        if not data_tables and not charts and data:
            data_tables, charts = self._build_visualizations_from_data(question, data)
        
        # If still no findings, use raw response
        if not findings:
            findings = [response[:500]]
        
        return AnalysisResult(
            findings=findings,
            data_tables=data_tables,
            charts=charts,
            needs_more_data=needs_more_data,
            confidence=confidence,
        )
    
    def _build_visualizations_from_data(
        self, 
        question: str, 
        data: Dict
    ) -> Tuple[List[Dict], List[Dict]]:
        """Build visualizations directly from gathered data."""
        tables = []
        charts = []
        
        for req_id, req_data in data.items():
            if not isinstance(req_data, dict):
                continue
                
            raw_data = req_data.get("data", {})
            desc = req_data.get("description", "Data")
            
            # Handle different data formats
            if isinstance(raw_data, list) and len(raw_data) > 0:
                # List of records - make a table
                first = raw_data[0]
                if isinstance(first, dict):
                    columns = list(first.keys())[:8]  # Limit columns
                    tables.append({
                        "title": desc[:50],
                        "summary": f"Data for: {desc}",
                        "columns": columns,
                        "rows": raw_data[:20],  # Limit rows
                    })
                    
                    # Try to make a chart if there's a year/time column
                    time_cols = [c for c in columns if any(t in c.lower() for t in ["year", "date", "month", "period"])]
                    numeric_cols = [c for c in columns if isinstance(first.get(c), (int, float))]
                    
                    if time_cols and numeric_cols:
                        charts.append({
                            "chart_type": "line",
                            "title": f"{desc[:40]} Over Time",
                            "x_axis": time_cols[0],
                            "y_axis": numeric_cols[0],
                            "data": raw_data[:30],
                            "series": [{"key": numeric_cols[0], "label": numeric_cols[0], "color": "#3B82F6"}]
                        })
                    elif numeric_cols and len(raw_data) <= 15:
                        # Bar chart for categorical comparison
                        name_col = next((c for c in columns if any(n in c.lower() for n in ["name", "org", "payer", "plan"])), columns[0])
                        charts.append({
                            "chart_type": "bar",
                            "title": f"{desc[:40]} Comparison",
                            "x_axis": name_col,
                            "y_axis": numeric_cols[0],
                            "data": raw_data[:15],
                            "series": [{"key": numeric_cols[0], "label": numeric_cols[0], "color": "#10B981"}]
                        })
                        
            elif isinstance(raw_data, dict):
                # Single record - convert to table format
                if raw_data:
                    columns = ["Metric", "Value"]
                    rows = [{"Metric": k, "Value": v} for k, v in list(raw_data.items())[:15]]
                    tables.append({
                        "title": desc[:50],
                        "summary": f"Key metrics for: {desc}",
                        "columns": columns,
                        "rows": rows,
                    })
        
        return tables, charts
    
    async def _validate(
        self, 
        question: str, 
        analysis: AnalysisResult
    ) -> Dict:
        """Validate the analysis results."""
        step = AgentStep(
            step_id=str(uuid.uuid4()),
            step_number=len(self.current_audit.steps) + 1,
            phase=AgentPhase.VALIDATING.value,
            description="Validating analysis",
            input_data=json.dumps({"findings": analysis.findings}, indent=2),
        )
        
        prompt = f"""{VALIDATOR_PROMPT}

User Question: {question}

Analysis Findings:
{json.dumps(analysis.findings, indent=2)}

Confidence: {analysis.confidence}

Validate this analysis."""

        response = await self._call_llm(prompt, step)
        
        # Parse validation
        validation = {"passes_validation": True, "issues": [], "suggestions": []}
        try:
            json_match = response.find("{")
            if json_match >= 0:
                json_str = response[json_match:]
                json_end = json_str.rfind("}") + 1
                validation = json.loads(json_str[:json_end])
        except:
            pass
        
        step.output_data = json.dumps(validation, indent=2)
        step.decision = "Passed" if validation.get("passes_validation", True) else "Issues found"
        
        self.current_audit.add_step(step)
        return validation
    
    async def _synthesize(
        self, 
        question: str, 
        analysis: AnalysisResult,
        validation: Dict
    ) -> str:
        """Synthesize a natural language response."""
        step = AgentStep(
            step_id=str(uuid.uuid4()),
            step_number=len(self.current_audit.steps) + 1,
            phase=AgentPhase.SYNTHESIZING.value,
            description="Generating response",
        )
        
        # Describe what visuals will be shown
        visual_desc = []
        if analysis.charts:
            for chart in analysis.charts[:3]:
                visual_desc.append(f"- CHART ({chart.get('chart_type', 'unknown')}): {chart.get('title', 'Untitled')}")
        if analysis.data_tables:
            for table in analysis.data_tables[:3]:
                row_count = len(table.get('rows', []))
                visual_desc.append(f"- TABLE: {table.get('title', 'Untitled')} ({row_count} rows)")
        
        visuals_info = "\n".join(visual_desc) if visual_desc else "None"
        
        prompt = f"""{SYNTHESIZER_PROMPT}

User Question: {question}

Key Findings:
{json.dumps(analysis.findings, indent=2)}

VISUALIZATIONS THAT WILL BE SHOWN BELOW YOUR RESPONSE:
{visuals_info}

Validation Notes:
{json.dumps(validation.get("suggestions", []), indent=2)}

Write a conversational response that INTERPRETS and CONTEXTUALIZES the data. 
Reference the charts/tables naturally but don't repeat all the numbers they contain."""

        response = await self._call_llm(prompt, step)
        
        step.output_data = response[:500] + "..."
        self.current_audit.add_step(step)
        
        return response
    
    async def _call_llm(self, prompt: str, step: AgentStep) -> str:
        """Call the LLM and record the call."""
        start_time = time.time()
        
        # For now, use synchronous Anthropic call
        # TODO: Make properly async
        import anthropic
        
        client = anthropic.Anthropic(api_key=self.api_key)
        
        response = client.messages.create(
            model=self.model,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        latency = int((time.time() - start_time) * 1000)
        
        # Extract response text
        response_text = response.content[0].text if response.content else ""
        
        # Calculate cost
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        total_tokens = input_tokens + output_tokens
        
        model_costs = LLM_COSTS.get(self.model.split("-20")[0], {"input": 3.0, "output": 15.0})
        cost = (input_tokens * model_costs["input"] / 1_000_000) + \
               (output_tokens * model_costs["output"] / 1_000_000)
        
        # Record call
        llm_call = LLMCall(
            call_id=str(uuid.uuid4()),
            phase=step.phase,
            model=self.model,
            prompt_tokens=input_tokens,
            completion_tokens=output_tokens,
            total_tokens=total_tokens,
            cost_usd=cost,
            latency_ms=latency,
            timestamp=datetime.now(timezone.utc).isoformat(),
            prompt_preview=prompt[:200],
            response_preview=response_text[:200],
        )
        step.llm_calls.append(llm_call)
        
        return response_text
    
    def _parse_requirements(self, llm_response: str) -> List[DataRequirement]:
        """Parse data requirements from LLM response."""
        requirements = []
        
        try:
            # Find JSON in response
            json_match = llm_response.find("{")
            if json_match >= 0:
                json_str = llm_response[json_match:]
                json_end = json_str.rfind("}") + 1
                parsed = json.loads(json_str[:json_end])
                
                for i, req in enumerate(parsed.get("requirements", [])):
                    requirements.append(DataRequirement(
                        requirement_id=str(uuid.uuid4()),
                        description=req.get("description", ""),
                        data_type=req.get("data_type", "unknown"),
                        query_approach=req.get("query_approach", "sql"),
                        specific_query=req.get("specific_query"),
                        priority=req.get("priority", 2),
                    ))
        except Exception as e:
            # Fallback: create a generic requirement
            requirements.append(DataRequirement(
                requirement_id=str(uuid.uuid4()),
                description="General lookup",
                data_type="unknown",
                query_approach="knowledge",
                priority=1,
            ))
        
        return requirements


# =============================================================================
# USAGE TRACKING / METRICS
# =============================================================================

class AgentMetrics:
    """Track agent usage metrics across runs."""
    
    def __init__(self):
        self.runs: List[Dict] = []
    
    def record_run(self, audit: AgentAudit):
        """Record a completed run."""
        self.runs.append(audit.summary())
    
    def get_summary(self) -> Dict:
        """Get aggregate metrics."""
        if not self.runs:
            return {"total_runs": 0}
        
        return {
            "total_runs": len(self.runs),
            "total_cost_usd": sum(r["cost_usd"] for r in self.runs),
            "total_tokens": sum(r["total_tokens"] for r in self.runs),
            "avg_latency_ms": sum(r["latency_ms"] for r in self.runs) / len(self.runs),
            "avg_llm_calls": sum(r["llm_calls"] for r in self.runs) / len(self.runs),
            "avg_tool_calls": sum(r["tool_calls"] for r in self.runs) / len(self.runs),
            "avg_confidence": sum(r["confidence"] for r in self.runs) / len(self.runs),
        }


# Singleton metrics tracker
_metrics = AgentMetrics()

def get_agent_metrics() -> AgentMetrics:
    return _metrics


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================

async def ask_agent(question: str, user_id: str = "anonymous") -> Tuple[str, Dict]:
    """
    Simple interface to ask the agent a question.
    
    Returns: (answer, audit_summary)
    """
    agent = MAAgentV2()
    answer, audit = await agent.answer(question, user_id)
    
    # Record metrics
    get_agent_metrics().record_run(audit)
    
    return answer, audit.summary()
