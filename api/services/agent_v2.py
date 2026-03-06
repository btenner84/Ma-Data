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

Output JSON:
```json
{
  "findings": ["Key finding 1", "Key finding 2", ...],
  "comparisons": {"metric": {"current": X, "prior_year": Y, "industry": Z}},
  "trends": [{"metric": "name", "direction": "up|down|stable", "magnitude": "X%"}],
  "needs_more_data": false,
  "additional_queries": [],
  "confidence": 0.9,
  "caveats": ["Any data limitations"]
}
```

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

TONE: Like a senior consultant briefing a colleague - knowledgeable, direct, insightful.

DO NOT:
- Use bullet points unless showing a specific list
- Dump raw numbers without context
- Use robotic phrasing like "Based on my analysis..."
- Structure response like a report

DO:
- Lead with the headline takeaway
- Explain what the numbers mean
- Compare to benchmarks and prior years
- Share your expert perspective
- Note any caveats naturally in the flow

Example good opening:
"The 2027 advance notice is actually pretty favorable - the 4.33% growth rate is above last year, and with V28 fully phased in, we finally have some stability on the risk model front."

NOT:
"Here are the key parameters from the 2027 Advance Notice:
- MA Growth Rate: 4.33%
- Risk Model: V28 at 100%
..."
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
        prompt = f"""{ANALYZER_PROMPT}

User Question: {question}

Gathered Data:
{json.dumps(data, default=str, indent=2)[:4000]}

Analyze this data and provide your findings."""

        response = await self._call_llm(prompt, step)
        
        # Parse analysis response
        try:
            # Try to extract JSON from response
            json_match = response.find("{")
            if json_match >= 0:
                json_str = response[json_match:]
                json_end = json_str.rfind("}") + 1
                parsed = json.loads(json_str[:json_end])
                
                return AnalysisResult(
                    findings=parsed.get("findings", []),
                    data_tables=parsed.get("data_tables", []),
                    charts=parsed.get("charts", []),
                    needs_more_data=parsed.get("needs_more_data", False),
                    confidence=parsed.get("confidence", 0.7),
                )
        except:
            pass
        
        # Fallback: treat entire response as a finding
        return AnalysisResult(
            findings=[response[:500]],
            data_tables=[],
            charts=[],
            needs_more_data=False,
            confidence=0.6,
        )
    
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
        
        prompt = f"""{SYNTHESIZER_PROMPT}

User Question: {question}

Key Findings:
{json.dumps(analysis.findings, indent=2)}

Data Tables Available:
{json.dumps(analysis.data_tables[:3], indent=2) if analysis.data_tables else "None"}

Validation Notes:
{json.dumps(validation.get("suggestions", []), indent=2)}

Now write a natural, conversational response as an MA expert."""

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
