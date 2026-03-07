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

# Import visualization service
from api.services.visualization_service import (
    VisualizationService,
    VizIntent,
    VizType,
    parse_viz_intents,
)

# Import schema context service
from api.services.schema_context import get_schema_prompt

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

PLANNER_PROMPT = """You are an MA data analyst. TODAY IS MARCH 4, 2026. Data available: 2013-2026.

═══════════════════════════════════════════════════════════════════════════════
WORKING SQL EXAMPLES - USE THESE PATTERNS
═══════════════════════════════════════════════════════════════════════════════

1. ENROLLMENT BY PAYER OVER TIME:
```sql
SELECT year, parent_org, SUM(enrollment) as enrollment
FROM fact_enrollment_unified
WHERE parent_org IN ('UnitedHealth Group, Inc.', 'Humana Inc.', 'CVS Health Corporation')
GROUP BY year, parent_org
ORDER BY year, parent_org
```

2. D-SNP ENROLLMENT BY PAYER:
```sql
SELECT year, parent_org, SUM(enrollment) as enrollment
FROM fact_enrollment_unified
WHERE snp_type = 'D-SNP'
  AND parent_org LIKE '%United%'
GROUP BY year, parent_org
ORDER BY year
```

3. ENROLLMENT BY STATE:
```sql
SELECT year, state, parent_org, SUM(enrollment) as enrollment
FROM fact_enrollment_unified
WHERE state = 'TX' AND year >= 2020
GROUP BY year, state, parent_org
ORDER BY year, enrollment DESC
```

4. ENROLLMENT BY PLAN TYPE:
```sql
SELECT year, plan_type, SUM(enrollment) as enrollment
FROM fact_enrollment_unified
WHERE parent_org LIKE '%Humana%'
GROUP BY year, plan_type
ORDER BY year, enrollment DESC
```

5. GROUP VS INDIVIDUAL ENROLLMENT:
```sql
SELECT year, group_type, parent_org, SUM(enrollment) as enrollment
FROM fact_enrollment_unified
WHERE parent_org IN ('UnitedHealth Group, Inc.', 'Humana Inc.')
GROUP BY year, group_type, parent_org
ORDER BY year, parent_org
```

6. STAR RATINGS - % IN 4+ STARS BY PAYER:
```sql
SELECT 
  star_year as year,
  parent_org,
  SUM(enrollment) as total_enrollment,
  SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) as fourplus_enrollment,
  ROUND(100.0 * SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) / 
        NULLIF(SUM(enrollment), 0), 1) as pct_fourplus
FROM stars_enrollment_unified
GROUP BY star_year, parent_org
HAVING SUM(enrollment) > 50000
ORDER BY parent_org, star_year
```

7. FIND MAJOR STAR RATING DROPS:
```sql
WITH yearly AS (
  SELECT star_year, parent_org, 
         ROUND(100.0 * SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) / 
               NULLIF(SUM(enrollment), 0), 1) as pct_fourplus,
         SUM(enrollment) as enrollment
  FROM stars_enrollment_unified
  GROUP BY star_year, parent_org
  HAVING SUM(enrollment) > 100000
)
SELECT curr.parent_org, curr.star_year as drop_year,
       prev.pct_fourplus as before, curr.pct_fourplus as after,
       ROUND(curr.pct_fourplus - prev.pct_fourplus, 1) as drop_amount,
       curr.enrollment
FROM yearly curr
JOIN yearly prev ON curr.parent_org = prev.parent_org 
  AND curr.star_year = prev.star_year + 1
WHERE curr.pct_fourplus - prev.pct_fourplus < -20
ORDER BY drop_amount
```

8. HCC COEFFICIENTS V24 vs V28:
```sql
SELECT hcc_code, hcc_label, 
       MAX(CASE WHEN model_year = 2024 THEN coefficient END) as v24,
       MAX(CASE WHEN model_year = 2027 THEN coefficient END) as v28
FROM hcc_coefficients_all
WHERE segment = 'Community, NonDual, Aged'
  AND hcc_code LIKE 'HCC%' AND hcc_code NOT LIKE 'RXHCC%'
GROUP BY hcc_code, hcc_label
ORDER BY hcc_code
```

═══════════════════════════════════════════════════════════════════════════════
ENTITY NAME MAPPING
═══════════════════════════════════════════════════════════════════════════════

User says → Use in SQL:
• "united", "unh", "uhc" → 'UnitedHealth Group, Inc.' or LIKE '%United%'
• "humana"               → 'Humana Inc.' or LIKE '%Humana%'  
• "cvs", "aetna"         → 'CVS Health Corporation' or LIKE '%CVS%'
• "elevance", "anthem"   → 'Elevance Health, Inc.' or LIKE '%Elevance%'
• "cigna"                → LIKE '%CIGNA%'
• "kaiser"               → LIKE '%Kaiser%'
• "centene"              → 'Centene Corporation' or LIKE '%Centene%'

═══════════════════════════════════════════════════════════════════════════════
KEY TABLES
═══════════════════════════════════════════════════════════════════════════════

• fact_enrollment_unified - THE main enrollment table (3M rows, 2013-2026)
  Columns: year, month, contract_id, state, parent_org, plan_type, 
           product_type, group_type, snp_type, enrollment

• stars_enrollment_unified - Star ratings + enrollment (10K rows, 2013-2026)
  Columns: star_year, contract_id, parent_org, overall_rating, enrollment,
           plan_type, group_type, snp_type, is_fourplus

• hcc_coefficients_all - Risk model coefficients (7K rows)
  Columns: model_year, hcc_code, hcc_label, segment, coefficient
  Note: model_year=2024 is V24, model_year=2027 is V28

• measure_stars_all_years - Individual measure scores (245K rows)
  Columns: year, contract_id, parent_org, measure_id, star_rating

═══════════════════════════════════════════════════════════════════════════════
COLUMN VALUES
═══════════════════════════════════════════════════════════════════════════════

plan_type: 'HMO/HMOPOS', 'Local PPO', 'Regional PPO', 'PFFS', 'MSA', 
           'Medicare Prescription Drug Plan', '1876 Cost', 'National PACE'

snp_type: 'D-SNP', 'C-SNP', 'I-SNP', 'Non-SNP', NULL

group_type: 'Individual', 'Group', NULL

state: Two-letter codes ('TX', 'CA', 'FL', etc.)

═══════════════════════════════════════════════════════════════════════════════
TIME INTERPRETATION
═══════════════════════════════════════════════════════════════════════════════

• "last 10 years" → year >= 2016 (current year is 2026)
• "last 5 years"  → year >= 2021
• "recent"        → year >= 2024
• "all time"      → no year filter (2013-2026)

═══════════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT
═══════════════════════════════════════════════════════════════════════════════

Return JSON with your data requirements:
```json
{
  "requirements": [
    {
      "tool": "query_database",
      "description": "Get enrollment by payer over time",
      "sql": "SELECT year, parent_org, SUM(enrollment) as enrollment FROM fact_enrollment_unified WHERE parent_org IN (...) GROUP BY year, parent_org ORDER BY year, parent_org"
    }
  ]
}
```

IMPORTANT RULES:
• ALWAYS use GROUP BY when aggregating enrollment
• ALWAYS use SUM(enrollment) not just enrollment
• Use LIKE '%name%' for fuzzy matching, exact names for IN clauses
• Include ORDER BY for consistent results"""

ANALYZER_PROMPT = """You are a Medicare Advantage data analyst. Analyze the query results and specify WHAT visualizations would best tell the story.

IMPORTANT: You specify WHAT to visualize. A separate system handles HOW to render it properly.

=== YOUR RESPONSIBILITIES ===

1. Extract KEY FINDINGS from the data (specific numbers, patterns)
2. Decide WHAT visualizations would tell the story best
3. Specify visualization INTENTS (type, focus, metric) - NOT raw chart specs

=== VISUALIZATION TYPES ===

| Type        | Use For                                    | Example                              |
|-------------|--------------------------------------------|-----------------------------------------|
| trajectory  | Time series by company/entity              | Recovery patterns over years            |
| ranking     | Top/bottom N by a metric                   | Biggest enrollment gainers              |
| change      | Gains vs losses (diverging)                | D-SNP enrollment changes                |
| comparison  | Before/after, A vs B                       | Pre-drop vs post-drop                   |
| trend       | Simple time series (single line)           | Total enrollment over time              |
| table       | Detailed breakdown                         | Full list with all metrics              |

=== MULTIPLE VISUALIZATIONS ===

For complex questions, specify 2-4 visualizations:
- Star rating drops/recovery → trajectory + ranking + table
- D-SNP gainers/losers → ranking (gainers) + ranking (losers) + table
- V24 vs V28 → comparison + ranking (increases) + ranking (decreases)

=== REQUIRED OUTPUT FORMAT ===
```json
{
  "findings": [
    "Humana had the largest 4+ star drop: -56.1 points in 2025, affecting 6.8M members",
    "Blue Cross Michigan recovered fully (15.3% → 100%) within 1 year",
    "67% of companies with major drops (>50 points) never recovered"
  ],
  
  "viz_intents": [
    {
      "viz_type": "trajectory",
      "title": "Recovery Trajectories: Companies with Major 4+ Star Drops",
      "description": "Shows year-over-year 4+ star percentage for companies that experienced drops",
      "metric": "pct_four_star",
      "time_field": "star_year",
      "group_by": "parent_org",
      "limit": 8
    },
    {
      "viz_type": "ranking",
      "title": "Largest 4+ Star Enrollment Drops",
      "description": "Companies ranked by magnitude of drop",
      "metric": "drop_amount",
      "dimension": "parent_org",
      "sort_order": "asc",
      "limit": 10
    },
    {
      "viz_type": "table",
      "title": "Drop and Recovery Details"
    }
  ],
  
  "needs_more_data": false,
  "additional_data_needed": [],
  "confidence": 0.85,
  "caveats": ["Recovery analysis limited to overall rating"]
}
```

=== VIZ_INTENT FIELDS ===

Required:
- viz_type: "trajectory" | "ranking" | "change" | "comparison" | "trend" | "table"
- title: Descriptive title that tells the story

For trajectory/trend:
- metric: The y-axis value (e.g., "pct_four_star", "enrollment")
- time_field: The x-axis (e.g., "year", "star_year")
- group_by: Field to create multiple lines (e.g., "parent_org")

For ranking/change:
- metric: What to rank by (e.g., "enrollment_change", "drop_amount")
- dimension: What entities to rank (e.g., "parent_org")
- sort_order: "asc" or "desc"
- limit: Number of items to show (default 10)

=== EXAMPLES ===

**Question: "Star rating drops and recovery patterns"**

viz_intents:
1. {viz_type: "trajectory", title: "Recovery Trajectories After Major Drops", metric: "pct_four_star", time_field: "star_year", group_by: "parent_org"}
2. {viz_type: "ranking", title: "Largest Star Rating Drops", metric: "drop_amount", dimension: "parent_org", sort_order: "asc", limit: 10}
3. {viz_type: "table", title: "Complete Drop and Recovery Data"}

**Question: "D-SNP changes Dec 2025 to Feb 2026"**

viz_intents:
1. {viz_type: "ranking", title: "Top D-SNP Gainers", metric: "enrollment_change", dimension: "parent_org", sort_order: "desc", limit: 10}
2. {viz_type: "ranking", title: "Top D-SNP Losers", metric: "enrollment_change", dimension: "parent_org", sort_order: "asc", limit: 10}
3. {viz_type: "table", title: "D-SNP Market Changes Summary"}

=== ANALYSIS REQUIREMENTS ===

1. BE SPECIFIC: Include actual numbers from the data in findings
2. IDENTIFY PATTERNS: Who are the outliers? What's the trend?
3. PROVIDE CONTEXT: Compare to benchmarks, industry norms
4. ACKNOWLEDGE GAPS: Set needs_more_data if analysis is incomplete

DO NOT create raw chart data - just specify what you want to visualize."""

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

=== CRITICAL: DATA-BACKED ANALYSIS ONLY ===

NEVER make claims you cannot prove with the data provided. Examples of BAD unsupported claims:
- "Recovery typically takes 3-4 years" (unless you have actual data showing this)
- "They should recover because they have resources" (opinion, not data)
- "This is due to CMS methodology changes" (unless you searched documents and found evidence)

ALWAYS back up claims with specific data:
- "CVS dropped 32 points in 2021 and recovered 15 points by 2024 - that's 3 years"
- "Looking at the 5 companies that had >30 point drops, 3 recovered within 2 years: X, Y, Z"
- "Humana's drop was concentrated in Member Experience measures - C18 dropped from 4 to 2"

If you don't have data to support a conclusion, say so:
- "I don't have measure-level data to identify which specific measures drove this"
- "To assess recovery likelihood, we'd need to compare their measure profile to past recoverers"

=== BEAUTIFUL OUTPUT STRUCTURE ===

Your response appears ABOVE the charts and tables. Structure it like a polished analyst report:

**PARAGRAPH 1: The Headline**
- Start with the most important finding
- Use specific numbers
- Make it punchy and memorable

**PARAGRAPH 2: The Story**  
- Explain what's happening and why it matters
- Compare winners vs losers, or before vs after
- Add business context

**PARAGRAPH 3: The Details**
- Reference the charts/tables: "The first chart below shows..."
- Point out what to look for in the visualizations
- Note any caveats or data limitations

=== FORMATTING ===

Use markdown for readability:
- **Bold** for key numbers and company names
- Bullet points for lists of 3+ items
- Keep paragraphs SHORT (2-3 sentences max)

=== EXAMPLE BEAUTIFUL RESPONSE ===

For "D-SNP changes Dec 2025 to Feb 2026":

---
**Humana dominated D-SNP growth** this quarter, adding **+141K members** (+18.6%) while market leader UnitedHealth actually lost **-96K** (-4.0%). This represents a significant shift in the dual eligible market.

The growth was concentrated among a few aggressive players:
- **CareSource** nearly tripled (+179%)
- **Molina** grew 39%
- **Devoted Health** up 59%

Meanwhile, several established players contracted. UnitedHealth's loss of nearly 100K members, combined with UCare Minnesota's apparent market exit (-100%), suggests competitive pressure on incumbents.

The first chart below shows the top gainers, while the second highlights the biggest losers. The summary table has the full market breakdown.

---

DO NOT:
- Repeat all the numbers from the tables
- Use corporate buzzwords
- Write walls of text
- Make unsupported predictions

DO:
- Be conversational but professional
- Use specific numbers
- Reference the visualizations naturally
- Keep it scannable
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
        
        # Call LLM for planning with dynamic schema context
        schema_context = get_schema_prompt()
        prompt = f"{PLANNER_PROMPT}\n\n{schema_context}\n\nUser Question: {question}"
        
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
    
    def _extract_company_from_question(self, question: str) -> str:
        """Extract company name from question for targeted queries."""
        import re
        question_lower = question.lower()
        
        # Common company patterns
        companies = {
            "humana": "Humana",
            "unitedhealth": "UnitedHealth",
            "united health": "UnitedHealth", 
            "cvs": "CVS Health",
            "aetna": "CVS Health",
            "anthem": "Elevance",
            "elevance": "Elevance",
            "centene": "Centene",
            "cigna": "CIGNA",
            "kaiser": "Kaiser",
            "bcbs": "Blue Cross Blue Shield",
            "blue cross": "Blue Cross",
        }
        
        for pattern, name in companies.items():
            if pattern in question_lower:
                return name
        
        return "Humana"  # Default to Humana as the most commonly asked about
    
    def _generate_measure_query(self, question: str) -> str:
        """Generate SQL for measure-level star rating analysis."""
        company = self._extract_company_from_question(question)
        return f"""
WITH company_contracts AS (
    SELECT DISTINCT contract_id 
    FROM summary_all_years 
    WHERE parent_organization LIKE '%{company}%'
)
SELECT 
    m.year,
    m.contract_id,
    s.parent_organization,
    m.measure_id,
    ms.measure_name,
    ms.domain,
    ms.weight,
    m.star_rating
FROM measure_stars_all_years m
JOIN summary_all_years s ON m.contract_id = s.contract_id AND m.year = s.year
JOIN stars_measure_specs ms ON m.measure_id = ms.measure_id
WHERE m.contract_id IN (SELECT contract_id FROM company_contracts)
AND m.year BETWEEN 2022 AND 2026
AND ms.weight >= 1
ORDER BY m.year, ms.weight DESC, m.measure_id
LIMIT 200
"""
    
    def _generate_domain_query(self, question: str) -> str:
        """Generate SQL for domain-level star rating breakdown."""
        company = self._extract_company_from_question(question)
        return f"""
SELECT 
    m.year,
    s.parent_organization,
    ms.domain,
    COUNT(*) as measure_count,
    ROUND(AVG(m.star_rating), 2) as avg_stars,
    SUM(CASE WHEN m.star_rating >= 4 THEN 1 ELSE 0 END) as measures_at_4_plus,
    SUM(CASE WHEN m.star_rating <= 2 THEN 1 ELSE 0 END) as measures_at_2_or_below
FROM measure_stars_all_years m
JOIN summary_all_years s ON m.contract_id = s.contract_id AND m.year = s.year
JOIN stars_measure_specs ms ON m.measure_id = ms.measure_id
WHERE s.parent_organization LIKE '%{company}%'
AND m.year BETWEEN 2020 AND 2026
GROUP BY m.year, s.parent_organization, ms.domain
ORDER BY m.year, ms.domain
"""
    
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
                # LOG THE SQL FOR DEBUGGING
                print(f"\n{'='*60}")
                print(f"EXECUTING SQL:")
                print(f"{'='*60}")
                print(req.specific_query)
                print(f"{'='*60}\n")
                
                tool_result = self.tools.query_database(
                    sql=req.specific_query,
                    context=req.description
                )
                if tool_result.success:
                    result = tool_result.data
                    # LOG RESULT COUNT
                    if isinstance(result, dict) and 'rows' in result:
                        print(f"SQL returned {len(result['rows'])} rows")
                else:
                    error = tool_result.error
                    print(f"SQL ERROR: {error}")
                    
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
                    tool_result = self.tools.lookup_knowledge(query=req.description)
                    result = tool_result.data if tool_result.success else None
                    error = tool_result.error if not tool_result.success else None
                    
            elif req.query_approach == "knowledge":
                tool_name = "lookup_knowledge"
                tool_result = self.tools.lookup_knowledge(query=req.description)
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
        """Analyze gathered data using the visualization service."""
        
        # Build prompt for analyzer - focuses on WHAT to visualize, not HOW
        prompt = f"""{ANALYZER_PROMPT}

User Question: {question}

Available Data Sources:
{self._summarize_data_sources(data)}

Full Data:
{json.dumps(data, default=str, indent=2)[:8000]}

Analyze this data and specify what visualizations would best tell the story."""

        response = await self._call_llm(prompt, step)
        
        # Parse analysis response
        findings = []
        confidence = 0.7
        needs_more_data = False
        additional_requirements = []
        
        viz_intents = []
        
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
                    needs_more_data = parsed.get("needs_more_data", False)
                    confidence = parsed.get("confidence", 0.7)
                    
                    # Parse viz_intents from the new format
                    viz_intents_raw = parsed.get("viz_intents", [])
                    for viz in viz_intents_raw:
                        viz_intents.append(VizIntent(
                            viz_type=viz.get("viz_type", "auto"),
                            title=viz.get("title", "Chart"),
                            description=viz.get("description", ""),
                            metric=viz.get("metric", ""),
                            dimension=viz.get("dimension"),
                            group_by=viz.get("group_by"),
                            time_field=viz.get("time_field"),
                            sort_order=viz.get("sort_order", "desc"),
                            limit=viz.get("limit"),
                        ))
                    
                    # Parse additional_data_needed into actual requirements
                    additional_data_needed = parsed.get("additional_data_needed", [])
                    for desc in additional_data_needed:
                        if isinstance(desc, str):
                            desc_lower = desc.lower()
                            
                            if "measure" in desc_lower and "stars" in desc_lower:
                                additional_requirements.append(DataRequirement(
                                    requirement_id=str(uuid.uuid4()),
                                    description=desc,
                                    data_type="stars",
                                    query_approach="sql",
                                    specific_query=self._generate_measure_query(question),
                                    priority=1,
                                ))
                            elif "domain" in desc_lower:
                                additional_requirements.append(DataRequirement(
                                    requirement_id=str(uuid.uuid4()),
                                    description=desc,
                                    data_type="stars",
                                    query_approach="sql", 
                                    specific_query=self._generate_domain_query(question),
                                    priority=1,
                                ))
                                
        except Exception as e:
            print(f"Error parsing analysis JSON: {e}")
        
        # Use VisualizationService to build charts and tables
        viz_service = VisualizationService()
        
        if viz_intents:
            # LLM specified what to visualize - use intents
            charts, data_tables = viz_service.build_from_intents(viz_intents, data)
        else:
            # Fallback to auto-generation based on question and data
            charts, data_tables = viz_service.auto_generate(question, data)
        
        # If still no findings, use raw response
        if not findings:
            findings = [response[:500]]
        
        return AnalysisResult(
            findings=findings,
            data_tables=data_tables,
            charts=charts,
            needs_more_data=needs_more_data,
            additional_requirements=additional_requirements,
            confidence=confidence,
        )
    
    def _summarize_data_sources(self, data: Dict) -> str:
        """Create a summary of available data sources for the analyzer."""
        summaries = []
        for source_id, source_data in data.items():
            if not isinstance(source_data, dict):
                continue
            
            desc = source_data.get("description", source_id)
            raw = source_data.get("data", {})
            
            if isinstance(raw, dict) and "rows" in raw:
                rows = raw.get("rows", [])
                cols = raw.get("columns", [])
                summaries.append(f"- {desc}: {len(rows)} rows, columns: {cols[:6]}")
            elif isinstance(raw, list):
                summaries.append(f"- {desc}: {len(raw)} items")
        
        return "\n".join(summaries) if summaries else "No data sources"
    
    # NOTE: _build_visualizations_from_data has been replaced by VisualizationService
    # The new service provides:
    # - Domain-aware chart building (knows MA data patterns)
    # - Proper scale handling (percentages get 0-100 domain)
    # - Chart validation before rendering
    # - Separation of "what to show" (LLM) from "how to show" (code)
    
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
            # Handle JSON in markdown code blocks
            import re
            
            # Try to extract JSON from ```json ... ``` blocks first
            json_block_match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', llm_response)
            if json_block_match:
                json_str = json_block_match.group(1)
            else:
                # Fallback: find first { and match to its closing }
                json_match = llm_response.find("{")
                if json_match >= 0:
                    json_str = llm_response[json_match:]
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
                    json_str = json_str[:end_idx]
                else:
                    json_str = None
            
            if json_str:
                parsed = json.loads(json_str)
                
                for i, req in enumerate(parsed.get("requirements", [])):
                    requirements.append(DataRequirement(
                        requirement_id=str(uuid.uuid4()),
                        description=req.get("description", ""),
                        data_type=req.get("data_type", "unknown"),
                        query_approach=req.get("query_approach", "sql"),
                        specific_query=req.get("specific_query"),
                        priority=req.get("priority", 2),
                    ))
                    
            if not requirements:
                raise ValueError("No requirements parsed")
                
        except Exception as e:
            print(f"Warning: Failed to parse requirements: {e}")
            # Fallback: create a SQL-based requirement for the question
            requirements.append(DataRequirement(
                requirement_id=str(uuid.uuid4()),
                description="Query star ratings enrollment data",
                data_type="stars",
                query_approach="sql",
                specific_query="""
                    SELECT star_year as year, parent_org,
                           SUM(enrollment) as enrollment,
                           ROUND(100.0 * SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) / NULLIF(SUM(enrollment), 0), 1) as pct_four_star
                    FROM stars_enrollment_unified
                    GROUP BY star_year, parent_org
                    HAVING SUM(enrollment) > 100000
                    ORDER BY parent_org, star_year
                """,
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
