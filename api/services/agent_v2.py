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

PLANNER_PROMPT = """You are a Medicare Advantage data analyst with FULL ACCESS to comprehensive MA data.

=== YOU HAVE ACCESS TO ===

**STRUCTURED DATA (SQL Queryable - 2013-2026):**
- Enrollment by contract, plan, parent organization, state
- Star Ratings: overall ratings + 40+ individual measures
- Risk Scores by contract
- USPCC rates, growth factors, HCC coefficients

**CMS DOCUMENTS (Searchable Full Text):**
- Rate Notices (Advance & Final) - 2016-2027
- Star Ratings Technical Notes - 2016-2027
- Call Letters - 2020-2027
- Risk Adjustment Fact Sheets - 2022-2027
- Payment Methodology documentation

**KNOWLEDGE BASE (Definitions & Context):**
- MA Glossary (HCC, RAF, QBP, CAI, etc.)
- CMS-HCC Model Versions (V12, V21, V22, V24, V28) with phase-in schedules
- Star measure definitions
- Policy timeline and key changes
- Top payer information

=== AVAILABLE TOOLS ===
- query_database: SQL queries against data tables
- search_documents: Search CMS document text
- get_rate_notice_metrics: Structured rate notice data (growth rates, V28 phase-in, Part D params)
- get_hcc_model_info: HCC model details (coefficients, segments, normalization)
- get_ma_policy_changes: Policy changes by year/category
- lookup_knowledge: Glossary and definitions

=== DATABASE SCHEMA ===

**ENROLLMENT TABLES:**
- fact_enrollment_all_years: year, month, contract_id, plan_id, parent_organization, enrollment, state
- enrollment_by_parent: year, parent_organization, total_enrollment (aggregated)
- fact_enrollment_by_state: year, month, state, parent_organization, enrollment (state aggregates)
- fact_enrollment_by_geography: year, month, state, county, fips, contract_id, enrollment (county-level!)

**STAR RATING TABLES:**
- stars_enrollment_unified: star_year, contract_id, parent_org, enrollment, overall_rating (1-5), plan_type, group_type, snp_type
  * THE MAIN TABLE for 4+ star analysis - already has enrollment joined!
- summary_all_years: year, contract_id, parent_organization, overall_rating (1-5 scale), plan_type
- measure_stars_all_years: year, contract_id, measure_id, star_rating (individual measure stars 1-5)
- measures_all_years: year, contract_id, measure_id, measure_key, measure_name, numeric_value (raw scores)
- cutpoints_all_years: year, measure_id, star_level (1-5), low_threshold, high_threshold
  * Use to see "what score gets 4 stars on measure X"
- stars_measure_specs: measure_id, measure_name, domain, weight, data_source
  * Use to see measure weights and which domain they belong to

**SNP (SPECIAL NEEDS PLAN) TABLES:**
- fact_snp: year, parent_org, snp_type (D-SNP/C-SNP/I-SNP), enrollment, contract_count
- fact_snp_historical: year, parent_org, snp_type, enrollment (longer history)
  * Use for D-SNP growth analysis, dual eligible trends

**DISENROLLMENT TABLES:**
- disenrollment_all_years: year, contract_id, parent_organization, disenrollment_count, disenrollment_rate
  * Use for member retention analysis, "which plans have high churn"

**ENTITY TRACKING (Contract ID Changes):**
- dim_entity: contract_id, entity_id, parent_org, first_year, last_year, predecessor_id, successor_id
  * CRITICAL for tracking contracts through mergers, ID changes over time
  * entity_id is stable across contract_id changes

**RISK SCORE TABLES:**
- fact_risk_scores_unified: year, contract_id, risk_score, member_months
- risk_scores_by_parent: year, parent_org, avg_risk_score, total_member_months

**RATE/BENCHMARK TABLES:**
- uspcc_projections: year, aged_uspcc, disabled_uspcc, esrd_uspcc
- hcc_coefficients_all: year, model_version, hcc_code, coefficient, description
- county_benchmarks: year, state, county, fips, benchmark_rate

=== EXAMPLE QUERIES ===

**4+ Star Enrollment by Parent Org (THE MAIN QUERY for star rating analysis):**
```sql
SELECT 
  star_year as year,
  parent_org,
  SUM(enrollment) as total_enrollment,
  SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) as four_star_enrollment,
  ROUND(100.0 * SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) / NULLIF(SUM(enrollment), 0), 1) as pct_four_star
FROM stars_enrollment_unified
WHERE star_year BETWEEN 2015 AND 2026
GROUP BY star_year, parent_org
HAVING SUM(enrollment) > 100000
ORDER BY parent_org, star_year
```

**Find Major 4+ Star Drops (>20 point drop year-over-year):**
```sql
WITH yearly AS (
  SELECT 
    star_year,
    parent_org,
    SUM(enrollment) as enrollment,
    ROUND(100.0 * SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) / NULLIF(SUM(enrollment), 0), 1) as pct_four_star
  FROM stars_enrollment_unified
  GROUP BY star_year, parent_org
  HAVING SUM(enrollment) > 100000
)
SELECT 
  curr.parent_org,
  curr.star_year as year,
  prev.pct_four_star as prev_pct,
  curr.pct_four_star as curr_pct,
  ROUND(curr.pct_four_star - prev.pct_four_star, 1) as change
FROM yearly curr
JOIN yearly prev ON curr.parent_org = prev.parent_org AND curr.star_year = prev.star_year + 1
WHERE curr.pct_four_star - prev.pct_four_star < -20
ORDER BY change ASC
```

**Single Company Star Rating History (e.g., Humana):**
```sql
SELECT 
  star_year as year,
  parent_org,
  SUM(enrollment) as enrollment,
  ROUND(100.0 * SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) / NULLIF(SUM(enrollment), 0), 1) as pct_four_star
FROM stars_enrollment_unified
WHERE parent_org LIKE '%Humana%'
GROUP BY star_year, parent_org
ORDER BY star_year
```

**Enrollment Trends by Organization:**
```sql
SELECT star_year as year, parent_org, SUM(enrollment) as enrollment
FROM stars_enrollment_unified 
WHERE parent_org IN ('Humana', 'UnitedHealth Group', 'CVS Health')
GROUP BY star_year, parent_org ORDER BY parent_org, star_year
```

**D-SNP Growth by Parent Organization:**
```sql
SELECT year, parent_org, snp_type, enrollment
FROM fact_snp_historical
WHERE snp_type = 'D-SNP'
ORDER BY parent_org, year
```

**State-Level Market Share:**
```sql
SELECT year, state, parent_organization, SUM(enrollment) as enrollment,
  ROUND(100.0 * SUM(enrollment) / SUM(SUM(enrollment)) OVER (PARTITION BY year, state), 1) as market_share
FROM fact_enrollment_by_state
WHERE year = 2026 AND month = 12
GROUP BY year, state, parent_organization
ORDER BY state, market_share DESC
```

**County-Level Enrollment (Geographic Analysis):**
```sql
SELECT year, state, county, fips, SUM(enrollment) as enrollment
FROM fact_enrollment_by_geography
WHERE state = 'FL' AND year = 2026
GROUP BY year, state, county, fips
ORDER BY enrollment DESC
```

**Star Rating Cutpoints (What score gets 4 stars?):**
```sql
SELECT year, measure_id, star_level, low_threshold, high_threshold
FROM cutpoints_all_years
WHERE measure_id = 'C01' AND star_level = 4
ORDER BY year
```

**Measure Weights and Domains:**
```sql
SELECT measure_id, measure_name, domain, weight
FROM stars_measure_specs
WHERE weight > 1
ORDER BY weight DESC
```

**Disenrollment Analysis:**
```sql
SELECT year, parent_organization, SUM(disenrollment_count) as disenrollment,
  AVG(disenrollment_rate) as avg_rate
FROM disenrollment_all_years
GROUP BY year, parent_organization
ORDER BY avg_rate DESC
```

**Contract ID Tracking (Entity Changes):**
```sql
SELECT contract_id, entity_id, parent_org, first_year, last_year, predecessor_id, successor_id
FROM dim_entity
WHERE parent_org LIKE '%Humana%'
ORDER BY first_year
```

=== MEASURE-LEVEL ANALYSIS QUERIES (Critical for deep analysis!) ===

**Get Specific Measure Stars for a Contract (which measures hurt them?):**
```sql
SELECT 
  m.year,
  m.contract_id,
  m.measure_id,
  m.star_rating,
  ms.measure_name,
  ms.domain,
  ms.weight
FROM measure_stars_all_years m
JOIN stars_measure_specs ms ON m.measure_id = ms.measure_id
WHERE m.contract_id IN (
  SELECT contract_id FROM summary_all_years 
  WHERE parent_organization LIKE '%Humana%'
)
AND m.year BETWEEN 2023 AND 2026
ORDER BY m.contract_id, m.year, ms.weight DESC
```

**Year-over-Year Measure Changes (identify what dropped):**
```sql
WITH measure_history AS (
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
  WHERE ms.weight >= 1  -- Focus on weighted measures
)
SELECT 
  curr.parent_organization,
  curr.measure_id,
  curr.measure_name,
  curr.domain,
  curr.weight,
  prev.star_rating as prev_stars,
  curr.star_rating as curr_stars,
  curr.star_rating - prev.star_rating as change
FROM measure_history curr
JOIN measure_history prev 
  ON curr.contract_id = prev.contract_id 
  AND curr.measure_id = prev.measure_id 
  AND curr.year = prev.year + 1
WHERE curr.star_rating - prev.star_rating <= -2  -- 2+ star drop on a measure
AND curr.year = 2025
ORDER BY curr.weight DESC, change ASC
```

**Recovery Analysis - Companies that recovered (with measure detail):**
```sql
WITH yearly_pct AS (
  SELECT 
    star_year,
    parent_org,
    SUM(enrollment) as enrollment,
    ROUND(100.0 * SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) / 
          NULLIF(SUM(enrollment), 0), 1) as pct_four_star
  FROM stars_enrollment_unified
  GROUP BY star_year, parent_org
  HAVING SUM(enrollment) > 50000
),
drops AS (
  SELECT 
    curr.parent_org,
    curr.star_year as drop_year,
    prev.pct_four_star as before_drop,
    curr.pct_four_star as after_drop
  FROM yearly_pct curr
  JOIN yearly_pct prev ON curr.parent_org = prev.parent_org AND curr.star_year = prev.star_year + 1
  WHERE prev.pct_four_star - curr.pct_four_star > 25  -- Major drop
)
SELECT 
  d.parent_org,
  d.drop_year,
  d.before_drop,
  d.after_drop,
  y.star_year,
  y.pct_four_star,
  CASE 
    WHEN y.pct_four_star >= d.before_drop * 0.9 THEN 'RECOVERED'
    WHEN y.pct_four_star >= d.after_drop + 20 THEN 'PARTIAL'
    ELSE 'NOT_RECOVERED'
  END as recovery_status,
  y.star_year - d.drop_year as years_since_drop
FROM drops d
JOIN yearly_pct y ON d.parent_org = y.parent_org AND y.star_year >= d.drop_year
ORDER BY d.parent_org, y.star_year
```

**Measure Performance for Specific Domain (e.g., Member Experience):**
```sql
SELECT 
  m.year,
  s.parent_organization,
  ms.domain,
  COUNT(*) as measure_count,
  AVG(m.star_rating) as avg_stars,
  SUM(CASE WHEN m.star_rating >= 4 THEN 1 ELSE 0 END) as measures_at_4_plus
FROM measure_stars_all_years m
JOIN summary_all_years s ON m.contract_id = s.contract_id AND m.year = s.year
JOIN stars_measure_specs ms ON m.measure_id = ms.measure_id
WHERE s.parent_organization LIKE '%Humana%'
AND ms.domain LIKE '%Experience%'  -- Or '%Outcome%', '%Process%', '%Access%'
GROUP BY m.year, s.parent_organization, ms.domain
ORDER BY m.year
```

=== MULTI-STEP ANALYSIS STRATEGY ===

For complex questions like "4+ star drops and recovery patterns", use MULTIPLE requirements:

1. **First**: Get the drops - who dropped, when, how much
2. **Second**: Get measure-level detail - which specific measures caused it  
3. **Third**: Get recovery data - for companies that dropped before, how did they recover?
4. **Fourth**: Compare current situation - how does Humana's measure profile compare to recoverers?

DO NOT make assumptions about recovery timelines or causes. 
Query the actual historical data to find:
- Real examples of companies that dropped and recovered
- How long it actually took them
- Which measures they improved
- What changed in their performance

=== TOOL-BASED QUERIES (for documents/knowledge) ===

**For Technical Notes questions (star rating methodology):**
{
  "description": "Star rating technical methodology for 2026",
  "data_type": "policy",
  "query_approach": "tool",
  "specific_query": "search_documents: tech_notes 2026",
  "priority": 1
}

**For Rate Notice questions (USPCC, growth, V28):**
{
  "description": "2027 advance notice rate parameters",
  "data_type": "policy",
  "query_approach": "tool",
  "specific_query": "get_rate_notice_metrics: 2027 advance",
  "priority": 1
}

**For HCC Model questions:**
{
  "description": "V28 model coefficients and changes",
  "data_type": "risk",
  "query_approach": "tool",
  "specific_query": "get_hcc_model_info: V28",
  "priority": 1
}

**For definitions:**
{
  "description": "What is CAI adjustment",
  "data_type": "policy",
  "query_approach": "knowledge",
  "specific_query": "CAI contract adjustment",
  "priority": 2
}

=== OUTPUT FORMAT ===
```json
{
  "question_type": "policy|enrollment|stars|risk|comparison|trend",
  "requirements": [
    {
      "description": "What data is needed",
      "data_type": "enrollment|stars|risk|policy|benchmark",
      "query_approach": "sql|tool|knowledge",
      "specific_query": "SQL query OR tool name with params",
      "priority": 1
    }
  ],
  "analysis_needed": "What analysis to perform",
  "visualization": "chart type if applicable (line for trends, bar for comparisons)"
}
```

IMPORTANT: 
- For DATA questions: Use SQL queries against tables
- For DOCUMENT/POLICY questions: Use tools (search_documents, get_rate_notice_metrics, get_hcc_model_info)
- For DEFINITIONS: Use knowledge lookup
- ALWAYS write complete, executable SQL queries when using sql approach"""

ANALYZER_PROMPT = """You are a Medicare Advantage analyst examining ACTUAL DATA to extract insights.

You have been given REAL query results from our database. Analyze them thoroughly.

Your analysis MUST:
1. Extract specific numbers and findings from the data - BE PRECISE
2. Identify patterns with ACTUAL EXAMPLES (company X did Y in year Z)
3. Calculate changes (year-over-year, percentages)
4. CREATE CHARTS AND TABLES - this is critical for visualization
5. IDENTIFY GAPS - what data is missing to fully answer the question?

=== DEEP ANALYSIS REQUIREMENTS ===

For star rating drop/recovery questions, you MUST analyze:
1. WHO dropped: List specific organizations with exact numbers
2. WHEN: Which years, magnitude of drop
3. RECOVERY: Did they recover? How long? To what level?
4. MEASURES: If measure data is available, which specific measures drove the change?

If measure-level data is NOT in the results, set "needs_more_data": true and specify:
- "We have overall ratings but need measure_stars_all_years to identify which measures drove the drop"

=== REQUIRED OUTPUT FORMAT ===
```json
{
  "findings": [
    "CVS Health dropped from 89.0% to 57.1% (31.9 point drop) in 2023",
    "CVS recovered to 92.3% by 2024 - full recovery in 1 year",
    "Humana dropped from 96.9% to 40.8% (56.1 points) in 2025 - largest drop in dataset"
  ],
  "data_tables": [
    {
      "title": "Major 4+ Star Drops (>25 points) and Recovery Status",
      "summary": "Shows each major drop, the year it occurred, and whether company recovered",
      "columns": ["Organization", "Drop Year", "Before", "After", "Drop", "Recovered?", "Years to Recover"],
      "rows": [
        {"Organization": "CVS Health", "Drop Year": 2023, "Before": 89.0, "After": 57.1, "Drop": -31.9, "Recovered?": "Yes", "Years to Recover": 1},
        {"Organization": "Humana", "Drop Year": 2025, "Before": 96.9, "After": 40.8, "Drop": -56.1, "Recovered?": "Ongoing", "Years to Recover": null}
      ]
    }
  ],
  "charts": [
    {
      "chart_type": "line",
      "title": "Recovery Trajectories After Major Drops",
      "x_axis": "year",
      "y_axis": "pct_four_star",
      "data": [
        {"year": 2022, "CVS": 89.0, "Humana": 96.9},
        {"year": 2023, "CVS": 57.1, "Humana": 96.5},
        {"year": 2024, "CVS": 92.3, "Humana": 40.8}
      ],
      "series": [
        {"key": "CVS", "label": "CVS Health", "color": "#3B82F6"},
        {"key": "Humana", "label": "Humana", "color": "#EF4444"}
      ]
    }
  ],
  "needs_more_data": true,
  "additional_data_needed": [
    "Measure-level stars from measure_stars_all_years to identify which measures drove Humana's drop",
    "Domain-level breakdown to see if drop was concentrated in Experience, Outcomes, or Process measures"
  ],
  "confidence": 0.85,
  "caveats": ["Recovery analysis limited to overall rating - measure-level recovery patterns not analyzed"]
}
```

=== CHART RULES ===
- LINE charts: For time-series data (year over year trends)
- BAR charts: For comparing entities (organizations, plans)
- AREA charts: For cumulative or stacked time-series
- x_axis and y_axis must EXACTLY match keys in the data array
- data array must contain actual numbers from the query results

=== EXAMPLE: Converting Query Results to Charts ===
If query returns:
| year | parent_organization | pct_four_star |
|------|---------------------|---------------|
| 2020 | Humana              | 95.2          |
| 2021 | Humana              | 92.1          |
| 2022 | Humana              | 40.8          |

Then create chart with:
"data": [
  {"year": 2020, "pct_four_star": 95.2},
  {"year": 2021, "pct_four_star": 92.1},
  {"year": 2022, "pct_four_star": 40.8}
]

For MULTIPLE organizations, add them all:
"data": [
  {"year": 2020, "Humana": 95.2, "CVS": 89.0},
  {"year": 2021, "Humana": 92.1, "CVS": 57.1}
]
"series": [
  {"key": "Humana", "label": "Humana", "color": "#3B82F6"},
  {"key": "CVS", "label": "CVS Health", "color": "#10B981"}
]

YOU MUST INCLUDE CHARTS AND TABLES. This is mandatory for good UX."""

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

=== OUTPUT STRUCTURE ===

Given the analyzed data, create a response that:
1. Leads with the KEY FINDING backed by specific numbers
2. Shows ACTUAL HISTORICAL EXAMPLES (not generalizations)
3. Provides MEASURE-LEVEL DETAIL when relevant
4. References the charts and tables that will appear below

IMPORTANT: Charts and data tables are displayed SEPARATELY below your text response. 
- Don't repeat all the numbers that are in the tables
- DO reference them: "As shown in the table below...", "The chart illustrates..."
- Your text should INTERPRET and ADD CONTEXT to the visuals

TONE: Like a senior consultant briefing a colleague - direct, data-driven, no fluff.

DO NOT:
- Make unsupported predictions or timelines
- Use vague phrases like "typically", "usually", "should" without data
- Guess at causation without evidence
- List all the raw numbers already in the tables

DO:
- Lead with specific, data-backed findings
- Show actual historical examples with real numbers
- Reference specific measures, domains, contracts when available
- Acknowledge what the data does NOT show
- Be specific about uncertainty

=== EXAMPLE: Good vs Bad Responses ===

Question: "How do companies recover from 4+ star drops?"

BAD (unsupported):
"Most companies recover within 2-3 years. Humana has strong resources so they should bounce back. The key is focusing on member experience measures."

GOOD (data-backed):
"Looking at the 8 major drops (>25 points) in our data since 2015:
- 5 recovered to within 10 points of prior level within 3 years (CVS, CIGNA, Healthfirst, Blue Cross MI, Highmark)
- 2 showed partial recovery but not full (Anthem, California Physicians)  
- 1 has never recovered (Centene - they've had 3 separate major drops)

The table below shows the specific trajectory of each. Notable: the fastest recoverers (Healthfirst, Blue Cross MI) were regional plans with concentrated markets. National players like CVS took 2-3 years.

For Humana, I don't have the measure-level breakdown yet to identify which specific measures drove their drop. That analysis would help predict their recovery path."
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
        
        additional_requirements = []
        
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
                    
                    # Parse additional_data_needed into actual requirements
                    additional_data_needed = parsed.get("additional_data_needed", [])
                    for desc in additional_data_needed:
                        if isinstance(desc, str):
                            # Try to determine if this is a SQL or tool request
                            desc_lower = desc.lower()
                            
                            if "measure" in desc_lower and "stars" in desc_lower:
                                # Measure-level query needed
                                additional_requirements.append(DataRequirement(
                                    requirement_id=str(uuid.uuid4()),
                                    description=desc,
                                    data_type="stars",
                                    query_approach="sql",
                                    specific_query=self._generate_measure_query(question),
                                    priority=1,
                                ))
                            elif "domain" in desc_lower:
                                # Domain-level breakdown
                                additional_requirements.append(DataRequirement(
                                    requirement_id=str(uuid.uuid4()),
                                    description=desc,
                                    data_type="stars",
                                    query_approach="sql", 
                                    specific_query=self._generate_domain_query(question),
                                    priority=1,
                                ))
                            else:
                                # Generic additional requirement
                                additional_requirements.append(DataRequirement(
                                    requirement_id=str(uuid.uuid4()),
                                    description=desc,
                                    data_type="unknown",
                                    query_approach="sql",
                                    priority=2,
                                ))
                                
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
            additional_requirements=additional_requirements,
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
            
            # Handle SQL result format: {"rows": [...], "columns": [...], "row_count": N}
            if isinstance(raw_data, dict) and "rows" in raw_data and "columns" in raw_data:
                rows = raw_data.get("rows", [])
                columns = raw_data.get("columns", [])
                
                if rows and columns:
                    # Use the actual columns from the query result
                    display_columns = columns[:10]  # Limit to 10 columns
                    
                    tables.append({
                        "title": desc[:50],
                        "summary": f"{len(rows)} rows returned",
                        "columns": display_columns,
                        "rows": rows[:100],  # Limit to 100 rows for display
                    })
                    
                    # Try to make a chart if there's a year/time column
                    first = rows[0] if rows else {}
                    time_cols = [c for c in display_columns if any(t in c.lower() for t in ["year", "date", "month", "period", "star_year"])]
                    numeric_cols = [c for c in display_columns if isinstance(first.get(c), (int, float))]
                    
                    if time_cols and numeric_cols and len(rows) > 1:
                        # Line chart for time series
                        charts.append({
                            "chart_type": "line",
                            "title": f"{desc[:40]} Over Time",
                            "x_axis": time_cols[0],
                            "y_axis": numeric_cols[0],
                            "data": rows[:50],
                            "series": [{"key": nc, "label": nc, "color": "#3B82F6"} for nc in numeric_cols[:3]]
                        })
                    elif numeric_cols and 2 <= len(rows) <= 20:
                        # Bar chart for categorical comparison
                        name_col = next((c for c in display_columns if any(n in c.lower() for n in ["name", "org", "payer", "plan", "parent"])), display_columns[0])
                        charts.append({
                            "chart_type": "bar",
                            "title": f"{desc[:40]}",
                            "x_axis": name_col,
                            "y_axis": numeric_cols[0],
                            "data": rows[:20],
                            "series": [{"key": numeric_cols[0], "label": numeric_cols[0], "color": "#10B981"}]
                        })
                continue
            
            # Handle direct list of records (e.g., from document search)
            if isinstance(raw_data, list) and len(raw_data) > 0:
                first = raw_data[0]
                if isinstance(first, dict):
                    columns = list(first.keys())[:8]  # Limit columns
                    tables.append({
                        "title": desc[:50],
                        "summary": f"Data for: {desc}",
                        "columns": columns,
                        "rows": raw_data[:50],  # Limit rows
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
                        
            elif isinstance(raw_data, dict) and raw_data:
                # Generic dict - convert to key-value table (but not SQL results)
                columns = ["Metric", "Value"]
                rows = [{"Metric": k, "Value": v} for k, v in list(raw_data.items())[:15] 
                        if not isinstance(v, (list, dict))]  # Skip nested structures
                if rows:
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
