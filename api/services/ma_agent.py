"""
MA Intelligence Agent
=====================

The main AI agent that answers questions about Medicare Advantage.
Orchestrates LLM providers, tools, and learning.

Features:
1. Multi-step reasoning with tool use
2. Self-learning from user feedback
3. Confidence scoring and source citations
4. Streaming responses
5. Full audit trail
"""

import os
import sys
import json
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, AsyncGenerator
from dataclasses import dataclass, asdict

# Add parent for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from api.services.llm_providers import (
    LLMProvider, LLMProviderFactory, Message, ToolDefinition, LLMResponse
)
from api.services.agent_tools import MAAgentTools, get_agent_tools, ToolResult
from api.services.learning_store import LearningStore, get_learning_store


@dataclass
class AgentResponse:
    """Complete response from the agent."""
    query_id: str
    question: str
    answer: str
    confidence: float
    sources: List[str]
    tools_used: List[Dict]
    sql_executed: List[str]
    warnings: List[str]
    metadata: Dict
    created_at: str = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()
    
    def to_dict(self) -> Dict:
        return asdict(self)


class MAIntelligenceAgent:
    """
    Main agent for MA Intelligence Platform.
    
    Handles:
    - Understanding user questions
    - Selecting and executing tools
    - Generating natural language responses
    - Learning from feedback
    """
    
    SYSTEM_PROMPT = """You are the MA Intelligence Assistant, an expert on Medicare Advantage.

You have access to:
- A comprehensive MA database with enrollment, star ratings, and risk scores (2007-2026)
- A knowledge base with MA terminology, star rating measures, and policy timeline
- Tools to query data, look up definitions, and trace data lineage

Your approach:
1. Understand what the user is asking
2. Use tools to get accurate data
3. Explain your findings clearly with citations
4. Note any data limitations or caveats

CRITICAL DATA RULES:
- ENROLLMENT is a point-in-time snapshot, NOT cumulative. When asked "what is enrollment in [year]", ALWAYS filter to the LATEST month available (e.g., month = (SELECT MAX(month) FROM fact_enrollment_unified WHERE year = [year])). NEVER sum across months unless explicitly asked for monthly trend.
- The fact_enrollment_unified table has monthly data. Each row is enrollment for one month.
- For "current enrollment" or "enrollment in 2026", use MAX(month) for that year.

TOOL USAGE STRATEGY:
- For definitions, methodology, HCC models, phase-in schedules, or policy questions: ALWAYS use lookup_knowledge FIRST. The knowledge base has authoritative information about CMS-HCC models (V12, V21, V22, V23, V24, V28), including exact phase-in percentages (e.g., V28: 67% V24/33% V28 in 2024, 33% V24/67% V28 in 2025, 100% V28 in 2026).
- For numeric data queries (enrollment, risk scores, stars): Use query_database.
- For CMS document citations: Use search_documents AFTER checking knowledge base.

RATE NOTICE / POLICY DOCUMENT QUESTIONS:
For rate notice questions, ALWAYS use get_rate_notice_metrics FIRST - it returns structured, verified data.

The structured data includes:
- MA growth rates (ma_growth_rate, effective_growth_rate)
- Risk adjustment (model version, phase-in percentages, normalization factor)
- Star bonuses (star_bonus_5star, star_bonus_4plus) and rebate percentages
- Part D parameters (deductible, OOP threshold, IRA caps)
- Key policy changes list

For HCC model questions, use get_hcc_model_info for structured data on:
- Phase-in schedule (V28 percentages by year)
- Model segments and HCC counts
- Changes from prior model version

Present rate notice information with:
- Lead with headline numbers (e.g., "2027: 4.33% growth rate, 100% V28")
- Specific percentages and dollar amounts
- Comparison to prior year
- Note if advance (proposed) vs final
- IRA impacts where relevant

ANALYTICAL REASONING FRAMEWORK:
For any comparative or trend analysis question, follow this structured approach:

1. IDENTIFY THE SCOPE: What entities (payer, state, plan type)? What metric (stars, enrollment, risk scores)? What time range?

2. GET THE BASELINE: Query the metric over time to establish the trend. Use query_database with:
   - stars_enrollment_unified for overall star ratings + enrollment
   - fact_enrollment_unified for enrollment by any dimension
   - fact_risk_scores_unified for risk score trends
   - measure_stars_all_years + measures_all_years for measure-level analysis

3. COMPARE TO BENCHMARK: Always compare entity vs industry/peers to isolate cause:
   - If entity changed but industry stayed flat = entity-specific issue
   - If both changed similarly = market-wide or policy-driven
   - Use GROUP BY with CASE WHEN to compare in a single query

4. DRILL INTO DRIVERS: For star ratings, check measure_stars_all_years to find which measures drove the change. For enrollment, segment by plan_type, snp_type, state. For risk scores, check model version changes.

5. FILTER FLEXIBILITY: All tables support filtering by parent_org, plan_type, snp_type, group_type, state. Use LIKE '%name%' for parent_org to handle variations.

Example analytical query patterns:
- Trend over time: SELECT year, AVG(metric), SUM(enrollment) FROM table WHERE filters GROUP BY year ORDER BY year
- Payer vs Industry: SELECT year, parent_org LIKE '%Payer%' as is_payer, AVG(metric) FROM table GROUP BY year, is_payer
- Measure decomposition: SELECT measure_key, AVG(star_rating) as payer, (SELECT AVG(star_rating) FROM same WHERE year=X) as industry FROM measure_stars JOIN enrollment

Available tables for star ratings analysis:
- stars_enrollment_unified: Contract-level overall ratings + enrollment (2013-2026)
- measure_stars_all_years: Contract-level measure star ratings (1-5) for each measure
- measures_all_years: Contract-level measure performance values (percentages)
- cutpoints_all_years: Star rating cutpoint thresholds by measure/year

When answering:
- Be precise with numbers (use exact values from queries)
- Cite your sources (table names, knowledge base entries)
- Explain methodology when showing calculations
- Note confidence level if data is incomplete

Domain expertise you should demonstrate:
- MA enrollment trends and market share
- Star ratings methodology and impact on bonus payments
- Risk adjustment and RAF scores
- Policy changes (IRA, V28 risk model, etc.)
- Payer strategies and M&A activity

Always be helpful, accurate, and professional. If unsure, say so and explain what additional information would help."""

    def __init__(
        self,
        provider: str = "anthropic",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self.provider = LLMProviderFactory.create(provider, api_key, model)
        self.tools = get_agent_tools()
        self.learning_store = get_learning_store()
        
        # Convert tools to LLM format
        self.tool_definitions = [
            ToolDefinition(
                name=t["name"],
                description=t["description"],
                parameters=t["parameters"],
            )
            for t in MAAgentTools.get_tool_definitions()
        ]
    
    def _build_messages(
        self,
        question: str,
        history: List[Dict] = None,
        learning_context: Dict = None,
    ) -> List[Message]:
        """Build message list for LLM."""
        messages = [Message(role="system", content=self.SYSTEM_PROMPT)]
        
        # Add learning context (few-shot examples)
        if learning_context:
            examples = learning_context.get("similar_examples", [])
            rules = learning_context.get("applicable_rules", [])
            
            if examples or rules:
                context_text = "\n\n--- RELEVANT CONTEXT ---\n"
                
                if rules:
                    context_text += "\nCorrections to note:\n"
                    for rule in rules[:2]:
                        context_text += f"- When asked about '{rule.trigger_pattern}': {rule.correct_behavior}\n"
                
                if examples:
                    context_text += "\nSimilar questions I've answered well:\n"
                    for ex in examples[:2]:
                        context_text += f"Q: {ex.question}\nA: {ex.answer[:300]}...\n\n"
                
                messages.append(Message(role="system", content=context_text))
        
        # Add conversation history
        if history:
            for msg in history[-10:]:  # Last 10 messages
                messages.append(Message(
                    role=msg.get("role", "user"),
                    content=msg.get("content", ""),
                ))
        
        # Add current question
        messages.append(Message(role="user", content=question))
        
        return messages
    
    async def _execute_tool(self, tool_name: str, arguments: Dict) -> ToolResult:
        """Execute a tool and return result."""
        tool_method = getattr(self.tools, tool_name, None)
        
        if tool_method is None:
            return ToolResult(
                success=False,
                data=None,
                error=f"Unknown tool: {tool_name}",
            )
        
        try:
            return tool_method(**arguments)
        except Exception as e:
            return ToolResult(
                success=False,
                data=None,
                error=str(e),
            )
    
    async def answer(
        self,
        question: str,
        user_id: str = "anonymous",
        history: List[Dict] = None,
        stream: bool = False,
    ) -> AgentResponse:
        """
        Answer a question about Medicare Advantage.
        
        Args:
            question: User's question
            user_id: User identifier for audit/learning
            history: Conversation history
            stream: Whether to stream the response
            
        Returns:
            AgentResponse with answer and metadata
        """
        import hashlib
        query_id = hashlib.md5(
            f"{question}:{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]
        
        # Get learning context
        learning_context = self.learning_store.get_learning_context(question)
        
        # Build messages
        messages = self._build_messages(question, history, learning_context)
        
        # Track tool usage
        tools_used = []
        sql_executed = []
        all_sources = []
        warnings = []
        
        # Agent loop - execute tools until LLM stops calling them
        max_iterations = 10
        for iteration in range(max_iterations):
            response = await self.provider.generate(
                messages=messages,
                tools=self.tool_definitions,
                temperature=0.3,  # Lower temperature for factual queries
            )
            
            # If no tool calls, we're done
            if not response.tool_calls:
                break
            
            # Execute each tool call
            for tool_call in response.tool_calls:
                tool_name = tool_call["name"]
                tool_args = tool_call["arguments"]
                
                # Execute tool
                result = await self._execute_tool(tool_name, tool_args)
                
                # Track usage
                tools_used.append({
                    "tool": tool_name,
                    "arguments": tool_args,
                    "success": result.success,
                    "error": result.error,
                })
                
                if result.sources:
                    all_sources.extend(result.sources)
                
                if result.metadata and result.metadata.get("sql"):
                    sql_executed.append(result.metadata["sql"])
                
                # Add assistant message with tool call
                messages.append(Message(
                    role="assistant",
                    content="",
                    tool_calls=[tool_call],
                ))
                
                # Add tool result message
                messages.append(Message(
                    role="tool",
                    content=json.dumps(result.data if result.success else {"error": result.error}),
                    tool_call_id=tool_call["id"],
                    name=tool_name,
                ))
        
        # Calculate confidence based on tool success rate and source count
        successful_tools = sum(1 for t in tools_used if t["success"])
        total_tools = len(tools_used) or 1
        confidence = 0.5 + (0.3 * successful_tools / total_tools) + (0.2 * min(len(all_sources), 5) / 5)
        
        # Apply rules (reduce confidence if corrections apply)
        if learning_context.get("applicable_rules"):
            confidence *= 0.9  # Slightly reduce confidence when corrections apply
            for rule in learning_context["applicable_rules"]:
                warnings.append(f"Note: Previous corrections apply to this type of question.")
        
        # Deduplicate sources
        all_sources = list(set(all_sources))
        
        agent_response = AgentResponse(
            query_id=query_id,
            question=question,
            answer=response.content,
            confidence=min(confidence, 1.0),
            sources=all_sources,
            tools_used=tools_used,
            sql_executed=sql_executed,
            warnings=warnings,
            metadata={
                "provider": self.provider.provider_name,
                "model": response.model,
                "latency_ms": response.latency_ms,
                "usage": response.usage,
                "learning_context_used": bool(learning_context.get("similar_examples") or learning_context.get("applicable_rules")),
            }
        )
        
        # Store as example if successful and high quality
        if confidence > 0.8 and len(tools_used) > 0:
            domain = self._detect_domain(question)
            self.learning_store.add_example(
                question=question,
                answer=response.content[:500],
                sql_used=sql_executed[0] if sql_executed else None,
                tools_used=[t["tool"] for t in tools_used],
                domain=domain,
            )
        
        return agent_response
    
    async def answer_streaming(
        self,
        question: str,
        user_id: str = "anonymous",
        history: List[Dict] = None,
    ) -> AsyncGenerator[str, None]:
        """
        Stream an answer to a question.
        
        Yields text chunks as they're generated.
        """
        # For streaming, we first execute all tools, then stream the final response
        import hashlib
        query_id = hashlib.md5(
            f"{question}:{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]
        
        learning_context = self.learning_store.get_learning_context(question)
        messages = self._build_messages(question, history, learning_context)
        
        tools_used = []
        
        # Execute tools first (non-streaming)
        for iteration in range(5):
            response = await self.provider.generate(
                messages=messages,
                tools=self.tool_definitions,
                temperature=0.3,
                stream=False,
            )
            
            if not response.tool_calls:
                break
            
            for tool_call in response.tool_calls:
                result = await self._execute_tool(tool_call["name"], tool_call["arguments"])
                tools_used.append(tool_call["name"])
                
                messages.append(Message(role="assistant", content="", tool_calls=[tool_call]))
                messages.append(Message(
                    role="tool",
                    content=json.dumps(result.data if result.success else {"error": result.error}),
                    tool_call_id=tool_call["id"],
                    name=tool_call["name"],
                ))
        
        # Now stream the final response
        stream_gen = await self.provider.generate(
            messages=messages,
            tools=None,  # No tools for final response
            temperature=0.3,
            stream=True,
        )
        
        async for chunk in stream_gen:
            yield chunk
    
    def _detect_domain(self, question: str) -> str:
        """Detect the domain of a question."""
        question_lower = question.lower()
        
        if any(kw in question_lower for kw in ['star', 'rating', 'quality', '4+', 'five star']):
            return "stars"
        if any(kw in question_lower for kw in ['risk', 'raf', 'hcc', 'acuity']):
            return "risk_scores"
        if any(kw in question_lower for kw in ['policy', 'cms', 'regulation', 'rule', 'ira']):
            return "policy"
        if any(kw in question_lower for kw in ['enroll', 'member', 'market share', 'growth']):
            return "enrollment"
        
        return "general"
    
    async def submit_feedback(
        self,
        query_id: str,
        user_id: str,
        rating: str,
        original_question: str,
        original_response: str,
        correction: Optional[str] = None,
        correct_answer: Optional[str] = None,
    ):
        """
        Submit feedback on a response.
        
        Args:
            query_id: ID of the query
            user_id: User providing feedback
            rating: 'positive', 'negative', or 'correction'
            original_question: Original question
            original_response: Original response
            correction: What was wrong (if correction)
            correct_answer: What should be said (if correction)
        """
        self.learning_store.add_feedback(
            query_id=query_id,
            user_id=user_id,
            rating=rating,
            original_question=original_question,
            original_response=original_response,
            correction=correction,
            correct_answer=correct_answer,
        )


# Singleton instance
_agent_instance: Optional[MAIntelligenceAgent] = None

def get_ma_agent(
    provider: str = "anthropic",
    model: Optional[str] = None,
) -> MAIntelligenceAgent:
    """Get or create singleton agent instance."""
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = MAIntelligenceAgent(provider=provider, model=model)
    return _agent_instance


# Simple synchronous wrapper for non-async contexts
def answer_question(
    question: str,
    user_id: str = "anonymous",
    history: List[Dict] = None,
) -> AgentResponse:
    """Synchronous wrapper for answering questions."""
    agent = get_ma_agent()
    return asyncio.run(agent.answer(question, user_id, history))
