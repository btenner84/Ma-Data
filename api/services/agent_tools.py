"""
MA Intelligence Agent Tools
============================

Tools available to the AI agent for answering questions.
Each tool is a capability the agent can use to gather information.

Tools:
1. query_database - Execute SQL against MA data
2. lookup_knowledge - Query the knowledge base (glossary, measures, policy)
3. get_payer_info - Look up payer details
4. calculate_metric - Perform calculations with explanations
5. search_similar_queries - Find similar past queries
6. get_data_lineage - Trace data back to source files
"""

import os
import sys
import json
import yaml
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db import get_engine
from api.services.document_search import get_document_search_service
from api.services.knowledge_extraction import get_knowledge_store

# Load knowledge base
KNOWLEDGE_BASE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'config',
    'ma_knowledge_base.yaml'
)

SEMANTIC_MODEL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'config',
    'semantic_model.yaml'
)


@dataclass
class ToolResult:
    """Result from a tool execution."""
    success: bool
    data: Any
    error: Optional[str] = None
    metadata: Optional[Dict] = None
    confidence: float = 1.0
    sources: List[str] = None
    
    def __post_init__(self):
        if self.sources is None:
            self.sources = []


class MAAgentTools:
    """Collection of tools available to the MA Intelligence Agent."""
    
    def __init__(self):
        self.engine = None
        self.knowledge_base = self._load_knowledge_base()
        self.semantic_model = self._load_semantic_model()
        
    def _get_engine(self):
        """Lazy load the database engine."""
        if self.engine is None:
            self.engine = get_engine()
        return self.engine
    
    def _load_knowledge_base(self) -> Dict:
        """Load the MA knowledge base."""
        try:
            with open(KNOWLEDGE_BASE_PATH, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load knowledge base: {e}")
            return {}
    
    def _load_semantic_model(self) -> Dict:
        """Load the semantic model."""
        try:
            with open(SEMANTIC_MODEL_PATH, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load semantic model: {e}")
            return {}
    
    # =========================================================================
    # TOOL: query_database
    # =========================================================================
    def query_database(
        self,
        sql: str,
        user_id: str = "ai_agent",
        context: str = "",
        limit: int = 1000,
    ) -> ToolResult:
        """
        Execute a SQL query against the MA database.
        
        Args:
            sql: SQL query to execute (read-only)
            user_id: User identifier for audit
            context: Context about why this query is being run
            limit: Max rows to return
            
        Returns:
            ToolResult with query results
        """
        # Security: only allow SELECT queries (including CTEs starting with WITH)
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
            return ToolResult(
                success=False,
                data=None,
                error="Only SELECT queries are allowed",
            )
        
        # Add LIMIT if not present
        if "LIMIT" not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {limit}"
        
        try:
            engine = self._get_engine()
            df, audit_id = engine.query_with_audit(
                sql,
                user_id=user_id,
                context=f"Agent query: {context[:200]}" if context else "Agent query"
            )
            
            # Convert to records
            records = df.to_dict(orient='records')
            
            # Get table info for sources
            tables = self._extract_tables_from_sql(sql)
            sources = [f"Table: {t}" for t in tables]
            
            return ToolResult(
                success=True,
                data={
                    "rows": records,
                    "row_count": len(records),
                    "columns": list(df.columns),
                },
                metadata={
                    "audit_id": audit_id,
                    "sql": sql,
                    "tables": tables,
                },
                sources=sources,
                confidence=1.0,  # Direct query = high confidence
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                data=None,
                error=str(e),
            )
    
    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """Extract table names from SQL query."""
        import re
        # Simple extraction - matches FROM/JOIN table names
        pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        return list(set(matches))
    
    # =========================================================================
    # TOOL: lookup_knowledge
    # =========================================================================
    def lookup_knowledge(
        self,
        query: str,
        category: Optional[str] = None,
    ) -> ToolResult:
        """
        Look up information in the MA knowledge base.
        
        Args:
            query: Term or concept to look up
            category: Optional category (glossary, star_measures, policy_timeline, calculations)
            
        Returns:
            ToolResult with knowledge base information
        """
        query_lower = query.lower().strip()
        results = []
        
        # Search glossary
        if category is None or category == "glossary":
            glossary = self.knowledge_base.get("glossary", {})
            for term, info in glossary.items():
                term_matches = query_lower in term.lower() or query_lower in info.get("term", "").lower()
                # Also check nested content for complex entries
                definition_matches = query_lower in str(info.get("definition", "")).lower()
                versions_match = any(query_lower in str(v).lower() for v in info.get("versions", {}).values()) if info.get("versions") else False
                
                if term_matches or definition_matches or versions_match:
                    # Return full entry for complex nested structures
                    entry = {
                        "type": "glossary",
                        "term": term,
                        "full_name": info.get("term"),
                        "definition": info.get("definition"),
                        "context": info.get("context"),
                        "related": info.get("related", []),
                    }
                    # Include additional nested data if present
                    if info.get("versions"):
                        entry["versions"] = info["versions"]
                    if info.get("model_components"):
                        entry["model_components"] = info["model_components"]
                    if info.get("segments"):
                        entry["segments"] = info["segments"]
                    if info.get("phase_in"):
                        entry["phase_in"] = info["phase_in"]
                    results.append(entry)
        
        # Search star measures
        if category is None or category == "star_measures":
            key_measures = self.knowledge_base.get("star_measures", {}).get("key_measures", {})
            for measure_id, measure in key_measures.items():
                if query_lower in measure_id.lower() or query_lower in measure.get("name", "").lower():
                    results.append({
                        "type": "star_measure",
                        "measure_id": measure_id,
                        "name": measure.get("name"),
                        "domain": measure.get("domain"),
                        "description": measure.get("description"),
                    })
        
        # Search policy timeline
        if category is None or category == "policy_timeline":
            timeline = self.knowledge_base.get("policy_timeline", {})
            for year, events in timeline.items():
                if isinstance(events, list):
                    for event in events:
                        if query_lower in str(event).lower():
                            results.append({
                                "type": "policy",
                                "year": year,
                                "event": event,
                            })
        
        # Search calculations
        if category is None or category == "calculations":
            calculations = self.knowledge_base.get("calculations", {})
            for domain, calcs in calculations.items():
                for calc_name, calc_info in calcs.items():
                    if query_lower in calc_name.lower():
                        results.append({
                            "type": "calculation",
                            "domain": domain,
                            "name": calc_name,
                            "formula": calc_info.get("formula"),
                            "note": calc_info.get("note"),
                        })
        
        # Search top payers
        if category is None or category == "payers":
            payers = self.knowledge_base.get("top_payers", [])
            for payer in payers:
                if query_lower in payer.get("canonical", "").lower():
                    results.append({
                        "type": "payer",
                        **payer
                    })
                elif any(query_lower in alias.lower() for alias in payer.get("aliases", [])):
                    results.append({
                        "type": "payer",
                        **payer
                    })
        
        if results:
            return ToolResult(
                success=True,
                data=results,
                sources=["MA Knowledge Base"],
                confidence=0.95,
            )
        else:
            return ToolResult(
                success=True,
                data=[],
                metadata={"note": f"No results found for '{query}'"},
                sources=["MA Knowledge Base"],
                confidence=0.5,
            )
    
    # =========================================================================
    # TOOL: get_payer_info
    # =========================================================================
    def get_payer_info(
        self,
        payer_name: str,
        year: int = 2026,
    ) -> ToolResult:
        """
        Get comprehensive information about a payer.
        
        Args:
            payer_name: Payer name or alias
            year: Year for data (default: current)
            
        Returns:
            ToolResult with payer details and metrics
        """
        # Find canonical name
        payer_name_lower = payer_name.lower()
        canonical_name = None
        payer_meta = None
        
        payers = self.knowledge_base.get("top_payers", [])
        for payer in payers:
            if payer_name_lower in payer.get("canonical", "").lower():
                canonical_name = payer["canonical"]
                payer_meta = payer
                break
            if any(payer_name_lower in alias.lower() for alias in payer.get("aliases", [])):
                canonical_name = payer["canonical"]
                payer_meta = payer
                break
        
        if not canonical_name:
            return ToolResult(
                success=False,
                data=None,
                error=f"Payer '{payer_name}' not found. Try: UnitedHealth, Humana, CVS, Elevance, Kaiser, Centene, Cigna, Molina",
            )
        
        # Query enrollment
        enrollment_result = self.query_database(
            f"""
            SELECT 
                year,
                SUM(enrollment) as total_enrollment,
                SUM(CASE WHEN snp_type = 'D-SNP' THEN enrollment ELSE 0 END) as dsnp_enrollment,
                SUM(CASE WHEN plan_type_simplified = 'HMO' THEN enrollment ELSE 0 END) as hmo_enrollment,
                SUM(CASE WHEN plan_type_simplified = 'PPO' THEN enrollment ELSE 0 END) as ppo_enrollment,
                COUNT(DISTINCT contract_id) as contract_count
            FROM fact_enrollment_unified
            WHERE parent_org = '{canonical_name}'
            AND year = {year}
            AND month = (SELECT MAX(month) FROM fact_enrollment_unified WHERE year = {year})
            GROUP BY year
            """,
            context=f"Payer info lookup for {payer_name}"
        )
        
        # Query star ratings
        stars_result = self.query_database(
            f"""
            SELECT 
                COUNT(*) as contract_count,
                AVG(overall_rating) as avg_stars,
                SUM(CASE WHEN overall_rating >= 4 THEN 1 ELSE 0 END) as four_plus_contracts
            FROM stars_enrollment_unified
            WHERE parent_org = '{canonical_name}'
            AND star_year = {year}
            """,
            context=f"Stars info for {payer_name}"
        )
        
        result = {
            "canonical_name": canonical_name,
            "market_position": payer_meta.get("market_position"),
            "specialty": payer_meta.get("specialty"),
            "aliases": payer_meta.get("aliases", []),
        }
        
        if enrollment_result.success and enrollment_result.data.get("rows"):
            result["enrollment"] = enrollment_result.data["rows"][0]
        
        if stars_result.success and stars_result.data.get("rows"):
            result["stars"] = stars_result.data["rows"][0]
        
        return ToolResult(
            success=True,
            data=result,
            sources=["MA Knowledge Base", "fact_enrollment_unified", "stars_enrollment_unified"],
            confidence=0.95,
        )
    
    # =========================================================================
    # TOOL: calculate_metric
    # =========================================================================
    def calculate_metric(
        self,
        metric: str,
        filters: Optional[Dict] = None,
        year: int = 2026,
        show_calculation: bool = True,
    ) -> ToolResult:
        """
        Calculate a specific metric with full explanation.
        
        Args:
            metric: Metric name (enrollment, market_share, yoy_growth, four_plus_pct, etc.)
            filters: Optional filters (parent_org, plan_type, snp_type, state)
            year: Year for calculation
            show_calculation: Include step-by-step calculation
            
        Returns:
            ToolResult with metric value and calculation details
        """
        filters = filters or {}
        metric_lower = metric.lower()
        
        calculations = self.knowledge_base.get("calculations", {})
        
        # ===========================================================================
        # UNIFIED FILTER BUILDER
        # Handles all filter combinations consistently across tables
        # ===========================================================================
        def build_enrollment_filter(filters: Dict, year: int, month: int = 1) -> str:
            """Build WHERE clause for fact_enrollment_unified."""
            clauses = [f"year = {year}", f"month = {month}"]
            for key, value in filters.items():
                if key == 'parent_org':
                    clauses.append(f"parent_org LIKE '%{value}%'")
                elif key in ['plan_type', 'product_type', 'snp_type', 'group_type', 'state']:
                    clauses.append(f"{key} = '{value}'")
            return " AND ".join(clauses)
        
        def build_stars_filter(filters: Dict, year: int) -> str:
            """Build WHERE clause for stars_enrollment_unified."""
            clauses = [f"star_year = {year}"]
            for key, value in filters.items():
                if key == 'parent_org':
                    clauses.append(f"parent_org LIKE '%{value}%'")
                elif key in ['plan_type', 'plan_type_normalized', 'snp_type', 'group_type']:
                    clauses.append(f"{key} = '{value}'")
            return " AND ".join(clauses)
        
        # Build filter clauses using unified builders
        enrollment_filter = build_enrollment_filter(filters, year)
        stars_filter = build_stars_filter(filters, year)
        
        # Calculate based on metric type
        if "enrollment" in metric_lower or "members" in metric_lower:
            sql = f"SELECT SUM(enrollment) as value FROM fact_enrollment_unified WHERE {enrollment_filter}"
            formula = calculations.get("enrollment", {}).get("total_ma", {}).get("formula", "SUM(enrollment)")
            
        elif "market_share" in metric_lower or "share" in metric_lower:
            # For market share, industry total should NOT include payer filters
            industry_filter = build_enrollment_filter({}, year)
            sql = f"""
            WITH total AS (
                SELECT SUM(enrollment) as industry_total
                FROM fact_enrollment_unified
                WHERE {industry_filter}
            ),
            payer AS (
                SELECT SUM(enrollment) as payer_total
                FROM fact_enrollment_unified
                WHERE {enrollment_filter}
            )
            SELECT 
                payer.payer_total as enrollment,
                total.industry_total as industry_total,
                ROUND(payer.payer_total * 100.0 / total.industry_total, 2) as value
            FROM payer, total
            """
            formula = calculations.get("enrollment", {}).get("market_share", {}).get("formula", "(payer / total) * 100")
            
        elif "yoy" in metric_lower or "growth" in metric_lower:
            enrollment_filter_prev = build_enrollment_filter(filters, year - 1)
            sql = f"""
            WITH current AS (
                SELECT SUM(enrollment) as current_value FROM fact_enrollment_unified WHERE {enrollment_filter}
            ),
            prior AS (
                SELECT SUM(enrollment) as prior_value FROM fact_enrollment_unified WHERE {enrollment_filter_prev}
            )
            SELECT 
                current.current_value,
                prior.prior_value,
                ROUND((current.current_value - prior.prior_value) * 100.0 / prior.prior_value, 2) as value
            FROM current, prior
            """
            formula = calculations.get("enrollment", {}).get("yoy_growth", {}).get("formula", "((current - prior) / prior) * 100")
            
        elif "four_plus" in metric_lower or "4+" in metric_lower or "4 star" in metric_lower:
            sql = f"""
            SELECT 
                SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) as four_plus_enrollment,
                SUM(enrollment) as total_enrollment,
                ROUND(SUM(CASE WHEN overall_rating >= 4 THEN enrollment ELSE 0 END) * 100.0 / SUM(enrollment), 2) as value
            FROM stars_enrollment_unified
            WHERE {stars_filter}
            """
            formula = calculations.get("stars", {}).get("four_plus_percent", {}).get("formula", "(4+ enrollment / total) * 100")
            
        else:
            return ToolResult(
                success=False,
                data=None,
                error=f"Unknown metric '{metric}'. Available: enrollment, market_share, yoy_growth, four_plus_pct",
            )
        
        # Execute query
        result = self.query_database(sql, context=f"Calculate {metric}")
        
        if not result.success:
            return result
        
        if not result.data.get("rows"):
            return ToolResult(
                success=False,
                data=None,
                error="No data found for the specified filters",
            )
        
        row = result.data["rows"][0]
        
        calculation_details = {
            "metric": metric,
            "value": row.get("value"),
            "year": year,
            "filters": filters,
            "formula": formula,
            "sql": sql if show_calculation else None,
            "raw_values": {k: v for k, v in row.items() if k != "value"},
        }
        
        return ToolResult(
            success=True,
            data=calculation_details,
            sources=result.sources,
            confidence=1.0,
            metadata=result.metadata,
        )
    
    # =========================================================================
    # TOOL: get_data_lineage
    # =========================================================================
    def get_data_lineage(
        self,
        table_name: str,
    ) -> ToolResult:
        """
        Get data lineage for a table (source files, transformations).
        
        Args:
            table_name: Table to trace
            
        Returns:
            ToolResult with lineage information
        """
        lineage_map = {
            "fact_enrollment_unified": {
                "source_files": [
                    "CMS Monthly Enrollment by Plan",
                    "CMS SNP Comprehensive Report",
                    "CMS Contract Info (CPSC)",
                ],
                "transformations": [
                    "Parse raw CSV/Excel files",
                    "Standardize column names",
                    "Join with SNP report for D-SNP/C-SNP/I-SNP classification",
                    "Join with CPSC for parent organization",
                    "Apply group type classification (EGHP + plan ID heuristic)",
                    "Load to silver layer as Parquet",
                    "Aggregate to gold layer star schema",
                ],
                "update_frequency": "Monthly",
                "grain": "Contract-Plan-Year-Month",
            },
            "fact_enrollment_geographic": {
                "source_files": ["CMS CPSC (Contract-Plan-State-County)"],
                "transformations": [
                    "Parse raw CSV files",
                    "Handle suppressed values (<11 enrollees = NULL)",
                    "Standardize state/county codes",
                    "Load to silver layer as Parquet",
                ],
                "update_frequency": "Monthly",
                "grain": "Contract-Plan-State-County-Year-Month",
                "notes": ["~1-3% of enrollment suppressed for HIPAA compliance"],
            },
            "stars_enrollment_unified": {
                "source_files": [
                    "CMS Medicare Star Ratings",
                    "CMS Part C/D Display Data",
                ],
                "transformations": [
                    "Parse star ratings CSV",
                    "Join with enrollment for enrollment-weighted analysis",
                    "Calculate domain-level scores",
                ],
                "update_frequency": "Annual (October release)",
                "grain": "Contract-Star Year",
            },
            "fact_risk_scores_unified": {
                "source_files": ["CMS Plan Payment Data (Risk Scores)"],
                "transformations": [
                    "Parse Excel files",
                    "Extract risk score components",
                    "Join with enrollment for weighting",
                ],
                "update_frequency": "Annual",
                "grain": "Contract-Plan-Year",
            },
            "dim_entity": {
                "source_files": ["CMS Contract Crosswalk Files"],
                "transformations": [
                    "Build entity chains from crosswalk files (2006-2026)",
                    "Track ID changes, mergers, consolidations",
                    "Assign stable entity_id for time-series tracking",
                ],
                "update_frequency": "Annual",
                "grain": "Contract (or Contract-Plan)",
            },
            "measure_stars_all_years": {
                "source_files": ["CMS Star Ratings - Measure Stars Files"],
                "transformations": [
                    "Parse Measure Stars CSV from CMS star ratings ZIPs",
                    "Extract 1-5 star ratings for each contract/measure",
                    "Normalize measure_id mapping across years",
                ],
                "update_frequency": "Annual (October release)",
                "grain": "Contract-Measure-Year",
                "columns": ["year", "contract_id", "measure_id", "star_rating"],
                "notes": ["Use with measures_all_years for measure metadata", "Join with stars_enrollment_unified for weighted analysis"],
            },
            "measures_all_years": {
                "source_files": ["CMS Star Ratings - Measure Data Files"],
                "transformations": [
                    "Parse measure performance CSVs",
                    "Extract numeric performance values (percentages)",
                    "Generate stable measure_key from measure_name",
                ],
                "update_frequency": "Annual",
                "grain": "Contract-Measure-Year",
                "columns": ["year", "contract_id", "measure_id", "measure_key", "measure_name", "numeric_value"],
                "notes": ["measure_id changes year-to-year, use measure_key for time-series"],
            },
        }
        
        if table_name in lineage_map:
            return ToolResult(
                success=True,
                data={
                    "table": table_name,
                    **lineage_map[table_name]
                },
                sources=lineage_map[table_name]["source_files"],
                confidence=1.0,
            )
        else:
            available = list(lineage_map.keys())
            return ToolResult(
                success=False,
                data=None,
                error=f"Unknown table '{table_name}'. Available: {available}",
            )
    
    # =========================================================================
    # TOOL: search_documents
    # =========================================================================
    def search_documents(
        self,
        query: str,
        doc_types: List[str] = None,
        years: List[int] = None,
        limit: int = 5,
    ) -> ToolResult:
        """
        Search CMS documents (rate notices, technical notes, call letters).
        
        Args:
            query: Search query
            doc_types: Filter by types (rate_notice_advance, rate_notice_final, tech_notes, call_letter)
            years: Filter by years
            limit: Max results
            
        Returns:
            ToolResult with matching documents and snippets
        """
        try:
            doc_service = get_document_search_service()
            results = doc_service.search(query, doc_types, years, limit)
            
            if not results:
                return ToolResult(
                    success=True,
                    data=[],
                    metadata={"note": f"No documents found for '{query}'"},
                    sources=["CMS Document Archive"],
                    confidence=0.5,
                )
            
            return ToolResult(
                success=True,
                data=[
                    {
                        "doc_type": r.doc_type,
                        "year": r.year,
                        "title": r.title,
                        "url": r.url,
                        "snippet": r.snippet,
                        "key_changes": r.key_changes[:3],  # Top 3 changes
                        "score": r.score,
                    }
                    for r in results
                ],
                sources=[f"{r.title}" for r in results],
                confidence=0.9,
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                data=None,
                error=str(e),
            )
    
    # =========================================================================
    # TOOL: get_document_content
    # =========================================================================
    def get_document_content(
        self,
        doc_type: str,
        year: int,
        section: str = None,
    ) -> ToolResult:
        """
        Get full or section-specific content from a CMS document.
        Use this when you need detailed information from a rate notice or technical note.
        
        Args:
            doc_type: Document type (rate_notice_advance, rate_notice_final, tech_notes, call_letter)
            year: Document year (e.g., 2027)
            section: Optional section to extract (e.g., "risk adjustment", "benchmark", "star ratings", "part d")
            
        Returns:
            ToolResult with document text content
        """
        try:
            doc_service = get_document_search_service()
            doc = doc_service.get_document(doc_type, year)
            
            if not doc:
                return ToolResult(
                    success=True,
                    data=None,
                    metadata={"note": f"Document not found: {doc_type} {year}"},
                    sources=[],
                    confidence=0.5,
                )
            
            full_text = doc.get('full_text', '')
            
            if not full_text:
                return ToolResult(
                    success=True,
                    data={"title": doc.get('title'), "url": doc.get('url'), "content": "Document text not available"},
                    metadata={"note": "PDF text extraction may have failed"},
                    sources=[doc.get('title', f'{doc_type} {year}')],
                    confidence=0.5,
                )
            
            # If section specified, try to extract that section
            content = full_text
            if section:
                section_lower = section.lower()
                content_lower = full_text.lower()
                
                # Define section markers for rate notices
                section_markers = {
                    "risk adjustment": ["risk adjustment", "cms-hcc", "hcc model", "normalization factor", "coding intensity"],
                    "benchmark": ["benchmark", "capitation rate", "county rate", "growth percentage", "per capita"],
                    "star ratings": ["star rating", "quality bonus", "rebate percentage", "qbp", "overall rating"],
                    "part d": ["part d", "prescription drug", "low-income", "lis", "coverage gap", "donut hole", "reinsurance"],
                    "esrd": ["esrd", "end-stage renal", "dialysis"],
                    "growth": ["growth", "increase", "change from", "compared to"],
                }
                
                # Find relevant sections
                markers = section_markers.get(section_lower, [section_lower])
                relevant_chunks = []
                
                for marker in markers:
                    pos = 0
                    while True:
                        pos = content_lower.find(marker, pos)
                        if pos == -1:
                            break
                        # Extract ~2000 chars around the match
                        start = max(0, pos - 500)
                        end = min(len(full_text), pos + 1500)
                        chunk = full_text[start:end]
                        relevant_chunks.append(chunk)
                        pos += len(marker)
                
                if relevant_chunks:
                    # Dedupe and combine chunks
                    content = "\n\n---\n\n".join(relevant_chunks[:5])  # Top 5 relevant sections
                else:
                    # Return first 5000 chars if no section match
                    content = full_text[:5000] + "..." if len(full_text) > 5000 else full_text
            else:
                # Return first 8000 chars for full doc request
                content = full_text[:8000] + "..." if len(full_text) > 8000 else full_text
            
            return ToolResult(
                success=True,
                data={
                    "title": doc.get('title'),
                    "year": year,
                    "doc_type": doc_type,
                    "url": doc.get('url'),
                    "section_requested": section,
                    "content": content,
                    "key_changes": doc.get('key_changes', []),
                },
                sources=[doc.get('title', f'{doc_type} {year}')],
                confidence=0.95,
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                data=None,
                error=str(e),
            )
    
    # =========================================================================
    # TOOL: get_policy_changes
    # =========================================================================
    def get_policy_changes(
        self,
        topic: str = None,
        year: int = None,
    ) -> ToolResult:
        """
        Get policy changes from CMS documents.
        
        Args:
            topic: Filter by topic (e.g., "risk adjustment", "star ratings")
            year: Filter by year
            
        Returns:
            ToolResult with policy changes
        """
        try:
            doc_service = get_document_search_service()
            
            if topic:
                changes = doc_service.get_policy_timeline(topic)
            else:
                changes = doc_service.get_key_changes(year=year)
            
            if not changes:
                return ToolResult(
                    success=True,
                    data=[],
                    metadata={"note": "No policy changes found"},
                    sources=["CMS Document Archive"],
                    confidence=0.7,
                )
            
            return ToolResult(
                success=True,
                data=changes[:20],  # Limit to 20
                sources=list(set(c['title'] for c in changes[:20])),
                confidence=0.9,
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                data=None,
                error=str(e),
            )
    
    # =========================================================================
    # TOOL: get_rate_notice_metrics
    # =========================================================================
    def get_rate_notice_metrics(
        self,
        year: int,
        notice_type: str = "advance",
    ) -> ToolResult:
        """
        Get STRUCTURED metrics from a rate notice (advance or final).
        This returns pre-extracted, verified data - much better than text search.
        
        Args:
            year: The payment year (e.g., 2027)
            notice_type: "advance" or "final"
            
        Returns:
            ToolResult with structured rate notice metrics including:
            - MA growth rate and effective growth rate
            - Risk adjustment model version and phase-in percentages
            - Star bonus and rebate percentages
            - Part D parameters (deductibles, thresholds, caps)
            - Key policy changes
        """
        try:
            knowledge_store = get_knowledge_store()
            metrics = knowledge_store.get_rate_notice(year, notice_type)
            
            if not metrics:
                # Try the other type
                other_type = "final" if notice_type == "advance" else "advance"
                metrics = knowledge_store.get_rate_notice(year, other_type)
                if metrics:
                    notice_type = other_type
            
            if not metrics:
                return ToolResult(
                    success=True,
                    data=None,
                    metadata={"note": f"No rate notice metrics found for {year} ({notice_type})"},
                    sources=[],
                    confidence=0.5,
                )
            
            return ToolResult(
                success=True,
                data=metrics.to_dict(),
                sources=[f"{year} {'Advance' if notice_type == 'advance' else 'Final'} Rate Notice"],
                confidence=0.98,
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                data=None,
                error=str(e),
            )
    
    # =========================================================================
    # TOOL: get_hcc_model_info
    # =========================================================================
    def get_hcc_model_info(
        self,
        model_version: str = "V28",
        year: int = None,
    ) -> ToolResult:
        """
        Get structured information about a CMS-HCC risk adjustment model.
        
        Args:
            model_version: Model version (e.g., "V28", "V24")
            year: Payment year (defaults to most recent)
            
        Returns:
            ToolResult with model parameters including:
            - Phase-in percentages
            - Normalization factors
            - Number of HCCs
            - Segments (Community Aged, Disabled, etc.)
            - Key changes from prior version
        """
        try:
            knowledge_store = get_knowledge_store()
            
            # If no year specified, try recent years
            if year is None:
                for y in [2027, 2026, 2025, 2024]:
                    model = knowledge_store.get_hcc_model(model_version, y)
                    if model:
                        break
            else:
                model = knowledge_store.get_hcc_model(model_version, year)
            
            if not model:
                return ToolResult(
                    success=True,
                    data=None,
                    metadata={"note": f"No model info found for {model_version} ({year})"},
                    sources=[],
                    confidence=0.5,
                )
            
            return ToolResult(
                success=True,
                data=model.to_dict(),
                sources=[f"CMS-HCC {model_version} Model Documentation"],
                confidence=0.98,
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                data=None,
                error=str(e),
            )
    
    # =========================================================================
    # TOOL: get_ma_policy_changes
    # =========================================================================
    def get_ma_policy_changes(
        self,
        year: int = None,
        category: str = None,
    ) -> ToolResult:
        """
        Get structured policy changes affecting Medicare Advantage.
        
        Args:
            year: Filter by effective year
            category: Filter by category ("risk_adjustment", "star_ratings", "part_d", "network", "snp")
            
        Returns:
            ToolResult with policy changes including:
            - Change title and description
            - Impact level and affected entities
            - Source document and dates
        """
        try:
            knowledge_store = get_knowledge_store()
            changes = knowledge_store.get_policy_changes(year=year, category=category)
            
            if not changes:
                return ToolResult(
                    success=True,
                    data=[],
                    metadata={"note": f"No policy changes found"},
                    sources=[],
                    confidence=0.7,
                )
            
            return ToolResult(
                success=True,
                data=[c.to_dict() for c in changes],
                sources=list(set(c.source_document for c in changes if c.source_document)),
                confidence=0.95,
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                data=None,
                error=str(e),
            )
    
    # =========================================================================
    # TOOL DEFINITIONS (for LLM function calling)
    # =========================================================================
    @classmethod
    def get_tool_definitions(cls) -> List[Dict]:
        """Return tool definitions in standard format for LLM function calling."""
        return [
            {
                "name": "query_database",
                "description": "Execute a SQL query against the MA data warehouse. Only SELECT/WITH queries allowed. Returns data rows, column info, and audit trail.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": """SQL SELECT query. Available tables and their filter columns:

ENROLLMENT:
- fact_enrollment_unified: year, month, contract_id, parent_org, plan_type, product_type, snp_type, group_type, state, enrollment
  CRITICAL: Use month=1 or MAX(month) for point-in-time totals, NOT SUM across months.

STARS:
- stars_enrollment_unified: star_year, contract_id, parent_org, plan_type, snp_type, group_type, enrollment, overall_rating, is_fourplus
- measure_stars_all_years: year, contract_id, parent_org, measure_id, star_rating
- measures_all_years: year, contract_id, measure_id, measure_key, measure_name, numeric_value
- cutpoints_all_years: year, measure_id, star_2, star_3, star_4, star_5

RISK SCORES:
- fact_risk_scores_unified: year, contract_id, parent_org, risk_score, enrollment

RATE NOTICE / POLICY DATA (all include audit fields: source_document, source_section, source_table, extracted_at):
- part_d_parameters: year, deductible, initial_coverage_limit, out_of_pocket_threshold, ira_oop_cap, ira_insulin_cap
- risk_adjustment_parameters: year, model_version, model_phasein_pct, normalization_factor, coding_intensity_adjustment
- ma_growth_rates: year, advance_growth_rate, final_growth_rate, effective_growth_rate
- star_bonus_structure: year, bonus_5_star, bonus_4_plus, rebate_5_star, rebate_4_plus
- hcc_coefficients_v28: model_version, hcc_code, hcc_label, community_aged, community_disabled, institutional

USPCC / COUNTY RATES (audit fields: source_file, source_table, source_row, extracted_at):
- national_uspcc: year, uspcc_aged, uspcc_disabled, uspcc_esrd_dialysis, uspcc_total_non_esrd (national FFS baseline)
- county_benchmarks: year, state_code, county_fips, county_name, ffs_rate, ma_benchmark, quartile (county-level rates from ratebooks)

PARSED RATE NOTICE TABLES (full audit: source_document, source_page, source_table_num, source_row, extracted_at):
- uspcc_projections: projection_year, calendar_year, uspcc_value, column_header (204 rows, FFS cost projections)
- hcc_coefficients_all: model_version, model_year, hcc_code, hcc_label, segment, coefficient (7,420 rows, all HCC models)
- esrd_rates: year, variable_name, variable_type, segment, value (3,718 rows, ESRD risk factors)
- demographic_factors: year, factor_type, factor_year, factor_name, value (575 rows, age/sex adjustments)
- benchmark_parameters: year, parameter_type, parameter_label, value (70 rows, applicable %s, quartiles)
- service_type_costs: year, service_type, cost_category, value (252 rows, cost breakdowns)

For parent_org filters, use LIKE '%Humana%' pattern (handles variations)."""
                        },
                        "context": {
                            "type": "string",
                            "description": "Brief context about why this query is being run (for audit)"
                        }
                    },
                    "required": ["sql"]
                }
            },
            {
                "name": "lookup_knowledge",
                "description": "FIRST check this for MA domain knowledge: glossary terms, CMS-HCC model versions (V12-V28) with exact phase-in percentages, star rating measures, policy timeline, calculations, payer info. Contains authoritative information about risk adjustment model transitions including V24-to-V28 phase-in schedule (67%/33% in 2024, 33%/67% in 2025, 100% in 2026).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Term or concept to look up (e.g., 'V28', 'HCC', 'D-SNP', 'RAF', 'star rating', 'phase-in')"
                        },
                        "category": {
                            "type": "string",
                            "enum": ["glossary", "star_measures", "policy_timeline", "calculations", "payers"],
                            "description": "Optional category to search in"
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "get_payer_info",
                "description": "Get comprehensive info about an MA payer: enrollment, star ratings, market position, specialty.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "payer_name": {
                            "type": "string",
                            "description": "Payer name or alias (e.g., 'Humana', 'UHC', 'CVS', 'Aetna')"
                        },
                        "year": {
                            "type": "integer",
                            "description": "Year for data (default: 2026)"
                        }
                    },
                    "required": ["payer_name"]
                }
            },
            {
                "name": "calculate_metric",
                "description": "Calculate a specific MA metric with full explanation and formula breakdown. Supports any combination of filters.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "metric": {
                            "type": "string",
                            "enum": ["enrollment", "market_share", "yoy_growth", "four_plus_pct"],
                            "description": "Metric to calculate"
                        },
                        "filters": {
                            "type": "object",
                            "description": "Optional filters (any combination): parent_org (payer name), plan_type (HMO, PPO, PFFS), snp_type (SNP, Non-SNP), group_type (Individual, Group), state (2-letter code)"
                        },
                        "year": {
                            "type": "integer",
                            "description": "Year for calculation (default: 2026)"
                        }
                    },
                    "required": ["metric"]
                }
            },
            {
                "name": "get_data_lineage",
                "description": "Get data lineage for a table: source files, transformations, update frequency.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Table to trace (fact_enrollment_unified, stars_enrollment_unified, etc.)"
                        }
                    },
                    "required": ["table_name"]
                }
            },
            {
                "name": "search_documents",
                "description": "Search CMS documents (rate notices, technical notes, call letters) for policy and methodology information.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query (e.g., 'risk adjustment V28', 'star ratings methodology', 'benchmark calculation')"
                        },
                        "doc_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Filter by document types: rate_notice_advance, rate_notice_final, tech_notes, call_letter"
                        },
                        "years": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Filter by years (e.g., [2024, 2025, 2026])"
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "get_policy_changes",
                "description": "Get timeline of policy changes from CMS documents. Useful for understanding what changed and when.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic to filter by (e.g., 'risk adjustment', 'star ratings', 'benchmark')"
                        },
                        "year": {
                            "type": "integer",
                            "description": "Specific year to get changes for"
                        }
                    }
                }
            },
            {
                "name": "get_rate_notice_metrics",
                "description": "Get STRUCTURED metrics from a rate notice. ALWAYS use this first for rate notice questions - returns pre-extracted data on growth rates, risk adjustment, star bonuses, Part D parameters, etc. Much more reliable than text search.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "integer",
                            "description": "Payment year (e.g., 2027)"
                        },
                        "notice_type": {
                            "type": "string",
                            "enum": ["advance", "final"],
                            "description": "Type of notice (default: advance)"
                        }
                    },
                    "required": ["year"]
                }
            },
            {
                "name": "get_hcc_model_info",
                "description": "Get structured info about CMS-HCC risk adjustment model: phase-in percentages, normalization factors, HCC counts, segments, changes from prior version.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "model_version": {
                            "type": "string",
                            "description": "Model version (e.g., 'V28', 'V24')"
                        },
                        "year": {
                            "type": "integer",
                            "description": "Payment year"
                        }
                    }
                }
            },
            {
                "name": "get_ma_policy_changes",
                "description": "Get structured policy changes affecting MA: title, description, impact level, affected entities, source. Filter by year or category.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "integer",
                            "description": "Filter by effective year"
                        },
                        "category": {
                            "type": "string",
                            "enum": ["risk_adjustment", "star_ratings", "part_d", "network", "snp"],
                            "description": "Filter by category"
                        }
                    }
                }
            }
        ]


# Singleton instance
_tools_instance = None

def get_agent_tools() -> MAAgentTools:
    """Get or create singleton agent tools instance."""
    global _tools_instance
    if _tools_instance is None:
        _tools_instance = MAAgentTools()
    return _tools_instance
