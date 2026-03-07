"""
Schema Context Service
======================

Provides the AI with actual database schema and values so it can 
reason about user queries intelligently - no hardcoded mappings needed.

The AI sees what's actually in the database and figures out how to
map user terms like "united" → "UnitedHealth Group, Inc."
"""

import os
import json
from typing import Dict, List, Optional
from dataclasses import dataclass
from functools import lru_cache


@dataclass
class SchemaContext:
    """Context about what's actually in the database."""
    
    # Key entity values
    parent_organizations: List[str]
    plan_types: List[str]
    snp_types: List[str]
    
    # Data ranges
    year_range: tuple  # (min_year, max_year)
    month_range: tuple  # (min_month, max_month) for monthly data
    
    # Column info by table
    table_columns: Dict[str, List[str]]
    
    def to_prompt_context(self) -> str:
        """Generate context string for LLM prompts."""
        
        lines = [
            "=== ACTUAL DATABASE VALUES (use these exact names in queries) ===",
            "",
            "PARENT ORGANIZATIONS (exact names - match user input to these):",
        ]
        for org in self.parent_organizations[:20]:
            lines.append(f"  • {org}")
        
        lines.extend([
            "",
            "PLAN TYPES (these are the only valid values):",
        ])
        for pt in self.plan_types:
            lines.append(f"  • {pt}")
        
        lines.extend([
            "",
            "SNP TYPES:",
        ])
        for st in self.snp_types:
            lines.append(f"  • {st}")
        
        lines.extend([
            "",
            f"DATA RANGE: {self.year_range[0]} - {self.year_range[1]}",
            f"MONTHLY DATA: Available from 2024 onwards",
            "",
            "=== QUERY INTERPRETATION RULES ===",
            "",
            "When user says...                    → Query for...",
            "─────────────────────────────────────────────────────────────────",
            '"united", "uhc"                      → parent_org LIKE \'%UnitedHealth%\'',
            '"humana"                             → parent_org LIKE \'%Humana%\'',
            '"cvs", "aetna"                       → parent_org LIKE \'%CVS%\'',
            '"elevance", "anthem"                 → parent_org LIKE \'%Elevance%\'',
            '"cigna"                              → parent_org LIKE \'%CIGNA%\'',
            '"kaiser"                             → parent_org LIKE \'%Kaiser%\'',
            '"centene"                            → parent_org LIKE \'%Centene%\'',
            "",
            '"traditional MA", "regular MA"       → Total MA enrollment (all plan_types)',
            '                                       NOT a specific plan_type!',
            '"MA-only" (no Part D)                → plan_type NOT LIKE \'%Drug%\'',
            '"SNP enrollment", "D-SNP"            → snp_type = \'D-SNP\'',
            '"non-SNP"                            → snp_type = \'Non-SNP\'',
            "",
            '"last 10 years"                      → year >= (current_year - 10)',
            '"recent", "lately"                   → year >= 2023',
            '"industry", "market total"           → SUM across all parent_org',
            "",
            "IMPORTANT: There is NO plan_type called 'Traditional MA' or 'Traditional'!",
            "If user asks for 'traditional MA enrollment', query TOTAL enrollment.",
        ])
        
        return "\n".join(lines)


class SchemaContextService:
    """Service to load and cache schema context from the database."""
    
    _instance = None
    _context: Optional[SchemaContext] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_context(self) -> SchemaContext:
        """Get schema context, loading from DB if needed."""
        if self._context is None:
            self._context = self._load_context()
        return self._context
    
    def _load_context(self) -> SchemaContext:
        """Load context from the database."""
        try:
            from db.duckdb_layer import MAQueryEngine
            engine = MAQueryEngine()
            
            # Get parent organizations
            df = engine.query('''
                SELECT DISTINCT parent_org
                FROM fact_enrollment_unified 
                WHERE year = 2026 AND parent_org IS NOT NULL
                GROUP BY parent_org
                ORDER BY SUM(enrollment) DESC
                LIMIT 30
            ''')
            parent_orgs = df['parent_org'].tolist()
            
            # Get plan types
            df = engine.query('SELECT DISTINCT plan_type FROM fact_enrollment_unified WHERE plan_type IS NOT NULL ORDER BY plan_type')
            plan_types = df['plan_type'].tolist()
            
            # Get SNP types
            df = engine.query('SELECT DISTINCT snp_type FROM fact_enrollment_unified WHERE snp_type IS NOT NULL')
            snp_types = df['snp_type'].tolist()
            
            # Get year range
            df = engine.query('SELECT MIN(year) as min_y, MAX(year) as max_y FROM fact_enrollment_unified')
            year_range = (int(df['min_y'].iloc[0]), int(df['max_y'].iloc[0]))
            
            # Get month range for recent data
            df = engine.query('SELECT MIN(month) as min_m, MAX(month) as max_m FROM fact_enrollment_unified WHERE year = 2026')
            month_range = (int(df['min_m'].iloc[0]), int(df['max_m'].iloc[0]))
            
            # Get table columns
            table_columns = {
                'fact_enrollment_unified': ['year', 'month', 'contract_id', 'plan_id', 'parent_org', 
                                           'plan_type', 'snp_type', 'enrollment', 'state', 'county'],
                'stars_enrollment_unified': ['star_year', 'contract_id', 'parent_org', 'overall_rating',
                                            'enrollment', 'part_c_summary', 'part_d_summary'],
                'fact_snp': ['year', 'contract_id', 'parent_org', 'snp_type', 'enrollment'],
            }
            
            return SchemaContext(
                parent_organizations=parent_orgs,
                plan_types=plan_types,
                snp_types=snp_types,
                year_range=year_range,
                month_range=month_range,
                table_columns=table_columns,
            )
            
        except Exception as e:
            print(f"Error loading schema context: {e}")
            # Return minimal context on error
            return SchemaContext(
                parent_organizations=["UnitedHealth Group, Inc.", "Humana Inc.", "CVS Health Corporation"],
                plan_types=["HMO/HMOPOS", "Local PPO", "Regional PPO"],
                snp_types=["D-SNP", "C-SNP", "I-SNP", "Non-SNP"],
                year_range=(2013, 2026),
                month_range=(1, 12),
                table_columns={},
            )
    
    def refresh(self):
        """Force refresh of context from database."""
        self._context = None
        return self.get_context()


# Singleton accessor
def get_schema_context() -> SchemaContext:
    """Get the schema context singleton."""
    return SchemaContextService().get_context()


def get_schema_prompt() -> str:
    """Get the schema context formatted for LLM prompts."""
    return get_schema_context().to_prompt_context()
