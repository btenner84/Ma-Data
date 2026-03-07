"""
Enrollment Service

Provides enrollment data queries using the unified data layer.
All queries are audited for lineage tracking.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import sys
import os
import math
import pandas as pd

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db import get_engine


PLAN_TYPE_MAP = {
    'HMO': ['HMO/HMOPOS', 'Medicare-Medicaid Plan HMO/HMOPOS'],
    'PPO': ['Local PPO', 'Regional PPO'],  # Combined - both PPO types
    'PFFS': ['PFFS'],
    'MSA': ['MSA'],
    'Cost': ['1876 Cost'],
    'PACE': ['National PACE'],
    'PDP': ['Medicare Prescription Drug Plan'],
    'Employer PDP': ['Employer/Union Only Direct Contract PDP'],
    'Dual (MMP)': ['Medicare-Medicaid Plan HMO/HMOPOS'],
}


def expand_plan_types(simplified_types: List[str]) -> List[str]:
    """Convert simplified plan type names to full CMS names."""
    if not simplified_types:
        return None
    expanded = []
    for t in simplified_types:
        if t in PLAN_TYPE_MAP:
            expanded.extend(PLAN_TYPE_MAP[t])
        else:
            expanded.append(t)
    return list(set(expanded))


def clean_nan(value):
    """Replace NaN/Inf with None for JSON serialization."""
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def clean_dict(d):
    """Recursively clean NaN values from dict."""
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [clean_dict(item) for item in d]
    else:
        return clean_nan(d)


class EnrollmentService:
    """Service for enrollment-related queries."""

    def __init__(self):
        self.engine = get_engine()

    def get_industry_totals(
        self,
        year: Optional[int] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get industry-level enrollment totals.
        """
        year_filter = f"WHERE year = {year}" if year else ""

        sql = f"""
            SELECT
                year,
                SUM(enrollment) as total_enrollment,
                COUNT(DISTINCT contract_id) as contract_count,
                COUNT(DISTINCT contract_id || '-' || plan_id) as plan_count
            FROM fact_enrollment_unified
            {year_filter}
            GROUP BY year
            ORDER BY year DESC
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_industry_totals"
        )

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'row_count': len(df)
        })

    def get_by_parent_org(
        self,
        year: int,
        limit: int = 20,
        product_type: str = "MAPD",  # Default to MA-only
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment by parent organization.
        Defaults to MA-only (MAPD). Set product_type=None for all.
        """
        filters = [f"year = {year}"]
        if product_type:
            filters.append(f"product_type = '{product_type}'")
        
        where_clause = " AND ".join(filters)
        
        sql = f"""
            SELECT
                parent_org,
                SUM(enrollment) as total_enrollment,
                SUM(plan_count) as plan_count,
                COUNT(DISTINCT contract_id) as contract_count
            FROM fact_enrollment_unified
            WHERE {where_clause}
            GROUP BY parent_org
            ORDER BY total_enrollment DESC
            LIMIT {limit}
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_by_parent_org"
        )

        # Calculate market share
        total = df['total_enrollment'].sum()
        df['market_share'] = (df['total_enrollment'] / total * 100).round(2) if total > 0 else 0

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'year': year,
            'total_enrollment': int(total) if total else 0
        })

    def get_by_state(
        self,
        year: int,
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment by state.

        Note: State totals may be ~1-3% lower than national due to suppression.
        """
        sql = f"""
            SELECT
                state,
                SUM(enrollment) as total_enrollment,
                SUM(plan_count) as plan_count,
                SUM(contract_count) as contract_count,
                COUNT(DISTINCT county) as county_count
            FROM fact_enrollment_by_geography
            WHERE year = {year}
            GROUP BY state
            ORDER BY total_enrollment DESC
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_by_state"
        )

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'year': year,
            'note': 'State totals may be ~1-3% lower than national due to HIPAA suppression'
        })

    def get_by_dimensions(
        self,
        year: int,
        plan_type: Optional[str] = None,
        product_type: Optional[str] = None,
        snp_type: Optional[str] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment by dimension combinations.

        Supports any combination of filters.
        Note: group_type removed - not available in fact_enrollment_all_years
        """
        filters = [f"year = {year}"]

        if plan_type:
            filters.append(f"plan_type = '{plan_type}'")
        if product_type:
            filters.append(f"product_type = '{product_type}'")
        if snp_type:
            filters.append(f"snp_type = '{snp_type}'")

        sql = f"""
            SELECT
                plan_type,
                product_type,
                snp_type,
                SUM(enrollment) as enrollment,
                COUNT(DISTINCT contract_id) as contract_count
            FROM fact_enrollment_unified
            WHERE {' AND '.join(filters)}
            GROUP BY plan_type, product_type, snp_type
            ORDER BY enrollment DESC
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_by_dimensions"
        )

        total = df['enrollment'].sum()
        df['pct_of_total'] = (df['enrollment'] / total * 100).round(2) if total > 0 else 0

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'year': year,
                'plan_type': plan_type,
                'product_type': product_type,
                'snp_type': snp_type
            }
        })

    def get_timeseries(
        self,
        parent_org: Optional[str] = None,
        state: Optional[str] = None,
        states: Optional[List[str]] = None,  # Multiple states (IN clause)
        plan_type: Optional[str] = None,
        plan_types: Optional[List[str]] = None,  # Multiple plan types
        product_type: Optional[str] = None,
        product_types: Optional[List[str]] = None,  # Multiple product types
        snp_type: Optional[str] = None,
        snp_types: Optional[List[str]] = None,  # Multiple SNP types
        group_type: Optional[str] = None,
        group_types: Optional[List[str]] = None,  # Multiple group types: Individual, Group
        data_source: str = "national",  # "national" (aggregated) or "geographic" (has state/county)
        start_year: int = 2007,
        end_year: int = 2026,
        month: int = 12,  # Month snapshot (default December = year-end)
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment timeseries with optional filters.

        Data Source - BOTH use same underlying CPSC data (fact_enrollment_unified), same years (2013-2026):
        - 'national': Aggregated to national level (no state dimension), all filters work
        - 'geographic': State/county dimensions available for filtering

        ALL FILTERS WORK ON BOTH SOURCES:
        - plan_type/plan_types: HMO, PPO, PFFS, MSA, PACE, Cost
        - product_types: MAPD (MA), PDP
        - snp_types: Non-SNP, D-SNP, C-SNP, I-SNP (SNP-Unknown also available)
        - group_types: Individual, Group
        - state (geographic only): State codes

        Returns year-over-year data for charting using December month (year-end snapshot),
        or the latest available month for years without December data.

        Note: County-level data may have ~2% suppression for counties with <10 enrollees.
        """
        # BOTH sources use the same table - just aggregate differently
        # This ensures same years (2013-2026) and all filters work on both!
        table = "fact_enrollment_unified"
        
        # Build common filters
        filters = [
            f"year >= {start_year}",
            f"year <= {end_year}",
        ]
        
        if parent_org:
            filters.append(f"parent_org = '{parent_org}'")
        
        # Handle multiple plan types
        expanded_plan_types = expand_plan_types(plan_types) if plan_types else None
        if expanded_plan_types:
            plan_list = ", ".join([f"'{p}'" for p in expanded_plan_types])
            filters.append(f"plan_type IN ({plan_list})")
        elif plan_type:
            expanded_single = expand_plan_types([plan_type])
            if expanded_single and len(expanded_single) > 1:
                plan_list = ", ".join([f"'{p}'" for p in expanded_single])
                filters.append(f"plan_type IN ({plan_list})")
            else:
                filters.append(f"plan_type = '{plan_type}'")
        
        # Handle product_type filter (default to MAPD for MA plans)
        if product_types:
            product_list = ", ".join([f"'{p}'" for p in product_types])
            filters.append(f"product_type IN ({product_list})")
        elif product_type:
            filters.append(f"product_type = '{product_type}'")
        
        # Handle SNP type filter - works on both sources!
        if snp_types:
            snp_list = ", ".join([f"'{s}'" for s in snp_types])
            filters.append(f"snp_type IN ({snp_list})")
        elif snp_type:
            if snp_type == 'Non-SNP':
                filters.append("snp_type = 'Non-SNP'")
            else:
                filters.append(f"snp_type = '{snp_type}'")
        
        # Handle group type filter - works on both sources!
        if group_types:
            group_list = ", ".join([f"'{g}'" for g in group_types])
            filters.append(f"group_type IN ({group_list})")
        elif group_type:
            filters.append(f"group_type = '{group_type}'")
        
        # Geographic source: add state filter if provided
        if data_source == "geographic":
            if states:
                state_list = ", ".join([f"'{s}'" for s in states])
                filters.append(f"state IN ({state_list})")
            elif state:
                filters.append(f"state = '{state}'")
        
        # Get latest month enrollment for each year
        # Use a simpler approach: get max month per year first, then filter
        sql = f"""
            WITH latest_months AS (
                SELECT year, MAX(month) as max_month 
                FROM {table} 
                WHERE year BETWEEN {start_year} AND {end_year}
                GROUP BY year
            ),
            base_data AS (
                SELECT * FROM {table}
                WHERE {' AND '.join(filters)}
            )
            SELECT
                b.year,
                lm.max_month as month_used,
                SUM(b.enrollment) as enrollment,
                SUM(b.plan_count) as plan_count,
                COUNT(DISTINCT b.parent_org) as parent_org_count
            FROM base_data b
            INNER JOIN latest_months lm ON b.year = lm.year AND b.month = lm.max_month
            GROUP BY b.year, lm.max_month
            ORDER BY b.year
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context=f"get_timeseries_{data_source}{'_snp_' + snp_type if snp_type else ''}"
        )

        if df.empty:
            return clean_dict({
                'years': [],
                'total_enrollment': [],
                'data': [],
                'audit_id': audit_id,
                'data_source': data_source,
                'filters': {
                    'parent_org': parent_org,
                    'state': state,
                    'data_source': data_source,
                    'start_year': start_year,
                    'end_year': end_year
                }
            })

        # Calculate YoY growth
        if len(df) > 1:
            df['enrollment_prev'] = df['enrollment'].shift(1)
            df['yoy_growth'] = ((df['enrollment'] - df['enrollment_prev']) / df['enrollment_prev'] * 100).round(2)
            df = df.drop(columns=['enrollment_prev'])

        return clean_dict({
            'years': df['year'].tolist(),
            'total_enrollment': df['enrollment'].tolist(),
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'data_source': data_source,
            'filters': {
                'parent_org': parent_org,
                'state': state,
                'plan_type': plan_type,
                'plan_types': plan_types,
                'product_type': product_type,
                'product_types': product_types,
                'snp_type': snp_type,
                'data_source': data_source,
                'start_year': start_year,
                'end_year': end_year
            }
        })

    def get_filters(self, user_id: str = "api") -> Dict:
        """
        Get available filter options for enrollment data.
        
        Note: group_type removed - not available in fact_enrollment_all_years
        """
        sql = """
            SELECT DISTINCT
                year,
                plan_type,
                product_type,
                state,
                parent_org
            FROM fact_enrollment_unified
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_filters"
        )

        # Get SNP types from the enrollment table itself
        snp_sql = """
            SELECT DISTINCT snp_type
            FROM fact_enrollment_unified
            WHERE snp_type IS NOT NULL AND snp_type != ''
            ORDER BY snp_type
        """
        try:
            snp_df, _ = self.engine.query_with_record_audit(
                snp_sql,
                user_id=user_id,
                context="get_snp_filters"
            )
            # Get unique SNP types, ensure Non-SNP is first
            raw_snp_types = [t for t in snp_df['snp_type'].dropna().unique().tolist() if t]
            # Order: Non-SNP first, then alphabetically
            snp_types = ['Non-SNP'] if 'Non-SNP' in raw_snp_types else []
            snp_types += sorted([t for t in raw_snp_types if t != 'Non-SNP'])
        except:
            snp_types = ['Non-SNP', 'C-SNP', 'D-SNP', 'I-SNP']

        # Simplify plan types for UI (map verbose CMS names to simple categories)
        plan_type_mapping = {
            'HMO/HMOPOS': 'HMO',
            'Local PPO': 'PPO',
            'Regional PPO': 'PPO',  # Combined with Local PPO
            'PFFS': 'PFFS',
            'MSA': 'MSA',
            'National PACE': 'PACE',
            '1876 Cost': 'Cost',
            'Medicare Prescription Drug Plan': 'PDP',
            'Employer/Union Only Direct Contract PDP': 'Employer PDP',
            'Medicare-Medicaid Plan HMO/HMOPOS': 'Dual (MMP)',
        }
        
        raw_plan_types = sorted(df['plan_type'].dropna().unique().tolist())
        plan_types_simplified = sorted(list(set(
            plan_type_mapping.get(pt, pt) for pt in raw_plan_types
        )))
        
        # Get contracts for contract filter
        contracts_sql = """
            SELECT DISTINCT contract_id
            FROM fact_enrollment_unified
            WHERE year = 2026
            ORDER BY contract_id
        """
        try:
            contracts_df, _ = self.engine.query_with_record_audit(
                contracts_sql,
                user_id=user_id,
                context="get_contract_filters"
            )
            contracts = contracts_df['contract_id'].tolist()
        except:
            contracts = []

        return clean_dict({
            'years': sorted(df['year'].dropna().unique().tolist()),
            'plan_types': raw_plan_types,
            'plan_types_simplified': plan_types_simplified,
            'product_types': sorted(df['product_type'].dropna().unique().tolist()),
            'states': sorted(df['state'].dropna().unique().tolist()),
            'snp_types': snp_types,
            'parent_orgs': sorted(df['parent_org'].dropna().unique().tolist()),
            'contracts': contracts,
            'audit_id': audit_id
        })

    def get_plan_details(
        self,
        contract_id: str,
        plan_id: str,
        year: int,
        user_id: str = "api"
    ) -> Dict:
        """
        Get detailed information for a specific plan.
        """
        sql = f"""
            SELECT
                contract_id,
                plan_id,
                year,
                parent_org,
                plan_type,
                product_type,
                group_type,
                snp_type,
                enrollment
            FROM fact_enrollment_unified
            WHERE contract_id = '{contract_id}'
              AND plan_id = '{plan_id}'
              AND year = {year}
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_plan_details"
        )

        if len(df) == 0:
            return clean_dict({'error': 'Plan not found', 'audit_id': audit_id})

        return clean_dict({
            'data': df.to_dict(orient='records')[0],
            'audit_id': audit_id
        })

    def trace_lineage(self, audit_id: str) -> Dict:
        """
        Trace data lineage for a previous query.

        Returns information about source files and transformations.
        """
        return self.engine.trace_query_lineage(audit_id)


# Singleton instance
_service_instance = None

def get_enrollment_service() -> EnrollmentService:
    """Get or create singleton enrollment service."""
    global _service_instance
    if _service_instance is None:
        _service_instance = EnrollmentService()
    return _service_instance
