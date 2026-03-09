"""
Unified Data Service
====================

Single entry point for all data queries across enrollment, stars, and risk scores.
Built on the Gold layer star schema with full audit trail support.

Usage:
    service = UnifiedDataService()
    
    # Get enrollment timeseries
    result = service.timeseries(
        metric='enrollment',
        filters={'parent_org': 'Humana Inc.', 'year_gte': 2015},
        group_by='year'
    )
    
    # Get dimensions for filter dropdowns
    payers = service.get_dimensions('parent_org')
    states = service.get_dimensions('state')
    
    # Full audit metadata available
    print(result.audit.sql)
    print(result.audit.source_files)
"""

import os
import uuid
import threading
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from db.duckdb_layer import DuckDBLayer, _duckdb_lock


# Map simplified UI plan types to actual CMS values
PLAN_TYPE_MAP = {
    'HMO': ['HMO', 'HMOPOS', 'HMO/HMOPOS', 'Medicare-Medicaid Plan HMO/HMOPOS'],
    'PPO': ['Local PPO', 'Regional PPO'],
    'PFFS': ['PFFS'],
    'MSA': ['MSA'],
    'PACE': ['National PACE'],
    'Cost': ['1876 Cost'],
    'PDP': ['Medicare Prescription Drug Plan', 'Employer/Union Only Direct Contract PDP'],
}

# Parent organization name normalization - maps variant names to canonical name
PARENT_ORG_NORMALIZE = {
    # Trailing punctuation variants
    "Molina Healthcare, Inc.,": "Molina Healthcare, Inc.",
    "American Health Companies, Inc": "American Health Companies, Inc.",
    
    # Encoding issues
    "America+s 1st Choice NY Holdings, LLC": "America's 1st Choice NY Holdings, LLC",
    "AmericaÆs 1st Choice of South Carolina, Inc.": "America's 1st Choice of South Carolina, Inc.",
    
    # Spelling variants
    "Acension Health": "Ascension Health Alliance",
}


def normalize_parent_org(name: str) -> str:
    """Normalize parent organization name to canonical form."""
    if not name:
        return name
    # Remove trailing/leading whitespace
    name = name.strip()
    # Apply direct mappings
    return PARENT_ORG_NORMALIZE.get(name, name)


def get_parent_org_variants(normalized_name: str) -> List[str]:
    """Get all raw name variants that map to this normalized name."""
    variants = [normalized_name]
    # Find all keys that map to this normalized name
    for raw, canonical in PARENT_ORG_NORMALIZE.items():
        if canonical == normalized_name:
            variants.append(raw)
    return variants


def build_parent_org_filter(parent_org: str, column: str = "parent_org") -> str:
    """Build SQL filter for parent_org that matches all variants."""
    variants = get_parent_org_variants(parent_org)
    if len(variants) == 1:
        return f"{column} = '{parent_org}'"
    else:
        variant_list = ", ".join([f"'{v}'" for v in variants])
        return f"{column} IN ({variant_list})"


def expand_plan_types(simplified_types: List[str]) -> List[str]:
    """Convert simplified plan type names to full CMS names."""
    if not simplified_types:
        return None
    expanded = []
    for t in simplified_types:
        t_upper = t.strip()
        if t_upper in PLAN_TYPE_MAP:
            expanded.extend(PLAN_TYPE_MAP[t_upper])
        else:
            # Check if it's already an exact CMS value
            expanded.append(t_upper)
    return list(set(expanded))


@dataclass
class AuditMetadata:
    """Metadata for tracing query results back to source."""
    query_id: str
    sql: str
    tables_queried: List[str]
    filters_applied: Dict[str, Any]
    row_count: int
    source_files: List[str] = field(default_factory=list)
    pipeline_run_id: Optional[str] = None
    executed_at: datetime = field(default_factory=datetime.now)
    execution_ms: float = 0


@dataclass
class DataResult:
    """Result from a data query with audit metadata."""
    data: Dict[str, Any]
    audit: AuditMetadata


class UnifiedDataService:
    """
    Unified service for querying all Gold layer tables.
    
    Features:
    - Consistent filtering across enrollment, stars, risk scores
    - Automatic audit logging
    - Support for both national and geographic enrollment
    - Entity-based queries (tracks contracts across changes)
    """
    
    ENROLLMENT_NATIONAL = 'gold_fact_enrollment_national'
    ENROLLMENT_GEOGRAPHIC = 'gold_fact_enrollment_geographic'
    FACT_STARS = 'gold_fact_stars'
    FACT_RISK_SCORES = 'gold_fact_risk_scores'
    DIM_ENTITY = 'gold_dim_entity'
    DIM_PLAN = 'gold_dim_plan'
    DIM_GEOGRAPHY = 'gold_dim_geography'
    DIM_TIME = 'gold_dim_time'
    
    ENROLLMENT_LEGACY = 'fact_enrollment_unified'
    STARS_LEGACY = 'summary_all_years'
    RISK_LEGACY = 'fact_risk_scores_unified'
    
    def __init__(self, use_gold: bool = True):
        """
        Initialize the service.
        
        Args:
            use_gold: Use Gold layer tables (True) or legacy tables (False)
        """
        self.db = DuckDBLayer()
        self.use_gold = use_gold
        self._audit_log = []
    
    def _get_table(self, table_type: str) -> str:
        """Get the appropriate table name based on mode."""
        if self.use_gold:
            return {
                'enrollment_national': self.ENROLLMENT_NATIONAL,
                'enrollment_geographic': self.ENROLLMENT_GEOGRAPHIC,
                'stars': self.FACT_STARS,
                'risk_scores': self.FACT_RISK_SCORES,
            }.get(table_type, self.ENROLLMENT_LEGACY)
        else:
            return {
                'enrollment_national': self.ENROLLMENT_LEGACY,
                'enrollment_geographic': self.ENROLLMENT_LEGACY,
                'stars': self.STARS_LEGACY,
                'risk_scores': self.RISK_LEGACY,
            }.get(table_type, self.ENROLLMENT_LEGACY)
    
    def _build_where_clause(self, filters: Dict[str, Any]) -> tuple:
        """
        Build SQL WHERE clause from filters dict.
        
        Supports:
        - Exact match: {'year': 2024}
        - IN clause: {'state': ['CA', 'TX']}
        - Range: {'year_gte': 2020, 'year_lte': 2024}
        - LIKE: {'parent_org_like': 'Humana%'}
        
        Returns: (where_clause_str, params_dict)
        """
        conditions = []
        params = {}
        
        for key, value in filters.items():
            if value is None:
                continue
            
            if key.endswith('_gte'):
                col = key[:-4]
                conditions.append(f"{col} >= ${key}")
                params[key] = value
            elif key.endswith('_lte'):
                col = key[:-4]
                conditions.append(f"{col} <= ${key}")
                params[key] = value
            elif key.endswith('_like'):
                col = key[:-5]
                conditions.append(f"{col} LIKE ${key}")
                params[key] = value
            elif isinstance(value, list):
                placeholders = ', '.join([f"'{v}'" for v in value])
                conditions.append(f"{key} IN ({placeholders})")
            else:
                conditions.append(f"{key} = ${key}")
                params[key] = value
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        return where_clause, params
    
    def _execute_query(self, sql: str, tables: List[str], filters: Dict) -> DataResult:
        """Execute query and return result with audit metadata. Thread-safe via lock."""
        query_id = str(uuid.uuid4())[:8]
        start_time = datetime.now()
        
        try:
            # Thread-safe query execution
            with _duckdb_lock:
                result = self.db.conn.execute(sql)
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description] if result.description else []
            
            data = [dict(zip(columns, row)) for row in rows]
            
            execution_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            source_files = []
            for row in data:
                if '_source_file' in row and row['_source_file']:
                    source_files.append(row['_source_file'])
            source_files = list(set(source_files))[:10]
            
            audit = AuditMetadata(
                query_id=query_id,
                sql=sql,
                tables_queried=tables,
                filters_applied=filters,
                row_count=len(data),
                source_files=source_files,
                executed_at=start_time,
                execution_ms=execution_ms,
            )
            
            self._audit_log.append(audit)
            
            return DataResult(data={'rows': data, 'columns': columns}, audit=audit)
            
        except Exception as e:
            audit = AuditMetadata(
                query_id=query_id,
                sql=sql,
                tables_queried=tables,
                filters_applied=filters,
                row_count=0,
                executed_at=start_time,
            )
            return DataResult(data={'error': str(e), 'rows': [], 'columns': []}, audit=audit)
    
    def query(
        self,
        domain: str,
        metrics: List[str],
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None
    ) -> DataResult:
        """
        Generic query interface.
        
        Args:
            domain: 'enrollment', 'stars', or 'risk_scores'
            metrics: List of metrics to return (e.g., ['enrollment', 'plan_count'])
            dimensions: Group by these columns (e.g., ['year', 'parent_org'])
            filters: Filter conditions
            order_by: Sort column
            limit: Max rows
        """
        table = self._get_table(domain)
        filters = filters or {}
        dimensions = dimensions or []
        
        select_cols = dimensions + metrics
        sql = f"SELECT {', '.join(select_cols)}"
        
        if dimensions:
            sql += f" FROM {table}"
            where_clause, _ = self._build_where_clause(filters)
            sql += f" WHERE {where_clause}"
            sql += f" GROUP BY {', '.join(dimensions)}"
        else:
            sql += f" FROM {table}"
            where_clause, _ = self._build_where_clause(filters)
            sql += f" WHERE {where_clause}"
        
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit:
            sql += f" LIMIT {limit}"
        
        return self._execute_query(sql, [table], filters)
    
    def timeseries(
        self,
        metric: str = 'enrollment',
        filters: Optional[Dict[str, Any]] = None,
        source: str = 'national',
        group_by: Optional[str] = None
    ) -> DataResult:
        """
        Get timeseries data for charting.
        
        Args:
            metric: 'enrollment', 'plan_count', 'risk_score', etc.
            filters: Filter conditions
            source: 'national' or 'geographic'
            group_by: Optional grouping (e.g., 'parent_org')
        """
        filters = filters or {}
        
        table = self._get_table(f'enrollment_{source}')
        
        agg_col = f"SUM({metric})" if metric in ['enrollment', 'plan_count'] else f"AVG({metric})"
        
        if group_by:
            sql = f"""
                SELECT year, {group_by}, {agg_col} as {metric}
                FROM {table}
                WHERE {self._build_where_clause(filters)[0]}
                GROUP BY year, {group_by}
                ORDER BY year, {group_by}
            """
        else:
            sql = f"""
                SELECT year, {agg_col} as {metric}
                FROM {table}
                WHERE {self._build_where_clause(filters)[0]}
                GROUP BY year
                ORDER BY year
            """
        
        return self._execute_query(sql, [table], filters)
    
    def get_dimensions(self, dimension: str, filters: Optional[Dict] = None) -> DataResult:
        """
        Get distinct values for a dimension (for filter dropdowns).
        
        Args:
            dimension: Column name (e.g., 'parent_org', 'state', 'plan_type')
            filters: Optional filters to narrow the list
        """
        filters = filters or {}
        
        if dimension in ['state', 'county', 'fips_code']:
            table = self.DIM_GEOGRAPHY if self.use_gold else self.ENROLLMENT_LEGACY
        elif dimension in ['plan_type', 'snp_type', 'group_type', 'product_type']:
            table = self.DIM_PLAN if self.use_gold else self.ENROLLMENT_LEGACY
        elif dimension == 'parent_org':
            table = self.DIM_ENTITY if self.use_gold else self.ENROLLMENT_LEGACY
        else:
            table = self.ENROLLMENT_LEGACY
        
        where_clause, _ = self._build_where_clause(filters)
        
        sql = f"""
            SELECT DISTINCT {dimension}
            FROM {table}
            WHERE {dimension} IS NOT NULL AND {where_clause}
            ORDER BY {dimension}
        """
        
        return self._execute_query(sql, [table], filters)
    
    def get_enrollment_summary(
        self,
        year: int = 2026,
        month: int = 1,
        filters: Optional[Dict] = None,
        source: str = 'national'
    ) -> DataResult:
        """Get enrollment summary for a specific period."""
        filters = filters or {}
        filters['year'] = year
        filters['month'] = month
        
        table = self._get_table(f'enrollment_{source}')
        where_clause, _ = self._build_where_clause(filters)
        
        sql = f"""
            SELECT 
                SUM(enrollment) as total_enrollment,
                COUNT(DISTINCT contract_id) as contract_count,
                COUNT(DISTINCT entity_id) as entity_count,
                COUNT(DISTINCT parent_org) as parent_org_count
            FROM {table}
            WHERE {where_clause}
        """
        
        return self._execute_query(sql, [table], filters)
    
    def get_stars_distribution(
        self,
        year: int = 2026,
        filters: Optional[Dict] = None
    ) -> DataResult:
        """Get distribution of star ratings."""
        filters = filters or {}
        filters['year'] = year
        
        table = self._get_table('stars')
        where_clause, _ = self._build_where_clause(filters)
        
        sql = f"""
            SELECT 
                CAST(overall_rating AS INT) as stars,
                COUNT(*) as contract_count
            FROM {table}
            WHERE overall_rating IS NOT NULL AND {where_clause}
            GROUP BY CAST(overall_rating AS INT)
            ORDER BY stars
        """
        
        return self._execute_query(sql, [table], filters)
    
    def get_risk_scores_by_parent(
        self,
        year: int = 2026,
        filters: Optional[Dict] = None
    ) -> DataResult:
        """Get average risk scores by parent org."""
        filters = filters or {}
        filters['year'] = year
        
        table = self._get_table('risk_scores')
        where_clause, _ = self._build_where_clause(filters)
        
        sql = f"""
            SELECT 
                parent_org,
                AVG(risk_score) as avg_risk_score,
                SUM(enrollment) as total_enrollment
            FROM {table}
            WHERE parent_org IS NOT NULL AND {where_clause}
            GROUP BY parent_org
            ORDER BY total_enrollment DESC
            LIMIT 20
        """
        
        return self._execute_query(sql, [table], filters)
    
    def get_audit_log(self, limit: int = 100) -> List[AuditMetadata]:
        """Get recent query audit log."""
        return self._audit_log[-limit:]
    
    def get_query_by_id(self, query_id: str) -> Optional[AuditMetadata]:
        """Look up a specific query by ID."""
        for audit in self._audit_log:
            if audit.query_id == query_id:
                return audit
        return None


    # =========================================================================
    # V5 METHODS - Full Gold Layer Support
    # =========================================================================
    
    def get_filters_v5(self) -> Dict:
        """Get all available filter options from Gold layer."""
        result = {}
        
        # Years from time dimension
        sql = "SELECT DISTINCT year FROM gold_dim_time ORDER BY year"
        years_result = self._execute_query(sql, ['gold_dim_time'], {})
        result['years'] = [r['year'] for r in years_result.data.get('rows', [])]
        
        # Parent orgs from entity dimension
        sql = "SELECT DISTINCT parent_org FROM gold_dim_entity WHERE parent_org IS NOT NULL ORDER BY parent_org"
        parents_result = self._execute_query(sql, ['gold_dim_entity'], {})
        # Normalize and dedupe parent org names
        raw_orgs = [r['parent_org'] for r in parents_result.data.get('rows', [])]
        normalized_orgs = list(set(normalize_parent_org(org) for org in raw_orgs))
        normalized_orgs.sort()
        result['parent_orgs'] = normalized_orgs
        
        # Plan attributes from plan dimension
        sql = "SELECT DISTINCT plan_type FROM gold_dim_plan WHERE plan_type IS NOT NULL ORDER BY plan_type"
        pt_result = self._execute_query(sql, ['gold_dim_plan'], {})
        result['plan_types'] = [r['plan_type'] for r in pt_result.data.get('rows', [])]
        
        sql = "SELECT DISTINCT product_type FROM gold_dim_plan WHERE product_type IS NOT NULL ORDER BY product_type"
        prod_result = self._execute_query(sql, ['gold_dim_plan'], {})
        result['product_types'] = [r['product_type'] for r in prod_result.data.get('rows', [])]
        
        sql = "SELECT DISTINCT snp_type FROM gold_dim_plan WHERE snp_type IS NOT NULL ORDER BY snp_type"
        snp_result = self._execute_query(sql, ['gold_dim_plan'], {})
        result['snp_types'] = [r['snp_type'] for r in snp_result.data.get('rows', [])]
        
        sql = "SELECT DISTINCT group_type FROM gold_dim_plan WHERE group_type IS NOT NULL ORDER BY group_type"
        grp_result = self._execute_query(sql, ['gold_dim_plan'], {})
        result['group_types'] = [r['group_type'] for r in grp_result.data.get('rows', [])]
        
        # Geography from geography dimension
        sql = "SELECT DISTINCT state FROM gold_dim_geography WHERE state IS NOT NULL ORDER BY state"
        state_result = self._execute_query(sql, ['gold_dim_geography'], {})
        result['states'] = [r['state'] for r in state_result.data.get('rows', [])]
        
        return result
    
    def get_enrollment_timeseries_v5(
        self,
        parent_org: Optional[str] = None,
        plan_types: Optional[List[str]] = None,
        product_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        counties: Optional[List[str]] = None,
        source: str = "national",
        start_year: int = 2015,
        end_year: int = 2026
    ) -> Dict:
        """Get enrollment timeseries with full filter support using Gold layer.
        
        Note: Fact tables are denormalized - no JOINs needed for filtering.
        Uses latest month per year (point-in-time snapshot, not cumulative).
        
        Source options:
        - "national": Monthly Enrollment by Contract (exact totals, no state, filters by contract-level dims)
        - "geographic": CPSC data (allows state/county, may have suppression <10)
        """
        
        # Determine table based on source
        # National table has exact totals but no state/county
        # Geographic table has state/county but may have suppression
        if source == "geographic":
            table = 'gold_fact_enrollment_geographic'
        else:
            table = 'gold_fact_enrollment_national'
        
        # Build WHERE conditions - query fact table directly (denormalized)
        conditions = [f"e.year >= {start_year}", f"e.year <= {end_year}"]
        
        if parent_org:
            conditions.append(build_parent_org_filter(parent_org, "e.parent_org"))
        if plan_types:
            # Expand simplified types (HMO -> HMO/HMOPOS, etc.)
            expanded = expand_plan_types(plan_types)
            pt_list = ", ".join([f"'{pt}'" for pt in expanded])
            conditions.append(f"e.plan_type IN ({pt_list})")
        if product_types:
            prod_list = ", ".join([f"'{pt}'" for pt in product_types])
            conditions.append(f"e.product_type IN ({prod_list})")
        if snp_types:
            snp_list = ", ".join([f"'{st}'" for st in snp_types])
            conditions.append(f"e.snp_type IN ({snp_list})")
        if group_types:
            grp_list = ", ".join([f"'{gt}'" for gt in group_types])
            conditions.append(f"e.group_type IN ({grp_list})")
        
        # State/county only apply to geographic source
        if source == "geographic":
            if states:
                state_list = ", ".join([f"'{s}'" for s in states])
                conditions.append(f"e.state IN ({state_list})")
            if counties:
                county_list = ", ".join([f"'{c}'" for c in counties])
                conditions.append(f"e.county IN ({county_list})")
        
        where_clause = " AND ".join(conditions)
        
        # For national source, also get the data_source per year to show monthly vs cpsc_fallback
        if source == "national":
            sql = f"""
                WITH latest_months AS (
                    SELECT year, MAX(month) as max_month
                    FROM {table}
                    WHERE year >= {start_year} AND year <= {end_year}
                    GROUP BY year
                )
                SELECT 
                    e.year,
                    SUM(e.enrollment) as enrollment,
                    COUNT(DISTINCT e.contract_id) as contract_count,
                    MAX(e.data_source) as data_source
                FROM {table} e
                INNER JOIN latest_months lm ON e.year = lm.year AND e.month = lm.max_month
                WHERE {where_clause}
                GROUP BY e.year
                ORDER BY e.year
            """
        else:
            sql = f"""
                WITH latest_months AS (
                    SELECT year, MAX(month) as max_month
                    FROM {table}
                    WHERE year >= {start_year} AND year <= {end_year}
                    GROUP BY year
                )
                SELECT 
                    e.year,
                    SUM(e.enrollment) as enrollment,
                    COUNT(DISTINCT e.contract_id) as contract_count,
                    'cpsc' as data_source
                FROM {table} e
                INNER JOIN latest_months lm ON e.year = lm.year AND e.month = lm.max_month
                WHERE {where_clause}
                GROUP BY e.year
                ORDER BY e.year
            """
        tables = [table]
        
        result = self._execute_query(sql, tables, {
            'parent_org': parent_org,
            'plan_types': plan_types,
            'product_types': product_types,
            'snp_types': snp_types,
            'group_types': group_types,
            'states': states,
            'source': source,
        })
        
        rows = result.data.get('rows', [])
        
        # Build response with data source info per year
        response = {
            'years': [r['year'] for r in rows],
            'enrollment': [r['enrollment'] for r in rows],
            'contract_count': [r['contract_count'] for r in rows],
            'data_sources': [r.get('data_source', 'unknown') for r in rows],
            'audit_id': result.audit.query_id,
            'filters': result.audit.filters_applied,
            'source': source,
        }
        
        return response
    
    def get_stars_timeseries_v5(
        self,
        parent_org: Optional[str] = None,
        plan_types: Optional[List[str]] = None,
        product_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        start_year: int = 2015,
        end_year: int = 2026
    ) -> Dict:
        """Get 4+ star enrollment percentage timeseries with full filter support.
        
        Joins stars to enrollment (both denormalized) to calculate enrollment-weighted %.
        Uses latest month per year for enrollment (point-in-time snapshot).
        """
        
        conditions = [f"s.year >= {start_year}", f"s.year <= {end_year}"]
        
        # Filter using parent_org from stars table (or enrollment)
        if parent_org:
            conditions.append(build_parent_org_filter(parent_org, "s.parent_org"))
        # Filter using dimension columns from enrollment table
        if plan_types:
            pt_list = ", ".join([f"'{pt}'" for pt in plan_types])
            conditions.append(f"e.plan_type IN ({pt_list})")
        if product_types:
            prod_list = ", ".join([f"'{pt}'" for pt in product_types])
            conditions.append(f"e.product_type IN ({prod_list})")
        if snp_types:
            snp_list = ", ".join([f"'{st}'" for st in snp_types])
            conditions.append(f"e.snp_type IN ({snp_list})")
        if group_types:
            grp_list = ", ".join([f"'{gt}'" for gt in group_types])
            conditions.append(f"e.group_type IN ({grp_list})")
        
        where_clause = " AND ".join(conditions)
        
        sql = f"""
            WITH latest_months AS (
                SELECT year, MAX(month) as max_month
                FROM gold_fact_enrollment_national
                WHERE year >= {start_year} AND year <= {end_year}
                GROUP BY year
            ),
            enrollment_snapshot AS (
                SELECT e.*
                FROM gold_fact_enrollment_national e
                INNER JOIN latest_months lm ON e.year = lm.year AND e.month = lm.max_month
            )
            SELECT 
                s.year,
                SUM(e.enrollment) as total_enrollment,
                SUM(CASE WHEN s.overall_rating >= 4 THEN e.enrollment ELSE 0 END) as fourplus_enrollment,
                COUNT(DISTINCT s.contract_id) as contract_count
            FROM gold_fact_stars s
            LEFT JOIN enrollment_snapshot e 
                ON s.contract_id = e.contract_id AND s.year = e.year
            WHERE s.overall_rating IS NOT NULL AND {where_clause}
            GROUP BY s.year
            ORDER BY s.year
        """
        
        result = self._execute_query(sql, ['gold_fact_stars', 'gold_fact_enrollment_national'], {
            'parent_org': parent_org,
            'plan_types': plan_types,
            'product_types': product_types,
            'snp_types': snp_types,
        })
        
        rows = result.data.get('rows', [])
        return {
            'years': [r['year'] for r in rows],
            'pct_fourplus': [
                round(r['fourplus_enrollment'] / r['total_enrollment'] * 100, 1) if r['total_enrollment'] and r['total_enrollment'] > 0 else 0
                for r in rows
            ],
            'total_enrollment': [r['total_enrollment'] for r in rows],
            'contract_count': [r['contract_count'] for r in rows],
            'audit_id': result.audit.query_id
        }
    
    def get_risk_timeseries_v5(
        self,
        parent_org: Optional[str] = None,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        start_year: int = 2015,
        end_year: int = 2024
    ) -> Dict:
        """Get risk score timeseries with full filter support.
        
        Note: Fact table is denormalized - no JOINs needed for filtering.
        """
        
        conditions = [f"year >= {start_year}", f"year <= {end_year}"]
        
        if parent_org:
            conditions.append(build_parent_org_filter(parent_org))
        if plan_types:
            pt_list = ", ".join([f"'{pt}'" for pt in plan_types])
            conditions.append(f"plan_type IN ({pt_list})")
        if snp_types:
            snp_list = ", ".join([f"'{st}'" for st in snp_types])
            conditions.append(f"snp_type IN ({snp_list})")
        if group_types:
            grp_list = ", ".join([f"'{gt}'" for gt in group_types])
            conditions.append(f"group_type IN ({grp_list})")
        
        where_clause = " AND ".join(conditions)
        
        sql = f"""
            SELECT 
                year,
                SUM(enrollment) as total_enrollment,
                SUM(risk_score * enrollment) as weighted_risk_sum,
                COUNT(DISTINCT contract_id) as contract_count
            FROM gold_fact_risk_scores
            WHERE risk_score IS NOT NULL AND {where_clause}
            GROUP BY year
            ORDER BY year
        """
        
        result = self._execute_query(sql, ['gold_fact_risk_scores'], {
            'parent_org': parent_org,
            'plan_types': plan_types,
            'snp_types': snp_types,
        })
        
        rows = result.data.get('rows', [])
        return {
            'years': [r['year'] for r in rows],
            'wavg_risk': [
                round(r['weighted_risk_sum'] / r['total_enrollment'], 4) if r['total_enrollment'] and r['total_enrollment'] > 0 else 0
                for r in rows
            ],
            'total_enrollment': [r['total_enrollment'] for r in rows],
            'contract_count': [r['contract_count'] for r in rows],
            'audit_id': result.audit.query_id
        }
    
    def get_summary_v5(
        self,
        parent_org: Optional[str] = None,
        year: int = 2026,
        plan_types: Optional[List[str]] = None,
        product_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None
    ) -> Dict:
        """Get comprehensive summary for a payer or industry."""
        
        # Get enrollment
        enrollment_ts = self.get_enrollment_timeseries_v5(
            parent_org=parent_org,
            plan_types=plan_types,
            product_types=product_types,
            snp_types=snp_types,
            group_types=group_types,
            start_year=year,
            end_year=year
        )
        
        # Get stars
        stars_ts = self.get_stars_timeseries_v5(
            parent_org=parent_org,
            plan_types=plan_types,
            product_types=product_types,
            snp_types=snp_types,
            start_year=year,
            end_year=year
        )
        
        # Get risk (use 2024 if year > 2024)
        risk_year = min(year, 2024)
        risk_ts = self.get_risk_timeseries_v5(
            parent_org=parent_org,
            plan_types=plan_types,
            snp_types=snp_types,
            start_year=risk_year,
            end_year=risk_year
        )
        
        return {
            'year': year,
            'parent_org': parent_org or 'Industry',
            'enrollment': enrollment_ts['enrollment'][0] if enrollment_ts['enrollment'] else 0,
            'contract_count': enrollment_ts['contract_count'][0] if enrollment_ts['contract_count'] else 0,
            'pct_fourplus': stars_ts['pct_fourplus'][0] if stars_ts['pct_fourplus'] else 0,
            'wavg_risk': risk_ts['wavg_risk'][0] if risk_ts['wavg_risk'] else 0,
            'filters': {
                'plan_types': plan_types,
                'product_types': product_types,
                'snp_types': snp_types,
                'group_types': group_types
            }
        }
    
    def get_counties_v5(self, states: List[str]) -> Dict:
        """Get counties for specified states from Gold layer geographic data."""
        if not states:
            return {"counties": []}
        
        states_str = ", ".join([f"'{s}'" for s in states])
        sql = f"""
        SELECT DISTINCT county
        FROM gold_fact_enrollment_geographic
        WHERE state IN ({states_str})
          AND county IS NOT NULL
        ORDER BY county
        """
        
        result = self._execute_query(sql, ['gold_fact_enrollment_geographic'], {'states': states})
        rows = result.data.get('rows', []) if result and result.data else []
        counties = [row['county'] for row in rows if 'county' in row]
        return {"counties": counties}
    
    def get_risk_contracts_v5(
        self,
        year: int,
        parent_org: Optional[str] = None,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None
    ) -> Dict:
        """Get risk score details by contract for a specific year."""
        
        where_clauses = [f"year = {year}"]
        
        if parent_org:
            where_clauses.append(build_parent_org_filter(parent_org))
        
        if plan_types:
            plan_types_str = ", ".join([f"'{pt}'" for pt in plan_types])
            where_clauses.append(f"plan_type IN ({plan_types_str})")
        
        if snp_types:
            snp_types_str = ", ".join([f"'{st}'" for st in snp_types])
            where_clauses.append(f"snp_type IN ({snp_types_str})")
        
        where_clause = " AND ".join(where_clauses)
        
        sql = f"""
        SELECT 
            contract_id,
            parent_org,
            plan_type,
            snp_type,
            SUM(enrollment) as total_enrollment,
            SUM(risk_score * enrollment) / NULLIF(SUM(enrollment), 0) as wavg_risk
        FROM gold_fact_risk_scores
        WHERE {where_clause}
        GROUP BY contract_id, parent_org, plan_type, snp_type
        ORDER BY total_enrollment DESC
        LIMIT 100
        """
        
        result = self._execute_query(sql)
        
        contracts = []
        if result:
            for row in result:
                contracts.append({
                    "contract_id": row[0],
                    "parent_org": row[1],
                    "plan_type": row[2],
                    "snp_type": row[3],
                    "enrollment": row[4],
                    "wavg_risk": round(row[5], 3) if row[5] else None
                })
        
        return {"contracts": contracts, "year": year}
    
    def get_geographic_metrics_v5(
        self,
        parent_org: Optional[str] = None,
        plan_types: Optional[List[str]] = None,
        product_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        year: int = 2026,
        month: Optional[int] = None
    ) -> Dict:
        """
        Get geographic metrics with TAM (Total Addressable Market) calculations.
        
        Joins geographic enrollment with dim_plan to get dimensions (plan_type, 
        snp_type, group_type) since geographic fact table has these as NULL.
        
        Returns:
        - county_count: Number of counties with enrollment matching filters
        - enrollment: Total enrollment in those counties
        - eligibles: Total Medicare eligibles (TAM) in those counties
        - market_share: enrollment / eligibles * 100
        - by_plan_type, by_product_type, by_snp_type, by_group_type: Breakdowns
        
        All metrics are traceable via audit metadata.
        """
        # Build base filters (on geographic fact table directly)
        base_conditions = [f"year = {year}"]
        
        if month:
            base_conditions.append(f"month = {month}")
        else:
            base_conditions.append(f"""month = (
                SELECT MAX(month) FROM gold_fact_enrollment_geographic WHERE year = {year}
            )""")
        
        if parent_org:
            base_conditions.append(build_parent_org_filter(parent_org))
        if states:
            state_list = ", ".join([f"'{s}'" for s in states])
            base_conditions.append(f"state IN ({state_list})")
        
        base_where = " AND ".join(base_conditions)
        
        # Build dimension filters (applied AFTER join with dim_plan)
        dim_filters = []
        if plan_types:
            expanded = expand_plan_types(plan_types)
            pt_list = ", ".join([f"'{pt}'" for pt in expanded])
            dim_filters.append(f"plan_type IN ({pt_list})")
        if product_types:
            prod_list = ", ".join([f"'{pt}'" for pt in product_types])
            dim_filters.append(f"product_type IN ({prod_list})")
        if snp_types:
            snp_list = ", ".join([f"'{st}'" for st in snp_types])
            dim_filters.append(f"snp_type IN ({snp_list})")
        if group_types:
            grp_list = ", ".join([f"'{gt}'" for gt in group_types])
            dim_filters.append(f"group_type IN ({grp_list})")
        
        dim_where = " AND ".join(dim_filters) if dim_filters else "1=1"
        
        # SIMPLE query - geographic table already has dimensions populated!
        # No joins with dim_plan needed
        sql = f"""
        WITH filtered_enrollment AS (
            SELECT fips, enrollment
            FROM gold_fact_enrollment_geographic
            WHERE {base_where} AND {dim_where}
        ),
        
        county_enrollment AS (
            SELECT fips, SUM(enrollment) as enrollment
            FROM filtered_enrollment
            GROUP BY fips
        ),
        
        county_eligibles AS (
            SELECT fips, eligibles
            FROM gold_dim_county
            WHERE year = {year}
              AND month = (SELECT MAX(month) FROM gold_dim_county WHERE year = {year})
        )
        
        SELECT 
            COUNT(DISTINCT ce.fips) as county_count,
            SUM(ce.enrollment) as total_enrollment,
            SUM(el.eligibles) as total_eligibles,
            ROUND(100.0 * SUM(ce.enrollment) / NULLIF(SUM(el.eligibles), 0), 2) as market_share
        FROM county_enrollment ce
        LEFT JOIN county_eligibles el ON ce.fips = el.fips
        """
        
        result = self._execute_query(sql, ['gold_fact_enrollment_geographic', 'gold_dim_county'], {
            'parent_org': parent_org,
            'plan_types': plan_types,
            'product_types': product_types,
            'snp_types': snp_types,
            'group_types': group_types,
            'states': states,
            'year': year,
        })
        
        rows = result.data.get('rows', [])
        summary = rows[0] if rows else {}
        
        # Get breakdowns by dimension
        breakdowns = {}
        
        # By plan type
        plan_sql = f"""
        SELECT plan_type, COUNT(DISTINCT fips_code) as counties, SUM(enrollment) as enrollment
        FROM filtered_enrollment
        GROUP BY plan_type
        ORDER BY enrollment DESC
        """
        # SIMPLE breakdowns - geographic table already has dimensions!
        # Base filter for all breakdown queries
        base_filter = f"{base_where} AND {dim_where}"
        
        # Helper function to build breakdown with TAM
        def build_breakdown_with_tam(dimension_col: str, base_filter: str, year: int):
            sql = f"""
            WITH geo_agg AS (
                SELECT {dimension_col} as dim_value, fips, SUM(enrollment) as enrollment
                FROM gold_fact_enrollment_geographic
                WHERE {base_filter}
                GROUP BY {dimension_col}, fips
            ),
            dim_summary AS (
                SELECT dim_value, COUNT(DISTINCT fips) as counties, SUM(enrollment) as enrollment
                FROM geo_agg
                GROUP BY dim_value
            ),
            dim_counties AS (
                SELECT DISTINCT dim_value, fips FROM geo_agg
            ),
            county_elig AS (
                SELECT fips, eligibles FROM gold_dim_county
                WHERE year = {year} AND month = (SELECT MAX(month) FROM gold_dim_county WHERE year = {year})
            ),
            dim_tam AS (
                SELECT dc.dim_value, SUM(ce.eligibles) as eligibles
                FROM dim_counties dc
                LEFT JOIN county_elig ce ON dc.fips = ce.fips
                GROUP BY dc.dim_value
            )
            SELECT 
                ds.dim_value as name,
                ds.counties,
                ds.enrollment,
                dt.eligibles,
                ROUND(100.0 * ds.enrollment / NULLIF(dt.eligibles, 0), 2) as market_share
            FROM dim_summary ds
            LEFT JOIN dim_tam dt ON ds.dim_value = dt.dim_value
            ORDER BY ds.enrollment DESC
            """
            result = self._execute_query(sql, ['gold_fact_enrollment_geographic', 'gold_dim_county'], {})
            return [
                {
                    'name': r['name'], 
                    'counties': r['counties'], 
                    'enrollment': r['enrollment'],
                    'eligibles': r['eligibles'],
                    'market_share': r['market_share']
                }
                for r in result.data.get('rows', [])
            ]
        
        # Build breakdowns with TAM for each dimension
        breakdowns['by_plan_type'] = build_breakdown_with_tam('plan_type', base_filter, year)
        breakdowns['by_product_type'] = build_breakdown_with_tam('product_type', base_filter, year)
        breakdowns['by_snp_type'] = build_breakdown_with_tam('snp_type', base_filter, year)
        breakdowns['by_group_type'] = build_breakdown_with_tam('group_type', base_filter, year)
        
        # By state (simpler, no TAM per state for now)
        state_sql = f"""
        SELECT state as name, COUNT(DISTINCT fips) as counties, SUM(enrollment) as enrollment
        FROM gold_fact_enrollment_geographic
        WHERE {base_filter}
        GROUP BY state ORDER BY enrollment DESC LIMIT 10
        """
        state_result = self._execute_query(state_sql, ['gold_fact_enrollment_geographic'], {})
        breakdowns['by_state'] = [
            {'name': r['name'], 'counties': r['counties'], 'enrollment': r['enrollment']}
            for r in state_result.data.get('rows', [])
        ]
        
        return {
            'year': year,
            'filters': {
                'parent_org': parent_org,
                'plan_types': plan_types,
                'product_types': product_types,
                'snp_types': snp_types,
                'group_types': group_types,
                'states': states,
            },
            'summary': {
                'county_count': summary.get('county_count', 0),
                'enrollment': summary.get('total_enrollment', 0),
                'eligibles': summary.get('total_eligibles', 0),
                'market_share': summary.get('market_share', 0),
            },
            'breakdowns': breakdowns,
            'audit': {
                'query_id': result.audit.query_id,
                'tables': result.audit.tables_queried,
                'source_files': result.audit.source_files[:5],
            }
        }


# Singleton instance
_data_service_instance = None

def get_data_service() -> UnifiedDataService:
    """Factory function to get a configured data service instance."""
    global _data_service_instance
    if _data_service_instance is None:
        use_gold = os.environ.get('USE_GOLD_LAYER', 'true').lower() == 'true'
        _data_service_instance = UnifiedDataService(use_gold=use_gold)
    return _data_service_instance
