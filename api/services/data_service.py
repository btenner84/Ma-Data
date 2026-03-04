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
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from db.duckdb_layer import DuckDBLayer


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
        """Execute query and return result with audit metadata."""
        query_id = str(uuid.uuid4())[:8]
        start_time = datetime.now()
        
        try:
            result = self.db.execute(sql)
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


def get_data_service() -> UnifiedDataService:
    """Factory function to get a configured data service instance."""
    use_gold = os.environ.get('USE_GOLD_LAYER', 'true').lower() == 'true'
    return UnifiedDataService(use_gold=use_gold)
