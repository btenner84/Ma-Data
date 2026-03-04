"""
Risk Scores Service - V3

Provides risk score data queries using the unified data layer.
Supports ALL filters: parent_org, plan_type, group_type, snp_type, state, county, year.

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


def clean_nan(value):
    """Replace NaN/Inf with None for JSON serialization."""
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def clean_dict(d: Dict) -> Dict:
    """Recursively clean NaN values from dict."""
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [clean_dict(item) for item in d]
    else:
        return clean_nan(d)


class RiskScoresService:
    """
    Service for risk score queries.

    Supports comprehensive filtering:
    - parent_org: Filter by parent organization (153 orgs)
    - plan_type: HMO, Local PPO, Regional PPO, PFFS, MSA, MMP
    - group_type: Individual, Group
    - snp_type: Non-SNP, SNP
    - state: Filter by state (via geographic join)
    - county: Filter by county (via geographic join)
    - year: 2006-2024

    Metrics:
    - simple_avg: Simple average of plan risk scores
    - wavg: Enrollment-weighted average (recommended)
    """

    def __init__(self):
        self.engine = get_engine()

    # =========================================================================
    # DIMENSION VALUE NORMALIZATION
    # =========================================================================

    def _normalize_plan_types(self, plan_types: List[str]) -> List[str]:
        """
        Normalize plan type values to match database.

        Risk scores table uses: HMO, Local PPO, Regional PPO, PFFS, MSA
        """
        mapping = {
            'HMO/HMOPOS': 'HMO',  # Enrollment uses full name, risk uses short
            'PPO': 'Local PPO',
            'RPPO': 'Regional PPO',
        }
        return [mapping.get(pt, pt) for pt in plan_types]

    def _normalize_group_types(self, group_types: List[str]) -> List[str]:
        """Normalize group type values."""
        # Current values match: Individual, Group
        return group_types

    def _normalize_snp_types(self, snp_types: List[str]) -> List[str]:
        """
        Normalize SNP type values.

        Data now has D-SNP, C-SNP, I-SNP subtypes from SNP file lookup.
        Pass through as-is for specific subtype filtering.
        """
        # SNP subtypes are now stored directly in the data
        # D-SNP, C-SNP, I-SNP, Non-SNP are all valid filter values
        return snp_types

    # =========================================================================
    # FILTER OPTIONS
    # =========================================================================

    def get_filters(self, user_id: str = "api") -> Dict:
        """
        Get all available filter options for risk score data.

        IMPORTANT: Uses risk_scores_by_parent_dims for filters to ensure
        filter values match the timeseries query table.
        """
        # Use dims table for consistent filter values with timeseries queries
        sql = """
            SELECT DISTINCT
                year,
                parent_org,
                plan_type,
                group_type,
                snp_type
            FROM risk_scores_by_parent_dims
            WHERE plan_type != 'Unknown'
              AND group_type != 'Unknown'
              AND snp_type != 'Unknown'
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_risk_filters"
        )

        # Get states from geography
        sql_states = """
            SELECT DISTINCT state
            FROM fact_enrollment_by_geography
            ORDER BY state
        """

        df_states, audit_id2 = self.engine.query_with_audit(
            sql_states,
            user_id=user_id,
            context="get_state_filters"
        )

        return {
            'years': sorted(df['year'].unique().tolist()),
            'parent_orgs': sorted(df['parent_org'].dropna().unique().tolist()),
            'plan_types': sorted(df['plan_type'].dropna().unique().tolist()),
            'group_types': sorted(df['group_type'].dropna().unique().tolist()),
            'snp_types': sorted(df['snp_type'].dropna().unique().tolist()),
            'states': df_states['state'].tolist(),
            'audit_id': audit_id
        }

    # =========================================================================
    # SUMMARY
    # =========================================================================

    def get_summary(
        self,
        year: Optional[int] = None,
        parent_orgs: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get risk score summary statistics.
        """
        filters = []

        if year:
            filters.append(f"r.year = {year}")
        if parent_orgs:
            parent_list = ", ".join([f"'{p}'" for p in parent_orgs])
            filters.append(f"r.parent_org IN ({parent_list})")
        if plan_types:
            plan_list = ", ".join([f"'{p}'" for p in plan_types])
            filters.append(f"r.plan_type IN ({plan_list})")
        if group_types:
            group_list = ", ".join([f"'{g}'" for g in group_types])
            filters.append(f"r.group_type IN ({group_list})")
        if snp_types:
            snp_list = ", ".join([f"'{t}'" for t in snp_types])
            filters.append(f"r.snp_type IN ({snp_list})")

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        if states:
            state_list = ", ".join([f"'{st}'" for st in states])
            sql = f"""
                WITH geo_parents AS (
                    SELECT DISTINCT parent_org
                    FROM fact_enrollment_by_geography
                    WHERE state IN ({state_list})
                )
                SELECT
                    r.year,
                    COUNT(DISTINCT r.contract_id) as contract_count,
                    COUNT(DISTINCT r.contract_id || '-' || r.plan_id) as plan_count,
                    SUM(r.enrollment) as total_enrollment,
                    ROUND(AVG(r.avg_risk_score), 4) as simple_avg_risk_score,
                    ROUND(SUM(r.avg_risk_score * r.enrollment) / NULLIF(SUM(r.enrollment), 0), 4) as wavg_risk_score,
                    ROUND(MIN(r.avg_risk_score), 4) as min_risk_score,
                    ROUND(MAX(r.avg_risk_score), 4) as max_risk_score,
                    ROUND(STDDEV(r.avg_risk_score), 4) as std_risk_score
                FROM fact_risk_scores_unified r
                INNER JOIN geo_parents g ON r.parent_org = g.parent_org
                {where_clause}
                GROUP BY r.year
                ORDER BY r.year DESC
            """
        else:
            sql = f"""
                SELECT
                    year,
                    COUNT(DISTINCT contract_id) as contract_count,
                    COUNT(DISTINCT contract_id || '-' || plan_id) as plan_count,
                    SUM(enrollment) as total_enrollment,
                    ROUND(AVG(avg_risk_score), 4) as simple_avg_risk_score,
                    ROUND(SUM(avg_risk_score * enrollment) / NULLIF(SUM(enrollment), 0), 4) as wavg_risk_score,
                    ROUND(MIN(avg_risk_score), 4) as min_risk_score,
                    ROUND(MAX(avg_risk_score), 4) as max_risk_score,
                    ROUND(STDDEV(avg_risk_score), 4) as std_risk_score
                FROM fact_risk_scores_unified r
                {where_clause}
                GROUP BY year
                ORDER BY year DESC
            """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_risk_summary"
        )

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'year': year,
                'parent_orgs': parent_orgs,
                'plan_types': plan_types,
                'group_types': group_types,
                'snp_types': snp_types,
                'states': states
            }
        })

    # =========================================================================
    # TIMESERIES
    # =========================================================================

    def get_timeseries(
        self,
        parent_orgs: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        metric: str = "wavg",  # "wavg" or "avg"
        include_industry_total: bool = True,
        group_by: Optional[str] = None,  # "plan_type", "snp_type", "group_type"
        user_id: str = "api"
    ) -> Dict:
        """
        Get risk score timeseries for charting.

        Uses pre-aggregated risk_scores_by_parent table for accurate industry totals.
        Uses risk_scores_by_parent_dims when filtering by plan_type/snp_type/group_type.

        NOTE: Risk score data covers ~57% of total MA enrollment (only contracts
        in CMS Plan Payment Data file). See 'data_coverage' in response.

        Metrics:
        - wavg: Enrollment-weighted average (recommended)
        - avg: Simple average

        group_by options:
        - None: One line per selected parent_org (or industry total)
        - "plan_type": Break down by HMO, PPO, etc.
        - "snp_type": Break down by SNP status
        - "group_type": Break down by Individual vs Group
        """
        # Normalize dimension values to match database
        # (handles cases where filter values differ from DB values)
        normalized_plan_types = self._normalize_plan_types(plan_types) if plan_types else None
        normalized_group_types = self._normalize_group_types(group_types) if group_types else None
        normalized_snp_types = self._normalize_snp_types(snp_types) if snp_types else None

        # Use dimensions table when filtering by plan_type, snp_type, or group_type
        has_dim_filters = normalized_plan_types or normalized_snp_types or normalized_group_types or group_by
        table_name = "risk_scores_by_parent_dims" if has_dim_filters else "risk_scores_by_parent"

        filters = []

        if normalized_plan_types:
            plan_list = ", ".join([f"'{p}'" for p in normalized_plan_types])
            filters.append(f"r.plan_type IN ({plan_list})")
        if normalized_group_types:
            group_list = ", ".join([f"'{g}'" for g in normalized_group_types])
            filters.append(f"r.group_type IN ({group_list})")
        if normalized_snp_types:
            snp_list = ", ".join([f"'{t}'" for t in normalized_snp_types])
            filters.append(f"r.snp_type IN ({snp_list})")

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""
        and_clause = "AND " + " AND ".join(filters) if filters else ""

        # Determine grouping column
        group_col = group_by if group_by in ['plan_type', 'snp_type', 'group_type'] else None

        if parent_orgs:
            parent_list = ", ".join([f"'{p}'" for p in parent_orgs])
            group_select = f", r.{group_col}" if group_col else ", r.parent_org"
            group_by_clause = f"r.year, r.{group_col}" if group_col else "r.year, r.parent_org"

            sql = f"""
                SELECT
                    r.year{group_select} as series_name,
                    SUM(r.enrollment) as enrollment,
                    ROUND(AVG(r.simple_avg_risk_score), 4) as avg_risk_score,
                    ROUND(SUM(r.wavg_risk_score * r.enrollment) / NULLIF(SUM(r.enrollment), 0), 4) as wavg_risk_score
                FROM {table_name} r
                WHERE r.parent_org IN ({parent_list})
                {and_clause}
                GROUP BY {group_by_clause}
                ORDER BY r.year
            """
        else:
            # Industry total or grouped
            group_select = f", r.{group_col}" if group_col else ", 'Industry Total' as"
            group_by_clause = f"r.year, r.{group_col}" if group_col else "r.year"

            sql = f"""
                SELECT
                    r.year{group_select} series_name,
                    SUM(r.enrollment) as enrollment,
                    ROUND(AVG(r.simple_avg_risk_score), 4) as avg_risk_score,
                    ROUND(SUM(r.wavg_risk_score * r.enrollment) / NULLIF(SUM(r.enrollment), 0), 4) as wavg_risk_score
                FROM {table_name} r
                {where_clause}
                GROUP BY {group_by_clause}
                ORDER BY r.year
            """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_risk_timeseries"
        )

        # Pivot to timeseries format
        years = sorted(df['year'].unique().tolist())
        series = {}
        enrollment = {}

        metric_col = 'wavg_risk_score' if metric == 'wavg' else 'avg_risk_score'

        for name in df['series_name'].unique():
            name_data = df[df['series_name'] == name].set_index('year')
            series[name] = [
                clean_nan(float(name_data.loc[y, metric_col])) if y in name_data.index and pd.notna(name_data.loc[y, metric_col]) else None
                for y in years
            ]
            enrollment[name] = [
                clean_nan(float(name_data.loc[y, 'enrollment'])) if y in name_data.index and pd.notna(name_data.loc[y, 'enrollment']) else None
                for y in years
            ]

        # Add industry total if requested and we have parent_orgs selected
        if include_industry_total and parent_orgs and not group_col:
            # Use same pre-aggregated table for industry total
            total_sql = f"""
                SELECT
                    year,
                    SUM(enrollment) as enrollment,
                    ROUND(SUM(wavg_risk_score * enrollment) / NULLIF(SUM(enrollment), 0), 4) as wavg_risk_score,
                    ROUND(AVG(simple_avg_risk_score), 4) as avg_risk_score
                FROM {table_name} r
                {where_clause}
                GROUP BY year
                ORDER BY year
            """

            df_total, _ = self.engine.query_with_audit(
                total_sql,
                user_id=user_id,
                context="get_risk_timeseries_total"
            )

            total_data = df_total.set_index('year')
            series['Industry Total'] = [
                clean_nan(float(total_data.loc[y, metric_col])) if y in total_data.index and pd.notna(total_data.loc[y, metric_col]) else None
                for y in years
            ]
            enrollment['Industry Total'] = [
                clean_nan(float(total_data.loc[y, 'enrollment'])) if y in total_data.index and pd.notna(total_data.loc[y, 'enrollment']) else None
                for y in years
            ]

        return clean_dict({
            'years': years,
            'series': series,
            'enrollment': enrollment,
            'metric': metric,
            'group_by': group_by,
            'audit_id': audit_id,
            'filters': {
                'parent_orgs': parent_orgs,
                'plan_types': plan_types,
                'group_types': group_types,
                'snp_types': snp_types,
                'states': states
            },
            'data_coverage': {
                'note': 'Risk scores from CMS Plan Payment Data cover ~57% of total MA enrollment',
                'reason': 'Only contracts in CMS payment file have published risk scores',
                'snp_note': 'SNP subtypes (D-SNP, C-SNP, I-SNP) from CMS SNP lookup file'
            }
        })

    # =========================================================================
    # BY PARENT ORG
    # =========================================================================

    def get_by_parent(
        self,
        year: int = 2024,
        plan_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        limit: int = 50,
        user_id: str = "api"
    ) -> Dict:
        """
        Get risk scores by parent organization.
        """
        filters = [f"r.year = {year}"]

        if plan_types:
            plan_list = ", ".join([f"'{p}'" for p in plan_types])
            filters.append(f"r.plan_type IN ({plan_list})")
        if group_types:
            group_list = ", ".join([f"'{g}'" for g in group_types])
            filters.append(f"r.group_type IN ({group_list})")
        if snp_types:
            snp_list = ", ".join([f"'{t}'" for t in snp_types])
            filters.append(f"r.snp_type IN ({snp_list})")

        where_clause = "WHERE " + " AND ".join(filters)

        if states:
            state_list = ", ".join([f"'{st}'" for st in states])
            sql = f"""
                WITH geo_parents AS (
                    SELECT DISTINCT parent_org
                    FROM fact_enrollment_by_geography
                    WHERE state IN ({state_list})
                )
                SELECT
                    r.parent_org,
                    SUM(r.enrollment) as total_enrollment,
                    COUNT(DISTINCT r.contract_id) as contract_count,
                    ROUND(AVG(r.avg_risk_score), 4) as simple_avg_risk_score,
                    ROUND(SUM(r.avg_risk_score * r.enrollment) / NULLIF(SUM(r.enrollment), 0), 4) as wavg_risk_score,
                    ROUND(MIN(r.avg_risk_score), 4) as min_risk_score,
                    ROUND(MAX(r.avg_risk_score), 4) as max_risk_score
                FROM fact_risk_scores_unified r
                INNER JOIN geo_parents g ON r.parent_org = g.parent_org
                {where_clause}
                GROUP BY r.parent_org
                ORDER BY total_enrollment DESC
                LIMIT {limit}
            """
        else:
            sql = f"""
                SELECT
                    parent_org,
                    SUM(enrollment) as total_enrollment,
                    COUNT(DISTINCT contract_id) as contract_count,
                    ROUND(AVG(avg_risk_score), 4) as simple_avg_risk_score,
                    ROUND(SUM(avg_risk_score * enrollment) / NULLIF(SUM(enrollment), 0), 4) as wavg_risk_score,
                    ROUND(MIN(avg_risk_score), 4) as min_risk_score,
                    ROUND(MAX(avg_risk_score), 4) as max_risk_score
                FROM fact_risk_scores_unified r
                {where_clause}
                GROUP BY parent_org
                ORDER BY total_enrollment DESC
                LIMIT {limit}
            """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_risk_by_parent"
        )

        return clean_dict({
            'year': year,
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'plan_types': plan_types,
                'group_types': group_types,
                'snp_types': snp_types,
                'states': states
            }
        })

    # =========================================================================
    # BY STATE
    # =========================================================================

    def get_by_state(
        self,
        year: int = 2024,
        plan_types: Optional[List[str]] = None,
        parent_orgs: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get risk scores by state.
        Joins risk scores with geographic enrollment.
        """
        filters = [f"r.year = {year}"]

        if plan_types:
            plan_list = ", ".join([f"'{p}'" for p in plan_types])
            filters.append(f"r.plan_type IN ({plan_list})")
        if parent_orgs:
            parent_list = ", ".join([f"'{p}'" for p in parent_orgs])
            filters.append(f"r.parent_org IN ({parent_list})")

        where_clause = "WHERE " + " AND ".join(filters)

        # Join with geography to get state
        sql = f"""
            WITH parent_risk AS (
                SELECT
                    parent_org,
                    SUM(enrollment) as enrollment,
                    SUM(avg_risk_score * enrollment) as weighted_score
                FROM fact_risk_scores_unified r
                {where_clause}
                GROUP BY parent_org
            ),
            state_enrollment AS (
                SELECT
                    state,
                    parent_org,
                    SUM(enrollment) as state_enrollment
                FROM fact_enrollment_by_geography
                WHERE year = {year}
                GROUP BY state, parent_org
            )
            SELECT
                se.state,
                SUM(se.state_enrollment) as total_enrollment,
                COUNT(DISTINCT se.parent_org) as parent_count,
                ROUND(SUM(pr.weighted_score * se.state_enrollment / pr.enrollment) / NULLIF(SUM(se.state_enrollment), 0), 4) as wavg_risk_score
            FROM state_enrollment se
            INNER JOIN parent_risk pr ON se.parent_org = pr.parent_org
            GROUP BY se.state
            ORDER BY total_enrollment DESC
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_risk_by_state"
        )

        return clean_dict({
            'year': year,
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'plan_types': plan_types,
                'parent_orgs': parent_orgs
            }
        })

    # =========================================================================
    # BY DIMENSIONS
    # =========================================================================

    def get_by_dimensions(
        self,
        year: int = 2024,
        parent_orgs: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get risk scores broken down by all dimensions.
        Shows plan_type x snp_type x group_type breakdown.
        """
        filters = [f"r.year = {year}"]

        if parent_orgs:
            parent_list = ", ".join([f"'{p}'" for p in parent_orgs])
            filters.append(f"r.parent_org IN ({parent_list})")

        where_clause = "WHERE " + " AND ".join(filters)

        if states:
            state_list = ", ".join([f"'{st}'" for st in states])
            sql = f"""
                WITH geo_parents AS (
                    SELECT DISTINCT parent_org
                    FROM fact_enrollment_by_geography
                    WHERE state IN ({state_list})
                )
                SELECT
                    r.plan_type,
                    r.snp_type,
                    r.group_type,
                    SUM(r.enrollment) as enrollment,
                    COUNT(DISTINCT r.contract_id) as contract_count,
                    ROUND(AVG(r.avg_risk_score), 4) as simple_avg,
                    ROUND(SUM(r.avg_risk_score * r.enrollment) / NULLIF(SUM(r.enrollment), 0), 4) as wavg
                FROM fact_risk_scores_unified r
                INNER JOIN geo_parents g ON r.parent_org = g.parent_org
                {where_clause}
                GROUP BY r.plan_type, r.snp_type, r.group_type
                ORDER BY enrollment DESC
            """
        else:
            sql = f"""
                SELECT
                    plan_type,
                    snp_type,
                    group_type,
                    SUM(enrollment) as enrollment,
                    COUNT(DISTINCT contract_id) as contract_count,
                    ROUND(AVG(avg_risk_score), 4) as simple_avg,
                    ROUND(SUM(avg_risk_score * enrollment) / NULLIF(SUM(enrollment), 0), 4) as wavg
                FROM fact_risk_scores_unified r
                {where_clause}
                GROUP BY plan_type, snp_type, group_type
                ORDER BY enrollment DESC
            """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_risk_by_dimensions"
        )

        return clean_dict({
            'year': year,
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'parent_orgs': parent_orgs,
                'states': states
            }
        })

    # =========================================================================
    # DISTRIBUTION
    # =========================================================================

    def get_distribution(
        self,
        year: int = 2024,
        parent_orgs: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        bins: int = 20,
        user_id: str = "api"
    ) -> Dict:
        """
        Get risk score distribution (histogram data).
        """
        filters = [f"year = {year}"]

        if parent_orgs:
            parent_list = ", ".join([f"'{p}'" for p in parent_orgs])
            filters.append(f"parent_org IN ({parent_list})")
        if plan_types:
            plan_list = ", ".join([f"'{p}'" for p in plan_types])
            filters.append(f"plan_type IN ({plan_list})")

        where_clause = "WHERE " + " AND ".join(filters)

        # Get min/max for binning
        sql_range = f"""
            SELECT
                MIN(avg_risk_score) as min_score,
                MAX(avg_risk_score) as max_score
            FROM fact_risk_scores_unified
            {where_clause}
        """

        df_range, _ = self.engine.query_with_audit(
            sql_range,
            user_id=user_id,
            context="get_risk_range"
        )

        min_score = df_range['min_score'].iloc[0]
        max_score = df_range['max_score'].iloc[0]
        bin_width = (max_score - min_score) / bins

        # Get distribution
        sql = f"""
            SELECT
                FLOOR((avg_risk_score - {min_score}) / {bin_width}) * {bin_width} + {min_score} as bin_start,
                FLOOR((avg_risk_score - {min_score}) / {bin_width}) * {bin_width} + {min_score} + {bin_width} as bin_end,
                COUNT(*) as plan_count,
                SUM(enrollment) as enrollment
            FROM fact_risk_scores_unified
            {where_clause}
            GROUP BY FLOOR((avg_risk_score - {min_score}) / {bin_width})
            ORDER BY bin_start
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_risk_distribution"
        )

        return clean_dict({
            'year': year,
            'min_score': clean_nan(float(min_score)) if min_score is not None else None,
            'max_score': clean_nan(float(max_score)) if max_score is not None else None,
            'bin_width': clean_nan(float(bin_width)) if bin_width is not None else None,
            'distribution': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'parent_orgs': parent_orgs,
                'plan_types': plan_types
            }
        })

    # =========================================================================
    # PLAN DETAIL
    # =========================================================================

    def get_plan_detail(
        self,
        contract_id: str,
        plan_id: str,
        user_id: str = "api"
    ) -> Dict:
        """
        Get risk score history for a specific plan.
        """
        sql = f"""
            SELECT
                year,
                contract_id,
                plan_id,
                contract_name,
                parent_org,
                plan_type,
                group_type,
                snp_type,
                avg_risk_score,
                enrollment
            FROM fact_risk_scores_unified
            WHERE contract_id = '{contract_id}'
              AND plan_id = '{plan_id}'
            ORDER BY year
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_plan_risk_detail"
        )

        return clean_dict({
            'contract_id': contract_id,
            'plan_id': plan_id,
            'history': df.to_dict(orient='records'),
            'audit_id': audit_id
        })

    # =========================================================================
    # CONTRACT DETAILS (FOR AUDITING)
    # =========================================================================

    def get_contract_details(
        self,
        year: int,
        parent_org: Optional[str] = None,
        plan_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get contract-level details for auditing weighted average calculations.

        Returns all contracts with their risk scores and enrollment for the given
        year and filters, allowing users to verify calculations.
        """
        filters = [f"year = {year}"]

        if parent_org:
            filters.append(f"parent_org = '{parent_org}'")
        if plan_types:
            normalized = self._normalize_plan_types(plan_types)
            plan_list = ", ".join([f"'{p}'" for p in normalized])
            filters.append(f"plan_type IN ({plan_list})")
        if group_types:
            group_list = ", ".join([f"'{g}'" for g in group_types])
            filters.append(f"group_type IN ({group_list})")
        if snp_types:
            normalized = self._normalize_snp_types(snp_types)
            snp_list = ", ".join([f"'{t}'" for t in normalized])
            filters.append(f"snp_type IN ({snp_list})")

        where_clause = "WHERE " + " AND ".join(filters)

        sql = f"""
            SELECT
                contract_id,
                plan_id,
                contract_name,
                parent_org,
                plan_type,
                group_type,
                snp_type,
                ROUND(avg_risk_score, 4) as risk_score,
                COALESCE(enrollment, 0) as enrollment,
                ROUND(COALESCE(avg_risk_score * enrollment, 0), 2) as weighted_score
            FROM fact_risk_scores_unified
            {where_clause}
            ORDER BY enrollment DESC NULLS LAST
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_contract_details"
        )

        # Calculate summary stats
        total_enrollment = df['enrollment'].sum()
        total_weighted = df['weighted_score'].sum()
        wavg = round(total_weighted / total_enrollment, 4) if total_enrollment > 0 else None
        simple_avg = round(df['risk_score'].mean(), 4) if len(df) > 0 else None

        return clean_dict({
            'year': year,
            'parent_org': parent_org,
            'contracts': df.to_dict(orient='records'),
            'summary': {
                'contract_count': len(df),
                'total_enrollment': total_enrollment,
                'weighted_avg': wavg,
                'simple_avg': simple_avg,
                'total_weighted_score': total_weighted
            },
            'audit_id': audit_id,
            'filters': {
                'plan_types': plan_types,
                'group_types': group_types,
                'snp_types': snp_types
            }
        })

    # =========================================================================
    # LINEAGE
    # =========================================================================

    def trace_lineage(self, audit_id: str) -> Dict:
        """Trace data lineage for a previous query."""
        return self.engine.trace_query_lineage(audit_id)


# Singleton instance
_service_instance = None

def get_risk_scores_service() -> RiskScoresService:
    """Get or create singleton risk scores service."""
    global _service_instance
    if _service_instance is None:
        _service_instance = RiskScoresService()
    return _service_instance
