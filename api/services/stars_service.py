"""
Stars Service - V3

Provides star rating data queries using the unified data layer.
Supports ALL filters: parent_org, plan_type, group_type, snp_type, state, county, measure, domain, etc.

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


def clean_dict(d):
    """Recursively clean NaN values from dict."""
    if isinstance(d, dict):
        return {k: clean_dict(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [clean_dict(item) for item in d]
    else:
        return clean_nan(d)


class StarsService:
    """
    Service for star rating queries.

    Supports comprehensive filtering:
    - parent_org: Filter by parent organization
    - plan_type: HMO/HMOPOS, Local PPO, Regional PPO, PFFS, 1876 Cost
    - group_type: Individual, Group
    - snp_type: Non-SNP, SNP
    - state: Filter by state (via geographic join)
    - star_year: 2015-2026
    - star_band: 2.0-5.0
    - measure_id: Filter by specific measure
    - domain: HD1-HD5 (Part C), DD1-DD4 (Part D)
    - part: C (medical), D (drug)
    """

    def __init__(self):
        self.engine = get_engine()

    # =========================================================================
    # FILTER OPTIONS
    # =========================================================================

    def get_filters(self, user_id: str = "api") -> Dict:
        """
        Get all available filter options for stars data.
        """
        # Get star-level filters
        sql_stars = """
            SELECT DISTINCT
                parent_org,
                plan_type,
                group_type,
                snp_type,
                star_year
            FROM stars_enrollment_unified
        """

        df_stars, audit_id1 = self.engine.query_with_audit(
            sql_stars,
            user_id=user_id,
            context="get_stars_filters"
        )

        # Get measure filters
        sql_measures = """
            SELECT DISTINCT
                measure_id,
                domain,
                part,
                data_source
            FROM stars_measure_specs
        """

        df_measures, audit_id2 = self.engine.query_with_audit(
            sql_measures,
            user_id=user_id,
            context="get_measure_filters"
        )

        # Get state filters from geography
        sql_states = """
            SELECT DISTINCT state
            FROM fact_enrollment_by_geography
            ORDER BY state
        """

        df_states, audit_id3 = self.engine.query_with_audit(
            sql_states,
            user_id=user_id,
            context="get_state_filters"
        )

        return clean_dict({
            'parent_orgs': sorted(df_stars['parent_org'].dropna().unique().tolist()),
            'plan_types': sorted(df_stars['plan_type'].dropna().unique().tolist()),
            'group_types': sorted(df_stars['group_type'].dropna().unique().tolist()),
            'snp_types': sorted(df_stars['snp_type'].dropna().unique().tolist()),
            'star_years': sorted(df_stars['star_year'].unique().tolist()),
            'states': df_states['state'].tolist(),
            'measure_ids': sorted(df_measures['measure_id'].dropna().unique().tolist()),
            'domains': sorted(df_measures['domain'].dropna().unique().tolist()),
            'parts': sorted(df_measures['part'].dropna().unique().tolist()),
            'data_sources': sorted(df_measures['data_source'].dropna().unique().tolist()),
            'audit_ids': [audit_id1, audit_id2, audit_id3]
        })

    # =========================================================================
    # DISTRIBUTION / 4+ STAR ANALYSIS
    # =========================================================================

    def get_distribution(
        self,
        parent_orgs: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        star_year: Optional[int] = None,
        include_industry_total: bool = True,
        data_source: str = "national",
        user_id: str = "api"
    ) -> Dict:
        """
        Get star rating distribution with enrollment weighting.

        Returns % of enrollment in each star band (1-5 stars).
        Supports geographic filtering via state join.
        
        Args:
            data_source: "national" uses ALL MA enrollment (correct 4+ star %)
                        "rated" uses only contracts with star ratings (legacy)
        """
        # Select table based on data source
        # national = ALL MA contracts (left join with stars) - correct percentages
        # rated = only contracts with star ratings - legacy behavior
        table = "stars_enrollment_national" if data_source == "national" else "stars_enrollment_unified"
        
        filters = []

        if star_year:
            filters.append(f"s.star_year = {star_year}")
        if plan_types:
            plan_list = ", ".join([f"'{p}'" for p in plan_types])
            filters.append(f"s.plan_type IN ({plan_list})")
        if group_types:
            group_list = ", ".join([f"'{g}'" for g in group_types])
            filters.append(f"s.group_type IN ({group_list})")
        if snp_types:
            snp_list = ", ".join([f"'{t}'" for t in snp_types])
            filters.append(f"s.snp_type IN ({snp_list})")

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        # If state filter, we need to join with geography (only for geographic source)
        if states and data_source != "national":
            state_list = ", ".join([f"'{st}'" for st in states])
            sql = f"""
                WITH geo_parents AS (
                    SELECT DISTINCT parent_org
                    FROM fact_enrollment_by_geography
                    WHERE state IN ({state_list})
                )
                SELECT
                    s.star_year,
                    s.parent_org,
                    SUM(s.enrollment) as enrollment,
                    SUM(CASE WHEN s.is_fourplus THEN s.enrollment ELSE 0 END) as fourplus_enrollment,
                    COUNT(DISTINCT s.contract_id) as contract_count
                FROM {table} s
                INNER JOIN geo_parents g ON s.parent_org = g.parent_org
                {where_clause}
                GROUP BY s.star_year, s.parent_org
                ORDER BY s.star_year, enrollment DESC
            """
        elif parent_orgs:
            parent_list = ", ".join([f"'{p}'" for p in parent_orgs])
            sql = f"""
                SELECT
                    star_year,
                    parent_org,
                    SUM(enrollment) as enrollment,
                    SUM(CASE WHEN is_fourplus THEN enrollment ELSE 0 END) as fourplus_enrollment,
                    COUNT(DISTINCT contract_id) as contract_count
                FROM {table} s
                {where_clause}
                {"AND" if where_clause else "WHERE"} parent_org IN ({parent_list})
                GROUP BY star_year, parent_org
                ORDER BY star_year, enrollment DESC
            """
        else:
            # Industry total
            sql = f"""
                SELECT
                    star_year,
                    'Industry Total' as parent_org,
                    SUM(enrollment) as enrollment,
                    SUM(CASE WHEN is_fourplus THEN enrollment ELSE 0 END) as fourplus_enrollment,
                    COUNT(DISTINCT contract_id) as contract_count
                FROM {table} s
                {where_clause}
                GROUP BY star_year
                ORDER BY star_year
            """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_stars_distribution"
        )

        # Calculate percentages
        df['fourplus_pct'] = (df['fourplus_enrollment'] / df['enrollment'] * 100).round(2)

        # Pivot to timeseries format
        years = sorted(df['star_year'].unique().tolist())
        series = {}

        for parent in df['parent_org'].unique():
            parent_data = df[df['parent_org'] == parent].set_index('star_year')
            series[parent] = [
                clean_nan(float(parent_data.loc[y, 'fourplus_pct'])) if y in parent_data.index and pd.notna(parent_data.loc[y, 'fourplus_pct']) else None
                for y in years
            ]

        return clean_dict({
            'years': years,
            'series': series,
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'data_source': data_source,
            'filters': {
                'parent_orgs': parent_orgs,
                'plan_types': plan_types,
                'group_types': group_types,
                'snp_types': snp_types,
                'states': states,
                'star_year': star_year,
                'data_source': data_source
            }
        })

    # =========================================================================
    # BY PARENT ORG
    # =========================================================================

    def get_by_parent(
        self,
        star_year: int = 2026,
        plan_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        limit: int = 50,
        user_id: str = "api"
    ) -> Dict:
        """
        Get star ratings by parent organization.
        """
        filters = [f"s.star_year = {star_year}"]

        if plan_types:
            plan_list = ", ".join([f"'{p}'" for p in plan_types])
            filters.append(f"s.plan_type IN ({plan_list})")
        if group_types:
            group_list = ", ".join([f"'{g}'" for g in group_types])
            filters.append(f"s.group_type IN ({group_list})")
        if snp_types:
            snp_list = ", ".join([f"'{t}'" for t in snp_types])
            filters.append(f"s.snp_type IN ({snp_list})")

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
                    s.parent_org,
                    SUM(s.enrollment) as total_enrollment,
                    SUM(CASE WHEN s.is_fourplus THEN s.enrollment ELSE 0 END) as fourplus_enrollment,
                    ROUND(SUM(s.enrollment * s.overall_rating) / SUM(s.enrollment), 2) as wavg_rating,
                    COUNT(DISTINCT s.contract_id) as contract_count
                FROM stars_enrollment_unified s
                INNER JOIN geo_parents g ON s.parent_org = g.parent_org
                {where_clause}
                GROUP BY s.parent_org
                ORDER BY total_enrollment DESC
                LIMIT {limit}
            """
        else:
            sql = f"""
                SELECT
                    parent_org,
                    SUM(enrollment) as total_enrollment,
                    SUM(CASE WHEN is_fourplus THEN enrollment ELSE 0 END) as fourplus_enrollment,
                    ROUND(SUM(enrollment * overall_rating) / SUM(enrollment), 2) as wavg_rating,
                    COUNT(DISTINCT contract_id) as contract_count
                FROM stars_enrollment_unified s
                {where_clause}
                GROUP BY parent_org
                ORDER BY total_enrollment DESC
                LIMIT {limit}
            """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_stars_by_parent"
        )

        df['fourplus_pct'] = (df['fourplus_enrollment'] / df['total_enrollment'].replace(0, float('nan')) * 100).round(2)

        return clean_dict({
            'star_year': star_year,
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
        star_year: int = 2026,
        plan_types: Optional[List[str]] = None,
        parent_orgs: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get star ratings by state.
        Joins stars with geographic enrollment.
        """
        filters = [f"g.year = {star_year}"]

        if plan_types:
            plan_list = ", ".join([f"'{p}'" for p in plan_types])
            filters.append(f"g.plan_type IN ({plan_list})")
        if parent_orgs:
            parent_list = ", ".join([f"'{p}'" for p in parent_orgs])
            filters.append(f"g.parent_org IN ({parent_list})")

        where_clause = "WHERE " + " AND ".join(filters)

        sql = f"""
            WITH state_enrollment AS (
                SELECT
                    g.state,
                    g.parent_org,
                    SUM(g.enrollment) as enrollment
                FROM fact_enrollment_by_geography g
                {where_clause}
                GROUP BY g.state, g.parent_org
            ),
            parent_stars AS (
                SELECT
                    parent_org,
                    overall_rating,
                    is_fourplus
                FROM stars_enrollment_unified
                WHERE star_year = {star_year}
            )
            SELECT
                se.state,
                SUM(se.enrollment) as total_enrollment,
                SUM(CASE WHEN ps.is_fourplus THEN se.enrollment ELSE 0 END) as fourplus_enrollment,
                ROUND(SUM(se.enrollment * ps.overall_rating) / NULLIF(SUM(se.enrollment), 0), 2) as wavg_rating,
                COUNT(DISTINCT se.parent_org) as parent_count
            FROM state_enrollment se
            LEFT JOIN parent_stars ps ON se.parent_org = ps.parent_org
            GROUP BY se.state
            ORDER BY total_enrollment DESC
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_stars_by_state"
        )

        df['fourplus_pct'] = (df['fourplus_enrollment'] / df['total_enrollment'].replace(0, float('nan')) * 100).round(2)

        return clean_dict({
            'star_year': star_year,
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'plan_types': plan_types,
                'parent_orgs': parent_orgs
            }
        })

    # =========================================================================
    # MEASURES
    # =========================================================================

    def get_measure_performance(
        self,
        year: int = 2026,
        parent_orgs: Optional[List[str]] = None,
        measure_ids: Optional[List[str]] = None,
        domains: Optional[List[str]] = None,
        parts: Optional[List[str]] = None,
        data_sources: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get measure-level performance data.

        Filters:
        - measure_ids: Specific measures
        - domains: HD1-HD5 (Part C), DD1-DD4 (Part D)
        - parts: C (medical), D (drug)
        - data_sources: HEDIS, CAHPS, HOS, Admin
        """
        filters = [f"m.year = {year}"]

        if parent_orgs:
            # Need to join with stars to filter by parent
            parent_list = ", ".join([f"'{p}'" for p in parent_orgs])
            filters.append(f"s.parent_org IN ({parent_list})")

        if measure_ids:
            measure_list = ", ".join([f"'{m}'" for m in measure_ids])
            filters.append(f"m.measure_id IN ({measure_list})")

        where_clause = "WHERE " + " AND ".join(filters)

        # Join with specs for domain/part filtering
        spec_filters = []
        if domains:
            domain_list = ", ".join([f"'{d}'" for d in domains])
            spec_filters.append(f"sp.domain IN ({domain_list})")
        if parts:
            part_list = ", ".join([f"'{p}'" for p in parts])
            spec_filters.append(f"sp.part IN ({part_list})")
        if data_sources:
            source_list = ", ".join([f"'{s}'" for s in data_sources])
            spec_filters.append(f"sp.data_source IN ({source_list})")

        spec_where = " AND " + " AND ".join(spec_filters) if spec_filters else ""

        if parent_orgs:
            sql = f"""
                SELECT
                    m.measure_id,
                    m.measure_name,
                    sp.domain,
                    sp.part,
                    sp.weight,
                    sp.data_source,
                    AVG(m.star_rating) as avg_rating,
                    COUNT(DISTINCT m.contract_id) as contract_count
                FROM measure_data m
                INNER JOIN stars_summary s ON m.contract_id = s.contract_id AND m.year = s.rating_year
                LEFT JOIN stars_measure_specs sp ON m.measure_id = sp.measure_id
                {where_clause}
                {spec_where}
                GROUP BY m.measure_id, m.measure_name, sp.domain, sp.part, sp.weight, sp.data_source
                ORDER BY sp.domain, m.measure_id
            """
        else:
            sql = f"""
                SELECT
                    m.measure_id,
                    m.measure_name,
                    sp.domain,
                    sp.part,
                    sp.weight,
                    sp.data_source,
                    AVG(m.star_rating) as avg_rating,
                    COUNT(DISTINCT m.contract_id) as contract_count
                FROM measure_data m
                LEFT JOIN stars_measure_specs sp ON m.measure_id = sp.measure_id
                {where_clause}
                {spec_where}
                GROUP BY m.measure_id, m.measure_name, sp.domain, sp.part, sp.weight, sp.data_source
                ORDER BY sp.domain, m.measure_id
            """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_measure_performance"
        )

        return clean_dict({
            'year': year,
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'parent_orgs': parent_orgs,
                'measure_ids': measure_ids,
                'domains': domains,
                'parts': parts,
                'data_sources': data_sources
            }
        })

    # =========================================================================
    # CUTPOINTS
    # =========================================================================

    def get_cutpoints(
        self,
        years: Optional[List[int]] = None,
        measure_ids: Optional[List[str]] = None,
        parts: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get star cutpoint thresholds by measure and year.
        """
        filters = []

        if years:
            year_list = ", ".join([str(y) for y in years])
            filters.append(f"year IN ({year_list})")
        if measure_ids:
            measure_list = ", ".join([f"'{m}'" for m in measure_ids])
            filters.append(f"measure_id IN ({measure_list})")
        if parts:
            part_list = ", ".join([f"'{p}'" for p in parts])
            filters.append(f"part IN ({part_list})")

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        sql = f"""
            SELECT
                year,
                part,
                measure_id,
                measure_name,
                star_rating,
                threshold
            FROM stars_cutpoints_2014_2026
            {where_clause}
            ORDER BY year, part, measure_id, star_rating
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_cutpoints"
        )

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'years': years,
                'measure_ids': measure_ids,
                'parts': parts
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
        metric: str = "fourplus_pct",  # or "wavg_rating"
        include_industry_total: bool = True,
        data_source: str = "national",
        user_id: str = "api"
    ) -> Dict:
        """
        Get star rating timeseries for charting.

        Metrics:
        - fourplus_pct: % of enrollment in 4+ star plans
        - wavg_rating: Enrollment-weighted average rating
        
        Args:
            data_source: "national" uses ALL MA enrollment (correct 4+ star %)
                        "rated" uses only contracts with star ratings
        """
        return self.get_distribution(
            parent_orgs=parent_orgs,
            plan_types=plan_types,
            group_types=group_types,
            snp_types=snp_types,
            states=states,
            include_industry_total=include_industry_total,
            data_source=data_source,
            user_id=user_id
        )

    # =========================================================================
    # CONTRACT DETAIL
    # =========================================================================

    def get_contract_detail(
        self,
        contract_id: str,
        year: int = 2026,
        user_id: str = "api"
    ) -> Dict:
        """
        Get detailed star rating info for a specific contract.
        """
        # Get contract summary
        sql_summary = f"""
            SELECT
                contract_id,
                parent_org,
                plan_type,
                group_type,
                snp_type,
                enrollment,
                overall_rating,
                star_band,
                is_fourplus
            FROM stars_enrollment_unified
            WHERE contract_id = '{contract_id}'
              AND star_year = {year}
        """

        df_summary, audit_id1 = self.engine.query_with_audit(
            sql_summary,
            user_id=user_id,
            context="get_contract_summary"
        )

        # Get measure-level detail
        sql_measures = f"""
            SELECT
                measure_id,
                measure_name,
                star_rating,
                raw_value
            FROM measure_data
            WHERE contract_id = '{contract_id}'
              AND year = {year}
            ORDER BY measure_id
        """

        df_measures, audit_id2 = self.engine.query_with_audit(
            sql_measures,
            user_id=user_id,
            context="get_contract_measures"
        )

        return clean_dict({
            'contract_id': contract_id,
            'year': year,
            'summary': df_summary.to_dict(orient='records')[0] if len(df_summary) > 0 else None,
            'measures': df_measures.to_dict(orient='records'),
            'audit_ids': [audit_id1, audit_id2]
        })

    # =========================================================================
    # LINEAGE
    # =========================================================================

    def trace_lineage(self, audit_id: str) -> Dict:
        """Trace data lineage for a previous query."""
        return self.engine.trace_query_lineage(audit_id)


# Singleton instance
_service_instance = None

def get_stars_service() -> StarsService:
    """Get or create singleton stars service."""
    global _service_instance
    if _service_instance is None:
        _service_instance = StarsService()
    return _service_instance
