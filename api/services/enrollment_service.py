"""
Enrollment Service

Provides enrollment data queries using the unified data layer.
All queries are audited for lineage tracking.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import sys
import os

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db import get_engine


class EnrollmentService:
    """Service for enrollment-related queries."""

    def __init__(self):
        self.engine = get_engine()

    def get_industry_totals(
        self,
        year: Optional[int] = None,
        month: int = 1,
        user_id: str = "api"
    ) -> Dict:
        """
        Get industry-level enrollment totals.

        Uses pre-computed agg_industry_totals for speed.
        """
        sql = """
            SELECT *
            FROM agg_industry_totals
            WHERE month = {month}
            {year_filter}
            ORDER BY year DESC
        """.format(
            month=month,
            year_filter=f"AND year = {year}" if year else ""
        )

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_industry_totals"
        )

        return {
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'row_count': len(df)
        }

    def get_by_parent_org(
        self,
        year: int,
        month: int = 1,
        limit: int = 20,
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment by parent organization.

        Uses pre-computed agg_by_parent_year for speed.
        """
        sql = f"""
            SELECT
                parent_org,
                total_enrollment,
                plan_count,
                contract_count,
                market_share,
                pct_hmo,
                pct_ppo,
                pct_dsnp,
                pct_mapd,
                pct_group
            FROM agg_by_parent_year
            WHERE year = {year} AND month = {month}
            ORDER BY total_enrollment DESC
            LIMIT {limit}
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_by_parent_org"
        )

        return {
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'year': year,
            'month': month
        }

    def get_by_state(
        self,
        year: int,
        month: int = 1,
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment by state.

        Uses pre-computed agg_by_state_year.
        Note: State totals may be ~1-3% lower than national due to suppression.
        """
        sql = f"""
            SELECT
                state,
                total_enrollment,
                enrollment_estimated,
                plan_count,
                contract_count,
                county_count,
                suppression_rate,
                pct_of_national
            FROM agg_by_state_year
            WHERE year = {year} AND month = {month}
            ORDER BY total_enrollment DESC
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_by_state"
        )

        return {
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'year': year,
            'month': month,
            'note': 'State totals may be ~1-3% lower than national due to HIPAA suppression'
        }

    def get_by_dimensions(
        self,
        year: int,
        month: int = 1,
        plan_type: Optional[str] = None,
        product_type: Optional[str] = None,
        group_type: Optional[str] = None,
        snp_type: Optional[str] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment by dimension combinations.

        Supports any combination of filters.
        """
        filters = [f"year = {year}", f"month = {month}"]

        if plan_type:
            filters.append(f"plan_type_simplified = '{plan_type}'")
        if product_type:
            filters.append(f"product_type = '{product_type}'")
        if group_type:
            filters.append(f"group_type = '{group_type}'")
        if snp_type:
            filters.append(f"snp_type = '{snp_type}'")

        sql = f"""
            SELECT
                plan_type_simplified,
                product_type,
                group_type,
                snp_type,
                enrollment,
                plan_count,
                pct_of_total
            FROM agg_by_dimensions
            WHERE {' AND '.join(filters)}
            ORDER BY enrollment DESC
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_by_dimensions"
        )

        return {
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'year': year,
                'month': month,
                'plan_type': plan_type,
                'product_type': product_type,
                'group_type': group_type,
                'snp_type': snp_type
            }
        }

    def get_timeseries(
        self,
        parent_org: Optional[str] = None,
        state: Optional[str] = None,
        plan_type: Optional[str] = None,
        product_type: Optional[str] = None,
        group_type: Optional[str] = None,
        snp_type: Optional[str] = None,
        start_year: int = 2015,
        end_year: int = 2026,
        month: int = 1,
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment timeseries with optional filters.

        Returns year-over-year data for charting.
        """
        # Choose the right table based on filters
        if state:
            # Use geographic table for state filter
            filters = [
                f"year >= {start_year}",
                f"year <= {end_year}",
                f"month = {month}",
                f"state = '{state}'"
            ]

            sql = f"""
                SELECT
                    year,
                    SUM(enrollment) as enrollment,
                    COUNT(DISTINCT plan_id) as plan_count
                FROM fact_enrollment_geographic
                WHERE {' AND '.join(filters)}
                GROUP BY year
                ORDER BY year
            """
        elif parent_org:
            # Use parent aggregation
            sql = f"""
                SELECT
                    year,
                    total_enrollment as enrollment,
                    plan_count,
                    market_share
                FROM agg_by_parent_year
                WHERE parent_org LIKE '%{parent_org}%'
                  AND year >= {start_year}
                  AND year <= {end_year}
                  AND month = {month}
                ORDER BY year
            """
        else:
            # Use unified fact with dimension filters
            filters = [
                f"year >= {start_year}",
                f"year <= {end_year}",
                f"month = {month}"
            ]

            if plan_type:
                filters.append(f"plan_type_simplified = '{plan_type}'")
            if product_type:
                filters.append(f"product_type = '{product_type}'")
            if group_type:
                filters.append(f"group_type = '{group_type}'")
            if snp_type:
                filters.append(f"snp_type = '{snp_type}'")

            sql = f"""
                SELECT
                    year,
                    SUM(enrollment) as enrollment,
                    COUNT(DISTINCT contract_id || '-' || plan_id) as plan_count,
                    COUNT(DISTINCT parent_org) as parent_org_count
                FROM fact_enrollment_unified
                WHERE {' AND '.join(filters)}
                GROUP BY year
                ORDER BY year
            """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_timeseries"
        )

        # Calculate YoY growth
        if len(df) > 1:
            df['enrollment_prev'] = df['enrollment'].shift(1)
            df['yoy_growth'] = ((df['enrollment'] - df['enrollment_prev']) / df['enrollment_prev'] * 100).round(2)
            df = df.drop(columns=['enrollment_prev'])

        return {
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'parent_org': parent_org,
                'state': state,
                'plan_type': plan_type,
                'product_type': product_type,
                'group_type': group_type,
                'snp_type': snp_type,
                'start_year': start_year,
                'end_year': end_year,
                'month': month
            }
        }

    def get_plan_details(
        self,
        contract_id: str,
        plan_id: str,
        year: int,
        user_id: str = "api"
    ) -> Dict:
        """
        Get detailed information for a specific plan.

        Joins across all tables to get complete picture.
        """
        sql = f"""
            SELECT
                u.contract_id,
                u.plan_id,
                u.year,
                u.month,
                u.parent_org,
                u.plan_type_simplified,
                u.product_type,
                u.group_type,
                u.group_type_confidence,
                u.snp_type,
                u.enrollment,
                e.entity_id,
                e.first_year,
                e.last_year,
                s.overall_rating as star_rating,
                s.part_c_rating,
                s.part_d_rating,
                r.avg_risk_score
            FROM fact_enrollment_unified u
            LEFT JOIN dim_entity e
                ON u.contract_id = e.current_contract_id
                AND u.plan_id = e.current_plan_id
            LEFT JOIN fact_star_ratings s
                ON u.contract_id = s.contract_id
                AND u.year = s.star_year
            LEFT JOIN fact_risk_scores r
                ON u.contract_id = r.contract_id
                AND u.plan_id = r.plan_id
                AND u.year = r.year
            WHERE u.contract_id = '{contract_id}'
              AND u.plan_id = '{plan_id}'
              AND u.year = {year}
              AND u.month = 1
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_plan_details"
        )

        if len(df) == 0:
            return {'error': 'Plan not found', 'audit_id': audit_id}

        return {
            'data': df.to_dict(orient='records')[0],
            'audit_id': audit_id
        }

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
