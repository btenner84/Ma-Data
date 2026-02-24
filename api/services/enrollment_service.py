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
        limit: int = 20,
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment by parent organization.
        """
        sql = f"""
            SELECT
                parent_org,
                SUM(enrollment) as total_enrollment,
                COUNT(DISTINCT contract_id || '-' || plan_id) as plan_count,
                COUNT(DISTINCT contract_id) as contract_count
            FROM fact_enrollment_unified
            WHERE year = {year}
            GROUP BY parent_org
            ORDER BY total_enrollment DESC
            LIMIT {limit}
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_by_parent_org"
        )

        # Calculate market share
        total = df['total_enrollment'].sum()
        df['market_share'] = (df['total_enrollment'] / total * 100).round(2)

        return {
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'year': year,
            'total_enrollment': int(total)
        }

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
                COUNT(DISTINCT contract_id || '-' || plan_id) as plan_count,
                COUNT(DISTINCT contract_id) as contract_count,
                COUNT(DISTINCT county) as county_count
            FROM fact_enrollment_by_geography
            WHERE year = {year}
            GROUP BY state
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
            'note': 'State totals may be ~1-3% lower than national due to HIPAA suppression'
        }

    def get_by_dimensions(
        self,
        year: int,
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
        filters = [f"year = {year}"]

        if plan_type:
            filters.append(f"plan_type = '{plan_type}'")
        if product_type:
            filters.append(f"product_type = '{product_type}'")
        if group_type:
            filters.append(f"group_type = '{group_type}'")
        if snp_type:
            filters.append(f"snp_type = '{snp_type}'")

        sql = f"""
            SELECT
                plan_type,
                product_type,
                group_type,
                snp_type,
                SUM(enrollment) as enrollment,
                COUNT(DISTINCT contract_id || '-' || plan_id) as plan_count
            FROM fact_enrollment_unified
            WHERE {' AND '.join(filters)}
            GROUP BY plan_type, product_type, group_type, snp_type
            ORDER BY enrollment DESC
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_by_dimensions"
        )

        # Calculate percentage
        total = df['enrollment'].sum()
        df['pct_of_total'] = (df['enrollment'] / total * 100).round(2) if total > 0 else 0

        return {
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'year': year,
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
        start_year: int = 2007,
        end_year: int = 2026,
        user_id: str = "api"
    ) -> Dict:
        """
        Get enrollment timeseries with optional filters.

        Returns year-over-year data for charting.
        """
        filters = [
            f"year >= {start_year}",
            f"year <= {end_year}"
        ]

        if parent_org:
            filters.append(f"parent_org = '{parent_org}'")
        if plan_type:
            filters.append(f"plan_type = '{plan_type}'")
        if product_type:
            filters.append(f"product_type = '{product_type}'")
        if group_type:
            filters.append(f"group_type = '{group_type}'")
        if snp_type:
            filters.append(f"snp_type = '{snp_type}'")

        # Use geographic table for state filter
        if state:
            filters.append(f"state = '{state}'")
            sql = f"""
                SELECT
                    year,
                    SUM(enrollment) as enrollment,
                    COUNT(DISTINCT contract_id || '-' || plan_id) as plan_count
                FROM fact_enrollment_by_geography
                WHERE {' AND '.join(filters)}
                GROUP BY year
                ORDER BY year
            """
        else:
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
            'years': df['year'].tolist(),
            'total_enrollment': df['enrollment'].tolist(),
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
                'end_year': end_year
            }
        }

    def get_filters(self, user_id: str = "api") -> Dict:
        """
        Get available filter options for enrollment data.
        """
        sql = """
            SELECT DISTINCT
                year,
                plan_type,
                product_type,
                group_type,
                snp_type,
                parent_org
            FROM fact_enrollment_unified
        """

        df, audit_id = self.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_filters"
        )

        return {
            'years': sorted(df['year'].dropna().unique().tolist()),
            'plan_types': sorted(df['plan_type'].dropna().unique().tolist()),
            'product_types': sorted(df['product_type'].dropna().unique().tolist()),
            'group_types': sorted(df['group_type'].dropna().unique().tolist()),
            'snp_types': sorted(df['snp_type'].dropna().unique().tolist()),
            'parent_orgs': sorted(df['parent_org'].dropna().unique().tolist()),
            'audit_id': audit_id
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
