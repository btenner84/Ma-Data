"""
Stars Service V2 - Using NEW Unified Tables

Provides star rating data queries using the NEW unified data layer (2008-2026).
All queries use record-level audit tracking.

NEW Tables Used:
- measures_all_years (328K rows, 2008-2026) - replaces measure_data
- summary_all_years (33K rows, 2009-2026) - replaces stars_summary
- cutpoints_all_years (7K rows, 2011-2026) - replaces stars_cutpoints_2014_2026
- domain_all_years (68K rows, 2008-2026) - replaces stars_domain

Schema Changes from V1:
- cutpoints: star_rating -> star_level
- summary: rating_year -> year, WIDE -> LONG format
- domain: WIDE -> LONG format
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


class StarsServiceV2:
    """
    Stars Service V2 - Using NEW Unified Tables.

    Key Differences from V1:
    - Uses measures_all_years (2008-2026) instead of measure_data
    - Uses summary_all_years (LONG format) instead of stars_summary
    - Uses cutpoints_all_years with star_level column
    - Uses domain_all_years (LONG format) instead of stars_domain
    - All queries use record-level audit via query_with_record_audit()
    """

    def __init__(self):
        self.engine = get_engine()

    # =========================================================================
    # FILTER OPTIONS
    # =========================================================================

    def get_filters(self, user_id: str = "api") -> Dict:
        """
        Get all available filter options from NEW unified tables.
        """
        # Get measure filters from measures_all_years
        sql_measures = """
            SELECT DISTINCT
                year,
                measure_id
            FROM measures_all_years
            ORDER BY year, measure_id
        """

        df_measures, audit_id1 = self.engine.query_with_record_audit(
            sql_measures,
            user_id=user_id,
            context="get_measures_filters_v2"
        )

        # Get summary filters
        sql_summary = """
            SELECT DISTINCT
                year,
                part
            FROM summary_all_years
            ORDER BY year
        """

        df_summary, audit_id2 = self.engine.query_with_record_audit(
            sql_summary,
            user_id=user_id,
            context="get_summary_filters_v2"
        )

        # Get domain filters
        sql_domains = """
            SELECT DISTINCT
                domain_name
            FROM domain_all_years
            ORDER BY domain_name
        """

        df_domains, audit_id3 = self.engine.query_with_record_audit(
            sql_domains,
            user_id=user_id,
            context="get_domain_filters_v2"
        )

        # Get state filters
        sql_states = """
            SELECT DISTINCT state
            FROM fact_enrollment_by_geography
            ORDER BY state
        """

        df_states, audit_id4 = self.engine.query_with_record_audit(
            sql_states,
            user_id=user_id,
            context="get_state_filters_v2"
        )

        return clean_dict({
            'years': sorted(df_measures['year'].dropna().unique().tolist()),
            'measure_ids': sorted(df_measures['measure_id'].dropna().unique().tolist()),
            'parts': sorted(df_summary['part'].dropna().unique().tolist()),
            'domain_names': sorted(df_domains['domain_name'].dropna().unique().tolist()),
            'states': df_states['state'].tolist(),
            'audit_ids': [audit_id1, audit_id2, audit_id3, audit_id4]
        })

    # =========================================================================
    # MEASURES
    # =========================================================================

    def get_measure_performance(
        self,
        year: int = 2026,
        measure_ids: Optional[List[str]] = None,
        parts: Optional[List[str]] = None,
        contract_ids: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get measure-level performance data from measures_all_years.

        NEW columns available:
        - measure_key: Stable key for cross-year tracking
        - numeric_value: Raw numeric value
        - raw_value: Original text value
        - _source_file: Source file for audit
        """
        filters = [f"year = {year}"]

        if measure_ids:
            measure_list = ", ".join([f"'{m}'" for m in measure_ids])
            filters.append(f"measure_id IN ({measure_list})")
        if parts:
            # Part is embedded in measure_id (C01, D01, etc.)
            part_patterns = []
            for p in parts:
                part_patterns.append(f"measure_id LIKE '{p}%'")
            filters.append(f"({' OR '.join(part_patterns)})")
        if contract_ids:
            contract_list = ", ".join([f"'{c}'" for c in contract_ids])
            filters.append(f"contract_id IN ({contract_list})")

        where_clause = "WHERE " + " AND ".join(filters)

        sql = f"""
            SELECT
                measure_id,
                measure_name,
                measure_key,
                AVG(star_rating) as avg_rating,
                AVG(numeric_value) as avg_numeric,
                COUNT(DISTINCT contract_id) as contract_count,
                COUNT(*) as total_records
            FROM measures_all_years
            {where_clause}
            GROUP BY measure_id, measure_name, measure_key
            ORDER BY measure_id
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_measure_performance_v2"
        )

        return clean_dict({
            'year': year,
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'measure_ids': measure_ids,
                'parts': parts,
                'contract_ids': contract_ids
            },
            'table_source': 'measures_all_years'
        })

    def get_measure_timeseries(
        self,
        measure_id: str,
        start_year: int = 2008,
        end_year: int = 2026,
        user_id: str = "api"
    ) -> Dict:
        """
        Get timeseries data for a specific measure across years.
        """
        sql = f"""
            SELECT
                year,
                AVG(star_rating) as avg_rating,
                AVG(numeric_value) as avg_numeric,
                COUNT(DISTINCT contract_id) as contract_count
            FROM measures_all_years
            WHERE measure_id = '{measure_id}'
              AND year >= {start_year}
              AND year <= {end_year}
            GROUP BY year
            ORDER BY year
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context=f"get_measure_timeseries_{measure_id}"
        )

        return clean_dict({
            'measure_id': measure_id,
            'years': df['year'].tolist(),
            'avg_ratings': df['avg_rating'].tolist(),
            'avg_numerics': df['avg_numeric'].tolist(),
            'contract_counts': df['contract_count'].tolist(),
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id
        })

    # =========================================================================
    # CUTPOINTS
    # =========================================================================

    def get_cutpoints(
        self,
        years: Optional[List[int]] = None,
        measure_ids: Optional[List[str]] = None,
        parts: Optional[List[str]] = None,
        star_levels: Optional[List[int]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get star cutpoint thresholds from cutpoints_all_years.

        NOTE: Uses star_level (not star_rating) - schema change from v1.
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
        if star_levels:
            level_list = ", ".join([str(l) for l in star_levels])
            filters.append(f"star_level IN ({level_list})")

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        sql = f"""
            SELECT
                year,
                part,
                measure_id,
                measure_name,
                star_level,
                threshold,
                threshold_text
            FROM cutpoints_all_years
            {where_clause}
            ORDER BY year, part, measure_id, star_level
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_cutpoints_v2"
        )

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'years': years,
                'measure_ids': measure_ids,
                'parts': parts,
                'star_levels': star_levels
            },
            'table_source': 'cutpoints_all_years',
            'note': 'Uses star_level column (not star_rating)'
        })

    def get_cutpoints_timeseries(
        self,
        measure_id: str,
        star_level: int = 4,
        start_year: int = 2011,
        end_year: int = 2026,
        user_id: str = "api"
    ) -> Dict:
        """
        Get cutpoint threshold timeseries for a measure at a specific star level.
        """
        sql = f"""
            SELECT
                year,
                threshold,
                threshold_text
            FROM cutpoints_all_years
            WHERE measure_id = '{measure_id}'
              AND star_level = {star_level}
              AND year >= {start_year}
              AND year <= {end_year}
            ORDER BY year
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context=f"get_cutpoints_timeseries_{measure_id}"
        )

        return clean_dict({
            'measure_id': measure_id,
            'star_level': star_level,
            'years': df['year'].tolist(),
            'thresholds': df['threshold'].tolist(),
            'threshold_texts': df['threshold_text'].tolist(),
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id
        })

    # =========================================================================
    # SUMMARY RATINGS
    # =========================================================================

    def get_summary_ratings(
        self,
        years: Optional[List[int]] = None,
        contract_ids: Optional[List[str]] = None,
        parts: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get summary ratings from summary_all_years.

        NOTE: Uses year column (not rating_year) and LONG format.
        """
        filters = []

        if years:
            year_list = ", ".join([str(y) for y in years])
            filters.append(f"year IN ({year_list})")
        if contract_ids:
            contract_list = ", ".join([f"'{c}'" for c in contract_ids])
            filters.append(f"contract_id IN ({contract_list})")
        if parts:
            part_list = ", ".join([f"'{p}'" for p in parts])
            filters.append(f"part IN ({part_list})")

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        sql = f"""
            SELECT
                year,
                contract_id,
                part,
                summary_rating,
                raw_value,
                organization_type,
                parent_organization,
                organization_name
            FROM summary_all_years
            {where_clause}
            ORDER BY year DESC, contract_id, part
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_summary_ratings_v2"
        )

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'years': years,
                'contract_ids': contract_ids,
                'parts': parts
            },
            'table_source': 'summary_all_years',
            'note': 'LONG format - one row per contract-year-part'
        })

    def get_summary_distribution(
        self,
        year: int = 2026,
        part: str = "C",
        user_id: str = "api"
    ) -> Dict:
        """
        Get distribution of summary ratings for a year.
        """
        sql = f"""
            SELECT
                summary_rating,
                COUNT(*) as contract_count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as pct_of_total
            FROM summary_all_years
            WHERE year = {year}
              AND part = '{part}'
              AND summary_rating IS NOT NULL
            GROUP BY summary_rating
            ORDER BY summary_rating
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context=f"get_summary_distribution_{year}_{part}"
        )

        return clean_dict({
            'year': year,
            'part': part,
            'ratings': df['summary_rating'].tolist(),
            'counts': df['contract_count'].tolist(),
            'percentages': df['pct_of_total'].tolist(),
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id
        })

    # =========================================================================
    # DOMAIN SCORES (NEW!)
    # =========================================================================

    def get_domain_scores(
        self,
        years: Optional[List[int]] = None,
        contract_ids: Optional[List[str]] = None,
        parts: Optional[List[str]] = None,
        domain_names: Optional[List[str]] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get domain scores from domain_all_years.

        This is a NEW endpoint not available in v1 - domain data was limited
        to 2023-2026 in old tables, now covers 2008-2026.
        """
        filters = []

        if years:
            year_list = ", ".join([str(y) for y in years])
            filters.append(f"year IN ({year_list})")
        if contract_ids:
            contract_list = ", ".join([f"'{c}'" for c in contract_ids])
            filters.append(f"contract_id IN ({contract_list})")
        if parts:
            part_list = ", ".join([f"'{p}'" for p in parts])
            filters.append(f"part IN ({part_list})")
        if domain_names:
            domain_list = ", ".join([f"'{d}'" for d in domain_names])
            filters.append(f"domain_name IN ({domain_list})")

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        sql = f"""
            SELECT
                year,
                contract_id,
                part,
                domain_name,
                star_rating,
                raw_value
            FROM domain_all_years
            {where_clause}
            ORDER BY year DESC, contract_id, part, domain_name
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context="get_domain_scores_v2"
        )

        return clean_dict({
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id,
            'filters': {
                'years': years,
                'contract_ids': contract_ids,
                'parts': parts,
                'domain_names': domain_names
            },
            'table_source': 'domain_all_years',
            'coverage': '2008-2026 (15 more years than v1)'
        })

    def get_domain_averages(
        self,
        year: int = 2026,
        part: Optional[str] = None,
        user_id: str = "api"
    ) -> Dict:
        """
        Get average domain scores across all contracts for a year.
        """
        filters = [f"year = {year}", "star_rating IS NOT NULL"]
        if part:
            filters.append(f"part = '{part}'")

        where_clause = "WHERE " + " AND ".join(filters)

        sql = f"""
            SELECT
                domain_name,
                part,
                AVG(star_rating) as avg_rating,
                COUNT(DISTINCT contract_id) as contract_count,
                MIN(star_rating) as min_rating,
                MAX(star_rating) as max_rating
            FROM domain_all_years
            {where_clause}
            GROUP BY domain_name, part
            ORDER BY part, domain_name
        """

        df, audit_id = self.engine.query_with_record_audit(
            sql,
            user_id=user_id,
            context=f"get_domain_averages_{year}"
        )

        return clean_dict({
            'year': year,
            'part': part,
            'data': df.to_dict(orient='records'),
            'audit_id': audit_id
        })

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
        Uses all NEW unified tables.
        """
        audit_ids = []

        # Get summary rating
        sql_summary = f"""
            SELECT
                contract_id,
                year,
                part,
                summary_rating,
                organization_type,
                parent_organization,
                organization_name
            FROM summary_all_years
            WHERE contract_id = '{contract_id}'
              AND year = {year}
        """

        df_summary, audit_id1 = self.engine.query_with_record_audit(
            sql_summary,
            user_id=user_id,
            context="get_contract_summary_v2"
        )
        audit_ids.append(audit_id1)

        # Get measure-level detail
        sql_measures = f"""
            SELECT
                measure_id,
                measure_name,
                measure_key,
                star_rating,
                numeric_value,
                raw_value
            FROM measures_all_years
            WHERE contract_id = '{contract_id}'
              AND year = {year}
            ORDER BY measure_id
        """

        df_measures, audit_id2 = self.engine.query_with_record_audit(
            sql_measures,
            user_id=user_id,
            context="get_contract_measures_v2"
        )
        audit_ids.append(audit_id2)

        # Get domain scores
        sql_domains = f"""
            SELECT
                part,
                domain_name,
                star_rating,
                raw_value
            FROM domain_all_years
            WHERE contract_id = '{contract_id}'
              AND year = {year}
            ORDER BY part, domain_name
        """

        df_domains, audit_id3 = self.engine.query_with_record_audit(
            sql_domains,
            user_id=user_id,
            context="get_contract_domains_v2"
        )
        audit_ids.append(audit_id3)

        return clean_dict({
            'contract_id': contract_id,
            'year': year,
            'summary': df_summary.to_dict(orient='records'),
            'measures': df_measures.to_dict(orient='records'),
            'domains': df_domains.to_dict(orient='records'),
            'audit_ids': audit_ids,
            'table_sources': ['summary_all_years', 'measures_all_years', 'domain_all_years']
        })

    # =========================================================================
    # LINEAGE
    # =========================================================================

    def trace_lineage(self, audit_id: str) -> Dict:
        """Trace data lineage for a previous query."""
        return self.engine.trace_query_lineage(audit_id)


# Singleton instance
_service_instance_v2 = None

def get_stars_service_v2() -> StarsServiceV2:
    """Get or create singleton stars service v2."""
    global _service_instance_v2
    if _service_instance_v2 is None:
        _service_instance_v2 = StarsServiceV2()
    return _service_instance_v2


# Feature flag check
STARS_SERVICE_V2_AVAILABLE = True
