#!/usr/bin/env python3
"""
Table Schemas and Primary Keys

Defines primary keys for each table to enable record-level audit tracking.
When a query is executed, we can identify exactly which records were accessed
by capturing their primary key values.
"""

from typing import Dict, List

# Primary key columns for each table
# These are used to uniquely identify records for audit tracking
TABLE_PRIMARY_KEYS: Dict[str, List[str]] = {
    # =================================================================
    # ENROLLMENT TABLES
    # =================================================================
    'fact_enrollment_unified': ['contract_id', 'plan_id', 'year'],
    'fact_enrollment_by_state': ['state', 'year'],
    'fact_enrollment_by_geography': ['state', 'county', 'year', 'contract_id'],

    # =================================================================
    # STARS TABLES - NEW UNIFIED
    # =================================================================
    'measures_all_years': ['contract_id', 'year', 'measure_id'],
    'summary_all_years': ['contract_id', 'year', 'part'],
    'cutpoints_all_years': ['year', 'part', 'measure_id', 'star_level'],
    'domain_all_years': ['contract_id', 'year', 'part', 'domain_name'],
    'dim_measure': ['measure_id', 'year'],
    'dim_entity': ['entity_id'],
    'disenrollment_all_years': ['contract_id', 'year'],

    # =================================================================
    # STARS TABLES - OLD (for backward compatibility)
    # =================================================================
    'stars_enrollment_unified': ['contract_id', 'star_year'],
    'stars_summary': ['contract_id', 'rating_year'],
    'measure_data': ['contract_id', 'year', 'measure_id'],
    'stars_cutpoints_2014_2026': ['year', 'part', 'measure_id', 'star_rating'],
    'stars_measure_specs': ['measure_id'],
    'stars_fourplus_by_year': ['parent_org', 'year'],

    # =================================================================
    # RISK SCORES TABLES
    # =================================================================
    'fact_risk_scores_unified': ['contract_id', 'plan_id', 'year'],
    'risk_scores_by_parent': ['parent_org', 'year'],
    'risk_scores_by_parent_dims': ['parent_org', 'year', 'plan_type', 'snp_type', 'group_type'],

    # =================================================================
    # SNP TABLES
    # =================================================================
    'fact_snp': ['contract_id', 'plan_id', 'year'],
    'fact_snp_historical': ['parent_org', 'year'],

    # =================================================================
    # AGGREGATION TABLES
    # =================================================================
    'agg_enrollment_by_year': ['year'],
    'agg_enrollment_by_plantype': ['year', 'plan_type'],
    'enrollment_by_parent': ['parent_org', 'year'],

    # =================================================================
    # LOOKUP TABLES
    # =================================================================
    'dim_county': ['state', 'county'],
    'parent_org_summary': ['parent_org'],
}


def get_primary_keys(table_name: str) -> List[str]:
    """
    Get primary key columns for a table.

    Args:
        table_name: Name of the table

    Returns:
        List of primary key column names, or empty list if unknown
    """
    return TABLE_PRIMARY_KEYS.get(table_name, [])


def has_primary_keys(table_name: str) -> bool:
    """Check if a table has defined primary keys."""
    return table_name in TABLE_PRIMARY_KEYS


def get_all_tables_with_keys() -> Dict[str, List[str]]:
    """Get all tables with their primary keys."""
    return TABLE_PRIMARY_KEYS.copy()
