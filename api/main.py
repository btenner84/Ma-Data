"""
MA Intelligence Platform - FastAPI Backend
Serves data from S3 Parquet files for the dashboard.
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
import boto3
from io import BytesIO
from functools import lru_cache
import zipfile
import os
import sys
import math


def sanitize_for_json(obj):
    """Recursively replace NaN, Inf values with None for JSON serialization."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif pd.isna(obj):
        return None
    return obj

# Add project root for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import unified data layer (DuckDB + Audit)
try:
    from db import get_engine
    from api.services.enrollment_service import get_enrollment_service
    from api.services.ai_query_service import get_ai_query_service
    UNIFIED_DATA_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Unified data layer not available: {e}")
    UNIFIED_DATA_AVAILABLE = False

# Import audit router
try:
    from api.audit_api import router as audit_router
    AUDIT_API_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Audit API not available: {e}")
    AUDIT_API_AVAILABLE = False

app = FastAPI(
    title="MA Intelligence Platform API",
    description="API for Medicare Advantage data intelligence",
    version="1.0.0"
)

# CORS for Next.js frontend
cors_origins = os.environ.get("CORS_ORIGINS", "http://localhost:3000,http://localhost:3001").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register audit API router
if AUDIT_API_AVAILABLE:
    app.include_router(audit_router)

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
s3 = boto3.client('s3')


# === Health Check ===

@app.get("/health")
def health_check():
    """Health check endpoint for Railway."""
    return {"status": "healthy", "service": "ma-intelligence-api"}


@app.get("/")
def root():
    """Root endpoint."""
    return {
        "service": "MA Intelligence Platform API",
        "version": "1.0.0",
        "docs": "/docs"
    }


# === Data Loading Utilities (DuckDB-based with fallback) ===

# Global DuckDB engine instance
_duckdb_engine = None

def get_duckdb_engine():
    """Get or create DuckDB engine singleton."""
    global _duckdb_engine
    if _duckdb_engine is None and UNIFIED_DATA_AVAILABLE:
        try:
            _duckdb_engine = get_engine()
        except Exception as e:
            print(f"Warning: Could not initialize DuckDB engine: {e}")
    return _duckdb_engine


@lru_cache(maxsize=32)
def load_parquet(s3_key: str) -> pd.DataFrame:
    """Load parquet file from S3 with caching (fallback for when DuckDB unavailable)."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def query_duckdb(sql: str, fallback_fn=None) -> pd.DataFrame:
    """
    Query using DuckDB with fallback to direct S3 if unavailable.

    Args:
        sql: SQL query to execute
        fallback_fn: Function to call if DuckDB unavailable
    """
    engine = get_duckdb_engine()
    if engine:
        try:
            return engine.query(sql)
        except Exception as e:
            print(f"DuckDB query failed: {e}")
            if fallback_fn:
                return fallback_fn()
    elif fallback_fn:
        return fallback_fn()
    return pd.DataFrame()


def get_enrollment_data():
    """Load unified enrollment data."""
    sql = """
        SELECT * FROM enrollment_by_parent
    """
    return query_duckdb(sql, lambda: load_parquet('processed/unified/enrollment_by_parent_annual.parquet'))


def get_enrollment_unified():
    """Load unified enrollment with all dimensions (plan_type, product_type, group_type)."""
    sql = """
        SELECT * FROM fact_enrollment_unified
    """
    def fallback():
        try:
            df = load_parquet('processed/unified/fact_enrollment_v6.parquet')
            return consolidate_parent_org_names(df)
        except:
            try:
                df = load_parquet('processed/unified/fact_enrollment_v4.parquet')
                return consolidate_parent_org_names(df)
            except:
                return pd.DataFrame()

    df = query_duckdb(sql, fallback)
    return consolidate_parent_org_names(df) if not df.empty else df


def get_enrollment_by_state_data():
    """Load enrollment data with state dimension."""
    sql = """
        SELECT * FROM fact_enrollment_by_state
    """
    return query_duckdb(sql, lambda: load_parquet('processed/unified/fact_enrollment_by_state.parquet'))


def get_enrollment_by_geography():
    """Load enrollment data with state and county dimensions."""
    sql = """
        SELECT * FROM fact_enrollment_by_geography
    """
    def fallback():
        try:
            df = load_parquet('processed/unified/fact_enrollment_by_geography.parquet')
            return consolidate_parent_org_names(df)
        except:
            return pd.DataFrame()

    df = query_duckdb(sql, fallback)
    return consolidate_parent_org_names(df) if not df.empty else df


def get_county_lookup():
    """Load county lookup table."""
    sql = """
        SELECT * FROM dim_county
    """
    return query_duckdb(sql, lambda: load_parquet('processed/unified/dim_county.parquet'))


def get_enrollment_by_year():
    """Load enrollment aggregated by year."""
    sql = """
        SELECT * FROM agg_enrollment_by_year
    """
    return query_duckdb(sql, lambda: load_parquet('processed/unified/agg_enrollment_by_year_v2.parquet'))


def get_enrollment_by_plantype():
    """Load enrollment by year and plan type."""
    sql = """
        SELECT * FROM agg_enrollment_by_plantype
    """
    return query_duckdb(sql, lambda: load_parquet('processed/unified/agg_enrollment_by_plantype_v2.parquet'))


def get_enrollment_by_product():
    """Load enrollment by year and product type (derived from plan type)."""
    sql = """
        SELECT
            year,
            product_type,
            SUM(enrollment) as enrollment,
            COUNT(DISTINCT contract_id) as contract_count,
            COUNT(DISTINCT parent_org) as parent_count
        FROM fact_enrollment_unified
        GROUP BY year, product_type
        ORDER BY year, product_type
    """
    def fallback():
        df = get_enrollment_unified()
        if df.empty:
            return pd.DataFrame()
        return df.groupby(['year', 'product_type']).agg({
            'enrollment': 'sum',
            'contract_count': 'sum',
            'parent_org': 'nunique'
        }).reset_index().rename(columns={'parent_org': 'parent_count'})

    return query_duckdb(sql, fallback)


def get_enrollment_detail():
    """Load detailed enrollment by contract/plan/county."""
    sql = """
        SELECT * FROM fact_enrollment_unified
        WHERE year = 2026
        LIMIT 10000
    """
    def fallback():
        try:
            return load_parquet('processed/fact_enrollment/2025/1/enrollment.parquet')
        except:
            return load_parquet('processed/fact_enrollment/2024/1/enrollment.parquet')

    return query_duckdb(sql, fallback)


def get_stars_summary():
    """Load unified stars summary."""
    sql = """
        SELECT * FROM stars_summary
    """
    return query_duckdb(sql, lambda: load_parquet('processed/unified/stars_summary.parquet'))


def get_measure_data():
    """Load complete measure-level data."""
    sql = """
        SELECT * FROM measure_data
    """
    return query_duckdb(sql, lambda: load_parquet('processed/unified/measure_data_complete.parquet'))


def get_parent_summary():
    """Load parent organization summary."""
    sql = """
        SELECT * FROM parent_org_summary
    """
    return query_duckdb(sql, lambda: load_parquet('processed/unified/parent_org_summary.parquet'))


def get_risk_scores():
    """Load risk scores summary."""
    sql = """
        SELECT * FROM fact_risk_scores
    """
    def fallback():
        try:
            return load_parquet('processed/unified/risk_scores_summary.parquet')
        except:
            return pd.DataFrame()

    return query_duckdb(sql, fallback)


def get_risk_scores_unified():
    """Load unified risk scores with enrollment for v2 endpoints."""
    sql = """
        SELECT * FROM fact_risk_scores
    """
    def fallback():
        try:
            df = load_parquet('processed/unified/fact_risk_scores_unified.parquet')
            return consolidate_parent_org_names(df)
        except:
            return pd.DataFrame()

    df = query_duckdb(sql, fallback)
    return consolidate_parent_org_names(df) if not df.empty else df


def get_risk_scores_by_parent_year():
    """Load risk scores aggregated by parent org and year."""
    sql = """
        SELECT * FROM risk_scores_by_parent
    """
    def fallback():
        try:
            df = load_parquet('processed/unified/risk_scores_by_parent_year.parquet')
            return consolidate_parent_org_names(df)
        except:
            return pd.DataFrame()

    df = query_duckdb(sql, fallback)
    return consolidate_parent_org_names(df) if not df.empty else df


def get_risk_scores_by_parent_dims():
    """Load risk scores with plan_type, snp_type, group_type dimensions."""
    def fallback():
        try:
            df = load_parquet('processed/unified/risk_scores_by_parent_dims.parquet')
            return consolidate_parent_org_names(df)
        except:
            return pd.DataFrame()

    return fallback()  # Use fallback for now, could add DuckDB query later


def get_risk_scores_summary_v2():
    """Load risk scores summary v2."""
    sql = """
        SELECT * FROM fact_risk_scores
    """
    def fallback():
        try:
            return load_parquet('processed/unified/risk_scores_summary_v2.parquet')
        except:
            return pd.DataFrame()

    return query_duckdb(sql, fallback)


def get_snp_enrollment():
    """Load SNP enrollment data (legacy)."""
    sql = """
        SELECT * FROM fact_snp
    """
    def fallback():
        try:
            return load_parquet('processed/unified/fact_snp_enrollment.parquet')
        except:
            return pd.DataFrame()

    return query_duckdb(sql, fallback)


def get_snp_by_parent():
    """Load SNP enrollment by parent org with full dimensions."""
    sql = """
        SELECT * FROM fact_snp
    """
    def fallback():
        try:
            # Load detailed SNP data (2023-2026 with D-SNP, C-SNP, I-SNP)
            detailed = load_parquet('processed/unified/fact_snp_by_parent.parquet')
        except:
            detailed = pd.DataFrame()

        try:
            # Load historical SNP data (2014-2022 with generic SNP type)
            historical = load_parquet('processed/unified/fact_snp_by_parent_historical.parquet')
            # Filter to years not in detailed
            if not detailed.empty:
                detailed_years = detailed['year'].unique()
                historical = historical[~historical['year'].isin(detailed_years)]
        except:
            historical = pd.DataFrame()

        # Combine both
        if not detailed.empty and not historical.empty:
            return pd.concat([historical, detailed], ignore_index=True)
        elif not detailed.empty:
            return detailed
        elif not historical.empty:
            return historical
        else:
            return pd.DataFrame()

    df = query_duckdb(sql, fallback)
    return consolidate_parent_org_names(df) if not df.empty else fallback()


def _get_snp_by_parent_fallback():
    """Fallback for SNP data loading."""
    try:
        # Load detailed SNP data (2023-2026 with D-SNP, C-SNP, I-SNP)
        detailed = load_parquet('processed/unified/fact_snp_by_parent.parquet')
    except:
        detailed = pd.DataFrame()

    try:
        # Load historical SNP data (2014-2022 with generic SNP type)
        historical = load_parquet('processed/unified/fact_snp_by_parent_historical.parquet')
        # Filter to years not in detailed
        if not detailed.empty:
            detailed_years = detailed['year'].unique()
            historical = historical[~historical['year'].isin(detailed_years)]
    except:
        historical = pd.DataFrame()

    # Combine both
    if not detailed.empty and not historical.empty:
        result = pd.concat([historical, detailed], ignore_index=True)
    elif not detailed.empty:
        result = detailed
    elif not historical.empty:
        result = historical
    else:
        return pd.DataFrame()

    return consolidate_parent_org_names(result)


def get_risk_scores_detail():
    """Load detailed risk scores from county-level data."""
    # This loads raw county data for filtering
    try:
        import zipfile
        response = s3.get_object(Bucket=S3_BUCKET, Key='raw/plan_payment/2024/plan_payment_2024.zip')
        zf = zipfile.ZipFile(BytesIO(response['Body'].read()))
        with zf.open('2024PartCCountyLevel.xlsx') as f:
            df = pd.read_excel(f, header=2)
            # Fix column - in 2024 the risk score is in AB PM/PM column
            df['risk_score'] = pd.to_numeric(df['Average AB PM/PM Payment'], errors='coerce')
            return df
    except:
        return pd.DataFrame()


# === Response Models ===

class EnrollmentTimeSeriesResponse(BaseModel):
    years: List[int]
    total_enrollment: List[int]
    by_parent: Optional[dict] = None


class StarsBandResponse(BaseModel):
    year: int
    bands: dict  # {star_rating: {count: N, enrollment: N, pct: N}}


class ContractDetail(BaseModel):
    contract_id: str
    year: int
    parent_org: str
    enrollment: Optional[int]
    overall_rating: Optional[float]
    measures: Optional[dict]


# === Enrollment Endpoints ===

# Mapping for simplified plan types
PLAN_TYPE_MAP = {
    'HMO': ['HMO', 'HMOPOS', 'HMO-POS', 'HMO/HMOPOS'],
    'PPO': ['Local PPO', 'PPO', 'Regional PPO'],  # Combined - includes Regional PPO
}

# Parent org name consolidation mapping (historical name -> current name)
# This ensures continuous timeseries when companies rebrand
PARENT_ORG_NAME_MAP = {
    # Anthem rebranded to Elevance Health in 2022
    'Anthem Inc.': 'Elevance Health, Inc.',
    'Anthem, Inc.': 'Elevance Health, Inc.',
    # CIGNA rebranded to The Cigna Group in 2023
    'CIGNA': 'The Cigna Group',
    'Cigna Corporation': 'The Cigna Group',
    'CIGNA Corporation': 'The Cigna Group',
    # CVS acquired Aetna in 2018
    'Aetna Inc.': 'CVS Health Corporation',
    'Aetna, Inc.': 'CVS Health Corporation',
    # WellCare merged with Centene in 2020
    'WellCare Health Plans, Inc.': 'Centene Corporation',
    # Molina variations
    'Molina Healthcare Inc.': 'Molina Healthcare, Inc.',
    # Kaiser variations
    'Kaiser Foundation Health Plan': 'Kaiser Foundation Health Plan, Inc.',
}


def consolidate_parent_org_names(df: pd.DataFrame) -> pd.DataFrame:
    """Apply parent org name consolidation to standardize historical names."""
    if 'parent_org' not in df.columns:
        return df
    df = df.copy()
    df['parent_org'] = df['parent_org'].replace(PARENT_ORG_NAME_MAP)
    return df


@app.get("/api/v2/enrollment/timeseries")
async def get_enrollment_timeseries_v2(
    parent_orgs: Optional[str] = None,  # Pipe-separated list (| separator)
    plan_types: Optional[str] = None,   # Comma-separated: HMO,PPO,PFFS,MSA (simplified)
    product_types: Optional[str] = None, # Comma-separated: MAPD,MA-only,PDP
    group_types: Optional[str] = None,  # Comma-separated: Group,Individual
    snp_types: Optional[str] = None,    # Comma-separated: D-SNP,C-SNP,I-SNP
    states: Optional[str] = None,       # Comma-separated state codes: CA,TX,FL
    counties: Optional[str] = None,     # Pipe-separated county names: Los Angeles|San Diego
    group_by: Optional[str] = None,     # Optional: plan_type, product_type, parent_org, group_type, state, county
    include_total: bool = True,         # Include "Industry Total" when filtering by payers
    view_mode: str = "enrollment"       # "enrollment" or "market_share"
):
    """
    Get enrollment time series with multi-dimensional filtering.
    Supports filtering by parent_orgs, plan_types, product_types, group_types, snp_types, states.
    Supports grouping to show multiple lines.
    view_mode: "enrollment" for absolute numbers, "market_share" for percentage of total
    """
    # Use geography data when filtering by states or counties
    if states or counties:
        df = get_enrollment_by_geography()
        if df.empty:
            return {"error": "No geographic enrollment data available"}
        if states:
            state_list = [s.strip().upper() for s in states.split(',')]
            df = df[df['state'].isin(state_list)]
        if counties:
            county_list = [c.strip() for c in counties.split('|')]
            df = df[df['county'].isin(county_list)]
    # Use SNP-specific data when filtering by SNP types
    elif snp_types:
        snp_list = [s.strip() for s in snp_types.split(',')]

        # Non-SNP or combined filters - use stars_enrollment_unified which has all dimensions
        if 'Non-SNP' in snp_list or 'SNP' in snp_list:
            # Use stars_enrollment_unified which has snp_type + plan_type + group_type
            stars_df = load_parquet('processed/unified/stars_enrollment_unified.parquet')
            stars_df = consolidate_parent_org_names(stars_df)
            if 'star_year' in stars_df.columns:
                stars_df = stars_df.rename(columns={'star_year': 'year'})

            # Filter by SNP type
            if 'Non-SNP' in snp_list and 'SNP' not in snp_list:
                df = stars_df[stars_df['snp_type'] == 'Non-SNP'].copy()
            elif 'SNP' in snp_list and 'Non-SNP' not in snp_list:
                df = stars_df[stars_df['snp_type'] == 'SNP'].copy()
            else:
                # Both selected - use all data
                df = stars_df.copy()
        else:
            # D-SNP, C-SNP, I-SNP use the detailed SNP data
            df = get_snp_by_parent()
            if df.empty:
                return {"error": "No SNP enrollment data available"}
            df = df[df['snp_type'].isin(snp_list)]
    else:
        df = get_enrollment_unified()

    if df.empty:
        return {"error": "No unified enrollment data available"}

    # Calculate industry total BEFORE filtering (for market share and include_total)
    df_for_total = df.copy()
    # Apply non-payer filters to total calculation
    if plan_types and 'plan_type' in df_for_total.columns:
        plan_list = [p.strip() for p in plan_types.split(',')]
        expanded_plan_types = []
        for pt in plan_list:
            if pt in PLAN_TYPE_MAP:
                expanded_plan_types.extend(PLAN_TYPE_MAP[pt])
            else:
                expanded_plan_types.append(pt)
        df_for_total = df_for_total[df_for_total['plan_type'].isin(expanded_plan_types)]
    if product_types and 'product_type' in df_for_total.columns:
        product_list = [p.strip() for p in product_types.split(',')]
        df_for_total = df_for_total[df_for_total['product_type'].isin(product_list)]
    if group_types and 'group_type' in df_for_total.columns:
        group_list = [g.strip() for g in group_types.split(',')]
        df_for_total = df_for_total[df_for_total['group_type'].isin(group_list)]

    industry_total_by_year = df_for_total.groupby('year')['enrollment'].sum().to_dict()

    # Apply filters including parent_orgs
    if parent_orgs:
        # Use | as separator since parent org names contain commas
        parent_list = [p.strip() for p in parent_orgs.split('|')]
        df = df[df['parent_org'].isin(parent_list)]

    if plan_types and 'plan_type' in df.columns:
        plan_list = [p.strip() for p in plan_types.split(',')]
        # Expand simplified types (HMO -> HMO, HMOPOS; PPO -> Local PPO, Regional PPO)
        expanded_plan_types = []
        for pt in plan_list:
            if pt in PLAN_TYPE_MAP:
                expanded_plan_types.extend(PLAN_TYPE_MAP[pt])
            else:
                expanded_plan_types.append(pt)
        df = df[df['plan_type'].isin(expanded_plan_types)]

    if product_types and 'product_type' in df.columns:
        product_list = [p.strip() for p in product_types.split(',')]
        df = df[df['product_type'].isin(product_list)]

    if group_types and 'group_type' in df.columns:
        group_list = [g.strip() for g in group_types.split(',')]
        df = df[df['group_type'].isin(group_list)]

    # Auto-group by parent_org when multiple payers are selected
    effective_group_by = group_by
    if parent_orgs and not group_by:
        parent_list = [p.strip() for p in parent_orgs.split('|')]
        if len(parent_list) > 1:
            effective_group_by = 'parent_org'

    # Group and aggregate
    if effective_group_by and effective_group_by in ['plan_type', 'product_type', 'parent_org', 'group_type', 'state', 'county']:
        if effective_group_by not in df.columns:
            return {"error": f"Cannot group by {effective_group_by} - column not available"}

        # Return multiple series - only aggregate columns that exist
        agg_dict = {'enrollment': 'sum'}
        if 'contract_count' in df.columns:
            agg_dict['contract_count'] = 'sum'
        grouped = df.groupby(['year', effective_group_by]).agg(agg_dict).reset_index()

        # Pivot to get series per group
        pivot = grouped.pivot(index='year', columns=effective_group_by, values='enrollment').fillna(0)
        years = pivot.index.tolist()
        series = {col: pivot[col].tolist() for col in pivot.columns}

        # Add industry total if filtering by parent_orgs and include_total is True
        if parent_orgs and include_total:
            series['Industry Total'] = [industry_total_by_year.get(y, 0) for y in years]

        # Convert to market share if requested
        if view_mode == "market_share":
            for col in series:
                if col != 'Industry Total':
                    series[col] = [
                        round(v / industry_total_by_year.get(y, 1) * 100, 2) if industry_total_by_year.get(y, 0) > 0 else 0
                        for v, y in zip(series[col], years)
                    ]
            # Set industry total to 100% (it represents the full market)
            if 'Industry Total' in series:
                series['Industry Total'] = [100.0 for _ in years]

        return {
            "years": years,
            "group_by": effective_group_by,
            "series": series,
            "view_mode": view_mode
        }
    else:
        # Return single aggregated series - only aggregate columns that exist
        agg_dict = {'enrollment': 'sum'}
        if 'contract_count' in df.columns:
            agg_dict['contract_count'] = 'sum'
        by_year = df.groupby('year').agg(agg_dict).reset_index()

        years = by_year['year'].tolist()
        enrollment = by_year['enrollment'].tolist()

        # Add industry total for single payer view
        if parent_orgs and include_total:
            series = {
                parent_orgs.split('|')[0]: enrollment,
                'Industry Total': [industry_total_by_year.get(y, 0) for y in years]
            }
            # Convert to market share if requested
            if view_mode == "market_share":
                payer_name = parent_orgs.split('|')[0]
                series[payer_name] = [
                    round(v / industry_total_by_year.get(y, 1) * 100, 2) if industry_total_by_year.get(y, 0) > 0 else 0
                    for v, y in zip(series[payer_name], years)
                ]
                series['Industry Total'] = [100.0 for _ in years]
            return {
                "years": years,
                "group_by": "parent_org",
                "series": series,
                "view_mode": view_mode
            }

        result = {
            "years": years,
            "total_enrollment": enrollment,
            "view_mode": view_mode
        }
        if 'contract_count' in by_year.columns:
            result["contract_count"] = by_year['contract_count'].tolist()
        return result


@app.get("/api/v2/enrollment/filters")
async def get_enrollment_filters():
    """Get available filter options for enrollment data."""
    df = get_enrollment_unified()

    if df.empty:
        return {"error": "No unified enrollment data available"}

    # Get parent orgs - find most recent year with actual parent data (not just "Unknown")
    years_desc = sorted(df['year'].unique(), reverse=True)
    sorted_parents = []
    for year in years_desc:
        year_data = df[df['year'] == year]
        real_parents = year_data[year_data['parent_org'] != 'Unknown']
        if len(real_parents) > 0:
            parent_enrollment = real_parents.groupby('parent_org')['enrollment'].sum()
            parent_enrollment = parent_enrollment.sort_values(ascending=False)
            sorted_parents = parent_enrollment.index.tolist()
            break

    # If no real parents found, get all
    if not sorted_parents:
        sorted_parents = df['parent_org'].unique().tolist()

    # Get group_types if available
    group_types = []
    if 'group_type' in df.columns:
        group_types = sorted(df['group_type'].dropna().unique().tolist())

    # Simplified plan types (group similar types)
    plan_types = sorted(df['plan_type'].dropna().unique().tolist())

    # Create simplified version (PPO includes both Local and Regional PPO)
    simplified_plan_types = []
    for pt in plan_types:
        if pt in ['HMO', 'HMOPOS', 'HMO-POS', 'HMO/HMOPOS']:
            if 'HMO' not in simplified_plan_types:
                simplified_plan_types.append('HMO')
        elif pt in ['Local PPO', 'Regional PPO', 'PPO']:
            if 'PPO' not in simplified_plan_types:
                simplified_plan_types.append('PPO')
        elif pt not in simplified_plan_types:
            simplified_plan_types.append(pt)

    # Get SNP types (includes "SNP" for all historical + detailed types)
    snp_df = get_snp_by_parent()
    snp_types = ['Non-SNP', 'SNP']  # Non-SNP first (excludes all SNP), then generic "all SNP"
    if not snp_df.empty and 'snp_type' in snp_df.columns:
        detailed_types = [t for t in sorted(snp_df['snp_type'].dropna().unique().tolist()) if t != 'SNP']
        snp_types.extend(detailed_types)

    # Get states from state-level enrollment data
    state_df = get_enrollment_by_state_data()
    states = []
    if not state_df.empty and 'state' in state_df.columns:
        states = sorted(state_df['state'].dropna().unique().tolist())

    return {
        "years": sorted(df['year'].unique().tolist()),
        "plan_types": plan_types,  # Original types
        "plan_types_simplified": sorted(simplified_plan_types),  # Grouped types
        "product_types": sorted(df['product_type'].dropna().unique().tolist()),
        "group_types": group_types,
        "snp_types": snp_types,  # D-SNP, C-SNP, I-SNP
        "states": states,  # State codes: CA, TX, FL, etc.
        "parent_orgs": sorted_parents  # All parents, sorted by enrollment
    }


@app.get("/api/v2/enrollment/counties")
async def get_counties_for_states(states: Optional[str] = None):
    """Get counties for selected states."""
    county_df = get_county_lookup()
    if county_df.empty:
        return {"counties": []}

    if states:
        state_list = [s.strip().upper() for s in states.split(',')]
        county_df = county_df[county_df['state'].isin(state_list)]

    # Return as list of {state, county} objects
    counties = county_df.to_dict('records')
    return {"counties": counties}


@app.get("/api/v2/enrollment/snp")
async def get_snp_timeseries(
    snp_types: Optional[str] = None,  # Comma-separated: D-SNP,C-SNP,I-SNP
    group_by: Optional[str] = None    # Optional: snp_type
):
    """Get SNP enrollment time series."""
    df = get_snp_enrollment()

    if df.empty:
        return {"error": "No SNP data available", "years": [], "total_enrollment": []}

    # Apply filters
    if snp_types:
        type_list = [t.strip() for t in snp_types.split(',')]
        df = df[df['snp_type'].isin(type_list)]

    # Group and aggregate
    if group_by == 'snp_type':
        grouped = df.groupby(['year', 'snp_type'])['enrollment'].sum().reset_index()
        pivot = grouped.pivot(index='year', columns='snp_type', values='enrollment').fillna(0)
        years = pivot.index.tolist()
        series = {col: pivot[col].tolist() for col in pivot.columns}

        return {
            "years": years,
            "group_by": "snp_type",
            "series": series
        }
    else:
        by_year = df.groupby('year')['enrollment'].sum().reset_index()

        return {
            "years": by_year['year'].tolist(),
            "total_enrollment": by_year['enrollment'].tolist()
        }


@app.get("/api/enrollment/timeseries")
async def get_enrollment_timeseries(
    parent_org: Optional[str] = None,
    product_type: Optional[str] = None,
    state: Optional[str] = None
):
    """Get enrollment time series with optional filters."""
    df = get_enrollment_data()

    if parent_org:
        df = df[df['parent_org'] == parent_org]

    # Aggregate by year
    by_year = df.groupby('year').agg({
        'total_enrollment': 'sum',
        'contract_count': 'sum',
        'plan_count': 'sum',
        'county_count': 'sum'
    }).reset_index()

    return {
        "years": by_year['year'].tolist(),
        "total_enrollment": by_year['total_enrollment'].tolist(),
        "contract_count": by_year['contract_count'].tolist(),
        "plan_count": by_year['plan_count'].tolist()
    }


@app.get("/api/enrollment/by-parent")
async def get_enrollment_by_parent(
    year: Optional[int] = None,
    top_n: int = 20
):
    """Get enrollment breakdown by parent organization."""
    df = get_enrollment_data()

    if year:
        df = df[df['year'] == year]
    else:
        # Use latest year
        year = df['year'].max()
        df = df[df['year'] == year]

    # Sort by enrollment
    df = df.sort_values('total_enrollment', ascending=False).head(top_n)

    return {
        "year": int(year),
        "data": df[['parent_org', 'total_enrollment', 'contract_count', 'plan_count', 'county_count']].to_dict('records')
    }


@app.get("/api/enrollment/market-share")
async def get_market_share(year: Optional[int] = None):
    """Get market share by parent organization."""
    df = get_enrollment_data()

    if year:
        df = df[df['year'] == year]
    else:
        year = df['year'].max()
        df = df[df['year'] == year]

    total = df['total_enrollment'].sum()
    df['market_share'] = (df['total_enrollment'] / total * 100).round(2)
    df = df.sort_values('market_share', ascending=False)

    return {
        "year": int(year),
        "total_enrollment": int(total),
        "data": df[['parent_org', 'total_enrollment', 'market_share']].to_dict('records')
    }


# === Stars Endpoints ===

def get_enrollment_detail(enrollment_year: int = 2026, month: int = 1):
    """Load detailed enrollment data from fact_enrollment by year/month."""
    try:
        key = f'processed/fact_enrollment/{enrollment_year}/{month:02d}/data.parquet'
        return load_parquet(key)
    except:
        try:
            # Try without zero-padding
            key = f'processed/fact_enrollment/{enrollment_year}/{month}/data.parquet'
            return load_parquet(key)
        except:
            return pd.DataFrame()


@app.get("/api/stars/distribution")
async def get_stars_distribution(
    parent_orgs: Optional[str] = None,  # Pipe-separated list
    star_year: Optional[int] = 2025  # Star rating year
):
    """
    Get star rating distribution by enrollment (FAST - uses pre-computed unified table).
    Returns industry distribution plus optional payer breakdowns.
    """
    # Use pre-computed unified table (LRU cached, instant after first load)
    df = get_stars_enrollment_unified()

    if df.empty:
        return {"error": "Stars data not available"}

    # Filter to requested star year
    df = df[df['star_year'] == star_year]

    if df.empty:
        return {"error": f"No star ratings found for {star_year}"}

    payment_year = star_year + 1

    # Calculate industry distribution
    industry_dist = df.groupby('star_band').agg({
        'enrollment': 'sum',
        'contract_id': 'nunique'
    }).reset_index()
    industry_dist.columns = ['star_band', 'enrollment', 'contracts']

    total_enrollment = int(industry_dist['enrollment'].sum())
    industry_dist['pct'] = (industry_dist['enrollment'] / total_enrollment * 100).round(1)

    result = {
        "star_year": int(star_year),
        "payment_year": int(payment_year),
        "total_enrollment": total_enrollment,
        "total_contracts": int(industry_dist['contracts'].sum()),
        "columns": {
            "Industry": {
                "total_enrollment": total_enrollment,
                "distribution": {
                    float(row['star_band']): {
                        "enrollment": int(row['enrollment']),
                        "contracts": int(row['contracts']),
                        "pct": float(row['pct'])
                    }
                    for _, row in industry_dist.iterrows()
                }
            }
        }
    }

    # Add payer breakdowns if requested
    if parent_orgs:
        parent_list = [p.strip() for p in parent_orgs.split('|')]
        for parent in parent_list:
            payer_data = df[df['parent_org'] == parent]
            if payer_data.empty:
                continue

            payer_dist = payer_data.groupby('star_band').agg({
                'enrollment': 'sum',
                'contract_id': 'nunique'
            }).reset_index()
            payer_dist.columns = ['star_band', 'enrollment', 'contracts']

            payer_total = int(payer_dist['enrollment'].sum())
            payer_dist['pct'] = (payer_dist['enrollment'] / payer_total * 100).round(1) if payer_total > 0 else 0

            result["columns"][parent] = {
                "total_enrollment": payer_total,
                "distribution": {
                    float(row['star_band']): {
                        "enrollment": int(row['enrollment']),
                        "contracts": int(row['contracts']),
                        "pct": float(row['pct'])
                    }
                    for _, row in payer_dist.iterrows()
                }
            }

    return result


@app.get("/api/stars/distribution-timeseries")
async def get_stars_distribution_timeseries(
    parent_org: Optional[str] = None,  # Single parent org (or "Industry")
    plan_types: Optional[str] = None,   # Comma-separated: HMO,PPO,PFFS
    group_types: Optional[str] = None,  # Comma-separated: Group,Individual
    snp_types: Optional[str] = None,    # Comma-separated: SNP,Non-SNP
):
    """
    Get star distribution over time (FAST - uses pre-computed unified table).
    Returns % in each star band for each year.
    """
    df = get_stars_enrollment_unified()

    if df.empty:
        return {"error": "Stars data not available"}

    # Apply filters
    if parent_org and parent_org != "Industry":
        df = df[df['parent_org'] == parent_org]

    if plan_types:
        plan_list = [p.strip() for p in plan_types.split(',')]
        if 'plan_type_normalized' in df.columns:
            normalized_list = []
            for pt in plan_list:
                if pt in ['HMO', 'HMO/HMOPOS']:
                    normalized_list.append('HMO/HMOPOS')
                elif pt in ['PPO', 'Local PPO', 'Regional PPO']:
                    normalized_list.append('PPO')
                elif pt == 'PFFS':
                    normalized_list.append('PFFS')
                elif pt == 'MSA':
                    normalized_list.append('MSA')
                else:
                    normalized_list.append(pt)
            df = df[df['plan_type_normalized'].isin(normalized_list)]

    if group_types:
        group_list = [g.strip() for g in group_types.split(',')]
        df = df[df['group_type'].isin(group_list)]

    if snp_types:
        snp_list = [s.strip() for s in snp_types.split(',')]
        if any(s in ['D-SNP', 'C-SNP', 'I-SNP', 'SNP'] for s in snp_list):
            df = df[df['snp_type'] == 'SNP']
        elif 'Non-SNP' in snp_list:
            df = df[df['snp_type'] == 'Non-SNP']

    if df.empty:
        return {"error": "No data for selected filters", "years": [], "distribution": {}}

    # Get all years
    years = sorted(df['star_year'].unique().tolist())
    star_bands = [5.0, 4.5, 4.0, 3.5, 3.0, 2.5]

    # Build distribution for each year
    distribution = {band: [] for band in star_bands}
    distribution['4+'] = []
    totals = []

    for year in years:
        year_df = df[df['star_year'] == year]
        total = year_df['enrollment'].sum()
        totals.append(int(total))

        fourplus_enrollment = 0
        for band in star_bands:
            band_enrollment = year_df[year_df['star_band'] == band]['enrollment'].sum()
            pct = round(band_enrollment / total * 100, 1) if total > 0 else 0
            distribution[band].append(pct)
            if band >= 4.0:
                fourplus_enrollment += band_enrollment

        fourplus_pct = round(fourplus_enrollment / total * 100, 1) if total > 0 else 0
        distribution['4+'].append(fourplus_pct)

    return {
        "years": years,
        "distribution": distribution,
        "totals": totals,
        "filters": {
            "parent_org": parent_org or "Industry",
            "plan_types": plan_types,
            "group_types": group_types,
            "snp_types": snp_types
        }
    }


def get_stars_enrollment_unified():
    """Load unified stars + enrollment table with all dimensions (FAST)."""
    sql = """
        SELECT * FROM stars_enrollment_unified
    """
    def fallback():
        try:
            df = load_parquet('processed/unified/stars_enrollment_unified.parquet')
            return consolidate_parent_org_names(df)
        except:
            return pd.DataFrame()

    df = query_duckdb(sql, fallback)
    return consolidate_parent_org_names(df) if not df.empty else df


@app.get("/api/stars/fourplus-timeseries")
async def get_stars_fourplus_timeseries(
    parent_orgs: Optional[str] = None,  # Pipe-separated list
    plan_types: Optional[str] = None,   # Comma-separated: HMO,PPO,PFFS,MSA
    group_types: Optional[str] = None,  # Comma-separated: Group,Individual
    snp_types: Optional[str] = None,    # Comma-separated: D-SNP,C-SNP,I-SNP
    include_total: bool = True          # Include "Industry" when filtering by payers
):
    """
    Get 4+ star enrollment percentage time series.
    Returns % of enrollment in 4+ star rated contracts over time.
    Uses unified table for instant filtering (~50ms with any filter combo).
    """
    # Load unified table (single S3 read, cached in memory by LRU)
    df = get_stars_enrollment_unified()

    if df.empty:
        return {"error": "Stars data not available - run build_stars_enrollment_unified.py"}

    # Apply filters in memory (instant)
    if plan_types:
        plan_list = [p.strip() for p in plan_types.split(',')]
        # Use normalized plan type column for cleaner filtering
        if 'plan_type_normalized' in df.columns:
            # Map filter values to normalized values
            normalized_list = []
            for pt in plan_list:
                if pt in ['HMO', 'HMO/HMOPOS']:
                    normalized_list.append('HMO/HMOPOS')
                elif pt in ['PPO', 'Local PPO', 'Regional PPO']:
                    normalized_list.append('PPO')
                elif pt == 'PFFS':
                    normalized_list.append('PFFS')
                elif pt == 'MSA':
                    normalized_list.append('MSA')
                else:
                    normalized_list.append(pt)
            df = df[df['plan_type_normalized'].isin(normalized_list)]
        else:
            # Fall back to original plan_type
            expanded = []
            for pt in plan_list:
                if pt in PLAN_TYPE_MAP:
                    expanded.extend(PLAN_TYPE_MAP[pt])
                else:
                    expanded.append(pt)
            df = df[df['plan_type'].isin(expanded)]

    if group_types:
        group_list = [g.strip() for g in group_types.split(',')]
        df = df[df['group_type'].isin(group_list)]

    if snp_types:
        snp_list = [s.strip() for s in snp_types.split(',')]
        # Handle SNP filtering - we only have SNP/Non-SNP in data
        # If any specific SNP type is selected (D-SNP, C-SNP, I-SNP), filter to SNP
        if any(s in ['D-SNP', 'C-SNP', 'I-SNP', 'SNP'] for s in snp_list):
            df = df[df['snp_type'] == 'SNP']
        elif 'Non-SNP' in snp_list:
            df = df[df['snp_type'] == 'Non-SNP']

    if df.empty:
        return {"error": "No data matches the selected filters"}

    # Get available years
    years = sorted(df['star_year'].unique().tolist())

    # Parse parent orgs
    parent_list = [p.strip() for p in parent_orgs.split('|')] if parent_orgs else []

    # Calculate 4+ star % per year
    series = {}

    # Industry (filtered) data
    if include_total or not parent_list:
        industry_pcts = []
        for year in years:
            year_df = df[df['star_year'] == year]
            total = year_df['enrollment'].sum()
            fourplus = year_df[year_df['is_fourplus']]['enrollment'].sum()
            pct = round((fourplus / total * 100), 1) if total > 0 else 0
            industry_pcts.append(pct)
        series['Industry'] = industry_pcts

    # Per-payer data
    for parent in parent_list:
        payer_pcts = []
        for year in years:
            payer_year_df = df[(df['star_year'] == year) & (df['parent_org'] == parent)]
            if payer_year_df.empty:
                payer_pcts.append(None)
            else:
                total = payer_year_df['enrollment'].sum()
                fourplus = payer_year_df[payer_year_df['is_fourplus']]['enrollment'].sum()
                pct = round((fourplus / total * 100), 1) if total > 0 else 0
                payer_pcts.append(pct)
        series[parent] = payer_pcts

    return {
        "years": years,
        "series": series,
        "filters_applied": {
            "plan_types": plan_types,
            "group_types": group_types,
            "snp_types": snp_types
        }
    }


@app.get("/api/stars/summary")
async def get_stars_summary_endpoint(year: Optional[int] = None):
    """Get stars ratings summary."""
    df = get_stars_summary()

    if year:
        df = df[df['rating_year'] == year]

    # Replace NaN with None for JSON serialization
    df_clean = df.head(100).fillna('')

    return {
        "years": sorted(df['rating_year'].unique().tolist()),
        "total_contracts": len(df),
        "data": df_clean.to_dict('records')
    }


def parse_star_rating(value):
    """Parse star rating from various formats like '3.5 out of 5 stars', '3.5', etc."""
    import re
    if pd.isna(value):
        return None
    val_str = str(value).strip()

    # Skip non-rated entries
    skip_patterns = ['not enough', 'too new', 'not applicable', 'n/a']
    if any(p in val_str.lower() for p in skip_patterns):
        return None

    # Try to extract number from "X out of 5" format
    match = re.search(r'([\d.]+)\s*(?:out\s*of\s*5)?', val_str, re.IGNORECASE)
    if match:
        try:
            rating = float(match.group(1))
            if 1 <= rating <= 5:
                return rating
        except:
            pass
    return None


@app.get("/api/stars/by-band")
async def get_stars_by_band(year: Optional[int] = None):
    """Get enrollment distribution by star rating band."""
    stars = get_stars_summary()

    if year:
        stars = stars[stars['rating_year'] == year]
    else:
        year = stars['rating_year'].max()
        stars = stars[stars['rating_year'] == year]

    # Find the year-specific overall column (e.g., "2025 Overall")
    rating_col = f"{year} Overall"
    if rating_col not in stars.columns:
        # Fallback to generic overall column
        for col in stars.columns:
            if 'overall' in col.lower() and str(year) not in col:
                rating_col = col
                break

    if rating_col not in stars.columns:
        return {"error": f"No rating column found for {year}", "year": int(year)}

    # Parse star ratings
    stars['rating'] = stars[rating_col].apply(parse_star_rating)
    stars['band'] = stars['rating'].apply(lambda x: int(round(x)) if pd.notna(x) else None)

    # Count by band
    band_counts = stars['band'].value_counts().to_dict()

    return {
        "year": int(year),
        "bands": {str(int(k)): int(v) for k, v in band_counts.items() if pd.notna(k)}
    }


@app.get("/api/stars/measures")
async def get_measure_performance(
    year: Optional[int] = None,
    contract_id: Optional[str] = None,
    measure_id: Optional[str] = None
):
    """Get measure-level performance data."""
    df = get_measure_data()

    if year:
        df = df[df['year'] == year]
    if contract_id:
        df = df[df['contract_id'] == contract_id]
    if measure_id:
        df = df[df['measure_id'] == measure_id]

    # Limit response size
    df = df.head(1000)

    return {
        "count": len(df),
        "data": df.to_dict('records')
    }


@app.get("/api/stars/measure-summary")
async def get_measure_summary(year: Optional[int] = None):
    """Get summary statistics by measure."""
    df = get_measure_data()

    if year:
        df = df[df['year'] == year]
    else:
        year = df['year'].max()
        df = df[df['year'] == year]

    # Filter to only records with valid star ratings
    df = df[df['star_rating'].notna()]

    # Aggregate by measure
    summary = df.groupby('measure_id').agg({
        'star_rating': ['mean', 'count'],
        'contract_id': 'nunique'
    }).reset_index()
    summary.columns = ['measure_id', 'avg_rating', 'rating_count', 'contract_count']
    summary = summary.sort_values('measure_id')

    # Convert to records with NaN handling
    records = []
    for _, row in summary.iterrows():
        records.append({
            'measure_id': row['measure_id'],
            'avg_rating': float(row['avg_rating']) if pd.notna(row['avg_rating']) else None,
            'rating_count': int(row['rating_count']),
            'contract_count': int(row['contract_count'])
        })

    return {
        "year": int(year),
        "measures": records
    }


@app.get("/api/stars/cutpoints")
async def get_cutpoints(year: Optional[int] = 2026):
    """
    Get star rating cutpoints for each measure.
    Cutpoints are the thresholds that determine star ratings.
    """
    # Try to load cutpoints data
    try:
        df = load_parquet(f'processed/stars/cutpoints/{year}/data.parquet')
    except:
        # Return empty if no data
        return {
            "year": year,
            "measures": [],
            "error": f"Cutpoints data for {year} not yet processed"
        }

    if df.empty:
        return {
            "year": year,
            "measures": [],
            "error": f"No cutpoints data for {year}"
        }

    # Convert to records
    measures = []
    for _, row in df.iterrows():
        measures.append({
            "measure_id": str(row.get('measure_id', '')),
            "measure_key": str(row.get('measure_key', '')),  # Stable key for cross-year tracking (IDs change!)
            "measure_name": str(row.get('measure_name', '')),
            "domain": str(row.get('domain', '')),
            "weight": float(row.get('weight', 1)) if pd.notna(row.get('weight')) else 1,
            "lower_is_better": bool(row.get('lower_is_better', False)),
            "cut_5": str(row.get('cut_5', '')) if pd.notna(row.get('cut_5')) else None,
            "cut_4": str(row.get('cut_4', '')) if pd.notna(row.get('cut_4')) else None,
            "cut_3": str(row.get('cut_3', '')) if pd.notna(row.get('cut_3')) else None,
            "cut_2": str(row.get('cut_2', '')) if pd.notna(row.get('cut_2')) else None,
        })

    return {
        "year": year,
        "measures": measures
    }


def get_measure_stars_data():
    """Load measure-level star ratings data."""
    try:
        return load_parquet('processed/unified/fact_measure_stars.parquet')
    except:
        return pd.DataFrame()


@app.get("/api/stars/measure-enrollment")
async def get_measure_enrollment(
    measure_key: Optional[str] = None,
    star_year: Optional[int] = None,
    parent_org: Optional[str] = None,
):
    """
    Get enrollment distribution by star band for a specific measure.
    Shows what % of enrollment is at each star level (1-5) for that measure.
    """
    df = get_measure_stars_data()

    if df.empty:
        return {"error": "Measure stars data not available - run build_measure_stars.py"}

    # Apply filters
    if star_year:
        df = df[df['star_year'] == star_year]

    if measure_key:
        df = df[df['measure_key'] == measure_key]

    if parent_org and parent_org != "Industry":
        df = df[df['parent_org'] == parent_org]

    if df.empty:
        return {"error": "No data for selected filters"}

    # Filter to records with valid star ratings and enrollment
    df = df[df['star_rating'].notna() & df['enrollment'].notna()]

    if df.empty:
        return {"error": "No enrollment data available for this measure"}

    # Group by star rating and sum enrollment
    distribution = df.groupby('star_rating').agg({
        'enrollment': 'sum',
        'contract_id': 'nunique'
    }).reset_index()

    total_enrollment = distribution['enrollment'].sum()
    distribution['pct'] = (distribution['enrollment'] / total_enrollment * 100).round(1)

    # Format response
    bands = {}
    for _, row in distribution.iterrows():
        rating = int(row['star_rating'])
        bands[rating] = {
            "enrollment": int(row['enrollment']),
            "contracts": int(row['contract_id']),
            "pct": float(row['pct'])
        }

    # Calculate 4+ star summary
    fourplus_enrollment = sum(bands.get(r, {}).get('enrollment', 0) for r in [4, 5])
    fourplus_pct = round(fourplus_enrollment / total_enrollment * 100, 1) if total_enrollment > 0 else 0

    return {
        "measure_key": measure_key,
        "star_year": star_year,
        "parent_org": parent_org or "Industry",
        "total_enrollment": int(total_enrollment),
        "distribution": bands,
        "fourplus": {
            "enrollment": int(fourplus_enrollment),
            "pct": fourplus_pct
        }
    }


@app.get("/api/stars/measure-enrollment-timeseries")
async def get_measure_enrollment_timeseries(
    measure_key: str,
    parent_org: Optional[str] = None,
    part: Optional[str] = None,
):
    """
    Get enrollment distribution by star band for a measure across all years.
    Returns data suitable for showing % at 4+ stars over time.

    Args:
        measure_key: The measure identifier
        parent_org: Optional parent organization filter
        part: Optional 'C' or 'D' to filter by Part C or Part D ratings
              (some measures like Call Center have both C and D ratings)
    """
    df = get_measure_stars_data()

    if df.empty:
        return {"error": "Measure stars data not available"}

    # Filter by measure
    df = df[df['measure_key'] == measure_key]

    if parent_org and parent_org != "Industry":
        df = df[df['parent_org'] == parent_org]

    # Filter by part (C or D) if specified - important for dual-rated measures
    # like "Call Center" which has different Part C and Part D ratings
    if part and part in ['C', 'D']:
        df = df[df['measure_id'].str.startswith(part)]

    # Filter to valid data
    df = df[df['star_rating'].notna() & df['enrollment'].notna()]

    if df.empty:
        return {"error": "No data for this measure"}

    # Deduplicate by contract-year to avoid double-counting enrollment
    # (some measures still have duplicates within the same part)
    df = df.drop_duplicates(subset=['contract_id', 'star_year'], keep='first')

    years = sorted(df['star_year'].unique())
    result = {
        "measure_key": measure_key,
        "parent_org": parent_org or "Industry",
        "years": [int(y) for y in years],
        "distribution": {},
        "fourplus_pct": [],
        "fourplus_enrollment": [],
        "total_enrollment": []
    }

    for star in [1, 2, 3, 4, 5]:
        result["distribution"][star] = []

    for year in years:
        year_df = df[df['star_year'] == year]
        total = year_df['enrollment'].sum()
        result["total_enrollment"].append(int(total))

        fourplus_enrollment = 0
        for star in [1, 2, 3, 4, 5]:
            star_enrollment = year_df[year_df['star_rating'] == star]['enrollment'].sum()
            pct = round(star_enrollment / total * 100, 1) if total > 0 else 0
            result["distribution"][star].append(pct)
            if star >= 4:
                fourplus_enrollment += star_enrollment

        fourplus_pct = round(fourplus_enrollment / total * 100, 1) if total > 0 else 0
        result["fourplus_pct"].append(fourplus_pct)
        result["fourplus_enrollment"].append(int(fourplus_enrollment))

    return result


@app.get("/api/stars/measure-enrollment-contracts")
async def get_measure_enrollment_contracts(
    measure_key: str,
    year: int,
    parent_org: Optional[str] = None,
    part: Optional[str] = None,
):
    """
    Get contract-level breakdown for 4★+ enrollment audit.
    Returns list of contracts with their star ratings and enrollment for a specific measure/year.
    """
    df = get_measure_stars_data()

    if df.empty:
        return {"error": "Measure stars data not available"}

    # Filter by measure and year
    df = df[(df['measure_key'] == measure_key) & (df['star_year'] == year)]

    if parent_org and parent_org != "Industry":
        df = df[df['parent_org'] == parent_org]

    # Filter by part (C or D) if specified
    if part and part in ['C', 'D']:
        df = df[df['measure_id'].str.startswith(part)]

    # Filter to valid data
    df = df[df['star_rating'].notna() & df['enrollment'].notna()]

    if df.empty:
        return {"error": "No data for this measure/year"}

    # Deduplicate by contract to avoid double-counting
    df = df.drop_duplicates(subset=['contract_id'], keep='first')

    # Build contract list
    contracts = []
    for _, row in df.iterrows():
        contracts.append({
            "contract_id": str(row['contract_id']),
            "parent_org": str(row['parent_org']) if pd.notna(row['parent_org']) else None,
            "star_rating": int(row['star_rating']),
            "enrollment": int(row['enrollment']),
        })

    # Sort by enrollment descending
    contracts.sort(key=lambda x: x['enrollment'], reverse=True)

    # Calculate totals
    total_enrollment = sum(c['enrollment'] for c in contracts)
    fourplus_contracts = [c for c in contracts if c['star_rating'] >= 4]
    fourplus_enrollment = sum(c['enrollment'] for c in fourplus_contracts)
    fourplus_pct = round(fourplus_enrollment / total_enrollment * 100, 1) if total_enrollment > 0 else 0

    return {
        "measure_key": measure_key,
        "year": year,
        "parent_org": parent_org or "Industry",
        "contracts": contracts,
        "summary": {
            "total_contracts": len(contracts),
            "fourplus_contracts": len(fourplus_contracts),
            "total_enrollment": total_enrollment,
            "fourplus_enrollment": fourplus_enrollment,
            "fourplus_pct": fourplus_pct,
        }
    }


@app.get("/api/stars/cutpoints-timeseries")
async def get_cutpoints_timeseries():
    """
    Get cutpoints for ALL measures across ALL years.
    Organized by measure_key (stable identifier) for cross-year trending.
    Returns data suitable for line charts showing cutpoint evolution.
    """
    import re

    years = [2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019]
    all_data = []

    for year in years:
        try:
            df = load_parquet(f'processed/stars/cutpoints/{year}/data.parquet')
            df['year'] = year
            all_data.append(df)
        except:
            continue

    if not all_data:
        return {"measures": [], "years": [], "error": "No cutpoints data available"}

    combined = pd.concat(all_data, ignore_index=True)

    # Helper to extract numeric value from cutpoint string
    def extract_numeric(val, lower_is_better=False):
        if pd.isna(val) or not val:
            return None
        val_str = str(val)

        # For lower-is-better measures (e.g., "> 7 % to <= 9 %"):
        # - The 4★ range means you need to be ABOVE lower bound and AT/BELOW upper bound
        # - We graph the UPPER bound (the maximum allowed to achieve this star level)
        # For higher-is-better measures (e.g., ">= 76 % to < 84 %"):
        # - We graph the LOWER bound (the minimum required to achieve this star level)

        if lower_is_better:
            # Extract the upper bound (number after "<=")
            match = re.search(r'<=?\s*([0-9.]+)', val_str)
            if match:
                try:
                    return float(match.group(1))
                except:
                    pass
            # Fallback: get last number
            matches = re.findall(r'[\d.]+', val_str)
            if matches:
                try:
                    return float(matches[-1])
                except:
                    pass
        else:
            # Extract the lower bound (first number, typically after ">=")
            match = re.search(r'[\d.]+', val_str)
            if match:
                try:
                    return float(match.group())
                except:
                    pass
        return None

    # Group by measure_key to build time series
    measures = {}
    for measure_key in combined['measure_key'].unique():
        if not measure_key:
            continue

        mdf = combined[combined['measure_key'] == measure_key].sort_values('year')

        # Get measure metadata from most recent year
        latest = mdf[mdf['year'] == mdf['year'].max()].iloc[0]

        # Build yearly data
        yearly = {}
        for _, row in mdf.iterrows():
            yr = int(row['year'])
            is_lower = bool(row.get('lower_is_better', False))
            yearly[yr] = {
                "measure_id": row.get('measure_id', ''),
                "cut_5": row.get('cut_5'),
                "cut_4": row.get('cut_4'),
                "cut_3": row.get('cut_3'),
                "cut_2": row.get('cut_2'),
                "cut_5_num": extract_numeric(row.get('cut_5'), is_lower),
                "cut_4_num": extract_numeric(row.get('cut_4'), is_lower),
                "cut_3_num": extract_numeric(row.get('cut_3'), is_lower),
                "cut_2_num": extract_numeric(row.get('cut_2'), is_lower),
            }

        # Determine first year this measure appeared
        first_year = int(mdf['year'].min())
        years_active = sorted([int(y) for y in mdf['year'].unique()])

        measures[measure_key] = {
            "measure_key": measure_key,
            "measure_name": str(latest.get('measure_name', '')),
            "part": str(latest.get('part', '')),
            "domain": str(latest.get('domain', '')),
            "weight": float(latest.get('weight', 1)) if pd.notna(latest.get('weight')) else 1,
            "lower_is_better": bool(latest.get('lower_is_better', False)),
            "data_source": str(latest.get('data_source', 'HEDIS')) if pd.notna(latest.get('data_source')) else 'HEDIS',
            "cutpoint_method": str(latest.get('cutpoint_method', 'Clustering')) if pd.notna(latest.get('cutpoint_method')) else 'Clustering',
            "first_year": first_year,
            "years_active": years_active,
            "yearly": yearly,
        }

    return sanitize_for_json({
        "years": sorted(years),
        "measures": list(measures.values())
    })


# === Contract Detail Endpoints ===

@app.get("/api/contract/{contract_id}")
async def get_contract_detail(contract_id: str, year: Optional[int] = None):
    """Get detailed contract information."""
    stars = get_stars_summary()
    measures = get_measure_data()

    stars = stars[stars['contract_id'] == contract_id]
    measures = measures[measures['contract_id'] == contract_id]

    if year:
        stars = stars[stars['rating_year'] == year]
        measures = measures[measures['year'] == year]

    return {
        "contract_id": contract_id,
        "stars_data": stars.to_dict('records'),
        "measures": measures.to_dict('records')
    }


@app.get("/api/parent/{parent_org}")
async def get_parent_detail(parent_org: str):
    """Get all contracts and data for a parent organization."""
    stars = get_stars_summary()
    enrollment = get_enrollment_data()

    stars = stars[stars['parent_org'] == parent_org]
    enrollment = enrollment[enrollment['parent_org'] == parent_org]

    return {
        "parent_org": parent_org,
        "contracts": stars['contract_id'].unique().tolist(),
        "enrollment_history": enrollment[['year', 'total_enrollment', 'contract_count']].to_dict('records'),
        "ratings_history": stars[['rating_year', 'contract_id']].groupby('rating_year').size().to_dict()
    }


# === Lookup Endpoints ===

@app.get("/api/lookup/parents")
async def list_parent_orgs():
    """List all parent organizations."""
    df = get_parent_summary()
    df = df.sort_values('total_enrollment', ascending=False)
    return {
        "count": len(df),
        "parents": df[['parent_org', 'total_enrollment']].head(100).to_dict('records')
    }


@app.get("/api/lookup/years")
async def list_available_years():
    """List available data years."""
    enrollment = get_enrollment_data()
    stars = get_stars_summary()
    measures = get_measure_data()

    return {
        "enrollment_years": sorted(enrollment['year'].unique().tolist()),
        "stars_years": sorted(stars['rating_year'].unique().tolist()),
        "measure_years": sorted(measures['year'].unique().tolist())
    }


# === Risk Score Endpoints ===

@app.get("/api/risk-scores/summary")
async def get_risk_scores_summary(year: Optional[int] = None):
    """Get risk score summary statistics."""
    df = get_risk_scores()

    if df.empty:
        return {"error": "No risk score data available"}

    if year:
        df = df[df['year'] == year]
    else:
        year = int(df['year'].max())
        df = df[df['year'] == year]

    row = df.iloc[0] if len(df) > 0 else {}

    return {
        "year": int(year),
        "record_count": int(row.get('record_count', 0)),
        "mean_risk_score": float(row.get('avg_risk_score', 0)),
        "min_risk_score": float(row.get('min_risk_score', 0)),
        "max_risk_score": float(row.get('max_risk_score', 0)),
    }


@app.get("/api/risk-scores/timeseries")
async def get_risk_scores_timeseries():
    """Get risk score trends over time."""
    df = get_risk_scores()

    if df.empty:
        return {"error": "No risk score data available", "years": [], "avg_risk_score": []}

    df = df.sort_values('year')

    return {
        "years": df['year'].tolist(),
        "avg_risk_score": df['avg_risk_score'].round(3).tolist(),
        "record_count": df['record_count'].tolist()
    }


@app.get("/api/risk-scores/distribution")
async def get_risk_score_distribution(
    year: Optional[int] = None,
    plan_type: Optional[str] = None
):
    """Get risk score distribution by plan type."""
    df = get_risk_scores_detail()

    if df.empty:
        return {"error": "No risk score data available"}

    # Filter by plan type
    if plan_type and 'Plan Type' in df.columns:
        df = df[df['Plan Type'] == plan_type]

    df = df[df['risk_score'].notna()]
    df = df[(df['risk_score'] > 0.3) & (df['risk_score'] < 5)]

    # Create bins
    bins = [0, 0.8, 0.9, 1.0, 1.1, 1.2, 1.5, 2.0, 5.0]
    labels = ['<0.8', '0.8-0.9', '0.9-1.0', '1.0-1.1', '1.1-1.2', '1.2-1.5', '1.5-2.0', '>2.0']

    df['bin'] = pd.cut(df['risk_score'], bins=bins, labels=labels, include_lowest=True)
    distribution = df['bin'].value_counts().sort_index().to_dict()

    return {
        "year": 2024,
        "plan_type": plan_type or "All",
        "distribution": {str(k): int(v) for k, v in distribution.items() if pd.notna(k)}
    }


@app.get("/api/risk-scores/by-plan-type")
async def get_risk_scores_by_plan_type():
    """Get risk scores breakdown by plan type."""
    df = get_risk_scores_detail()

    if df.empty:
        return {"error": "No risk score data available"}

    df = df[df['risk_score'].notna()]
    df = df[(df['risk_score'] > 0.3) & (df['risk_score'] < 5)]

    by_type = df.groupby('Plan Type').agg({
        'risk_score': ['mean', 'min', 'max', 'count']
    }).round(3)
    by_type.columns = ['avg_risk_score', 'min_risk_score', 'max_risk_score', 'count']
    by_type = by_type.reset_index()

    return {
        "year": 2024,
        "data": by_type.to_dict('records')
    }


@app.get("/api/risk-scores/by-state")
async def get_risk_scores_by_state():
    """Get risk scores by state."""
    df = get_risk_scores_detail()

    if df.empty:
        return {"error": "No risk score data available"}

    df = df[df['risk_score'].notna()]
    df = df[(df['risk_score'] > 0.3) & (df['risk_score'] < 5)]

    by_state = df.groupby('State Abbreviation').agg({
        'risk_score': ['mean', 'count']
    }).round(3)
    by_state.columns = ['avg_risk_score', 'count']
    by_state = by_state.reset_index().sort_values('avg_risk_score', ascending=False)

    return {
        "year": 2024,
        "data": by_state.to_dict('records')
    }


# === Risk Score V2 Endpoints ===

@app.get("/api/v2/risk-scores/filters")
async def get_risk_score_filters_v2():
    """Get available filter options for risk scores."""
    # Use dimensions file which has snp_type, plan_type, group_type
    df = get_risk_scores_by_parent_dims()

    if df.empty:
        return {"error": "No risk score data available"}

    # Get unique values for filters
    years = sorted(df['year'].dropna().unique().tolist())

    # Get parent orgs sorted by latest year's enrollment
    parent_orgs = []
    if 'parent_org' in df.columns:
        # Get parent orgs with most enrollment in latest year
        latest_year = max(years) if years else 2024
        latest = df[df['year'] == latest_year]
        if 'enrollment' in latest.columns:
            by_parent = latest.groupby('parent_org')['enrollment'].sum().sort_values(ascending=False)
            parent_orgs = by_parent.index.dropna().tolist()
        else:
            parent_orgs = sorted(df['parent_org'].dropna().unique().tolist())

    plan_types = []
    if 'plan_type' in df.columns:
        plan_types = sorted(df['plan_type'].dropna().unique().tolist())

    plan_types_simplified = ['HMO', 'PPO', 'PFFS', 'MSA']  # PPO includes Regional PPO

    snp_types = []
    if 'snp_type' in df.columns:
        snp_types = sorted(df['snp_type'].dropna().unique().tolist())

    group_types = []
    if 'group_type' in df.columns:
        group_types = sorted(df['group_type'].dropna().unique().tolist())

    return {
        "years": years,
        "parent_orgs": parent_orgs[:100],  # Top 100 by enrollment
        "plan_types": plan_types,
        "plan_types_simplified": plan_types_simplified,
        "snp_types": snp_types,
        "group_types": group_types
    }


@app.get("/api/v2/risk-scores/timeseries")
async def get_risk_scores_timeseries_v2(
    parent_orgs: Optional[str] = None,  # Pipe-separated list
    plan_types: Optional[str] = None,   # Comma-separated: HMO,PPO,PFFS,MSA
    snp_types: Optional[str] = None,    # Comma-separated: D-SNP,C-SNP,I-SNP
    group_types: Optional[str] = None,  # Comma-separated: Group,Individual
    group_by: Optional[str] = None,     # Optional: plan_type, parent_org
    include_total: bool = True,         # Include "Industry Total" when filtering
    metric: str = "wavg"                # "avg" or "wavg" (weighted average)
):
    """
    Get risk score time series with multi-dimensional filtering.
    Supports filtering by parent_orgs, plan_types, snp_types, group_types.
    metric: "avg" for simple average, "wavg" for enrollment-weighted average
    """
    # Use dimensions file when filtering by plan_type, snp_type, or group_type
    # Otherwise use simple parent-year file (faster)
    has_dim_filters = plan_types or snp_types or group_types
    if has_dim_filters:
        df = get_risk_scores_by_parent_dims()
    else:
        df = get_risk_scores_by_parent_year()
    use_preagg = True

    if df.empty:
        return {"error": "No risk score data available", "years": [], "series": {}}

    # Filter out rows without risk scores
    if 'avg_risk_score' in df.columns:
        df = df[df['avg_risk_score'].notna()]
    if use_preagg and 'simple_avg_risk_score' in df.columns:
        df = df[df['simple_avg_risk_score'].notna()]

    if df.empty:
        return {"error": "No risk score data after filtering", "years": [], "series": {}}

    # Get all years for industry total calculation
    all_years = sorted(df['year'].unique())

    # Calculate industry total BEFORE filtering by payers
    if use_preagg:
        industry_by_year = df.groupby('year').agg({
            'simple_avg_risk_score': 'mean',
            'wavg_risk_score': lambda x: (x * df.loc[x.index, 'enrollment']).sum() / df.loc[x.index, 'enrollment'].sum() if df.loc[x.index, 'enrollment'].sum() > 0 else x.mean(),
            'enrollment': 'sum'
        }).reset_index()
    else:
        industry_agg = df.groupby('year').apply(
            lambda g: pd.Series({
                'avg': g['avg_risk_score'].mean(),
                'wavg': (g['avg_risk_score'] * g['enrollment'].fillna(1)).sum() / g['enrollment'].fillna(1).sum() if g['enrollment'].fillna(1).sum() > 0 else g['avg_risk_score'].mean(),
                'enrollment': g['enrollment'].sum()
            })
        ).reset_index()
        industry_by_year = industry_agg

    # Apply filters
    if parent_orgs:
        parent_list = [p.strip() for p in parent_orgs.split('|')]
        df = df[df['parent_org'].isin(parent_list)]

    if plan_types and 'plan_type' in df.columns:
        plan_list = [p.strip() for p in plan_types.split(',')]
        # Expand simplified types
        expanded = []
        for pt in plan_list:
            if pt in PLAN_TYPE_MAP:
                expanded.extend(PLAN_TYPE_MAP[pt])
            else:
                expanded.append(pt)
        df = df[df['plan_type'].isin(expanded)]

    if snp_types and 'snp_type' in df.columns:
        snp_list = [s.strip() for s in snp_types.split(',')]
        df = df[df['snp_type'].isin(snp_list)]

    if group_types and 'group_type' in df.columns:
        group_list = [g.strip() for g in group_types.split(',')]
        df = df[df['group_type'].isin(group_list)]

    if df.empty:
        return {"error": "No data after filtering", "years": [], "series": {}}

    # Auto-group by parent_org when payers selected (even single payer)
    effective_group_by = group_by
    if parent_orgs and not group_by:
        effective_group_by = 'parent_org'

    # Build series
    series = {}
    enrollment_series = {}

    if effective_group_by and effective_group_by in df.columns:
        # Group by the specified dimension
        for group_val in df[effective_group_by].dropna().unique():
            group_df = df[df[effective_group_by] == group_val]

            if use_preagg:
                # Pre-aggregated data
                group_by_year = group_df.groupby('year').agg({
                    'simple_avg_risk_score': 'mean',
                    'wavg_risk_score': 'mean',
                    'enrollment': 'sum'
                }).reindex(all_years)
            else:
                # Raw data - need to aggregate
                group_by_year = group_df.groupby('year').apply(
                    lambda g: pd.Series({
                        'avg': g['avg_risk_score'].mean(),
                        'wavg': (g['avg_risk_score'] * g['enrollment'].fillna(1)).sum() / g['enrollment'].fillna(1).sum() if g['enrollment'].fillna(1).sum() > 0 else g['avg_risk_score'].mean(),
                        'enrollment': g['enrollment'].sum()
                    })
                ).reindex(all_years)

            if metric == 'wavg':
                if use_preagg:
                    values = group_by_year['wavg_risk_score'].round(4).tolist()
                else:
                    values = group_by_year['wavg'].round(4).tolist()
            else:
                if use_preagg:
                    values = group_by_year['simple_avg_risk_score'].round(4).tolist()
                else:
                    values = group_by_year['avg'].round(4).tolist()

            series[str(group_val)] = [v if pd.notna(v) else None for v in values]
            enrollment_series[str(group_val)] = [
                int(v) if pd.notna(v) else None
                for v in group_by_year['enrollment'].tolist()
            ]
    else:
        # Single series (total)
        if use_preagg:
            by_year = df.groupby('year').agg({
                'simple_avg_risk_score': 'mean',
                'wavg_risk_score': lambda x: (x * df.loc[x.index, 'enrollment']).sum() / df.loc[x.index, 'enrollment'].sum() if df.loc[x.index, 'enrollment'].sum() > 0 else x.mean(),
                'enrollment': 'sum'
            }).reindex(all_years)
        else:
            by_year = df.groupby('year').apply(
                lambda g: pd.Series({
                    'avg': g['avg_risk_score'].mean(),
                    'wavg': (g['avg_risk_score'] * g['enrollment'].fillna(1)).sum() / g['enrollment'].fillna(1).sum() if g['enrollment'].fillna(1).sum() > 0 else g['avg_risk_score'].mean(),
                    'enrollment': g['enrollment'].sum()
                })
            ).reindex(all_years)

        if metric == 'wavg':
            if use_preagg:
                values = by_year['wavg_risk_score'].round(4).tolist()
            else:
                values = by_year['wavg'].round(4).tolist()
        else:
            if use_preagg:
                values = by_year['simple_avg_risk_score'].round(4).tolist()
            else:
                values = by_year['avg'].round(4).tolist()

        series['Total'] = [v if pd.notna(v) else None for v in values]
        enrollment_series['Total'] = [
            int(v) if pd.notna(v) else None
            for v in by_year['enrollment'].tolist()
        ]

    # Add industry total if requested
    if include_total and parent_orgs:
        if metric == 'wavg':
            if 'wavg' in industry_by_year.columns:
                industry_values = industry_by_year.set_index('year').reindex(all_years)['wavg'].round(4).tolist()
            else:
                industry_values = industry_by_year.set_index('year').reindex(all_years)['wavg_risk_score'].round(4).tolist()
        else:
            if 'avg' in industry_by_year.columns:
                industry_values = industry_by_year.set_index('year').reindex(all_years)['avg'].round(4).tolist()
            else:
                industry_values = industry_by_year.set_index('year').reindex(all_years)['simple_avg_risk_score'].round(4).tolist()

        series['Industry Total'] = [v if pd.notna(v) else None for v in industry_values]
        enrollment_series['Industry Total'] = [
            int(v) if pd.notna(v) else None
            for v in industry_by_year.set_index('year').reindex(all_years)['enrollment'].tolist()
        ]

    return {
        "years": [int(y) for y in all_years],
        "series": series,
        "enrollment": enrollment_series,
        "metric": metric,
        "group_by": effective_group_by
    }


@app.get("/api/v2/risk-scores/summary")
async def get_risk_scores_summary_v2_endpoint(year: Optional[int] = None):
    """Get risk score summary statistics (v2)."""
    df = get_risk_scores_summary_v2()

    if df.empty:
        return {"error": "No risk score data available"}

    if year:
        df = df[df['year'] == year]
    else:
        year = int(df['year'].max())
        df = df[df['year'] == year]

    row = df.iloc[0] if len(df) > 0 else {}

    # Also get enrollment-weighted average from unified data
    unified = get_risk_scores_unified()
    wavg = None
    total_enrollment = 0
    if not unified.empty:
        year_data = unified[unified['year'] == year]
        year_data = year_data[year_data['avg_risk_score'].notna()]
        if not year_data.empty and 'enrollment' in year_data.columns:
            total_enrollment = int(year_data['enrollment'].sum())
            weighted = (year_data['avg_risk_score'] * year_data['enrollment'].fillna(1)).sum()
            total_weight = year_data['enrollment'].fillna(1).sum()
            if total_weight > 0:
                wavg = round(weighted / total_weight, 4)

    return {
        "year": int(year),
        "record_count": int(row.get('record_count', 0)),
        "contract_count": int(row.get('contract_count', 0)),
        "mean_risk_score": float(row.get('avg_risk_score', 0)),
        "wavg_risk_score": wavg,
        "min_risk_score": float(row.get('min_risk_score', 0)),
        "max_risk_score": float(row.get('max_risk_score', 0)),
        "std_risk_score": float(row.get('std_risk_score', 0)) if pd.notna(row.get('std_risk_score')) else None,
        "total_enrollment": total_enrollment
    }


# === Enrollment Detail Endpoints ===

@app.get("/api/enrollment/by-plan-type")
async def get_enrollment_by_plan_type(year: Optional[int] = None):
    """Get enrollment breakdown by plan type (using county-level data)."""
    # Use the risk scores detail data which has plan type and enrollment info
    df = get_risk_scores_detail()

    if df.empty:
        return {"error": "No data available"}

    # Find plan type column
    plan_type_col = 'Plan Type' if 'Plan Type' in df.columns else None
    if not plan_type_col:
        return {"error": "Plan type column not found", "columns": df.columns.tolist()[:20]}

    # Find enrollment column - look for Enrolled column
    enrollment_col = None
    for col in ['Enrolled', 'Enrollment', 'Total Enrollment', 'enrollment']:
        if col in df.columns:
            enrollment_col = col
            break

    if not enrollment_col:
        # If no enrollment column, just count records
        by_type = df.groupby(plan_type_col).size().reset_index(name='plan_count')
        by_type.columns = ['plan_type', 'plan_count']
        by_type['total_enrollment'] = 0
        by_type = by_type.sort_values('plan_count', ascending=False)
    else:
        # Aggregate by plan type
        df[enrollment_col] = pd.to_numeric(df[enrollment_col], errors='coerce').fillna(0)
        by_type = df.groupby(plan_type_col).agg({
            enrollment_col: ['sum', 'count']
        }).round(0)
        by_type.columns = ['total_enrollment', 'plan_count']
        by_type = by_type.reset_index()
        by_type.columns = ['plan_type', 'total_enrollment', 'plan_count']
        by_type = by_type.sort_values('total_enrollment', ascending=False)

    return {
        "year": 2024,
        "data": by_type.to_dict('records')
    }


@app.get("/api/enrollment/by-state")
async def get_enrollment_by_state(year: Optional[int] = None):
    """Get enrollment breakdown by state (using county-level data)."""
    # Use the risk scores detail data which has state info
    df = get_risk_scores_detail()

    if df.empty:
        return {"error": "No data available"}

    # Find state column
    state_col = 'State Abbreviation' if 'State Abbreviation' in df.columns else None
    if not state_col:
        return {"error": "State column not found", "columns": df.columns.tolist()[:20]}

    # Find enrollment column
    enrollment_col = None
    for col in ['Enrolled', 'Enrollment', 'Total Enrollment', 'enrollment']:
        if col in df.columns:
            enrollment_col = col
            break

    if not enrollment_col:
        # If no enrollment column, just count records
        by_state = df.groupby(state_col).size().reset_index(name='plan_count')
        by_state.columns = ['state', 'plan_count']
        by_state['total_enrollment'] = 0
        by_state = by_state.sort_values('plan_count', ascending=False)
    else:
        # Aggregate by state
        df[enrollment_col] = pd.to_numeric(df[enrollment_col], errors='coerce').fillna(0)
        by_state = df.groupby(state_col).agg({
            enrollment_col: ['sum', 'count']
        }).round(0)
        by_state.columns = ['total_enrollment', 'plan_count']
        by_state = by_state.reset_index()
        by_state.columns = ['state', 'total_enrollment', 'plan_count']
        by_state = by_state.sort_values('total_enrollment', ascending=False)

    return {
        "year": 2024,
        "data": by_state.to_dict('records')
    }


@app.get("/api/stars/by-plan-type")
async def get_stars_by_plan_type(year: Optional[int] = None):
    """Get star ratings breakdown by organization type."""
    stars = get_stars_summary()

    if stars.empty:
        return {"error": "No stars data available"}

    if year:
        stars = stars[stars['rating_year'] == year]
    else:
        year = int(stars['rating_year'].max())
        stars = stars[stars['rating_year'] == year]

    # Find org type column (org_type in stars data)
    plan_type_col = None
    for col in ['org_type', 'Plan Type', 'plan_type', 'PlanType', 'organization_type']:
        if col in stars.columns:
            plan_type_col = col
            break

    if not plan_type_col:
        return {
            "year": int(year),
            "error": "Organization type column not found in stars data",
            "columns": stars.columns.tolist()[:20]
        }

    # Find the overall rating column
    rating_col = f"{year} Overall"
    if rating_col not in stars.columns:
        for col in stars.columns:
            if 'overall' in col.lower() and str(year) in col:
                rating_col = col
                break
        # Fallback to any overall column
        if rating_col not in stars.columns:
            for col in stars.columns:
                if 'overall' in col.lower():
                    rating_col = col
                    break

    if rating_col not in stars.columns:
        return {
            "year": int(year),
            "error": f"Overall rating column not found for {year}",
            "columns": stars.columns.tolist()[:20]
        }

    # Parse star ratings
    stars_copy = stars.copy()
    stars_copy['rating'] = stars_copy[rating_col].apply(parse_star_rating)
    stars_copy = stars_copy[stars_copy['rating'].notna()]

    # Aggregate by org type
    by_type = stars_copy.groupby(plan_type_col).agg({
        'rating': ['mean', 'count'],
        'contract_id': 'nunique'
    }).round(2)
    by_type.columns = ['avg_rating', 'rating_count', 'contract_count']
    by_type = by_type.reset_index()
    by_type.columns = ['plan_type', 'avg_rating', 'rating_count', 'contract_count']
    by_type = by_type.sort_values('contract_count', ascending=False)

    return {
        "year": int(year),
        "data": by_type.to_dict('records')
    }


def get_measures_2026_from_unified(engine) -> tuple:
    """
    Get 2026 measures from unified tables (consistent measure_key normalization).
    Uses composite key (measure_key + part) to distinguish Part C vs Part D.
    Returns (measures_df, measures_keys_list).
    """
    try:
        measures_2026_sql = """
        SELECT * FROM (
            SELECT DISTINCT 
                m.measure_id, 
                m.measure_key,
                CASE WHEN m.measure_id LIKE 'D%' THEN 'D' ELSE 'C' END as part,
                m.measure_key || '_' || CASE WHEN m.measure_id LIKE 'D%' THEN 'D' ELSE 'C' END as measure_key_part,
                m.measure_name,
                m.measure_name || ' (Part ' || CASE WHEN m.measure_id LIKE 'D%' THEN 'D' ELSE 'C' END || ')' as measure_name_with_part,
                FALSE as lower_is_better,
                CASE 
                    WHEN m.measure_id IN ('C30', 'D04') THEN 5.0
                    WHEN m.measure_id IN ('C18', 'D08', 'D09', 'D10') THEN 3.0
                    WHEN m.measure_id IN ('C22', 'C23', 'C24', 'C25', 'C26', 'C27', 'C28', 'C29', 'C31', 'C32', 'D02', 'D05', 'D06') THEN 2.0
                    ELSE 1.0
                END as weight
            FROM (SELECT DISTINCT measure_id, measure_key, measure_name FROM measures_all_years WHERE year = 2026) m
        ) sub
        ORDER BY part, measure_id
        """
        measures_2026 = engine.query(measures_2026_sql)
        # Use composite key for matching
        measures_2026_keys = measures_2026['measure_key_part'].tolist()
        return measures_2026, measures_2026_keys
    except Exception as e:
        print(f"Error loading 2026 measures from unified: {e}")
        return pd.DataFrame(), []


def get_weights_by_year_from_unified(engine, years: list) -> dict:
    """
    Get measure weights by year from unified tables.
    Returns {year: {measure_key_part: weight}}.
    Uses composite key (measure_key + part) to distinguish Part C vs Part D.
    """
    weights_by_year = {}
    try:
        # Get measure_key mapping with part from measures_all_years
        key_map_sql = f"""
        SELECT DISTINCT 
            year, 
            measure_id, 
            measure_key,
            CASE WHEN measure_id LIKE 'D%' THEN 'D' ELSE 'C' END as part,
            measure_key || '_' || CASE WHEN measure_id LIKE 'D%' THEN 'D' ELSE 'C' END as measure_key_part
        FROM measures_all_years 
        WHERE year BETWEEN {min(years)} AND {max(years)}
        """
        key_map_df = engine.query(key_map_sql)
        
        for _, row in key_map_df.iterrows():
            y = int(row['year'])
            mid = str(row['measure_id'])
            mkey_part = str(row['measure_key_part'])
            
            if y not in weights_by_year:
                weights_by_year[y] = {}
            
            # CMS weight rules
            if mid in ('C30', 'D04'):
                weight = 5.0
            elif mid in ('C18', 'D08', 'D09', 'D10'):
                weight = 3.0
            elif mid in ('C22', 'C23', 'C24', 'C25', 'C26', 'C27', 'C28', 'C29', 'C31', 'C32', 'D02', 'D05', 'D06'):
                weight = 2.0
            else:
                weight = 1.0
            weights_by_year[y][mkey_part] = weight
    except Exception as e:
        print(f"Error loading weights from unified: {e}")
    return weights_by_year


def get_measure_performance_aggregates():
    """Load pre-computed measure performance aggregates."""
    try:
        return load_parquet('processed/stars/measure_performance/aggregates.parquet')
    except:
        return pd.DataFrame()


@app.get("/api/stars/measure-performance")
async def get_measure_performance_table(
    parent_org: Optional[str] = None,  # None or "_INDUSTRY_" = industry, else specific payer
    plan_type: Optional[str] = None,
    snp_type: Optional[str] = None,
    group_type: Optional[str] = None,
    avg_type: str = "weighted",  # "simple" or "weighted"
):
    """
    Get measure performance table data.
    Returns average performance % for each measure by year.
    
    Queries directly from unified tables (measures_all_years + stars_enrollment_unified).

    Supports any combination of filters:
    - parent_org: payer name (e.g., 'Humana Inc.')
    - plan_type: HMO/HMOPOS, Local PPO, Regional PPO, PFFS, 1876 Cost
    - snp_type: SNP, Non-SNP
    - group_type: Individual, Group

    Args:
        avg_type: "simple" (mean) or "weighted" (enrollment-weighted)
    """
    from db import get_engine
    engine = get_engine()
    
    # Build filter clause for unified tables
    filter_parts = []
    if parent_org and parent_org not in ["_INDUSTRY_", "Industry"]:
        filter_parts.append(f"e.parent_org LIKE '%{parent_org}%'")
    if plan_type:
        filter_parts.append(f"e.plan_type = '{plan_type}'")
    if snp_type:
        filter_parts.append(f"e.snp_type = '{snp_type}'")
    if group_type:
        filter_parts.append(f"e.group_type = '{group_type}'")
    
    filter_sql = " AND " + " AND ".join(filter_parts) if filter_parts else ""
    
    # Query measures with enrollment from unified tables
    sql = f"""
    SELECT 
        m.year,
        m.contract_id,
        m.measure_id,
        m.measure_name,
        m.measure_key,
        m.numeric_value as performance_pct,
        COALESCE(e.parent_org, 'Unknown') as parent_org,
        COALESCE(e.enrollment, 0) as enrollment,
        e.plan_type,
        e.snp_type,
        e.group_type
    FROM measures_all_years m
    LEFT JOIN (
        SELECT contract_id, star_year, parent_org, enrollment, plan_type, snp_type, group_type
        FROM stars_enrollment_unified
    ) e ON m.contract_id = e.contract_id AND m.year = e.star_year
    WHERE m.numeric_value IS NOT NULL
      AND m.measure_key IS NOT NULL
      {filter_sql}
    """
    
    df = engine.query(sql)
    
    if df.empty:
        return {"error": "Measure performance data not available", "years": [], "measures": []}
    
    # Build display name for filters
    display_org = "Industry"
    if parent_org and parent_org not in ["_INDUSTRY_", "Industry"]:
        display_org = parent_org
    if plan_type:
        display_org += f" ({plan_type})"
    if snp_type:
        display_org += f" [{snp_type}]"
    if group_type:
        display_org += f" - {group_type}"

    years = sorted(df['year'].unique())
    
    # Use unified tables for 2026 measures (consistent measure_key normalization)
    measures_2026, measures_2026_keys = get_measures_2026_from_unified(engine)
    weights_by_year = get_weights_by_year_from_unified(engine, years)
    
    # Add part column and composite key to data
    df['part'] = df['measure_id'].apply(lambda x: 'D' if str(x).startswith('D') else 'C')
    df['measure_key_part'] = df['measure_key'] + '_' + df['part']
    
    # Aggregate data by measure_key_part and year (distinguishes Part C vs Part D)
    aggregated = {}  # {measure_key_part: {year: {value, contract_count, enrollment, measure_id, part, measure_name}}}
    
    for mkey_part in df['measure_key_part'].dropna().unique():
        mdf = df[df['measure_key_part'] == mkey_part]
        aggregated[mkey_part] = {}
        
        for year in years:
            ydf = mdf[mdf['year'] == year]
            if ydf.empty:
                continue
            
            # Filter to valid performance values
            valid = ydf[ydf['performance_pct'].notna()]
            if valid.empty:
                continue
            
            total_enrollment = valid['enrollment'].sum()
            
            # Calculate based on avg_type
            if avg_type == 'weighted' and total_enrollment > 0:
                value = (valid['performance_pct'] * valid['enrollment']).sum() / total_enrollment
            else:
                value = valid['performance_pct'].mean()
            
            sample = valid.iloc[0]
            aggregated[mkey_part][year] = {
                'value': float(value),
                'contract_count': len(valid),
                'enrollment': int(total_enrollment),
                'measure_id': str(sample['measure_id']),
                'part': str(sample['part']),
                'measure_key': str(sample['measure_key']),
                'measure_name': str(sample['measure_name']) if pd.notna(sample['measure_name']) else '',
            }
    
    # Build response with measures as rows, years as columns
    result_measures = []
    processed_keys = set()
    
    # First: measures in 2026 (in order by 2026 measure_id, using composite key)
    for _, m2026 in measures_2026.iterrows():
        mkey_part = m2026['measure_key_part']
        if mkey_part in processed_keys or mkey_part not in aggregated:
            continue
        processed_keys.add(mkey_part)
        
        yearly_data = {}
        for year in years:
            if year in aggregated[mkey_part]:
                yearly_data[int(year)] = {
                    'value': aggregated[mkey_part][year]['value'],
                    'contract_count': aggregated[mkey_part][year]['contract_count'],
                    'enrollment': aggregated[mkey_part][year]['enrollment'],
                    'measure_id': aggregated[mkey_part][year]['measure_id'],
                }
            else:
                yearly_data[int(year)] = None
        
        # Get weights for this measure across years (using composite key)
        measure_weights = {}
        for wy in weights_by_year:
            if mkey_part in weights_by_year[wy]:
                measure_weights[wy] = weights_by_year[wy][mkey_part]
        
        result_measures.append({
            'measure_id': str(m2026['measure_id']),
            'measure_key': str(m2026['measure_key']),
            'measure_key_part': mkey_part,
            'measure_name': str(m2026['measure_name_with_part']),  # Includes (Part C/D)
            'part': str(m2026['part']),
            'lower_is_better': bool(m2026['lower_is_better']) if pd.notna(m2026['lower_is_better']) else False,
            'in_2026': True,
            'yearly': yearly_data,
            'weights': measure_weights,
        })
    
    # Second: measures NOT in 2026 (discontinued, using composite key)
    discontinued_keys = [k for k in aggregated.keys() if k not in processed_keys]
    
    for mkey_part in sorted(discontinued_keys):
        # Get measure info from most recent year
        latest_year = max(aggregated[mkey_part].keys())
        sample_data = aggregated[mkey_part][latest_year]
        
        yearly_data = {}
        for year in years:
            if year in aggregated[mkey_part]:
                yearly_data[int(year)] = {
                    'value': aggregated[mkey_part][year]['value'],
                    'contract_count': aggregated[mkey_part][year]['contract_count'],
                    'enrollment': aggregated[mkey_part][year]['enrollment'],
                    'measure_id': aggregated[mkey_part][year]['measure_id'],
                }
            else:
                yearly_data[int(year)] = None
        
        # Get weights (using composite key)
        measure_weights = {}
        for wy in weights_by_year:
            if mkey_part in weights_by_year[wy]:
                measure_weights[wy] = weights_by_year[wy][mkey_part]
        
        part = sample_data['part']
        measure_name = sample_data['measure_name']
        measure_name_with_part = f"{measure_name} (Part {part})" if measure_name else mkey_part
        
        result_measures.append({
            'measure_id': sample_data['measure_id'],
            'measure_key': sample_data['measure_key'],
            'measure_key_part': mkey_part,
            'measure_name': measure_name_with_part,
            'part': part,
            'lower_is_better': False,
            'in_2026': False,
            'yearly': yearly_data,
            'weights': measure_weights,
        })

    # Validation stats
    total_measures = len(result_measures)
    measures_in_2026 = len([m for m in result_measures if m['in_2026']])

    return {
        'parent_org': display_org,
        'avg_type': avg_type,
        'years': [int(y) for y in years],
        'measures': result_measures,
        'validation': {
            'total_measures': total_measures,
            'measures_in_2026': measures_in_2026,
            'discontinued_measures': total_measures - measures_in_2026,
        }
    }


@app.get("/api/stars/measure-stars")
async def get_measure_stars_table(
    parent_org: Optional[str] = None,
    plan_type: Optional[str] = None,
    snp_type: Optional[str] = None,
    group_type: Optional[str] = None,
    avg_type: str = "weighted",  # "weighted", "simple", or "pct_fourplus"
):
    """
    Get measure stars table data.
    Returns average star ratings (1-5) for each measure by year.
    
    Supports any combination of filters:
    - parent_org: payer name (e.g., 'Humana Inc.')
    - plan_type: HMO/HMOPOS, Local PPO, Regional PPO, PFFS, 1876 Cost
    - snp_type: SNP, Non-SNP
    - group_type: Individual, Group
    
    avg_type options:
    - "weighted": enrollment-weighted average star rating
    - "simple": simple average star rating
    - "pct_fourplus": % of enrollment in 4+ star contracts
    """
    from db import get_engine
    engine = get_engine()
    
    # Build filter clause for unified tables
    filter_parts = []
    if parent_org and parent_org not in ["_INDUSTRY_", "Industry"]:
        filter_parts.append(f"e.parent_org LIKE '%{parent_org}%'")
    if plan_type:
        filter_parts.append(f"e.plan_type = '{plan_type}'")
    if snp_type:
        filter_parts.append(f"e.snp_type = '{snp_type}'")
    if group_type:
        filter_parts.append(f"e.group_type = '{group_type}'")
    
    filter_sql = " AND " + " AND ".join(filter_parts) if filter_parts else ""
    
    # Get measure stars with enrollment for weighting
    # Use stars_enrollment_unified which has data from 2013-2026
    sql = f"""
    SELECT 
        ms.year,
        ms.contract_id,
        ms.measure_id,
        ms.star_rating,
        ms._source_file,
        COALESCE(e.parent_org, 'Unknown') as parent_org,
        COALESCE(e.enrollment, 0) as enrollment,
        e.plan_type,
        e.snp_type,
        e.group_type,
        m.measure_name,
        m.measure_key
    FROM measure_stars_all_years ms
    LEFT JOIN (
        SELECT contract_id, star_year, parent_org, enrollment, plan_type, snp_type, group_type
        FROM stars_enrollment_unified
    ) e ON ms.contract_id = e.contract_id AND ms.year = e.star_year
    LEFT JOIN (
        SELECT DISTINCT measure_id, measure_name, measure_key, year
        FROM measures_all_years
    ) m ON ms.measure_id = m.measure_id AND ms.year = m.year
    WHERE ms.star_rating IS NOT NULL
    {filter_sql}
    """
    
    df = engine.query(sql)
    
    if df.empty:
        return {"error": "Measure stars data not available", "years": [], "measures": []}
    
    # Build display name for filters
    display_org = "Industry"
    if parent_org and parent_org not in ["_INDUSTRY_", "Industry"]:
        display_org = parent_org
    if plan_type:
        display_org += f" ({plan_type})"
    if snp_type:
        display_org += f" [{snp_type}]"
    if group_type:
        display_org += f" - {group_type}"
    
    years = sorted(df['year'].unique())
    
    # Use unified tables for 2026 measures (consistent measure_key normalization)
    measures_2026, measures_2026_keys = get_measures_2026_from_unified(engine)
    weights_by_year = get_weights_by_year_from_unified(engine, years)
    
    # Add part column and composite key to data
    df['part'] = df['measure_id'].apply(lambda x: 'D' if str(x).startswith('D') else 'C')
    df['measure_key_part'] = df['measure_key'] + '_' + df['part']
    
    # Aggregate by year/measure_key_part (distinguishes Part C vs Part D)
    aggregated = []
    for mkey_part in df['measure_key_part'].dropna().unique():
        mdf = df[df['measure_key_part'] == mkey_part]
        for year in years:
            ydf = mdf[mdf['year'] == year]
            if ydf.empty:
                continue
            
            total_enrollment = ydf['enrollment'].sum()
            
            # Calculate based on avg_type
            if avg_type == 'pct_fourplus':
                # % of enrollment in 4+ star contracts - requires enrollment data
                if total_enrollment <= 0:
                    # No enrollment data = can't calculate this metric, skip
                    continue
                fourplus_enrollment = ydf[ydf['star_rating'] >= 4]['enrollment'].sum()
                value = (fourplus_enrollment / total_enrollment) * 100
            elif avg_type == 'weighted' and total_enrollment > 0:
                value = (ydf['star_rating'] * ydf['enrollment']).sum() / total_enrollment
            else:
                value = ydf['star_rating'].mean()
            
            sample = ydf.iloc[0]
            aggregated.append({
                'year': int(year),
                'measure_key': str(sample['measure_key']),
                'measure_key_part': mkey_part,
                'part': str(sample['part']),
                'measure_id': str(sample['measure_id']),
                'measure_name': str(sample['measure_name']) if pd.notna(sample['measure_name']) else mkey_part,
                'avg_star': float(value),
                'contract_count': len(ydf),
                'total_enrollment': int(total_enrollment),
                'fourplus_count': int(len(ydf[ydf['star_rating'] >= 4])),
                'fourplus_enrollment': int(ydf[ydf['star_rating'] >= 4]['enrollment'].sum()),
            })
    
    agg_df = pd.DataFrame(aggregated)
    
    # Build response
    result_measures = []
    processed_keys = set()
    
    # First: measures in 2026 (using composite key)
    for _, m2026 in measures_2026.iterrows():
        mkey_part = m2026['measure_key_part']
        if mkey_part in processed_keys:
            continue
        processed_keys.add(mkey_part)
        
        measure_df = agg_df[agg_df['measure_key_part'] == mkey_part]
        if measure_df.empty:
            continue
        
        yearly_data = {}
        for year in years:
            year_row = measure_df[measure_df['year'] == year]
            if not year_row.empty:
                row = year_row.iloc[0]
                yearly_data[int(year)] = {
                    'value': round(float(row['avg_star']), 2) if pd.notna(row['avg_star']) else None,
                    'contract_count': int(row['contract_count']),
                    'enrollment': int(row['total_enrollment']),
                    'measure_id': str(row['measure_id']),
                }
            else:
                yearly_data[int(year)] = None
        
        # Get weights (using composite key)
        measure_weights = {}
        for wy in weights_by_year:
            if mkey_part in weights_by_year[wy]:
                measure_weights[wy] = weights_by_year[wy][mkey_part]
        
        result_measures.append({
            'measure_id': str(m2026['measure_id']),
            'measure_key': str(m2026['measure_key']),
            'measure_key_part': mkey_part,
            'measure_name': str(m2026['measure_name_with_part']),  # Includes (Part C/D)
            'part': str(m2026['part']),
            'lower_is_better': bool(m2026['lower_is_better']) if pd.notna(m2026['lower_is_better']) else False,
            'in_2026': True,
            'yearly': yearly_data,
            'weights': measure_weights,
        })
    
    # Second: discontinued measures (using composite key)
    all_keys = agg_df['measure_key_part'].dropna().unique()
    discontinued_keys = [k for k in all_keys if k not in processed_keys]
    
    for mkey_part in sorted(discontinued_keys):
        measure_df = agg_df[agg_df['measure_key_part'] == mkey_part]
        if measure_df.empty:
            continue
        
        sample_row = measure_df.sort_values('year', ascending=False).iloc[0]
        
        yearly_data = {}
        for year in years:
            year_row = measure_df[measure_df['year'] == year]
            if not year_row.empty:
                row = year_row.iloc[0]
                yearly_data[int(year)] = {
                    'value': round(float(row['avg_star']), 2) if pd.notna(row['avg_star']) else None,
                    'contract_count': int(row['contract_count']),
                    'enrollment': int(row['total_enrollment']),
                    'measure_id': str(row['measure_id']),
                }
            else:
                yearly_data[int(year)] = None
        
        measure_weights = {}
        for wy in weights_by_year:
            if mkey_part in weights_by_year[wy]:
                measure_weights[wy] = weights_by_year[wy][mkey_part]
        
        part = str(sample_row['part'])
        measure_name = str(sample_row.get('measure_name', ''))
        measure_name_with_part = f"{measure_name} (Part {part})" if measure_name else mkey_part
        
        result_measures.append({
            'measure_id': str(sample_row['measure_id']),
            'measure_key': str(sample_row.get('measure_key', '')),
            'measure_key_part': mkey_part,
            'measure_name': measure_name_with_part,
            'part': part,
            'lower_is_better': False,
            'in_2026': False,
            'yearly': yearly_data,
            'weights': measure_weights,
        })
    
    total_measures = len(result_measures)
    measures_in_2026 = len([m for m in result_measures if m['in_2026']])
    
    return {
        'parent_org': display_org,
        'avg_type': avg_type,
        'years': [int(y) for y in years],
        'measures': result_measures,
        'validation': {
            'total_measures': total_measures,
            'measures_in_2026': measures_in_2026,
            'discontinued_measures': total_measures - measures_in_2026,
        }
    }


@app.get("/api/stars/measure-stars/detail")
async def get_measure_stars_detail(
    measure_key: str,
    year: int,
    parent_org: Optional[str] = None,
    plan_type: Optional[str] = None,
    snp_type: Optional[str] = None,
    group_type: Optional[str] = None,
):
    """
    Get contract-level star rating detail for a specific measure/year.
    Supports all filter combinations (parent_org, plan_type, snp_type, group_type).
    """
    from db import get_engine
    engine = get_engine()
    
    # Build filter clause for unified tables
    filter_parts = []
    if parent_org and parent_org not in ["_INDUSTRY_", "Industry"]:
        filter_parts.append(f"e.parent_org LIKE '%{parent_org}%'")
    if plan_type:
        filter_parts.append(f"e.plan_type = '{plan_type}'")
    if snp_type:
        filter_parts.append(f"e.snp_type = '{snp_type}'")
    if group_type:
        filter_parts.append(f"e.group_type = '{group_type}'")
    
    filter_sql = " AND " + " AND ".join(filter_parts) if filter_parts else ""
    
    # Use stars_enrollment_unified for consistent enrollment data (2013-2026)
    sql = f"""
    SELECT 
        ms.contract_id,
        ms.star_rating,
        COALESCE(e.parent_org, 'Unknown') as parent_org,
        COALESCE(e.enrollment, 0) as enrollment,
        e.plan_type,
        e.snp_type,
        e.group_type
    FROM measure_stars_all_years ms
    LEFT JOIN (
        SELECT contract_id, star_year, parent_org, enrollment, plan_type, snp_type, group_type
        FROM stars_enrollment_unified
    ) e ON ms.contract_id = e.contract_id AND ms.year = e.star_year
    WHERE ms.year = {year}
      AND ms.measure_id IN (
          SELECT DISTINCT measure_id FROM measures_all_years 
          WHERE measure_key = '{measure_key}' AND year = {year}
      )
      AND ms.star_rating IS NOT NULL
      {filter_sql}
    """
    
    df = engine.query(sql)
    
    contracts = []
    for _, row in df.iterrows():
        contracts.append({
            'contract_id': str(row['contract_id']),
            'parent_org': str(row['parent_org']) if pd.notna(row['parent_org']) else None,
            'star_rating': int(row['star_rating']) if pd.notna(row['star_rating']) else None,
            'enrollment': int(row['enrollment']) if pd.notna(row['enrollment']) else None,
        })
    
    return {
        'measure_key': measure_key,
        'year': year,
        'parent_org': parent_org or "Industry",
        'contract_count': len(contracts),
        'contracts': sorted(contracts, key=lambda x: x['contract_id']),
    }


@app.get("/api/stars/audit-download")
async def download_stars_audit_package(
    year: int,
    data_type: str = "overall",  # "overall", "measures", "cutpoints"
    parent_org: Optional[str] = None,
):
    """
    Download a ZIP package with raw CMS star ratings files needed to replicate calculations,
    plus a README explaining how they connect.
    
    Args:
        year: Star year (e.g., 2024)
        data_type: "overall" for overall ratings, "measures" for measure-level, "cutpoints" for cutpoints
        parent_org: Optional filter (included in README for context)
    """
    import boto3
    import zipfile
    from datetime import datetime
    
    try:
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        # Create ZIP in memory
        zip_buffer = BytesIO()
        files_included = []
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            
            # Helper to add file from S3
            def add_s3_file(s3_key, local_path, description):
                try:
                    response = s3.get_object(Bucket=bucket, Key=s3_key)
                    data = response['Body'].read()
                    zf.writestr(local_path, data)
                    files_included.append((local_path, description, s3_key))
                    return True
                except Exception as e:
                    print(f"Failed to add {s3_key}: {e}")
                    return False
            
            # === FILE 1: Star Ratings Data (main ratings file) ===
            star_files = [
                f"docs/stars/data_tables/{year}_star_ratings_data.zip",
                f"docs/stars/data_tables/{year}_star_ratings.zip",
                f"raw/stars/{year}_ratings.zip",
                f"raw/stars/{year}_star_ratings.zip",
            ]
            for sf in star_files:
                if add_s3_file(sf, f"1_star_ratings/star_ratings_{year}.zip",
                              "Star Ratings - Overall ratings and measure performance by contract"):
                    break
            
            # === FILE 1b: Star Display file (additional ratings data) ===
            display_files = [
                f"raw/stars/{year}_display.zip",
            ]
            for df_path in display_files:
                add_s3_file(df_path, f"1_star_ratings/star_display_{year}.zip",
                           "Star Display - Additional star ratings display data")
            
            # === FILE 2: Star Cutpoints (thresholds for each star level) ===
            cutpoint_files = [
                (f"docs/stars/cut_points/{year}_cutpoints.xlsx", f"2_cutpoints/cutpoints_{year}.xlsx"),
                (f"docs/stars/data_tables/star_cutpoints_{year}.zip", f"2_cutpoints/star_cutpoints_{year}.zip"),
                (f"docs/stars/data_tables/{year}_cutpoints.zip", f"2_cutpoints/cutpoints_{year}.zip"),
                (f"raw/stars/cutpoints_{year}.zip", f"2_cutpoints/cutpoints_{year}.zip"),
            ]
            for s3_key, local_path in cutpoint_files:
                if add_s3_file(s3_key, local_path,
                              "Star Cutpoints - Performance thresholds for each star rating level"):
                    break
            
            # === FILE 3: Enrollment Data (for weighting) ===
            # Try multiple enrollment sources
            enrollment_files = [
                (f"raw/enrollment/by_plan/{year}-01/enrollment_plan_{year}_01.zip", f"3_enrollment/enrollment_{year}_01.zip"),
                (f"raw/enrollment/by_plan/{year}-12/enrollment_plan_{year}_12.zip", f"3_enrollment/enrollment_{year}_12.zip"),
                (f"raw/enrollment/by_plan/{year-1}-12/enrollment_plan_{year-1}_12.zip", f"3_enrollment/enrollment_{year-1}_12.zip"),
            ]
            for s3_key, local_path in enrollment_files:
                if add_s3_file(s3_key, local_path,
                              "Monthly Enrollment - Used for enrollment-weighted averages"):
                    break
            
            # === FILE 4: Contract Info / Crosswalk ===
            crosswalk_key = f"raw/crosswalks/crosswalk_{year}.zip"
            add_s3_file(crosswalk_key, f"4_crosswalk/crosswalk_{year}.zip",
                       "Contract Crosswalk - Maps contract changes and parent organizations")
            
            # === Create comprehensive README ===
            readme_content = f"""# Star Ratings Data Audit Package
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Query Parameters
- Star Year: {year}
- Data Type: {data_type}
- Parent Organization Filter: {parent_org or 'All (Industry)'}

## Files Included

"""
            for local_path, description, s3_key in files_included:
                readme_content += f"### {local_path}\n"
                readme_content += f"- **Description**: {description}\n"
                readme_content += f"- **Source**: s3://{bucket}/{s3_key}\n\n"
            
            readme_content += """
## How to Replicate Our Calculations

### Overall Star Ratings
1. Open the star ratings file (1_star_ratings/)
2. Look for the "Part C & D Overall" or "Summary" sheet
3. Each contract has an overall star rating (1-5)
4. To calculate enrollment-weighted averages:
   - Join with enrollment data (3_enrollment/) on contract_id
   - Weight = contract enrollment / total enrollment
   - Weighted Avg = SUM(rating * weight)

### Measure-Level Performance
1. Open the star ratings file (1_star_ratings/)
2. Look for "Part C" and "Part D" sheets with measure data
3. Each contract has performance values for each measure (usually percentages)
4. Measure IDs (C01, C02, D01, etc.) correspond to specific measures
5. See cutpoints file (2_cutpoints/) for how performance maps to stars

### Measure-Level Star Ratings
1. Open the star ratings file, find "Measure Stars" sheet
2. Each contract/measure combination has a 1-5 star rating
3. Stars are assigned based on cutpoints:
   - Performance >= 5-star cutpoint -> 5 stars
   - Performance >= 4-star cutpoint -> 4 stars
   - etc.

### Cutpoints Reference
The cutpoints file (2_cutpoints/) contains thresholds for each measure:
- Column: measure_id, measure_name, 2_star_cutpoint, 3_star_cutpoint, 4_star_cutpoint, 5_star_cutpoint
- Performance values between cutpoints determine star level

### Weighted Average Calculation
```
For each measure/year:
1. Get all contracts with valid data for that measure
2. Join with enrollment data
3. If weighted average:
   - numerator = SUM(value * enrollment)
   - denominator = SUM(enrollment)
   - weighted_avg = numerator / denominator
4. If simple average:
   - simple_avg = MEAN(value)
```

### % of Enrollees in 4+ Star Contracts
```
For each measure/year:
1. Get all contracts with star ratings for that measure
2. Join with enrollment data
3. fourplus_enrollment = SUM(enrollment WHERE star >= 4)
4. total_enrollment = SUM(enrollment)
5. pct_fourplus = fourplus_enrollment / total_enrollment * 100
```

## Data Sources

Our platform uses these CMS data sources:
- Star Ratings: https://www.cms.gov/medicare/health-drug-plans/part-c-d-performance-data
- Enrollment: https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data
- Cutpoints: Published annually with Star Ratings methodology

## Questions?

All calculations are transparent and reproducible using these source files.
Contact support for additional assistance.
"""
            
            zf.writestr("README.md", readme_content)
        
        # Return ZIP file
        zip_buffer.seek(0)
        
        from fastapi.responses import StreamingResponse
        
        filename = f"stars_audit_{year}_{data_type}.zip"
        
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )
        
    except Exception as e:
        return {"error": f"Failed to create audit package: {str(e)}"}


@lru_cache(maxsize=1)
def get_measure_performance_contracts():
    """Load contract-level measure performance data."""
    return load_parquet('processed/stars/measure_performance/contract_level.parquet')


@app.get("/api/stars/measure-performance/detail")
async def get_measure_performance_detail(
    measure_key: str,
    year: int,
    parent_org: Optional[str] = None,  # None or "_INDUSTRY_" = all, else specific payer
    plan_type: Optional[str] = None,
    snp_type: Optional[str] = None,
    group_type: Optional[str] = None,
):
    """
    Get contract-level detail for a specific measure/year.
    Used for auditing/drilling down into aggregate numbers.
    Queries directly from unified tables for consistency.
    
    Supports all filter combinations (parent_org, plan_type, snp_type, group_type).
    """
    from db import get_engine
    engine = get_engine()
    
    # Build filter clause for unified tables
    filter_parts = []
    if parent_org and parent_org not in ["_INDUSTRY_", "Industry"]:
        filter_parts.append(f"e.parent_org LIKE '%{parent_org}%'")
    if plan_type:
        filter_parts.append(f"e.plan_type = '{plan_type}'")
    if snp_type:
        filter_parts.append(f"e.snp_type = '{snp_type}'")
    if group_type:
        filter_parts.append(f"e.group_type = '{group_type}'")
    
    filter_sql = " AND " + " AND ".join(filter_parts) if filter_parts else ""
    
    # Query directly from unified tables
    sql = f"""
    SELECT 
        m.contract_id,
        m.measure_id,
        m.measure_name,
        m.numeric_value as performance_pct,
        COALESCE(e.parent_org, 'Unknown') as parent_org,
        COALESCE(e.enrollment, 0) as enrollment,
        e.plan_type,
        e.snp_type,
        e.group_type
    FROM measures_all_years m
    LEFT JOIN (
        SELECT contract_id, star_year, parent_org, enrollment, plan_type, snp_type, group_type
        FROM stars_enrollment_unified
    ) e ON m.contract_id = e.contract_id AND m.year = e.star_year
    WHERE m.year = {year}
      AND m.measure_key = '{measure_key}'
      AND m.numeric_value IS NOT NULL
      {filter_sql}
    """
    
    filtered = engine.query(sql)
    
    if filtered.empty:
        return {"error": f"No data for {measure_key} in {year}", "contracts": []}

    # Build display name for filters
    display_org = "Industry"
    if parent_org and parent_org not in ["_INDUSTRY_", "Industry"]:
        display_org = parent_org
    if plan_type:
        display_org += f" ({plan_type})"
    if snp_type:
        display_org += f" [{snp_type}]"
    if group_type:
        display_org += f" - {group_type}"

    if filtered.empty:
        return {"error": f"No data for {measure_key} in {year} for {display_org}", "contracts": []}

    # Get measure info
    sample = filtered.iloc[0]
    measure_name = str(sample['measure_name']) if pd.notna(sample['measure_name']) else measure_key
    measure_id = str(sample['measure_id']) if pd.notna(sample['measure_id']) else ''

    # Build contract list sorted by performance
    contracts = []
    for _, row in filtered.sort_values('performance_pct', ascending=False).iterrows():
        contracts.append({
            'contract_id': str(row['contract_id']),
            'parent_org': str(row['parent_org']) if pd.notna(row['parent_org']) else None,
            'performance_pct': float(row['performance_pct']) if pd.notna(row['performance_pct']) else None,
            'enrollment': int(row['enrollment']) if pd.notna(row['enrollment']) else 0,
        })

    return {
        'measure_key': measure_key,
        'measure_id': measure_id,
        'measure_name': measure_name,
        'year': year,
        'parent_org': display_org,
        'contract_count': len(contracts),
        'contracts': contracts,
    }


@app.get("/api/stars/measure-performance/payers")
async def get_measure_performance_payers():
    """Get list of payers with measure performance data."""
    df = get_measure_performance_aggregates()

    if df.empty:
        return {"payers": []}

    # Get payers sorted by total enrollment
    payers = df[df['parent_org'] != '_INDUSTRY_'].groupby('parent_org').agg({
        'total_enrollment': 'sum',
        'contract_count': 'sum'
    }).reset_index()
    payers = payers.sort_values('total_enrollment', ascending=False)

    return {
        'payers': [
            {
                'parent_org': row['parent_org'],
                'total_enrollment': int(row['total_enrollment']),
                'contract_count': int(row['contract_count'])
            }
            for _, row in payers.head(100).iterrows()
        ]
    }


@app.get("/api/stars/measure-performance/contracts")
async def get_measure_performance_contracts_list():
    """Get list of contracts with measure performance data."""
    df = get_measure_performance_contracts()

    if df.empty:
        return {"contracts": []}

    # Get contracts sorted by most recent year's enrollment
    latest_year = df['year'].max()
    latest = df[df['year'] == latest_year]

    contracts = latest.groupby(['contract_id', 'parent_org']).agg({
        'enrollment': 'first'
    }).reset_index()
    contracts = contracts.sort_values('enrollment', ascending=False)

    return {
        'contracts': [
            {
                'contract_id': row['contract_id'],
                'parent_org': str(row['parent_org']) if pd.notna(row['parent_org']) else None,
                'enrollment': int(row['enrollment']) if pd.notna(row['enrollment']) else 0,
            }
            for _, row in contracts.head(500).iterrows()
        ]
    }


@app.get("/api/stars/measure-performance/contract/{contract_id}")
async def get_contract_measure_performance(contract_id: str):
    """
    Get measure performance for a specific contract across all years.
    Includes cutpoint band position for each measure.
    """
    df = get_measure_performance_contracts()

    if df.empty:
        return {"error": "Contract-level data not available"}

    # Filter to this contract
    contract_df = df[df['contract_id'] == contract_id]

    if contract_df.empty:
        return {"error": f"No data for contract {contract_id}"}

    # Get contract info
    sample = contract_df.iloc[0]
    parent_org = str(sample['parent_org']) if pd.notna(sample['parent_org']) else None

    # Get all years
    years = sorted(df['year'].unique())

    # Load measure stars for all years (has actual star ratings per contract/measure)
    measure_stars_by_year = {}
    for year in years:
        try:
            ms = load_parquet(f'processed/stars/measure_stars/{year}/data.parquet')
            if not ms.empty:
                # Filter to this contract
                contract_stars = ms[ms['contract_id'] == contract_id]
                if not contract_stars.empty:
                    measure_stars_by_year[year] = contract_stars
        except:
            pass

    # Load cutpoints for band position calculation (most recent year only)
    latest_year = max(years)
    cutpoints_latest = None
    try:
        cutpoints_latest = load_parquet(f'processed/stars/cutpoints/{latest_year}/data.parquet')
    except:
        pass

    # Load cutpoints for all years to get year-specific weights
    cutpoints_by_year = {}
    for year in years:
        try:
            cp = load_parquet(f'processed/stars/cutpoints/{year}/data.parquet')
            if not cp.empty:
                # Build measure_id -> weight lookup for this year
                cutpoints_by_year[year] = {
                    str(row['measure_id']): float(row['weight']) if pd.notna(row['weight']) else 1.0
                    for _, row in cp.iterrows()
                }
        except:
            pass

    # Get 2026 measures for ordering (using measure_key)
    try:
        cutpoints_2026 = load_parquet('processed/stars/cutpoints/2026/data.parquet')
        measures_2026 = cutpoints_2026[['measure_id', 'measure_key', 'measure_name', 'part', 'lower_is_better', 'weight']].drop_duplicates()
    except:
        measures_2026 = pd.DataFrame()

    # Detect if this is an MA-PD contract (has both Part C and Part D measures)
    # For MA-PD, D02/D03 should have weight 0 as they duplicate C28/C29
    is_ma_pd = False
    if latest_year in measure_stars_by_year:
        ms_latest = measure_stars_by_year[latest_year]
        measure_ids = set(ms_latest['measure_id'].astype(str))
        has_part_c = any(m.startswith('C') for m in measure_ids)
        has_part_d = any(m.startswith('D') for m in measure_ids)
        is_ma_pd = has_part_c and has_part_d

    # Build measure data
    result_measures = []
    processed_keys = set()

    import re

    def parse_cutpoint_value(val, lower_is_better=False):
        """
        Parse cutpoint value to get the threshold for that star level.

        For ranges like '> 7 % to <= 9 %' or '>= 76 % to < 84 %':
        - Lower-is-better: extract UPPER bound (max value to achieve this level)
        - Higher-is-better: extract LOWER bound (min value to achieve this level)

        Examples:
        - Lower-is-better '> 7 % to <= 9 %' -> 9 (upper bound)
        - Higher-is-better '>= 76 % to < 84 %' -> 76 (lower bound)
        - '≥ 84%' or '<= 7%' -> 84 or 7 (single threshold)
        - 83 or 83.0 -> 83.0
        """
        if pd.isna(val):
            return None
        if isinstance(val, (int, float)):
            return float(val)

        val_str = str(val).strip()

        # Pattern to find numbers like 76, 84.5, etc.
        numbers = re.findall(r'(\d+\.?\d*)', val_str)
        if numbers:
            # For ranges with 'to', choose bound based on measure direction
            if ' to ' in val_str.lower() and len(numbers) >= 2:
                if lower_is_better:
                    return float(numbers[1])  # Upper bound for lower-is-better
                else:
                    return float(numbers[0])  # Lower bound for higher-is-better
            return float(numbers[0])  # Single value
        return None

    def get_band_position_from_star(value, star_rating, cutpoints_row, lower_is_better):
        """
        Determine position within the star band (top/middle/bottom third).
        Uses actual star rating from measure_stars file + cutpoints to find position within band.
        Returns 'top', 'middle', 'bottom', or None.
        """
        if pd.isna(value) or star_rating is None:
            return None

        # Get cutpoints for all levels
        cuts = {}
        for star in [5, 4, 3, 2]:
            cut_col = f'cut_{star}'
            if cut_col in cutpoints_row:
                parsed = parse_cutpoint_value(cutpoints_row[cut_col], lower_is_better)
                if parsed is not None:
                    cuts[star] = parsed

        if not cuts:
            return None

        # Find the band boundaries for this star rating
        band_low = None
        band_high = None

        if lower_is_better:
            # Lower is better: the cuts define upper bounds
            if star_rating == 5:
                band_low = 0
                band_high = cuts.get(5, 100)
            elif star_rating == 4:
                band_low = cuts.get(5, 0)
                band_high = cuts.get(4, 100)
            elif star_rating == 3:
                band_low = cuts.get(4, 0)
                band_high = cuts.get(3, 100)
            elif star_rating == 2:
                band_low = cuts.get(3, 0)
                band_high = cuts.get(2, 100)
            else:  # 1 star
                band_low = cuts.get(2, 0)
                band_high = 100
        else:
            # Higher is better: the cuts define lower bounds
            if star_rating == 5:
                band_low = cuts.get(5, 0)
                band_high = 100
            elif star_rating == 4:
                band_low = cuts.get(4, 0)
                band_high = cuts.get(5, 100)
            elif star_rating == 3:
                band_low = cuts.get(3, 0)
                band_high = cuts.get(4, 100)
            elif star_rating == 2:
                band_low = cuts.get(2, 0)
                band_high = cuts.get(3, 100)
            else:  # 1 star
                band_low = 0
                band_high = cuts.get(2, 100)

        if band_low is None or band_high is None or band_high == band_low:
            return None

        # Calculate position within band
        band_range = band_high - band_low
        if lower_is_better:
            # For lower is better, lower value = better = top of band
            position_pct = (band_high - value) / band_range
        else:
            # For higher is better, higher value = better = top of band
            position_pct = (value - band_low) / band_range

        # Clamp to 0-1 range
        position_pct = max(0, min(1, position_pct))

        if position_pct >= 0.67:
            return 'top'
        elif position_pct >= 0.33:
            return 'middle'
        else:
            return 'bottom'

    # First: measures in 2026
    for _, m2026 in measures_2026.iterrows():
        mkey = m2026['measure_key']
        if mkey in processed_keys:
            continue
        processed_keys.add(mkey)

        measure_data = contract_df[contract_df['measure_key'] == mkey]
        if measure_data.empty:
            continue

        lower_is_better = bool(m2026['lower_is_better']) if pd.notna(m2026['lower_is_better']) else False

        yearly_data = {}
        latest_year_with_data = None
        latest_value = None
        latest_measure_id = None
        latest_star_rating = None

        for year in years:
            year_row = measure_data[measure_data['year'] == year]
            if not year_row.empty:
                row = year_row.iloc[0]
                value = float(row['performance_pct']) if pd.notna(row['performance_pct']) else None
                measure_id = str(row['measure_id'])

                # Get star rating from measure_stars if available
                star_rating = None
                if year in measure_stars_by_year:
                    ms_year = measure_stars_by_year[year]
                    # Match by measure_id for this year
                    ms_row = ms_year[ms_year['measure_id'] == measure_id]
                    if not ms_row.empty:
                        star_rating = int(ms_row.iloc[0]['star_rating']) if pd.notna(ms_row.iloc[0]['star_rating']) else None

                # Get year-specific weight
                year_weight = cutpoints_by_year.get(year, {}).get(measure_id, 1.0)
                # For MA-PD, D02/D03 have weight 0
                if is_ma_pd and measure_id in ('D02', 'D03'):
                    year_weight = 0.0

                yearly_data[int(year)] = {
                    'value': value,
                    'measure_id': measure_id,
                    'star_rating': star_rating,
                    'weight': year_weight,
                }

                # Track latest year with data
                if value is not None:
                    latest_year_with_data = year
                    latest_value = value
                    latest_measure_id = measure_id
                    latest_star_rating = star_rating
            else:
                yearly_data[int(year)] = None

        # Calculate band position only for the most recent year using actual star rating
        latest_band_position = None
        # Get cutpoints for this measure (for simulation)
        cutpoints_data = None
        if cutpoints_latest is not None:
            cp_row = cutpoints_latest[cutpoints_latest['measure_id'] == str(m2026['measure_id'])]
            if not cp_row.empty:
                row = cp_row.iloc[0]
                cutpoints_data = {
                    'cut_2': parse_cutpoint_value(row.get('cut_2'), lower_is_better),
                    'cut_3': parse_cutpoint_value(row.get('cut_3'), lower_is_better),
                    'cut_4': parse_cutpoint_value(row.get('cut_4'), lower_is_better),
                    'cut_5': parse_cutpoint_value(row.get('cut_5'), lower_is_better),
                }
                # Calculate band position
                if latest_year_with_data and latest_value is not None and latest_star_rating is not None:
                    band_position = get_band_position_from_star(latest_value, latest_star_rating, row, lower_is_better)
                    latest_band_position = {
                        'year': int(latest_year_with_data),
                        'star_rating': latest_star_rating,
                        'position': band_position,
                    }

        # Determine weight (for MA-PD, D02/D03 have weight 0 to avoid double-counting with C28/C29)
        measure_weight = float(m2026['weight']) if pd.notna(m2026['weight']) else 1.0
        if is_ma_pd and str(m2026['measure_id']) in ('D02', 'D03'):
            measure_weight = 0.0

        result_measures.append({
            'measure_id': str(m2026['measure_id']),
            'measure_key': mkey,
            'measure_name': str(m2026['measure_name']),
            'part': str(m2026['part']) if pd.notna(m2026['part']) else 'C',
            'lower_is_better': lower_is_better,
            'weight': measure_weight,
            'in_2026': True,
            'yearly': yearly_data,
            'latest_band': latest_band_position,
            'cutpoints': cutpoints_data,
        })

    # Second: discontinued measures
    all_measure_keys = contract_df['measure_key'].dropna().unique()
    for mkey in sorted(all_measure_keys):
        if mkey in processed_keys:
            continue

        measure_data = contract_df[contract_df['measure_key'] == mkey]
        if measure_data.empty:
            continue

        sample_row = measure_data.sort_values('year', ascending=False).iloc[0]
        lower_is_better = bool(sample_row.get('lower_is_better', False)) if pd.notna(sample_row.get('lower_is_better')) else False

        yearly_data = {}
        latest_year_with_data = None
        latest_value = None
        latest_measure_id = None
        latest_star_rating = None

        for year in years:
            year_row = measure_data[measure_data['year'] == year]
            if not year_row.empty:
                row = year_row.iloc[0]
                value = float(row['performance_pct']) if pd.notna(row['performance_pct']) else None
                measure_id = str(row['measure_id'])

                # Get star rating from measure_stars if available
                star_rating = None
                if year in measure_stars_by_year:
                    ms_year = measure_stars_by_year[year]
                    ms_row = ms_year[ms_year['measure_id'] == measure_id]
                    if not ms_row.empty:
                        star_rating = int(ms_row.iloc[0]['star_rating']) if pd.notna(ms_row.iloc[0]['star_rating']) else None

                # Get year-specific weight
                year_weight = cutpoints_by_year.get(year, {}).get(measure_id, 1.0)
                # For MA-PD, D02/D03 have weight 0
                if is_ma_pd and measure_id in ('D02', 'D03'):
                    year_weight = 0.0

                yearly_data[int(year)] = {
                    'value': value,
                    'measure_id': measure_id,
                    'star_rating': star_rating,
                    'weight': year_weight,
                }

                if value is not None:
                    latest_year_with_data = year
                    latest_value = value
                    latest_measure_id = measure_id
                    latest_star_rating = star_rating
            else:
                yearly_data[int(year)] = None

        # Get cutpoints for this measure (for simulation) - discontinued measures use their last measure_id
        cutpoints_data = None
        latest_band_position = None
        if cutpoints_latest is not None and latest_measure_id:
            cp_row = cutpoints_latest[cutpoints_latest['measure_id'] == latest_measure_id]
            if not cp_row.empty:
                row = cp_row.iloc[0]
                cutpoints_data = {
                    'cut_2': parse_cutpoint_value(row.get('cut_2'), lower_is_better),
                    'cut_3': parse_cutpoint_value(row.get('cut_3'), lower_is_better),
                    'cut_4': parse_cutpoint_value(row.get('cut_4'), lower_is_better),
                    'cut_5': parse_cutpoint_value(row.get('cut_5'), lower_is_better),
                }
                if latest_year_with_data and latest_value is not None and latest_star_rating is not None:
                    band_position = get_band_position_from_star(latest_value, latest_star_rating, row, lower_is_better)
                    latest_band_position = {
                        'year': int(latest_year_with_data),
                        'star_rating': latest_star_rating,
                        'position': band_position,
                    }

        # Determine weight for discontinued measures
        disc_measure_id = str(sample_row['measure_id'])
        disc_weight = 1.0
        # For MA-PD, D02/D03 have weight 0 to avoid double-counting with C28/C29
        if is_ma_pd and disc_measure_id in ('D02', 'D03'):
            disc_weight = 0.0

        result_measures.append({
            'measure_id': disc_measure_id,
            'measure_key': mkey,
            'measure_name': str(sample_row.get('measure_name', mkey)) if pd.notna(sample_row.get('measure_name')) else mkey,
            'part': str(sample_row.get('part', 'C')) if pd.notna(sample_row.get('part')) else 'C',
            'lower_is_better': lower_is_better,
            'weight': disc_weight,
            'in_2026': False,
            'yearly': yearly_data,
            'latest_band': latest_band_position,
            'cutpoints': cutpoints_data,
        })

    # Calculate weighted average star rating for latest year
    # Use measure_stars directly to include improvement measures (C30, D04) that have no performance %
    # Weights come from the cutpoints file (which now has correct CMS weights per year)
    total_weight = 0
    weighted_sum = 0

    # Build weight lookup from cutpoints for the latest year
    weights_lookup = {}
    if cutpoints_latest is not None and not cutpoints_latest.empty:
        for _, cp_row in cutpoints_latest.iterrows():
            mid = str(cp_row['measure_id'])
            w = float(cp_row['weight']) if pd.notna(cp_row['weight']) else 1.0
            weights_lookup[mid] = w

    if latest_year in measure_stars_by_year:
        ms_latest = measure_stars_by_year[latest_year]

        for _, row in ms_latest.iterrows():
            mid = str(row['measure_id'])
            star = row['star_rating']

            if pd.isna(star):
                continue

            weight = weights_lookup.get(mid, 1)

            # For MA-PD contracts, D02 and D03 duplicate C28 and C29 - don't double count
            if is_ma_pd and mid in ('D02', 'D03'):
                weight = 0

            if weight == 0:  # Measures with weight 0 not included in overall
                continue

            total_weight += weight
            weighted_sum += int(star) * weight

    weighted_avg_star = round(weighted_sum / total_weight, 2) if total_weight > 0 else None

    # Calculate yearly weighted averages using year-specific weights
    yearly_weighted_avgs = {}
    for year in years:
        year_int = int(year)  # Convert numpy.int64 to Python int

        if year not in measure_stars_by_year:
            yearly_weighted_avgs[year_int] = None
            continue

        ms_year = measure_stars_by_year[year]
        year_weights = cutpoints_by_year.get(year, {})

        year_total_weight = 0
        year_weighted_sum = 0

        for _, row in ms_year.iterrows():
            mid = str(row['measure_id'])
            star = row['star_rating']

            if pd.isna(star):
                continue

            weight = year_weights.get(mid, 1.0)

            # For MA-PD contracts, D02/D03 have weight 0
            if is_ma_pd and mid in ('D02', 'D03'):
                weight = 0

            if weight == 0:
                continue

            year_total_weight += weight
            year_weighted_sum += int(star) * weight

        yearly_weighted_avgs[year_int] = round(year_weighted_sum / year_total_weight, 2) if year_total_weight > 0 else None

    return {
        'contract_id': contract_id,
        'parent_org': parent_org,
        'years': [int(y) for y in years],
        'measures': result_measures,
        'weighted_avg_star': weighted_avg_star,
        'total_weight': total_weight,
        'yearly_weighted_avgs': yearly_weighted_avgs,
    }


# ================================================================
# V3 UNIFIED DATA LAYER ENDPOINTS (DuckDB + Full Audit)
# ================================================================
# These endpoints use the new unified data architecture with:
# - DuckDB for fast SQL over S3 Parquet
# - Full audit logging (who queried what, when)
# - Data lineage tracing (results -> source files)
# ================================================================

@app.get("/api/v3/enrollment/timeseries")
async def get_enrollment_timeseries_v3(
    parent_org: Optional[str] = None,
    parent_orgs: Optional[str] = None,  # Pipe-separated: UnitedHealth|Humana
    state: Optional[str] = None,
    states: Optional[str] = None,  # Comma-separated state codes
    plan_type: Optional[str] = None,
    plan_types: Optional[str] = None,  # Comma-separated: HMO/HMOPOS,Local PPO
    product_type: Optional[str] = None,
    product_types: Optional[str] = None,  # Comma-separated: MAPD,MA-only,PDP
    snp_type: Optional[str] = None,
    snp_types: Optional[str] = None,  # Comma-separated: Non-SNP,D-SNP,C-SNP,I-SNP
    group_type: Optional[str] = None,
    group_types: Optional[str] = None,  # Comma-separated: Individual,Group
    data_source: str = "national",  # "national" (exact, no geo) or "geographic" (has state/county, suppressed)
    include_total: bool = True,  # Include Industry Total when filtering by payers
    start_year: int = 2015,
    end_year: int = 2026,
    month: int = 1,
    user_id: str = "api"
):
    """
    Get enrollment timeseries using unified data layer (DuckDB + Audit).

    Data Source:
    - 'national': Exact totals from by-contract files (no geography, no suppression)
    - 'geographic': CPSC data with state/county detail (suppressed <10 enrollees per county)

    Payer Filtering:
    - parent_orgs: Pipe-separated for multiple payers
    - include_total: Whether to include Industry Total line (default True)

    Plan Type Filtering:
    - Use comma-separated for multiple: plan_types=HMO/HMOPOS,Local PPO

    Product Type Filtering:
    - 'MAPD': MA + Part D plans
    - 'MA-only': MA without Part D
    - 'PDP': Standalone Part D
    - Use comma-separated for multiple: product_types=MAPD,MA-only

    SNP Filtering:
    - 'Non-SNP': Total enrollment minus all SNP enrollment
    - 'D-SNP', 'C-SNP', 'I-SNP': Specific SNP type enrollment

    Group Type Filtering:
    - 'Individual': Direct enrollment plans
    - 'Group': Employer-sponsored plans

    Returns audit_id for lineage tracing.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available", "fallback": "/api/v2/enrollment/timeseries"}

    try:
        service = get_enrollment_service()

        # Support both plan_type (single) and plan_types (comma-separated)
        effective_plan_types = None
        if plan_types:
            effective_plan_types = [p.strip() for p in plan_types.split(',')]
        elif plan_type:
            effective_plan_types = [plan_type]

        # Support both product_type (single) and product_types (comma-separated)
        effective_product_types = None
        if product_types:
            effective_product_types = [p.strip() for p in product_types.split(',')]
        elif product_type:
            effective_product_types = [product_type]

        # Support both snp_type (single) and snp_types (comma-separated)
        effective_snp_types = None
        if snp_types:
            effective_snp_types = [s.strip() for s in snp_types.split(',')]
        elif snp_type:
            effective_snp_types = [snp_type]

        # Support both group_type (single) and group_types (comma-separated)
        effective_group_types = None
        if group_types:
            effective_group_types = [g.strip() for g in group_types.split(',')]
        elif group_type:
            effective_group_types = [group_type]

        # Support both state (single) and states (comma-separated)
        # FIX: Pass full list of states instead of just the first one
        effective_state = state
        effective_states = None
        if states and not state:
            effective_states = [s.strip() for s in states.split(',')]

        # Handle multiple parent_orgs - return series with each payer + Industry Total
        parent_list = []
        if parent_orgs:
            parent_list = [p.strip() for p in parent_orgs.split('|')]
        elif parent_org:
            parent_list = [parent_org]

        if len(parent_list) > 0:
            # Get data for each payer separately and combine into series
            # FIX: Collect all years and align series to consistent year range
            payer_data = {}  # {payer: {year: enrollment}}
            all_years = set()

            for payer in parent_list:
                result = service.get_timeseries(
                    parent_org=payer,
                    state=effective_state,
                    states=effective_states,
                    plan_types=effective_plan_types,
                    product_types=effective_product_types,
                    snp_types=effective_snp_types,
                    group_types=effective_group_types,
                    data_source=data_source,
                    start_year=start_year,
                    end_year=end_year,
                    month=month,
                    user_id=user_id
                )
                if result.get('years') and result.get('total_enrollment'):
                    # Store as {year: enrollment} for easy lookup
                    payer_data[payer] = dict(zip(result['years'], result['total_enrollment']))
                    all_years.update(result['years'])

            # Add Industry Total if requested
            if include_total:
                total_result = service.get_timeseries(
                    parent_org=None,  # No parent filter = industry total
                    state=effective_state,
                    states=effective_states,
                    plan_types=effective_plan_types,
                    product_types=effective_product_types,
                    snp_types=effective_snp_types,
                    group_types=effective_group_types,
                    data_source=data_source,
                    start_year=start_year,
                    end_year=end_year,
                    month=month,
                    user_id=user_id
                )
                if total_result.get('years') and total_result.get('total_enrollment'):
                    payer_data['Industry Total'] = dict(zip(total_result['years'], total_result['total_enrollment']))
                    all_years.update(total_result['years'])

            if all_years and payer_data:
                # Create consistent year array (sorted)
                years = sorted(all_years)
                
                # Build series with consistent length, using None for missing years
                series = {}
                for payer, year_data in payer_data.items():
                    series[payer] = [year_data.get(y) for y in years]

                return {
                    "years": years,
                    "series": series,
                    "group_by": "parent_org",
                    "data_source": data_source
                }

        # Single or no payer - use standard service
        result = service.get_timeseries(
            parent_org=parent_list[0] if parent_list else None,
            state=effective_state,
            states=effective_states,
            plan_types=effective_plan_types,
            product_types=effective_product_types,
            snp_types=effective_snp_types,
            group_types=effective_group_types,
            data_source=data_source,
            start_year=start_year,
            end_year=end_year,
            month=month,
            user_id=user_id
        )
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


# ==================== EXPORT ENDPOINTS ====================
from fastapi.responses import StreamingResponse

@app.get("/api/v3/enrollment/export")
async def export_enrollment_data(
    year: Optional[int] = None,
    parent_org: Optional[str] = None,
    states: Optional[str] = None,
    plan_types: Optional[str] = None,
    format: str = "xlsx"
):
    """Export enrollment data as Excel file."""
    try:
        db = get_engine()
        if not db:
            raise HTTPException(status_code=503, detail="Database not available")
        
        conditions = []
        if year:
            conditions.append(f"year = {year}")
        if parent_org:
            conditions.append(f"parent_org = '{parent_org}'")
        if states:
            state_list = [f"'{s.strip()}'" for s in states.split(",")]
            conditions.append(f"state IN ({', '.join(state_list)})")
        if plan_types:
            type_list = [f"'{t.strip()}'" for t in plan_types.split(",")]
            conditions.append(f"plan_type IN ({', '.join(type_list)})")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        sql = f"""
            SELECT * FROM fact_enrollment_unified
            WHERE {where_clause}
            ORDER BY year DESC, enrollment DESC
            LIMIT 50000
        """
        
        df = db.query(sql)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Enrollment', index=False)
        output.seek(0)
        
        filename = f"enrollment_export_{year or 'all'}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/v3/enrollment/audit-download")
async def download_enrollment_audit_package(
    year: int,
    parent_org: Optional[str] = None,
    plan_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    group_types: Optional[str] = None,
    states: Optional[str] = None,
    data_source: str = "national"
):
    """
    Download a ZIP package with ALL raw CMS files needed to replicate the enrollment calculation,
    plus a README explaining how they connect.
    """
    import boto3
    import zipfile
    from datetime import datetime
    
    try:
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        # Create ZIP in memory
        zip_buffer = BytesIO()
        files_included = []
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            
            # Helper to add file from S3
            def add_s3_file(s3_key, local_path, description):
                try:
                    response = s3.get_object(Bucket=bucket, Key=s3_key)
                    data = response['Body'].read()
                    zf.writestr(local_path, data)
                    files_included.append((local_path, description, s3_key))
                    return True
                except:
                    return False
            
            # Find best available month (prefer Dec, then work backwards)
            def find_month(prefix_pattern, year):
                for month in ['12', '11', '10', '02', '01']:
                    key = prefix_pattern.format(year=year, month=month)
                    try:
                        s3.head_object(Bucket=bucket, Key=key)
                        return month, key
                    except:
                        continue
                return None, None
            
            # === FILE 1: Primary Enrollment File ===
            if data_source == "geographic" or states:
                # CPSC file for geographic data
                month, key = find_month("raw/enrollment/cpsc/{year}-{month}/cpsc_enrollment_{year}_{month}.zip", year)
                if key:
                    add_s3_file(key, f"1_enrollment/cpsc_enrollment_{year}_{month}.zip", 
                               "CPSC Enrollment - County-level enrollment with geographic detail")
            else:
                # Monthly enrollment by plan for national data
                month, key = find_month("raw/enrollment/by_plan/{year}-{month}/enrollment_plan_{year}_{month}.zip", year)
                if key:
                    add_s3_file(key, f"1_enrollment/enrollment_plan_{year}_{month}.zip",
                               "Monthly Enrollment by Plan - Contract/plan level enrollment with parent org")
            
            # === FILE 2: SNP Classification (if SNP filter used or for completeness) ===
            month_snp, key_snp = find_month("raw/snp/{year}-{month}/snp_{year}_{month}.zip", year)
            if key_snp:
                add_s3_file(key_snp, f"2_snp_classification/snp_{year}_{month_snp}.zip",
                           "SNP Classification - Identifies D-SNP, C-SNP, I-SNP plans")
            
            # === FILE 3: Contract Crosswalk (for tracking contract changes) ===
            crosswalk_key = f"raw/crosswalks/crosswalk_{year}.zip"
            try:
                s3.head_object(Bucket=bucket, Key=crosswalk_key)
                add_s3_file(crosswalk_key, f"3_crosswalk/crosswalk_{year}.zip",
                           "Contract Crosswalk - Maps contract ID changes over time")
            except:
                pass
            
            # === FILE 4: CPSC file too if using national (for geographic reference) ===
            if data_source != "geographic" and not states:
                month_cpsc, key_cpsc = find_month("raw/enrollment/cpsc/{year}-{month}/cpsc_enrollment_{year}_{month}.zip", year)
                if key_cpsc:
                    add_s3_file(key_cpsc, f"4_geographic_reference/cpsc_enrollment_{year}_{month_cpsc}.zip",
                               "CPSC Enrollment (Reference) - Use for geographic breakdowns")
            
            # === Create comprehensive README ===
            readme_content = f"""# Enrollment Data Audit Package
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Query Parameters
- Year: {year}
- Parent Organization: {parent_org or 'All'}
- Plan Types: {plan_types or 'All'}
- SNP Types: {snp_types or 'All'}
- Group Types: {group_types or 'All'}
- States: {states or 'All (National)'}
- Data Source: {'CPSC (Geographic)' if data_source == 'geographic' or states else 'Monthly Enrollment by Plan (National)'}

## Files Included

"""
            for local_path, description, s3_key in files_included:
                readme_content += f"""### {local_path}
**{description}**
- Source: s3://{bucket}/{s3_key}
- Extract the ZIP to access CSV/Excel files

"""

            readme_content += f"""
## How to Replicate the Calculation

### Step 1: Start with the Enrollment File
Open `1_enrollment/` and extract the ZIP file.

The CMS enrollment file contains these key columns:
- **Contract Number**: CMS contract ID (e.g., H1234, S5678)
- **Plan ID**: Plan within contract (001, 002, etc.)
- **Parent Organization**: Ultimate parent company (e.g., "Humana Inc.")
- **Plan Type**: HMO, PPO, PDP, etc.
- **Enrollment**: Number of members

### Step 2: Apply Filters

"""
            if parent_org:
                readme_content += f"""**Parent Organization Filter:**
Filter where `Parent Organization` = "{parent_org}"

"""
            if plan_types:
                readme_content += f"""**Plan Type Filter:**
Filter where `Plan Type` IN ({plan_types})

"""
            if snp_types:
                readme_content += f"""**SNP Type Filter:**
To filter by SNP type, you need to JOIN with the SNP classification file:
1. Open `2_snp_classification/` and extract the ZIP
2. Join on Contract Number + Plan ID
3. Filter where SNP Type = {snp_types}

SNP Types:
- D-SNP = Dual Eligible Special Needs Plan
- C-SNP = Chronic Condition Special Needs Plan  
- I-SNP = Institutional Special Needs Plan

"""
            if group_types:
                readme_content += f"""**Group Type Filter:**
Group type is derived from Plan ID:
- Plan IDs 800-899 = Group/Employer plans
- Plan IDs 001-799 = Individual plans

Filter: {group_types}

"""
            if states:
                readme_content += f"""**Geographic Filter:**
Use the CPSC file for state/county filtering:
1. Open `1_enrollment/` (CPSC file)
2. Filter where State IN ({states})
3. Note: Values marked "*" are suppressed (<10 enrollees)

"""

            readme_content += f"""### Step 3: Aggregate
Sum the `Enrollment` column for all rows matching your filters.

### SQL Query Used (Our Platform)
```sql
SELECT year, SUM(enrollment) as enrollment
FROM {'fact_enrollment_by_geography' if data_source == 'geographic' or states else 'fact_enrollment_unified'}
WHERE year = {year}
"""
            if parent_org:
                readme_content += f"  AND parent_org = '{parent_org}'\n"
            if plan_types:
                pt_formatted = plan_types.replace(",", "', '")
                readme_content += f"  AND plan_type IN ('{pt_formatted}')\n"
            if snp_types:
                st_formatted = snp_types.replace(",", "', '")
                readme_content += f"  AND snp_type IN ('{st_formatted}')\n"
            if group_types:
                gt_formatted = group_types.replace(",", "', '")
                readme_content += f"  AND group_type IN ('{gt_formatted}')\n"
            if states:
                states_formatted = states.replace(",", "', '")
                readme_content += f"  AND state IN ('{states_formatted}')\n"
            
            readme_content += """GROUP BY year
```

## File Relationships

```
┌─────────────────────────────────┐
│  Monthly Enrollment by Plan    │
│  (Contract + Plan + Parent Org)│
└────────────┬────────────────────┘
             │ JOIN on Contract + Plan ID
             ▼
┌─────────────────────────────────┐
│  SNP Classification File       │
│  (D-SNP, C-SNP, I-SNP flags)  │
└─────────────────────────────────┘

┌─────────────────────────────────┐
│  CPSC Enrollment               │
│  (State + County detail)       │
│  (Use for geographic analysis) │
└─────────────────────────────────┘

┌─────────────────────────────────┐
│  Contract Crosswalk            │
│  (Track contract ID changes)   │
│  (Use for historical analysis) │
└─────────────────────────────────┘
```

## Data Dictionary

| Field | Description | Source File |
|-------|-------------|-------------|
| Contract Number | CMS contract ID (H=MA, S=PDP) | Enrollment |
| Plan ID | Plan identifier (001-899) | Enrollment |
| Parent Organization | Ultimate parent company | Enrollment |
| Plan Type | HMO, PPO, PDP, PFFS, etc. | Enrollment |
| SNP Type | D-SNP, C-SNP, I-SNP, or blank | SNP File |
| State | 2-letter state code | CPSC |
| County | County name | CPSC |
| Enrollment | Member count | All |

## Notes
- Enrollment data is a point-in-time snapshot (month-end)
- CPSC suppresses values <10 for privacy (shown as "*")
- Plan IDs 800+ indicate employer/group plans
- Contract IDs: H=MA-only, R=Regional PPO, S=PDP
- Parent org mapping maintained by CMS, may change with M&A

## Questions?
This package contains all raw CMS files needed to independently 
verify the enrollment calculation from the MA Intelligence Platform.
"""
            zf.writestr("README.md", readme_content)
            
            # === Add processed data for comparison ===
            db = get_engine()
            if db:
                conditions = [f"year = {year}"]
                if parent_org:
                    conditions.append(f"parent_org = '{parent_org}'")
                if plan_types:
                    type_list = [f"'{t.strip()}'" for t in plan_types.split(",")]
                    conditions.append(f"plan_type IN ({', '.join(type_list)})")
                if snp_types:
                    snp_list = [f"'{t.strip()}'" for t in snp_types.split(",")]
                    conditions.append(f"snp_type IN ({', '.join(snp_list)})")
                if group_types:
                    group_list = [f"'{t.strip()}'" for t in group_types.split(",")]
                    conditions.append(f"group_type IN ({', '.join(group_list)})")
                if states:
                    state_list = [f"'{s.strip()}'" for s in states.split(",")]
                    conditions.append(f"state IN ({', '.join(state_list)})")
                
                where_clause = " AND ".join(conditions)
                table = 'fact_enrollment_by_geography' if data_source == 'geographic' or states else 'fact_enrollment_unified'
                
                sql = f"SELECT * FROM {table} WHERE {where_clause} LIMIT 50000"
                try:
                    df = db.query(sql)
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name='Processed Data', index=False)
                        
                        # Add summary sheet
                        summary = df.groupby(['year']).agg({'enrollment': 'sum'}).reset_index()
                        summary.to_excel(writer, sheet_name='Summary', index=False)
                    excel_buffer.seek(0)
                    zf.writestr("5_processed_comparison/our_calculation.xlsx", excel_buffer.getvalue())
                    files_included.append(("5_processed_comparison/our_calculation.xlsx", 
                                          "Our processed calculation for comparison", ""))
                except:
                    pass
        
        zip_buffer.seek(0)
        
        filename = f"enrollment_audit_{year}_{parent_org.replace(' ', '_').replace(',', '').replace('.', '') if parent_org else 'all'}.zip"
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Audit download failed: {str(e)}")


@app.get("/api/v4/risk-scores/export")
async def export_risk_scores_data(
    year: int,
    parent_org: Optional[str] = None,
    format: str = "xlsx"
):
    """Export risk scores data as Excel file."""
    try:
        db = get_engine()
        if not db:
            raise HTTPException(status_code=503, detail="Database not available")
        
        conditions = [f"year = {year}"]
        if parent_org and parent_org != "Industry Total":
            conditions.append(f"parent_org = '{parent_org}'")
        
        where_clause = " AND ".join(conditions)
        
        sql = f"""
            SELECT * FROM fact_risk_scores_unified
            WHERE {where_clause}
            ORDER BY enrollment DESC
            LIMIT 50000
        """
        
        df = db.query(sql)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Risk Scores', index=False)
        output.seek(0)
        
        filename = f"risk_scores_{year}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/stars/export")
async def export_stars_data(
    year: int,
    parent_org: Optional[str] = None,
    format: str = "xlsx"
):
    """Export stars + enrollment data as Excel file."""
    try:
        db = get_engine()
        if not db:
            raise HTTPException(status_code=503, detail="Database not available")
        
        conditions = [f"year = {year}"]
        if parent_org:
            conditions.append(f"parent_org = '{parent_org}'")
        
        where_clause = " AND ".join(conditions)
        
        sql = f"""
            SELECT * FROM stars_enrollment_unified
            WHERE {where_clause}
            ORDER BY enrollment DESC
            LIMIT 50000
        """
        
        df = db.query(sql)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Stars', index=False)
        output.seek(0)
        
        filename = f"stars_{year}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/stars/measure-export")
async def export_measure_performance(
    year: int,
    measure_key: Optional[str] = None,
    parent_org: Optional[str] = None,
    format: str = "xlsx"
):
    """Export measure performance data as Excel file."""
    try:
        db = get_engine()
        if not db:
            raise HTTPException(status_code=503, detail="Database not available")
        
        conditions = [f"year = {year}"]
        if measure_key:
            conditions.append(f"measure_id = '{measure_key}'")
        
        where_clause = " AND ".join(conditions)
        
        sql = f"""
            SELECT * FROM measures_all_years
            WHERE {where_clause}
            ORDER BY measure_id, contract_id
            LIMIT 100000
        """
        
        df = db.query(sql)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Measures', index=False)
        output.seek(0)
        
        filename = f"measures_{year}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/stars/contract-export")
async def export_contract_performance(
    contract_id: str,
    format: str = "xlsx"
):
    """Export contract measure performance as Excel file."""
    try:
        db = get_engine()
        if not db:
            raise HTTPException(status_code=503, detail="Database not available")
        
        sql = f"""
            SELECT * FROM measures_all_years
            WHERE contract_id = '{contract_id}'
            ORDER BY year DESC, measure_id
            LIMIT 50000
        """
        
        df = db.query(sql)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Contract Performance', index=False)
        output.seek(0)
        
        filename = f"contract_{contract_id}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/stars/distribution-export")
async def export_distribution(
    year: int,
    payers: Optional[str] = None,
    format: str = "xlsx"
):
    """Export star distribution data as Excel file."""
    try:
        db = get_engine()
        if not db:
            raise HTTPException(status_code=503, detail="Database not available")
        
        conditions = [f"year = {year}"]
        if payers:
            payer_list = [f"'{p.strip()}'" for p in payers.split("|") if p.strip()]
            if payer_list:
                conditions.append(f"parent_org IN ({', '.join(payer_list)})")
        
        where_clause = " AND ".join(conditions)
        
        sql = f"""
            SELECT * FROM stars_enrollment_unified
            WHERE {where_clause}
            ORDER BY overall_rating DESC, enrollment DESC
            LIMIT 50000
        """
        
        df = db.query(sql)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Distribution', index=False)
        output.seek(0)
        
        filename = f"star_distribution_{year}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/stars/cutpoints-export")
async def export_cutpoints(
    year: Optional[int] = None,
    format: str = "xlsx"
):
    """Export cutpoints data as Excel file."""
    try:
        db = get_engine()
        if not db:
            raise HTTPException(status_code=503, detail="Database not available")
        
        if year:
            sql = f"""
                SELECT * FROM cutpoints_all_years
                WHERE year = {year}
                ORDER BY measure_id
            """
            filename = f"cutpoints_{year}.xlsx"
        else:
            sql = """
                SELECT * FROM cutpoints_all_years
                ORDER BY year DESC, measure_id
            """
            filename = "cutpoints_all_years.xlsx"
        
        df = db.query(sql)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Cutpoints', index=False)
        output.seek(0)
        
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@app.get("/api/v3/enrollment/by-parent")
async def get_enrollment_by_parent_v3(
    year: int = 2026,
    month: int = 1,
    limit: int = 20,
    user_id: str = "api"
):
    """
    Get enrollment by parent organization using unified data layer.

    Returns audit_id for lineage tracing.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available", "fallback": "/api/enrollment/by-parent"}

    try:
        service = get_enrollment_service()
        result = service.get_by_parent_org(
            year=year,
            limit=limit,
            user_id=user_id
        )
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/enrollment/by-state")
async def get_enrollment_by_state_v3(
    year: int = 2026,
    month: int = 1,
    user_id: str = "api"
):
    """
    Get enrollment by state using unified data layer.

    Note: State totals may be 1-3% lower than national due to HIPAA suppression.
    Returns audit_id for lineage tracing.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        service = get_enrollment_service()
        result = service.get_by_state(
            year=year,
            user_id=user_id
        )
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/enrollment/by-dimensions")
async def get_enrollment_by_dimensions_v3(
    year: int = 2026,
    month: int = 1,
    plan_type: Optional[str] = None,
    product_type: Optional[str] = None,
    snp_type: Optional[str] = None,
    user_id: str = "api"
):
    """
    Get enrollment by dimension combinations (plan_type, product_type, snp_type).

    Supports any filter combination. Returns audit_id for lineage tracing.
    Note: group_type not available in fact_enrollment_all_years
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        service = get_enrollment_service()
        result = service.get_by_dimensions(
            year=year,
            plan_type=plan_type,
            product_type=product_type,
            snp_type=snp_type,
            user_id=user_id
        )
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/enrollment/plan/{contract_id}/{plan_id}")
async def get_plan_details_v3(
    contract_id: str,
    plan_id: str,
    year: int = 2026,
    user_id: str = "api"
):
    """
    Get detailed information for a specific plan.

    Joins data from enrollment, stars, and risk scores.
    Returns audit_id for lineage tracing.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        service = get_enrollment_service()
        result = service.get_plan_details(
            contract_id=contract_id,
            plan_id=plan_id,
            year=year,
            user_id=user_id
        )
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/enrollment/filters")
async def get_enrollment_filters_v3(user_id: str = "api"):
    """
    Get available filter options for enrollment data.

    Returns all available values for filtering enrollment queries.
    Returns audit_id for lineage tracing.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        service = get_enrollment_service()
        result = service.get_filters(user_id=user_id)
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/enrollment/counties")
async def get_enrollment_counties_v3(
    states: Optional[str] = None,
    user_id: str = "api"
):
    """
    Get available counties for given states.

    Args:
        states: Comma-separated list of state codes (e.g., "CA,TX,FL")

    Returns counties with enrollment data.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        service = get_enrollment_service()
        state_list = states.split(",") if states else []

        # Query counties from geographic table
        if state_list:
            state_filter = "WHERE state IN ({})".format(",".join([f"'{s}'" for s in state_list]))
        else:
            state_filter = ""

        sql = f"""
            SELECT DISTINCT state, county
            FROM fact_enrollment_by_geography
            {state_filter}
            ORDER BY state, county
        """
        df, audit_id = service.engine.query_with_audit(
            sql,
            user_id=user_id,
            context="get_counties"
        )

        return {
            "counties": df.to_dict(orient="records"),
            "audit_id": audit_id
        }
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/ai/query")
async def ai_query(
    question: str,
    execute: bool = True,
    user_id: str = "ai_query"
):
    """
    Natural language query endpoint.

    Translates questions into SQL and returns results with full audit trail.

    Example questions:
    - "What is the total MA enrollment in 2025?"
    - "Who are the top 10 payers by enrollment?"
    - "What is the D-SNP enrollment trend?"
    - "What is UnitedHealth's market share?"
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        service = get_ai_query_service()
        result = service.query(
            question=question,
            user_id=user_id,
            execute=execute
        )
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/ai/explain")
async def ai_explain(question: str):
    """
    Explain how a natural language question would be interpreted.

    Returns:
    - Entities, measures, dimensions identified
    - Generated SQL
    - Source table and data lineage
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        service = get_ai_query_service()
        result = service.explain_query(question)
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/ai/suggestions")
async def ai_suggestions(partial: str = ""):
    """
    Get query suggestions based on partial input.

    Useful for autocomplete in UI.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"suggestions": [
            "What is the total MA enrollment?",
            "Who are the top 10 payers?",
            "What is the D-SNP enrollment trend?",
            "Which states have the highest enrollment?",
        ]}

    try:
        service = get_ai_query_service()
        suggestions = service.get_suggestions(partial)
        return {"suggestions": suggestions}
    except Exception as e:
        return {"error": str(e), "suggestions": []}


@app.get("/api/v3/lineage/{audit_id}")
async def trace_lineage(audit_id: str):
    """
    Trace data lineage for a previous query.

    Returns:
    - Query details (SQL, timestamp, user)
    - Tables accessed
    - Source files (CMS data files that produced the data)
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        service = get_enrollment_service()
        result = service.trace_lineage(audit_id)
        return result
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/tables")
async def list_available_tables():
    """
    List all available tables in the unified data layer.

    Returns table names, types (fact/dimension/aggregation), and descriptions.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        engine = get_engine()
        tables = engine.get_available_tables()
        return {"tables": tables}
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/sql")
async def execute_sql(
    sql: str,
    user_id: str = "api",
    context: str = "direct_sql"
):
    """
    Execute a SQL query directly against the unified data layer.

    USE WITH CAUTION - prefer semantic endpoints when possible.
    All queries are logged for audit.
    """
    if not UNIFIED_DATA_AVAILABLE:
        return {"error": "Unified data layer not available"}

    try:
        engine = get_engine()
        df, audit_id = engine.query_with_audit(
            sql=sql,
            user_id=user_id,
            context=context
        )
        return {
            "data": df.to_dict(orient='records'),
            "row_count": len(df),
            "columns": list(df.columns),
            "audit_id": audit_id
        }
    except Exception as e:
        return {"error": str(e), "status": "error"}


@app.get("/api/v3/status")
async def unified_layer_status():
    """
    Check status of the unified data layer.

    Returns availability and table registration status.
    """
    status = {
        "unified_data_available": UNIFIED_DATA_AVAILABLE,
        "version": "3.0.0",
        "features": [
            "DuckDB SQL over S3 Parquet",
            "Full audit logging",
            "Data lineage tracing",
            "AI natural language queries"
        ]
    }

    if UNIFIED_DATA_AVAILABLE:
        try:
            engine = get_engine()
            tables = engine.get_available_tables()
            status["tables_registered"] = len(tables)
            status["status"] = "healthy"
        except Exception as e:
            status["status"] = "degraded"
            status["error"] = str(e)
    else:
        status["status"] = "unavailable"
        status["fallback"] = "Using v2 endpoints with direct S3 reads"

    return status


# ================================================================
# V3 STARS ENDPOINTS - Full filter support with audit/lineage
# ================================================================

# Import stars service
try:
    from api.services.stars_service import get_stars_service
    STARS_SERVICE_AVAILABLE = True
except ImportError:
    STARS_SERVICE_AVAILABLE = False

# Import stars service V2 (NEW unified tables)
try:
    from api.services.stars_service_v2 import get_stars_service_v2, STARS_SERVICE_V2_AVAILABLE
except ImportError:
    STARS_SERVICE_V2_AVAILABLE = False

# Import risk scores service
try:
    from api.services.risk_scores_service import get_risk_scores_service
    RISK_SERVICE_AVAILABLE = True
except ImportError:
    RISK_SERVICE_AVAILABLE = False


@app.get("/api/v3/stars/filters")
async def get_stars_filters_v3():
    """Get all available filter options for stars data."""
    if not STARS_SERVICE_AVAILABLE:
        return {"error": "Stars service not available"}
    try:
        service = get_stars_service()
        return service.get_filters()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/stars/distribution")
async def get_stars_distribution_v3(
    parent_orgs: Optional[str] = None,  # Pipe-separated: "Humana|UnitedHealth"
    plan_types: Optional[str] = None,   # Comma-separated: "HMO/HMOPOS,Local PPO"
    group_types: Optional[str] = None,  # Comma-separated: "Individual,Group"
    snp_types: Optional[str] = None,    # Comma-separated: "Non-SNP,SNP"
    states: Optional[str] = None,       # Comma-separated: "FL,TX,CA"
    star_year: Optional[int] = None,
    include_industry_total: bool = True,
    data_source: str = "national",      # "national" = ALL MA enrollment, "rated" = only rated contracts
    user_id: str = "api"
):
    """
    Get 4+ star enrollment distribution with full filter support.

    Filters:
    - parent_orgs: Pipe-separated parent org names
    - plan_types: Comma-separated (HMO/HMOPOS, Local PPO, Regional PPO, PFFS)
    - group_types: Comma-separated (Individual, Group)
    - snp_types: Comma-separated (Non-SNP, SNP)
    - states: Comma-separated state abbreviations
    - star_year: Specific year or all years
    - data_source: "national" uses ALL MA enrollment (correct 4+ %), "rated" uses only rated contracts

    Returns audit_id for lineage tracing.
    """
    if not STARS_SERVICE_AVAILABLE:
        return {"error": "Stars service not available"}
    try:
        service = get_stars_service()
        return service.get_distribution(
            parent_orgs=parent_orgs.split("|") if parent_orgs else None,
            plan_types=plan_types.split(",") if plan_types else None,
            group_types=group_types.split(",") if group_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            states=states.split(",") if states else None,
            star_year=star_year,
            include_industry_total=include_industry_total,
            data_source=data_source,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/stars/by-parent")
async def get_stars_by_parent_v3(
    star_year: int = 2026,
    plan_types: Optional[str] = None,
    group_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    states: Optional[str] = None,
    limit: int = 50,
    user_id: str = "api"
):
    """Get star ratings by parent organization with full filter support."""
    if not STARS_SERVICE_AVAILABLE:
        return {"error": "Stars service not available"}
    try:
        service = get_stars_service()
        return service.get_by_parent(
            star_year=star_year,
            plan_types=plan_types.split(",") if plan_types else None,
            group_types=group_types.split(",") if group_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            states=states.split(",") if states else None,
            limit=limit,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/stars/by-state")
async def get_stars_by_state_v3(
    star_year: int = 2026,
    plan_types: Optional[str] = None,
    parent_orgs: Optional[str] = None,
    user_id: str = "api"
):
    """Get star ratings by state."""
    if not STARS_SERVICE_AVAILABLE:
        return {"error": "Stars service not available"}
    try:
        service = get_stars_service()
        return service.get_by_state(
            star_year=star_year,
            plan_types=plan_types.split(",") if plan_types else None,
            parent_orgs=parent_orgs.split("|") if parent_orgs else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/stars/measures")
async def get_stars_measures_v3(
    year: int = 2026,
    parent_orgs: Optional[str] = None,
    measure_ids: Optional[str] = None,
    domains: Optional[str] = None,      # HD1-HD5, DD1-DD4
    parts: Optional[str] = None,        # C, D
    data_sources: Optional[str] = None, # HEDIS, CAHPS, HOS, Admin
    user_id: str = "api"
):
    """Get measure-level performance with full filter support."""
    if not STARS_SERVICE_AVAILABLE:
        return {"error": "Stars service not available"}
    try:
        service = get_stars_service()
        return service.get_measure_performance(
            year=year,
            parent_orgs=parent_orgs.split("|") if parent_orgs else None,
            measure_ids=measure_ids.split(",") if measure_ids else None,
            domains=domains.split(",") if domains else None,
            parts=parts.split(",") if parts else None,
            data_sources=data_sources.split(",") if data_sources else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/stars/cutpoints")
async def get_stars_cutpoints_v3(
    years: Optional[str] = None,
    measure_ids: Optional[str] = None,
    parts: Optional[str] = None,
    user_id: str = "api"
):
    """Get star cutpoint thresholds by measure and year."""
    if not STARS_SERVICE_AVAILABLE:
        return {"error": "Stars service not available"}
    try:
        service = get_stars_service()
        return service.get_cutpoints(
            years=[int(y) for y in years.split(",")] if years else None,
            measure_ids=measure_ids.split(",") if measure_ids else None,
            parts=parts.split(",") if parts else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/stars/contract/{contract_id}")
async def get_stars_contract_v3(
    contract_id: str,
    year: int = 2026,
    user_id: str = "api"
):
    """Get detailed star rating info for a specific contract."""
    if not STARS_SERVICE_AVAILABLE:
        return {"error": "Stars service not available"}
    try:
        service = get_stars_service()
        return service.get_contract_detail(
            contract_id=contract_id,
            year=year,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


# ================================================================
# V4 STARS ENDPOINTS - NEW Unified Tables (2008-2026)
# Uses: measures_all_years, summary_all_years, cutpoints_all_years, domain_all_years
# ================================================================

@app.get("/api/v4/stars/filters")
async def get_stars_filters_v4():
    """Get all available filter options from NEW unified tables."""
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_filters()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/measures")
async def get_stars_measures_v4(
    year: int = 2026,
    measure_ids: Optional[str] = None,
    parts: Optional[str] = None,
    contract_ids: Optional[str] = None,
    user_id: str = "api"
):
    """
    Get measure performance from measures_all_years (2008-2026).

    NEW columns available: measure_key, numeric_value, raw_value
    """
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_measure_performance(
            year=year,
            measure_ids=measure_ids.split(",") if measure_ids else None,
            parts=parts.split(",") if parts else None,
            contract_ids=contract_ids.split(",") if contract_ids else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/measures/timeseries")
async def get_stars_measure_timeseries_v4(
    measure_id: str,
    start_year: int = 2008,
    end_year: int = 2026,
    user_id: str = "api"
):
    """Get timeseries for a specific measure across years."""
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_measure_timeseries(
            measure_id=measure_id,
            start_year=start_year,
            end_year=end_year,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/cutpoints")
async def get_stars_cutpoints_v4(
    years: Optional[str] = None,
    measure_ids: Optional[str] = None,
    parts: Optional[str] = None,
    star_levels: Optional[str] = None,
    user_id: str = "api"
):
    """
    Get cutpoint thresholds from cutpoints_all_years (2011-2026).

    NOTE: Uses star_level column (not star_rating from v3).
    """
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_cutpoints(
            years=[int(y) for y in years.split(",")] if years else None,
            measure_ids=measure_ids.split(",") if measure_ids else None,
            parts=parts.split(",") if parts else None,
            star_levels=[int(l) for l in star_levels.split(",")] if star_levels else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/cutpoints/timeseries")
async def get_stars_cutpoints_timeseries_v4(
    measure_id: str,
    star_level: int = 4,
    start_year: int = 2011,
    end_year: int = 2026,
    user_id: str = "api"
):
    """Get cutpoint threshold timeseries for a measure."""
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_cutpoints_timeseries(
            measure_id=measure_id,
            star_level=star_level,
            start_year=start_year,
            end_year=end_year,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/summary")
async def get_stars_summary_v4(
    years: Optional[str] = None,
    contract_ids: Optional[str] = None,
    parts: Optional[str] = None,
    user_id: str = "api"
):
    """
    Get summary ratings from summary_all_years (2009-2026).

    NOTE: Uses year column (not rating_year) and LONG format.
    """
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_summary_ratings(
            years=[int(y) for y in years.split(",")] if years else None,
            contract_ids=contract_ids.split(",") if contract_ids else None,
            parts=parts.split(",") if parts else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/summary/distribution")
async def get_stars_summary_distribution_v4(
    year: int = 2026,
    part: str = "C",
    user_id: str = "api"
):
    """Get distribution of summary ratings for a year."""
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_summary_distribution(
            year=year,
            part=part,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/domains")
async def get_stars_domains_v4(
    years: Optional[str] = None,
    contract_ids: Optional[str] = None,
    parts: Optional[str] = None,
    domain_names: Optional[str] = None,
    user_id: str = "api"
):
    """
    Get domain scores from domain_all_years (2008-2026).

    NEW endpoint - 15 more years of data than v1!
    """
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_domain_scores(
            years=[int(y) for y in years.split(",")] if years else None,
            contract_ids=contract_ids.split(",") if contract_ids else None,
            parts=parts.split(",") if parts else None,
            domain_names=domain_names.split(",") if domain_names else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/domains/averages")
async def get_stars_domain_averages_v4(
    year: int = 2026,
    part: Optional[str] = None,
    user_id: str = "api"
):
    """Get average domain scores across all contracts for a year."""
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_domain_averages(
            year=year,
            part=part,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v4/stars/contract/{contract_id}")
async def get_stars_contract_v4(
    contract_id: str,
    year: int = 2026,
    user_id: str = "api"
):
    """
    Get detailed star rating info for a contract.

    Includes: summary, measures, and domain scores from NEW unified tables.
    """
    if not STARS_SERVICE_V2_AVAILABLE:
        return {"error": "Stars service V2 not available"}
    try:
        service = get_stars_service_v2()
        return service.get_contract_detail(
            contract_id=contract_id,
            year=year,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


# ================================================================
# V3 RISK SCORES ENDPOINTS - Full filter support with audit/lineage
# ================================================================

@app.get("/api/v3/risk/filters")
async def get_risk_filters_v3():
    """Get all available filter options for risk score data."""
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_filters()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/risk/summary")
async def get_risk_summary_v3(
    year: Optional[int] = None,
    parent_orgs: Optional[str] = None,
    plan_types: Optional[str] = None,
    group_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    states: Optional[str] = None,
    user_id: str = "api"
):
    """Get risk score summary statistics with full filter support."""
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_summary(
            year=year,
            parent_orgs=parent_orgs.split("|") if parent_orgs else None,
            plan_types=plan_types.split(",") if plan_types else None,
            group_types=group_types.split(",") if group_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            states=states.split(",") if states else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/risk/timeseries")
async def get_risk_timeseries_v3(
    parent_orgs: Optional[str] = None,
    plan_types: Optional[str] = None,
    group_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    states: Optional[str] = None,
    metric: str = "wavg",  # "wavg" or "avg"
    include_industry_total: bool = True,
    group_by: Optional[str] = None,  # "plan_type", "snp_type", "group_type"
    user_id: str = "api"
):
    """
    Get risk score timeseries with full filter support.

    Metrics:
    - wavg: Enrollment-weighted average (recommended)
    - avg: Simple average

    group_by:
    - plan_type: Break down by HMO, PPO, etc.
    - snp_type: Break down by SNP status
    - group_type: Break down by Individual vs Group

    Returns audit_id for lineage tracing.
    """
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_timeseries(
            parent_orgs=parent_orgs.split("|") if parent_orgs else None,
            plan_types=plan_types.split(",") if plan_types else None,
            group_types=group_types.split(",") if group_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            states=states.split(",") if states else None,
            metric=metric,
            include_industry_total=include_industry_total,
            group_by=group_by,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/risk/by-parent")
async def get_risk_by_parent_v3(
    year: int = 2024,
    plan_types: Optional[str] = None,
    group_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    states: Optional[str] = None,
    limit: int = 50,
    user_id: str = "api"
):
    """Get risk scores by parent organization with full filter support."""
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_by_parent(
            year=year,
            plan_types=plan_types.split(",") if plan_types else None,
            group_types=group_types.split(",") if group_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            states=states.split(",") if states else None,
            limit=limit,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/risk/by-state")
async def get_risk_by_state_v3(
    year: int = 2024,
    plan_types: Optional[str] = None,
    parent_orgs: Optional[str] = None,
    user_id: str = "api"
):
    """Get risk scores by state."""
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_by_state(
            year=year,
            plan_types=plan_types.split(",") if plan_types else None,
            parent_orgs=parent_orgs.split("|") if parent_orgs else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/risk/by-dimensions")
async def get_risk_by_dimensions_v3(
    year: int = 2024,
    parent_orgs: Optional[str] = None,
    states: Optional[str] = None,
    user_id: str = "api"
):
    """Get risk scores broken down by all dimensions (plan_type x snp_type x group_type)."""
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_by_dimensions(
            year=year,
            parent_orgs=parent_orgs.split("|") if parent_orgs else None,
            states=states.split(",") if states else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/risk/distribution")
async def get_risk_distribution_v3(
    year: int = 2024,
    parent_orgs: Optional[str] = None,
    plan_types: Optional[str] = None,
    bins: int = 20,
    user_id: str = "api"
):
    """Get risk score distribution (histogram data)."""
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_distribution(
            year=year,
            parent_orgs=parent_orgs.split("|") if parent_orgs else None,
            plan_types=plan_types.split(",") if plan_types else None,
            bins=bins,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/risk/plan/{contract_id}/{plan_id}")
async def get_risk_plan_v3(
    contract_id: str,
    plan_id: str,
    user_id: str = "api"
):
    """Get risk score history for a specific plan."""
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_plan_detail(
            contract_id=contract_id,
            plan_id=plan_id,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v3/risk/contracts")
async def get_risk_contracts_v3(
    year: int,
    parent_org: Optional[str] = None,
    plan_types: Optional[str] = None,
    group_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    user_id: str = "api"
):
    """
    Get contract-level risk score details for auditing.

    Returns all contracts with their risk scores and enrollment,
    allowing users to verify weighted average calculations.
    """
    if not RISK_SERVICE_AVAILABLE:
        return {"error": "Risk scores service not available"}
    try:
        service = get_risk_scores_service()
        return service.get_contract_details(
            year=year,
            parent_org=parent_org,
            plan_types=plan_types.split(",") if plan_types else None,
            group_types=group_types.split(",") if group_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            user_id=user_id
        )
    except Exception as e:
        return {"error": str(e)}


# =====================================================
# DATA SOURCES DOWNLOAD ENDPOINTS - RAW CMS FILES
# =====================================================

def get_s3_presigned_url(s3_key: str, filename: str, expiration: int = 3600):
    """Generate a presigned URL for downloading a raw file from S3."""
    import boto3
    s3 = boto3.client('s3')
    bucket = 'ma-data123'
    
    url = s3.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': bucket,
            'Key': s3_key,
            'ResponseContentDisposition': f'attachment; filename="{filename}"'
        },
        ExpiresIn=expiration
    )
    return url

def find_raw_file(prefix: str, year: int, month: str = "12") -> tuple:
    """Find a raw file in S3, returns (s3_key, filename) or raises error."""
    import boto3
    s3 = boto3.client('s3')
    bucket = 'ma-data123'
    
    # List files matching the prefix and year/month
    search_prefix = f"{prefix}{year}-{month}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=search_prefix, MaxKeys=10)
    
    files = [obj for obj in response.get('Contents', []) if obj['Size'] > 1000]
    if files:
        key = files[0]['Key']
        filename = key.split('/')[-1]
        return key, filename
    
    # Try without month for yearly files
    search_prefix = f"{prefix}{year}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=search_prefix, MaxKeys=10)
    files = [obj for obj in response.get('Contents', []) if obj['Size'] > 1000]
    if files:
        key = files[0]['Key']
        filename = key.split('/')[-1]
        return key, filename
    
    return None, None


@app.get("/api/data-sources/cpsc")
async def download_cpsc_raw(
    year: int = 2024,
    month: str = "12",
    format: str = "raw"
):
    """
    Download RAW CPSC enrollment file from CMS.
    Returns the original ZIP file from S3.
    """
    try:
        # Find the raw CPSC file
        s3_key, filename = find_raw_file("raw/enrollment/cpsc/", year, month)
        
        if not s3_key:
            raise HTTPException(
                status_code=404, 
                detail=f"Raw CPSC file not found for {year}-{month}. Available years: 2013-2025"
            )
        
        # Generate presigned URL for direct download
        url = get_s3_presigned_url(s3_key, filename)
        
        # Redirect to presigned URL
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/data-sources/enrollment")
async def download_enrollment_raw(
    year: int = 2024,
    month: str = "12",
    format: str = "raw"
):
    """
    Download RAW Monthly Enrollment by Plan file from CMS.
    Returns the original ZIP file from S3.
    """
    try:
        # Find the raw enrollment file
        s3_key, filename = find_raw_file("raw/enrollment/by_plan/", year, month)
        
        if not s3_key:
            raise HTTPException(
                status_code=404, 
                detail=f"Raw enrollment file not found for {year}-{month}. Available years: 2007-2025"
            )
        
        # Generate presigned URL for direct download
        url = get_s3_presigned_url(s3_key, filename)
        
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/data-sources/enrollment-contract")
async def download_enrollment_contract_raw(
    year: int = 2024,
    month: str = "12",
    format: str = "raw"
):
    """
    Download RAW Monthly Enrollment by Contract file from CMS.
    Returns the original ZIP file from S3.
    """
    try:
        # Find the raw enrollment by contract file
        s3_key, filename = find_raw_file("raw/enrollment/by_contract/", year, month)
        
        if not s3_key:
            raise HTTPException(
                status_code=404, 
                detail=f"Raw enrollment by contract file not found for {year}-{month}. Available years: 2013-2026"
            )
        
        # Generate presigned URL for direct download
        url = get_s3_presigned_url(s3_key, filename)
        
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/data-sources/snp")
async def download_snp_raw(
    year: int = 2024,
    month: str = "12",
    format: str = "raw"
):
    """
    Download RAW SNP (Special Needs Plan) file from CMS.
    Returns the original ZIP file from S3.
    """
    try:
        # Find the raw SNP file
        s3_key, filename = find_raw_file("raw/snp/", year, month)
        
        if not s3_key:
            raise HTTPException(
                status_code=404, 
                detail=f"Raw SNP file not found for {year}-{month}. Available years: 2007-2024"
            )
        
        # Generate presigned URL for direct download
        url = get_s3_presigned_url(s3_key, filename)
        
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/data-sources/crosswalk")
async def download_crosswalk_raw(
    year: int = 2024,
    format: str = "raw"
):
    """
    Download RAW Contract Crosswalk file from CMS.
    Returns the original ZIP file from S3.
    """
    try:
        import boto3
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        # Crosswalk files are named crosswalk_YYYY.zip directly in raw/crosswalks/
        s3_key = f"raw/crosswalks/crosswalk_{year}.zip"
        
        # Verify file exists
        try:
            s3.head_object(Bucket=bucket, Key=s3_key)
        except:
            raise HTTPException(
                status_code=404, 
                detail=f"Raw crosswalk file not found for {year}. Available years: 2006-2026"
            )
        
        filename = f"crosswalk_{year}.zip"
        url = get_s3_presigned_url(s3_key, filename)
        
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/data-sources/stars")
async def download_stars_raw(
    year: int = 2024,
    format: str = "raw"
):
    """
    Download RAW Star Ratings file from CMS.
    Returns the original ZIP file from S3.
    """
    try:
        import boto3
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        # Stars files have different naming patterns by year
        search_prefix = f"raw/stars/{year}"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=search_prefix, MaxKeys=10)
        
        files = [obj for obj in response.get('Contents', []) if obj['Size'] > 1000]
        if not files:
            raise HTTPException(
                status_code=404, 
                detail=f"Raw stars file not found for {year}. Available years: 2007-2026"
            )
        
        s3_key = files[0]['Key']
        filename = s3_key.split('/')[-1]
        
        url = get_s3_presigned_url(s3_key, filename)
        
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/data-sources/risk-scores")
async def download_risk_scores_raw(
    year: int = 2024,
    format: str = "raw"
):
    """
    Download RAW Plan Payment (Risk Scores) file from CMS.
    Returns the original ZIP file from S3.
    """
    try:
        import boto3
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        # Plan payment files are yearly
        search_prefix = f"raw/plan_payment/{year}"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=search_prefix, MaxKeys=10)
        
        files = [obj for obj in response.get('Contents', []) if obj['Size'] > 1000]
        if not files:
            raise HTTPException(
                status_code=404, 
                detail=f"Raw risk scores file not found for {year}. Available years: 2006-2024"
            )
        
        s3_key = files[0]['Key']
        filename = s3_key.split('/')[-1]
        
        url = get_s3_presigned_url(s3_key, filename)
        
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/data-sources/ratebook")
async def download_ratebook_raw(
    year: int = 2026,
    format: str = "raw"
):
    """
    Download RAW Ratebook (County Benchmark Rates) file from CMS.
    Returns the original ZIP file from S3.
    
    Ratebook contains county-level MA payment rates - the actual $/month
    CMS pays plans per beneficiary by county.
    """
    try:
        import boto3
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        s3_key = f"raw/rates/ratebook/ratebook_{year}.zip"
        
        # Check if file exists
        try:
            s3.head_object(Bucket=bucket, Key=s3_key)
        except:
            raise HTTPException(
                status_code=404, 
                detail=f"Ratebook file not found for {year}. Available years: 2016-2026"
            )
        
        filename = f"ratebook_{year}.zip"
        url = get_s3_presigned_url(s3_key, filename)
        
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url=url, status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/data-sources/list")
async def list_available_raw_files():
    """
    List all available raw CMS files that can be downloaded.
    Returns years available for each data source.
    """
    try:
        import boto3
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        result = {}
        
        # CPSC files
        cpsc_years = set()
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix='raw/enrollment/cpsc/'):
            for obj in page.get('Contents', []):
                if obj['Size'] > 1000:
                    parts = obj['Key'].split('/')
                    if len(parts) > 3:
                        year_month = parts[3]
                        if '-' in year_month:
                            cpsc_years.add(int(year_month.split('-')[0]))
        result['cpsc'] = sorted(cpsc_years)
        
        # Monthly enrollment
        enrollment_years = set()
        for page in paginator.paginate(Bucket=bucket, Prefix='raw/enrollment/by_plan/'):
            for obj in page.get('Contents', []):
                if obj['Size'] > 1000:
                    parts = obj['Key'].split('/')
                    if len(parts) > 3:
                        year_month = parts[3]
                        if '-' in year_month:
                            enrollment_years.add(int(year_month.split('-')[0]))
        result['enrollment'] = sorted(enrollment_years)
        
        # SNP files
        snp_years = set()
        for page in paginator.paginate(Bucket=bucket, Prefix='raw/snp/'):
            for obj in page.get('Contents', []):
                if obj['Size'] > 100:
                    parts = obj['Key'].split('/')
                    if len(parts) > 2:
                        year_month = parts[2]
                        if '-' in year_month:
                            snp_years.add(int(year_month.split('-')[0]))
        result['snp'] = sorted(snp_years)
        
        # Stars files
        stars_years = set()
        response = s3.list_objects_v2(Bucket=bucket, Prefix='raw/stars/', MaxKeys=100)
        for obj in response.get('Contents', []):
            if obj['Size'] > 1000:
                filename = obj['Key'].split('/')[-1]
                for y in range(2007, 2030):
                    if str(y) in filename:
                        stars_years.add(y)
                        break
        result['stars'] = sorted(stars_years)
        
        # Risk scores (plan payment)
        risk_years = set()
        response = s3.list_objects_v2(Bucket=bucket, Prefix='raw/plan_payment/', MaxKeys=100)
        for obj in response.get('Contents', []):
            if obj['Size'] > 1000:
                parts = obj['Key'].split('/')
                if len(parts) > 2 and parts[2].isdigit():
                    risk_years.add(int(parts[2]))
        result['risk_scores'] = sorted(risk_years)
        
        # Crosswalks - files are named crosswalk_YYYY.zip
        crosswalk_years = set()
        response = s3.list_objects_v2(Bucket=bucket, Prefix='raw/crosswalks/', MaxKeys=100)
        import re
        for obj in response.get('Contents', []):
            if obj['Size'] > 100:
                filename = obj['Key'].split('/')[-1]
                # Match crosswalk_YYYY.zip pattern
                match = re.search(r'crosswalk_(\d{4})\.zip', filename)
                if match:
                    crosswalk_years.add(int(match.group(1)))
        result['crosswalk'] = sorted(crosswalk_years)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")


@app.get("/api/data-lineage")
async def get_data_lineage():
    """
    Comprehensive data lineage showing all tables, their sources, and coverage.
    Provides full audit trail from raw CMS files to gold layer tables.
    """
    try:
        import boto3
        from io import BytesIO
        import pandas as pd
        
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        lineage = {
            "gold_tables": [],
            "dimension_tables": [],
            "raw_sources": [],
            "processing_pipeline": []
        }
        
        # 1. Gold Tables
        gold_tables = [
            ("gold/fact_enrollment_national.parquet", "Enrollment (National)", "Monthly Enrollment by Contract"),
            ("gold/dim_plan.parquet", "Plan Dimensions", "CPSC Contract Info"),
            ("gold/dim_entity.parquet", "Entity/Organization", "CPSC Contract Info"),
        ]
        
        for key, name, source in gold_tables:
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                df = pd.read_parquet(BytesIO(obj['Body'].read()))
                
                years = sorted(df['year'].unique().tolist()) if 'year' in df.columns else []
                source_col = [c for c in df.columns if 'source' in c.lower()]
                
                table_info = {
                    "table": key,
                    "name": name,
                    "rows": len(df),
                    "columns": list(df.columns),
                    "years": years,
                    "year_range": f"{min(years)}-{max(years)}" if years else "N/A",
                    "source_tracking": source_col,
                    "raw_source": source,
                }
                
                # Get sample source files if available
                if source_col and len(source_col) > 0:
                    sample_sources = df[source_col[0]].dropna().unique()[:3].tolist()
                    table_info["sample_source_files"] = sample_sources
                
                lineage["gold_tables"].append(table_info)
            except Exception as e:
                lineage["gold_tables"].append({"table": key, "name": name, "error": str(e)[:50]})
        
        # 2. Processed CPSC Coverage
        cpsc_months = []
        response = s3.list_objects_v2(Bucket=bucket, Prefix='processed/fact_enrollment/', MaxKeys=500)
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                parts = obj['Key'].split('/')
                if len(parts) >= 4 and parts[2].isdigit():
                    cpsc_months.append(f"{parts[2]}-{parts[3]}")
        
        lineage["raw_sources"].append({
            "source": "CPSC (Geographic)",
            "location": "processed/fact_enrollment/YYYY/MM/data.parquet",
            "coverage": f"{len(cpsc_months)} months",
            "years": sorted(set([m.split('-')[0] for m in cpsc_months])),
            "raw_origin": "raw/enrollment/cpsc/*/cpsc_enrollment_*.zip",
            "cms_source": "CMS Monthly Enrollment by CPSC"
        })
        
        # 3. Monthly by Contract Coverage
        monthly_files = []
        response = s3.list_objects_v2(Bucket=bucket, Prefix='raw/enrollment/by_contract/', MaxKeys=500)
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.zip') and obj['Size'] > 50000:
                import re
                match = re.search(r'(\d{4})-(\d{2})', obj['Key'])
                if match:
                    monthly_files.append(f"{match.group(1)}-{match.group(2)}")
        
        lineage["raw_sources"].append({
            "source": "Monthly Enrollment by Contract",
            "location": "raw/enrollment/by_contract/YYYY-MM/*.zip",
            "coverage": f"{len(monthly_files)} months",
            "years": sorted(set([m.split('-')[0] for m in monthly_files])),
            "cms_source": "CMS Monthly Enrollment by Contract"
        })
        
        # 4. SNP Lookup
        try:
            obj = s3.get_object(Bucket=bucket, Key='processed/unified/snp_lookup.parquet')
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            lineage["dimension_tables"].append({
                "table": "snp_lookup",
                "name": "SNP Type Classification",
                "rows": len(df),
                "years": sorted(df['year'].unique().tolist()),
                "snp_types": sorted(df['snp_type'].unique().tolist()),
                "source_tracking": "_source_file",
                "raw_origin": "raw/snp/*/snp_*.zip"
            })
        except:
            pass
        
        # 5. Crosswalk Coverage
        crosswalk_years = []
        response = s3.list_objects_v2(Bucket=bucket, Prefix='raw/crosswalks/', MaxKeys=50)
        import re
        for obj in response.get('Contents', []):
            match = re.search(r'crosswalk_(\d{4})\.zip', obj['Key'])
            if match:
                crosswalk_years.append(int(match.group(1)))
        
        lineage["dimension_tables"].append({
            "table": "crosswalk",
            "name": "Contract Crosswalk",
            "years": sorted(set(crosswalk_years)),
            "purpose": "Track contract ID changes, mergers, acquisitions",
            "raw_origin": "raw/crosswalks/crosswalk_YYYY.zip"
        })
        
        # 6. Processing Pipeline Description
        lineage["processing_pipeline"] = [
            {
                "step": 1,
                "name": "Raw Ingestion",
                "description": "Download ZIP files from CMS website",
                "output": "raw/enrollment/cpsc/, raw/enrollment/by_contract/, raw/snp/, etc."
            },
            {
                "step": 2,
                "name": "Processing",
                "description": "Extract CSVs, normalize columns, convert to Parquet",
                "output": "processed/fact_enrollment/, processed/unified/"
            },
            {
                "step": 3,
                "name": "Gold Layer",
                "description": "Aggregate, join dimensions, add source tracking",
                "output": "gold/fact_enrollment_national.parquet, gold/dim_*.parquet"
            }
        ]
        
        return sanitize_for_json(lineage)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get lineage: {str(e)}")


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "1.0.0"}


# ================================================================
# RAW DATA SCHEMA API (For AI Tutorial Mode)
# ================================================================

@app.get("/api/data-schema/list")
async def list_available_data_schemas():
    """
    List all raw data sources available for schema extraction.
    Used by chat to let users select data sources for tutorials.
    Dynamically checks what years are actually in the database.
    Shows latest available month for each year.
    """
    try:
        from db import get_engine
        engine = get_engine()
        
        # Define sources and their tables
        sources_config = [
            {
                "id": "cpsc",
                "name": "CPSC Enrollment",
                "description": "County-level enrollment with geographic detail",
                "table": "fact_enrollment_all_years",
                "has_month": True,
                "key_columns": ["contract_id", "plan_id", "state", "county", "enrollment"],
                "join_keys": ["contract_id", "plan_id"]
            },
            {
                "id": "enrollment",
                "name": "Monthly Enrollment by Plan", 
                "description": "Plan-level enrollment with contract_id, plan_id, parent_org",
                "table": "gold_fact_enrollment_national",
                "has_month": True,
                "key_columns": ["contract_id", "plan_id", "enrollment", "parent_org"],
                "join_keys": ["contract_id", "plan_id"]
            },
            {
                "id": "stars",
                "name": "Star Ratings",
                "description": "Quality ratings and measures",
                "table": "summary_all_years",
                "has_month": False,
                "key_columns": ["contract_id", "overall_rating", "part_c_rating", "part_d_rating"],
                "join_keys": ["contract_id"]
            },
            {
                "id": "risk_scores",
                "name": "Risk Scores",
                "description": "Plan risk adjustment data",
                "table": "fact_risk_scores_unified",
                "has_month": False,
                "key_columns": ["contract_id", "plan_id", "risk_score_partc", "risk_score_partd"],
                "join_keys": ["contract_id", "plan_id"]
            },
            {
                "id": "snp",
                "name": "SNP Enrollment (by Parent Org)",
                "description": "SNP enrollment aggregated by parent org and type (C-SNP, D-SNP, I-SNP)",
                "table": "fact_snp_combined",  # Virtual - we'll union fact_snp + fact_snp_historical
                "has_month": False,
                "key_columns": ["parent_org", "snp_type", "enrollment", "year"],
                "join_keys": ["parent_org", "year"]
            },
            # CMS CROSSWALK FILES (raw from CMS - plan year-over-year transitions)
            {
                "id": "crosswalk",
                "name": "CMS Plan Crosswalk",
                "description": "Year-over-year plan transitions (previous→current, status, SNP changes)",
                "table": "raw_crosswalk",  # Special - loaded from S3 zip files
                "has_month": False,
                "is_raw_file": True,
                "key_columns": ["PREVIOUS_CONTRACT_ID", "CURRENT_CONTRACT_ID", "PREVIOUS_PLAN_NAME", "CURRENT_PLAN_NAME", "STATUS"],
                "join_keys": ["PREVIOUS_CONTRACT_ID", "CURRENT_CONTRACT_ID"]
            }
        ]
        
        data_sources = []
        for config in sources_config:
            try:
                # Special handling for combined SNP table
                if config['table'] == 'fact_snp_combined':
                    years_sql = """
                        SELECT DISTINCT year FROM (
                            SELECT year FROM fact_snp
                            UNION ALL
                            SELECT year FROM fact_snp_historical
                        ) ORDER BY year
                    """
                    result = engine.query(years_sql)
                    years = [int(row['year']) for _, row in result.iterrows()]
                    years_data = [{"year": y, "month": None} for y in years]
                elif config['table'] == 'raw_crosswalk':
                    # List crosswalk years from S3
                    import boto3
                    s3 = boto3.client('s3')
                    response = s3.list_objects_v2(Bucket='ma-data123', Prefix='raw/crosswalks/crosswalk_')
                    years = []
                    for obj in response.get('Contents', []):
                        key = obj['Key']
                        # Extract year from filename like crosswalk_2025.zip
                        if '_to_' not in key:  # Skip transition files like crosswalk_2024_to_2025.zip
                            try:
                                year_str = key.split('crosswalk_')[1].split('.zip')[0]
                                years.append(int(year_str))
                            except:
                                pass
                    years = sorted(set(years))
                    years_data = [{"year": y, "month": None} for y in years]
                elif config.get("has_month"):
                    # Get years with their latest month (Dec for past years, Feb for 2026)
                    years_sql = f"""
                        SELECT year, MAX(month) as latest_month 
                        FROM {config['table']} 
                        GROUP BY year 
                        ORDER BY year
                    """
                    result = engine.query(years_sql)
                    years_data = [{"year": int(row['year']), "month": int(row['latest_month'])} for _, row in result.iterrows()]
                    years = [d["year"] for d in years_data]
                else:
                    # Just get years
                    years_sql = f"SELECT DISTINCT year FROM {config['table']} ORDER BY year"
                    result = engine.query(years_sql)
                    years = [int(row['year']) for _, row in result.iterrows()]
                    years_data = [{"year": y, "month": None} for y in years]
                
                data_sources.append({
                    "id": config["id"],
                    "name": config["name"],
                    "description": config["description"],
                    "years": years,
                    "years_detail": years_data,
                    "has_month": config.get("has_month", False),
                    "key_columns": config["key_columns"],
                    "join_keys": config["join_keys"]
                })
            except Exception as e:
                print(f"Error getting years for {config['id']}: {e}")
                data_sources.append({
                    "id": config["id"],
                    "name": config["name"],
                    "description": config["description"],
                    "years": [],
                    "years_detail": [],
                    "has_month": config.get("has_month", False),
                    "key_columns": config["key_columns"],
                    "join_keys": config["join_keys"]
                })
        
        return {"data_sources": data_sources}
        
    except Exception as e:
        print(f"Error in data-schema/list: {e}")
        # Fallback to static if DB fails
        return {
            "data_sources": [
                {"id": "cpsc", "name": "CPSC Enrollment", "description": "County-level enrollment", "years": list(range(2013, 2027)), "years_detail": [], "has_month": True, "key_columns": ["contract_id", "plan_id", "state", "county"], "join_keys": ["contract_id", "plan_id"]},
                {"id": "enrollment", "name": "Monthly Enrollment", "description": "Contract-plan enrollment", "years": list(range(2007, 2027)), "years_detail": [], "has_month": True, "key_columns": ["contract_id", "plan_id", "enrollment"], "join_keys": ["contract_id", "plan_id"]},
                {"id": "stars", "name": "Star Ratings", "description": "Quality ratings", "years": list(range(2008, 2027)), "years_detail": [], "has_month": False, "key_columns": ["contract_id", "overall_rating"], "join_keys": ["contract_id"]},
                {"id": "risk_scores", "name": "Risk Scores", "description": "Risk adjustment data", "years": list(range(2006, 2025)), "years_detail": [], "has_month": False, "key_columns": ["contract_id", "risk_score_partc"], "join_keys": ["contract_id"]},
                {"id": "snp", "name": "SNP Classification", "description": "Special Needs Plans", "years": list(range(2007, 2025)), "years_detail": [], "has_month": False, "key_columns": ["contract_id", "snp_type"], "join_keys": ["contract_id"]}
            ]
        }


@app.get("/api/data-schema/{source_id}/{year}")
async def get_data_schema_and_sample(source_id: str, year: int, month: Optional[int] = None):
    """
    Get schema and sample rows from a processed data source.
    Returns column names, types, and 5 sample rows for AI context.
    Uses latest available month if not specified (Dec for past years, Feb for 2026).
    """
    try:
        from db import get_engine
        engine = get_engine()
        
        # Map source_id to actual table and whether it has month
        table_config = {
            "cpsc": {"table": "fact_enrollment_all_years", "has_month": True},
            "enrollment": {"table": "gold_fact_enrollment_national", "has_month": True},
            "stars": {"table": "summary_all_years", "has_month": False},
            "risk_scores": {"table": "fact_risk_scores_unified", "has_month": False},
            "snp": {"table": "fact_snp_historical", "has_month": False}
        }
        
        if source_id not in table_config:
            raise HTTPException(status_code=400, detail=f"Unknown source: {source_id}")
        
        config = table_config[source_id]
        table = config["table"]
        has_month = config["has_month"]
        
        # Get schema
        schema_sql = f"DESCRIBE {table}"
        schema_result = engine.query(schema_sql)
        columns = [{"name": row['column_name'], "type": row['column_type']} for _, row in schema_result.iterrows()]
        
        # Determine month filter for tables that have month
        month_filter = ""
        actual_month = None
        if has_month:
            if month:
                actual_month = month
            else:
                # Get latest month for this year
                month_sql = f"SELECT MAX(month) as m FROM {table} WHERE year = {year}"
                month_result = engine.query(month_sql)
                actual_month = int(month_result.iloc[0]['m']) if not month_result.empty else 12
            month_filter = f" AND month = {actual_month}"
        
        # Get sample rows
        sample_sql = f"SELECT * FROM {table} WHERE year = {year}{month_filter} LIMIT 5"
        sample_result = engine.query(sample_sql)
        sample_rows = sample_result.to_dict('records')
        
        # Get row count
        count_sql = f"SELECT COUNT(*) as cnt FROM {table} WHERE year = {year}{month_filter}"
        count_result = engine.query(count_sql)
        row_count = int(count_result.iloc[0]['cnt'])
        
        month_label = ""
        if has_month and actual_month:
            month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            month_label = f" ({month_names[actual_month]} {year})"
        
        return {
            "source_id": source_id,
            "year": year,
            "month": actual_month,
            "table": table,
            "schema": columns,
            "sample_rows": sample_rows,
            "total_rows": row_count,
            "description": f"Schema and sample data from {table} for {year}{month_label}"
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to get schema: {str(e)}")


# ================================================================
# DATA LINKING API - Link multiple data sources and export to Excel
# ================================================================

from pydantic import BaseModel
from typing import List

class DataSourceSelection(BaseModel):
    source_id: str
    year: int

class LinkDataRequest(BaseModel):
    sources: List[DataSourceSelection]
    
@app.post("/api/data-link")
async def link_data_sources(request: LinkDataRequest):
    """
    Link multiple data sources and return Excel files.
    Returns: individual source files + combined linked file + join logic used.
    """
    import io
    import pandas as pd
    from openpyxl import Workbook
    from openpyxl.utils.dataframe import dataframe_to_rows
    import base64
    
    if len(request.sources) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 data sources to link")
    
    try:
        from db import get_engine
        engine = get_engine()
        
        # Table config with join keys
        table_config = {
            "cpsc": {
                "table": "fact_enrollment_all_years", 
                "has_month": True,
                "join_keys": ["contract_id", "plan_id", "year"],
                "display_name": "CPSC Enrollment"
            },
            "enrollment": {
                "table": "fact_enrollment_national", 
                "has_month": True,
                "join_keys": ["contract_id", "plan_id", "year"],
                "display_name": "Monthly Enrollment"
            },
            "stars": {
                "table": "summary_all_years", 
                "has_month": False,
                "join_keys": ["contract_id", "year"],
                "display_name": "Star Ratings"
            },
            "risk_scores": {
                "table": "fact_risk_scores_unified", 
                "has_month": False,
                "join_keys": ["contract_id", "plan_id", "year"],
                "display_name": "Risk Scores"
            },
            "snp": {
                "table": "fact_snp_historical", 
                "has_month": False,
                "join_keys": ["contract_id", "plan_id", "year"],
                "display_name": "SNP Classification"
            }
        }
        
        # Fetch each data source
        dataframes = {}
        source_info = []
        
        for src in request.sources:
            if src.source_id not in table_config:
                raise HTTPException(status_code=400, detail=f"Unknown source: {src.source_id}")
            
            config = table_config[src.source_id]
            table = config["table"]
            
            # Build query with month filter if applicable
            month_filter = ""
            if config["has_month"]:
                month_sql = f"SELECT MAX(month) as m FROM {table} WHERE year = {src.year}"
                month_result = engine.query(month_sql)
                latest_month = int(month_result.iloc[0]['m']) if not month_result.empty else 12
                month_filter = f" AND month = {latest_month}"
            
            # Fetch data (limit to prevent huge downloads)
            sql = f"SELECT * FROM {table} WHERE year = {src.year}{month_filter} LIMIT 50000"
            df = engine.query(sql)
            
            key = f"{src.source_id}_{src.year}"
            dataframes[key] = df
            source_info.append({
                "key": key,
                "source_id": src.source_id,
                "year": src.year,
                "display_name": config["display_name"],
                "join_keys": config["join_keys"],
                "row_count": len(df),
                "columns": list(df.columns)
            })
        
        # Determine join keys (find common keys between sources)
        all_join_keys = [set(s["join_keys"]) for s in source_info]
        common_keys = all_join_keys[0]
        for keys in all_join_keys[1:]:
            common_keys = common_keys.intersection(keys)
        
        join_keys = list(common_keys)
        if not join_keys:
            join_keys = ["contract_id"]  # Fallback to contract_id
        
        # Perform the join
        keys = list(dataframes.keys())
        combined_df = dataframes[keys[0]].copy()
        
        # Add suffix to non-key columns
        suffix_map = {keys[0]: f"_{source_info[0]['source_id']}"}
        
        for i, key in enumerate(keys[1:], 1):
            right_df = dataframes[key]
            suffix = f"_{source_info[i]['source_id']}"
            suffix_map[key] = suffix
            
            # Rename columns to avoid conflicts (except join keys)
            left_cols = {c: f"{c}{suffix_map[keys[0]]}" for c in combined_df.columns if c not in join_keys}
            right_cols = {c: f"{c}{suffix}" for c in right_df.columns if c not in join_keys}
            
            if i == 1:  # First join, rename left side too
                combined_df = combined_df.rename(columns=left_cols)
            
            right_df_renamed = right_df.rename(columns=right_cols)
            
            # Perform outer join
            combined_df = pd.merge(
                combined_df, 
                right_df_renamed, 
                on=join_keys, 
                how='outer',
                suffixes=('', '_dup')
            )
        
        # Create Excel files as base64
        def df_to_excel_base64(df, sheet_name="Data"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            output.seek(0)
            return base64.b64encode(output.read()).decode('utf-8')
        
        # Generate individual source files
        source_files = []
        for key, df in dataframes.items():
            info = next(s for s in source_info if s["key"] == key)
            source_files.append({
                "filename": f"{info['source_id']}_{info['year']}.xlsx",
                "display_name": f"{info['display_name']} ({info['year']})",
                "row_count": len(df),
                "excel_base64": df_to_excel_base64(df, f"{info['source_id']}_{info['year']}")
            })
        
        # Generate combined file
        combined_file = {
            "filename": "combined_linked_data.xlsx",
            "display_name": "Combined Linked Data",
            "row_count": len(combined_df),
            "excel_base64": df_to_excel_base64(combined_df, "Combined")
        }
        
        # Build join logic explanation
        source_names = [s["display_name"] for s in source_info]
        join_logic = {
            "sources_linked": source_names,
            "join_keys_used": join_keys,
            "join_type": "OUTER JOIN (keeps all records from both sources)",
            "explanation": f"Linked {' + '.join(source_names)} using columns: {', '.join(join_keys)}",
            "sql_equivalent": f"SELECT * FROM {source_info[0]['source_id']} OUTER JOIN {source_info[1]['source_id']} ON {' AND '.join([f'a.{k} = b.{k}' for k in join_keys])}"
        }
        
        return {
            "success": True,
            "source_files": source_files,
            "combined_file": combined_file,
            "join_logic": join_logic,
            "summary": {
                "sources_count": len(request.sources),
                "total_source_rows": sum(s["row_count"] for s in source_info),
                "combined_rows": len(combined_df)
            }
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to link data: {str(e)}")


# ================================================================
# CMS DOCUMENTS API (Technical Notes, Rate Notices, etc.)
# ================================================================

@app.get("/api/documents/list")
async def list_cms_documents():
    """
    List all available CMS documents organized by category.
    Returns rate notices, technical notes, stars docs, HCC docs.
    """
    import boto3
    import re
    
    try:
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        
        documents = {
            "rate_notices": {
                "advance": [],
                "final": []
            },
            "technical_notes": {
                "stars": []
            },
            "stars_docs": {
                "rate_announcements": [],
                "cai_supplements": [],
                "fact_sheets": []
            },
            "hcc_docs": {
                "model": []
            }
        }
        
        # Helper to extract year from filename
        def extract_year(filename):
            match = re.search(r'(\d{4})', filename)
            return int(match.group(1)) if match else None
        
        # Rate Notice - Advance
        response = s3.list_objects_v2(Bucket=bucket, Prefix='documents/pdf/rate_notice_advance/', MaxKeys=100)
        for obj in response.get('Contents', []):
            filename = obj['Key'].split('/')[-1]
            if filename.endswith('.pdf'):
                year = filename.replace('.pdf', '')
                if year.isdigit():
                    documents["rate_notices"]["advance"].append({
                        "year": int(year),
                        "name": f"{year} Advance Rate Notice",
                        "type": "rate_notice_advance",
                        "key": obj['Key'],
                        "size_mb": round(obj['Size'] / 1024 / 1024, 1)
                    })
        
        # Rate Notice - Final
        response = s3.list_objects_v2(Bucket=bucket, Prefix='documents/pdf/rate_notice_final/', MaxKeys=100)
        for obj in response.get('Contents', []):
            filename = obj['Key'].split('/')[-1]
            if filename.endswith('.pdf'):
                year = filename.replace('.pdf', '')
                if year.isdigit():
                    documents["rate_notices"]["final"].append({
                        "year": int(year),
                        "name": f"{year} Final Rate Notice",
                        "type": "rate_notice_final",
                        "key": obj['Key'],
                        "size_mb": round(obj['Size'] / 1024 / 1024, 1)
                    })
        
        # Technical Notes - Stars
        response = s3.list_objects_v2(Bucket=bucket, Prefix='documents/pdf/tech_notes/', MaxKeys=100)
        for obj in response.get('Contents', []):
            filename = obj['Key'].split('/')[-1]
            if filename.endswith('.pdf'):
                year = filename.replace('.pdf', '')
                if year.isdigit():
                    documents["technical_notes"]["stars"].append({
                        "year": int(year),
                        "name": f"{year} Star Ratings Technical Notes",
                        "type": "tech_notes_stars",
                        "key": obj['Key'],
                        "size_mb": round(obj['Size'] / 1024 / 1024, 1)
                    })
        
        # Rate Announcements (from docs/stars/)
        response = s3.list_objects_v2(Bucket=bucket, Prefix='docs/stars/rate_announcements/', MaxKeys=100)
        for obj in response.get('Contents', []):
            filename = obj['Key'].split('/')[-1]
            if filename.endswith('.pdf'):
                year = extract_year(filename)
                if year:
                    documents["stars_docs"]["rate_announcements"].append({
                        "year": year,
                        "name": f"{year} Rate Announcement",
                        "type": "rate_announcement",
                        "key": obj['Key'],
                        "size_mb": round(obj['Size'] / 1024 / 1024, 1)
                    })
        
        # CAI Supplements (from docs/stars/)
        response = s3.list_objects_v2(Bucket=bucket, Prefix='docs/stars/cai/', MaxKeys=100)
        for obj in response.get('Contents', []):
            filename = obj['Key'].split('/')[-1]
            if filename.endswith('.pdf'):
                year = extract_year(filename)
                if year:
                    documents["stars_docs"]["cai_supplements"].append({
                        "year": year,
                        "name": f"{year} CAI Supplement",
                        "type": "cai_supplement",
                        "key": obj['Key'],
                        "size_mb": round(obj['Size'] / 1024 / 1024, 1)
                    })
        
        # Star Fact Sheets (from docs/stars/)
        response = s3.list_objects_v2(Bucket=bucket, Prefix='docs/stars/fact_sheets/', MaxKeys=100)
        for obj in response.get('Contents', []):
            filename = obj['Key'].split('/')[-1]
            if filename.endswith('.pdf'):
                year = extract_year(filename)
                if year:
                    documents["stars_docs"]["fact_sheets"].append({
                        "year": year,
                        "name": f"{year} Star Ratings Fact Sheet",
                        "type": "star_fact_sheet",
                        "key": obj['Key'],
                        "size_mb": round(obj['Size'] / 1024 / 1024, 1)
                    })
        
        # HCC Model Documentation
        response = s3.list_objects_v2(Bucket=bucket, Prefix='documents/pdf/hcc_model/', MaxKeys=100)
        for obj in response.get('Contents', []):
            filename = obj['Key'].split('/')[-1]
            if filename.endswith('.pdf'):
                year = filename.replace('.pdf', '')
                if year.isdigit():
                    documents["hcc_docs"]["model"].append({
                        "year": int(year),
                        "name": f"{year} HCC Model Documentation",
                        "type": "hcc_model",
                        "key": obj['Key'],
                        "size_mb": round(obj['Size'] / 1024 / 1024, 1)
                    })
        
        # Sort all lists by year descending
        documents["rate_notices"]["advance"].sort(key=lambda x: x["year"], reverse=True)
        documents["rate_notices"]["final"].sort(key=lambda x: x["year"], reverse=True)
        documents["technical_notes"]["stars"].sort(key=lambda x: x["year"], reverse=True)
        documents["stars_docs"]["rate_announcements"].sort(key=lambda x: x["year"], reverse=True)
        documents["stars_docs"]["cai_supplements"].sort(key=lambda x: x["year"], reverse=True)
        documents["stars_docs"]["fact_sheets"].sort(key=lambda x: x["year"], reverse=True)
        documents["hcc_docs"]["model"].sort(key=lambda x: x["year"], reverse=True)
        
        total = sum([
            len(documents["rate_notices"]["advance"]),
            len(documents["rate_notices"]["final"]),
            len(documents["technical_notes"]["stars"]),
            len(documents["stars_docs"]["rate_announcements"]),
            len(documents["stars_docs"]["cai_supplements"]),
            len(documents["stars_docs"]["fact_sheets"]),
            len(documents["hcc_docs"]["model"]),
        ])
        
        return {
            "documents": documents,
            "total_count": total
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list documents: {str(e)}")


@app.get("/api/documents/download/{doc_type}/{year}")
async def download_cms_document(doc_type: str, year: int):
    """
    Download a specific CMS document PDF.
    Supports: rate_notice_advance, rate_notice_final, tech_notes_stars,
              rate_announcement, cai_supplement, star_fact_sheet, hcc_model
    """
    import boto3
    from fastapi.responses import StreamingResponse
    from io import BytesIO
    
    # Map doc_type to S3 path pattern
    type_to_config = {
        "rate_notice_advance": ("documents/pdf/rate_notice_advance", "{year}.pdf"),
        "rate_notice_final": ("documents/pdf/rate_notice_final", "{year}.pdf"),
        "tech_notes_stars": ("documents/pdf/tech_notes", "{year}.pdf"),
        "rate_announcement": ("docs/stars/rate_announcements", "{year}_rate_announcement.pdf"),
        "cai_supplement": ("docs/stars/cai", "{year}_cai_supplement.pdf"),
        "star_fact_sheet": ("docs/stars/fact_sheets", "{year}_fact_sheet.pdf"),
        "hcc_model": ("documents/pdf/hcc_model", "{year}.pdf"),
    }
    
    if doc_type not in type_to_config:
        raise HTTPException(status_code=400, detail=f"Invalid document type. Use: {list(type_to_config.keys())}")
    
    try:
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        prefix, pattern = type_to_config[doc_type]
        key = f"{prefix}/{pattern.format(year=year)}"
        
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        
        filename = f"{doc_type}_{year}.pdf"
        
        return StreamingResponse(
            BytesIO(content),
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"Document not found: {doc_type} {year}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.get("/api/documents/content/{doc_type}/{year}")
async def get_document_content(doc_type: str, year: int, section: Optional[str] = None):
    """
    Get text content of a CMS document (for chat context).
    Returns full text or specific section.
    """
    import boto3
    
    type_to_prefix = {
        "rate_notice_advance": "documents/text/rate_notice_advance",
        "rate_notice_final": "documents/text/rate_notice_final",
        "tech_notes_stars": "documents/text/tech_notes",
        "hcc_model": "documents/text/hcc_model",
    }
    
    if doc_type not in type_to_prefix:
        raise HTTPException(status_code=400, detail=f"Invalid document type")
    
    try:
        s3 = boto3.client('s3')
        bucket = 'ma-data123'
        key = f"{type_to_prefix[doc_type]}/{year}.txt"
        
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # Optionally extract specific section
        if section:
            # Simple section extraction - look for section headers
            section_lower = section.lower()
            lines = content.split('\n')
            in_section = False
            section_content = []
            
            for line in lines:
                line_lower = line.lower()
                if section_lower in line_lower and len(line) < 200:
                    in_section = True
                elif in_section:
                    # End section at next major header
                    if line.strip() and line.strip()[0].isdigit() and '.' in line[:10]:
                        break
                    section_content.append(line)
            
            if section_content:
                content = '\n'.join(section_content)
        
        return {
            "doc_type": doc_type,
            "year": year,
            "section": section,
            "content": content[:50000],  # Limit to 50k chars for context
            "truncated": len(content) > 50000,
            "char_count": len(content)
        }
        
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"Document text not found: {doc_type} {year}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get content: {str(e)}")


# ================================================================
# RATE NOTICE AUDIT ENDPOINTS
# ================================================================

@app.get("/api/rate-notice/audit-download")
async def download_rate_notice_audit_package(
    year: Optional[int] = None,
    include_county_benchmarks: bool = True,
    format: str = "parquet"  # "parquet" or "csv"
):
    """
    Download comprehensive rate notice audit package.
    
    Includes all rate notice data with full audit fields:
    - Part D parameters (deductible, ICL, TrOOP by year)
    - Risk adjustment parameters (model version, phase-in, normalization)
    - MA growth rates (advance vs final, effective)
    - Star bonus structure (bonus %, rebate % by star level)
    - HCC coefficients (V28 model coefficients)
    - National USPCC (FFS baseline by year)
    - County benchmarks (MA rates by county/year) - optional, large file
    
    All tables include source_document, source_section, extracted_at columns.
    """
    import zipfile
    from io import BytesIO
    from fastapi.responses import StreamingResponse
    import boto3
    import pandas as pd
    
    s3 = boto3.client('s3')
    bucket = 'ma-data123'
    
    tables = [
        'part_d_parameters',
        'risk_adjustment_parameters',
        'ma_growth_rates',
        'star_bonus_structure',
        'hcc_coefficients_v28',
        'national_uspcc',
    ]
    
    if include_county_benchmarks:
        tables.append('county_benchmarks')
    
    try:
        zip_buffer = BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add metadata
            metadata = {
                "package_type": "rate_notice_audit",
                "created_at": datetime.utcnow().isoformat(),
                "year_filter": year,
                "format": format,
                "tables_included": tables,
                "audit_fields": [
                    "source_document",
                    "source_section", 
                    "source_table",
                    "extracted_at",
                    "data_type",
                    "verification_status"
                ],
                "description": "Complete rate notice data with full provenance tracking"
            }
            zf.writestr("_metadata.json", json.dumps(metadata, indent=2))
            
            # Add README
            readme = """# Rate Notice Audit Package

## Contents
This package contains CMS rate notice data with full audit/provenance tracking.

### Tables
- part_d_parameters: Part D standard benefit parameters by year
- risk_adjustment_parameters: HCC model version, phase-in, normalization factors
- ma_growth_rates: MA capitation rate changes (advance vs final)
- star_bonus_structure: Quality bonus and rebate percentages by star level
- hcc_coefficients_v28: CMS-HCC V28 model coefficients
- national_uspcc: National per capita FFS costs (USPCC baseline)
- county_benchmarks: County-level MA benchmark rates (from ratebooks)

### Audit Fields (present in all tables)
- source_document: The CMS document from which data was extracted
- source_section: Section within the document
- source_table: Table name within the document
- extracted_at: Timestamp when data was extracted
- data_type: "CMS Official" or "Statutory"
- verification_status: "verified" for manually validated data

### Data Sources
All data extracted from official CMS Rate Announcements and Ratebooks:
- Rate Announcements: https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats
- Ratebooks: https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Ratebooks
"""
            zf.writestr("README.md", readme)
            
            # Add each table
            for table_name in tables:
                try:
                    key = f"gold/rate_notice/{table_name}.parquet"
                    response = s3.get_object(Bucket=bucket, Key=key)
                    df = pd.read_parquet(BytesIO(response['Body'].read()))
                    
                    # Filter by year if specified
                    if year and 'year' in df.columns:
                        df = df[df['year'] == year]
                    
                    # Save in requested format
                    if format == "csv":
                        csv_buffer = BytesIO()
                        df.to_csv(csv_buffer, index=False)
                        zf.writestr(f"{table_name}.csv", csv_buffer.getvalue())
                    else:
                        parquet_buffer = BytesIO()
                        df.to_parquet(parquet_buffer, index=False)
                        zf.writestr(f"{table_name}.parquet", parquet_buffer.getvalue())
                        
                except Exception as e:
                    # Skip missing tables
                    continue
            
            # Add county benchmarks audit metadata
            if include_county_benchmarks:
                try:
                    response = s3.get_object(Bucket=bucket, Key="gold/rate_notice/county_benchmarks_audit.json")
                    audit_meta = response['Body'].read()
                    zf.writestr("county_benchmarks_audit.json", audit_meta)
                except:
                    pass
        
        zip_buffer.seek(0)
        
        year_suffix = f"_{year}" if year else "_all_years"
        filename = f"rate_notice_audit{year_suffix}.zip"
        
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Audit download failed: {str(e)}")


@app.get("/api/rate-notice/tables")
async def list_rate_notice_tables():
    """
    List all available rate notice tables with metadata.
    """
    import boto3
    import pandas as pd
    from io import BytesIO
    
    s3 = boto3.client('s3')
    bucket = 'ma-data123'
    
    tables = {}
    
    table_names = [
        'part_d_parameters',
        'risk_adjustment_parameters',
        'ma_growth_rates',
        'star_bonus_structure',
        'hcc_coefficients_v28',
        'national_uspcc',
        'county_benchmarks',
    ]
    
    for table_name in table_names:
        try:
            key = f"gold/rate_notice/{table_name}.parquet"
            response = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))
            
            # Get audit columns
            audit_cols = [c for c in df.columns if 'source' in c.lower() or 'extracted' in c.lower() or 'verification' in c.lower()]
            
            tables[table_name] = {
                "row_count": len(df),
                "columns": list(df.columns),
                "audit_columns": audit_cols,
                "year_range": [int(df['year'].min()), int(df['year'].max())] if 'year' in df.columns else None,
            }
        except Exception as e:
            tables[table_name] = {"error": str(e)}
    
    return {
        "tables": tables,
        "total_tables": len(table_names),
        "description": "Rate notice data tables with full audit/provenance tracking"
    }


@app.get("/api/rate-notice/county-benchmarks")
async def get_county_benchmarks(
    year: int,
    state: Optional[str] = None,
    limit: int = 100
):
    """
    Query county benchmark rates with audit fields.
    
    Returns MA benchmark rates by county from CMS ratebooks.
    Includes source_file, source_row, and extracted_at for audit.
    """
    import boto3
    import pandas as pd
    from io import BytesIO
    
    s3 = boto3.client('s3')
    bucket = 'ma-data123'
    
    try:
        response = s3.get_object(Bucket=bucket, Key="gold/rate_notice/county_benchmarks.parquet")
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        
        # Filter
        df = df[df['year'] == year]
        if state:
            df = df[df['state_code'].str.upper() == state.upper()]
        
        # Limit results
        df = df.head(limit)
        
        return {
            "data": df.to_dict(orient='records'),
            "count": len(df),
            "year": year,
            "state_filter": state,
            "audit_info": {
                "source": "CMS MA Ratebooks",
                "audit_columns": ["source_file", "source_table", "source_row", "extracted_at"],
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


# ================================================================
# V5 API ENDPOINTS - Proper Gold Layer Usage
# ================================================================

@app.get("/api/v5/filters")
async def get_filters_v5():
    """
    Get all available filter options from Gold layer dimension tables.
    Returns years, parent_orgs, plan_types, product_types, snp_types, group_types, states.
    """
    try:
        from api.services.data_service import get_data_service
        service = get_data_service()
        return service.get_filters_v5()
    except Exception as e:
        return {"error": str(e), "fallback": "/api/v3/enrollment/filters"}


@app.get("/api/v5/enrollment/timeseries")
async def get_enrollment_timeseries_v5(
    parent_org: Optional[str] = None,
    plan_types: Optional[str] = None,
    product_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    group_types: Optional[str] = None,
    states: Optional[str] = None,
    counties: Optional[str] = None,
    source: str = "national",  # "national" (exact totals) or "geographic" (CPSC with state/county)
    start_year: int = 2015,
    end_year: int = 2026
):
    """
    Get enrollment timeseries using Gold layer with full filter support.
    
    Source options:
    - "national": Uses Monthly Enrollment by Contract data (exact totals, no geography)
    - "geographic": Uses CPSC data (allows state/county filtering, may have suppression)
    
    Supports all dimension filters including geographic (state/county) when source=geographic.
    """
    try:
        from api.services.data_service import get_data_service
        service = get_data_service()
        return service.get_enrollment_timeseries_v5(
            parent_org=parent_org,
            plan_types=plan_types.split(",") if plan_types else None,
            product_types=product_types.split(",") if product_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            group_types=group_types.split(",") if group_types else None,
            states=states.split(",") if states else None,
            counties=counties.split("|") if counties else None,
            source=source,
            start_year=start_year,
            end_year=end_year
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v5/stars/timeseries")
async def get_stars_timeseries_v5(
    parent_org: Optional[str] = None,
    plan_types: Optional[str] = None,
    product_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    group_types: Optional[str] = None,
    start_year: int = 2015,
    end_year: int = 2026
):
    """
    Get 4+ star enrollment percentage timeseries using Gold layer.
    
    Returns % of enrollment in 4+ star contracts over time.
    """
    try:
        from api.services.data_service import get_data_service
        service = get_data_service()
        return service.get_stars_timeseries_v5(
            parent_org=parent_org,
            plan_types=plan_types.split(",") if plan_types else None,
            product_types=product_types.split(",") if product_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            group_types=group_types.split(",") if group_types else None,
            start_year=start_year,
            end_year=end_year
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v5/risk/timeseries")
async def get_risk_timeseries_v5(
    parent_org: Optional[str] = None,
    plan_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    group_types: Optional[str] = None,
    start_year: int = 2015,
    end_year: int = 2024
):
    """
    Get risk score timeseries using Gold layer.
    
    Returns enrollment-weighted average risk score over time.
    Note: Risk data only available through 2024.
    """
    try:
        from api.services.data_service import get_data_service
        service = get_data_service()
        return service.get_risk_timeseries_v5(
            parent_org=parent_org,
            plan_types=plan_types.split(",") if plan_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            group_types=group_types.split(",") if group_types else None,
            start_year=start_year,
            end_year=end_year
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v5/summary")
async def get_summary_v5(
    parent_org: Optional[str] = None,
    year: int = 2026,
    plan_types: Optional[str] = None,
    product_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    group_types: Optional[str] = None
):
    """
    Get comprehensive summary for a payer or industry using Gold layer.
    
    Returns enrollment, 4+ star %, and risk score for the specified filters.
    """
    try:
        from api.services.data_service import get_data_service
        service = get_data_service()
        return service.get_summary_v5(
            parent_org=parent_org,
            year=year,
            plan_types=plan_types.split(",") if plan_types else None,
            product_types=product_types.split(",") if product_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            group_types=group_types.split(",") if group_types else None
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v5/counties")
async def get_counties_v5(states: str):
    """
    Get counties for specified states from Gold layer.
    """
    try:
        from api.services.data_service import get_data_service
        service = get_data_service()
        return service.get_counties_v5(states=states.split(","))
    except Exception as e:
        return {"error": str(e), "counties": []}


@app.get("/api/v5/risk/contracts")
async def get_risk_contracts_v5(
    year: int,
    parent_org: Optional[str] = None,
    plan_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    group_types: Optional[str] = None
):
    """
    Get risk score details by contract for a specific year.
    """
    try:
        from api.services.data_service import get_data_service
        service = get_data_service()
        return service.get_risk_contracts_v5(
            year=year,
            parent_org=parent_org,
            plan_types=plan_types.split(",") if plan_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            group_types=group_types.split(",") if group_types else None
        )
    except Exception as e:
        return {"error": str(e), "contracts": []}


@app.get("/api/v5/enrollment/audit-download")
async def get_enrollment_audit_download_v5(
    parent_org: Optional[str] = None,
    plan_types: Optional[str] = None,
    product_types: Optional[str] = None,
    snp_types: Optional[str] = None,
    group_types: Optional[str] = None,
    states: Optional[str] = None
):
    """
    Download enrollment audit data as CSV.
    """
    from fastapi.responses import StreamingResponse
    import io
    import csv
    
    try:
        from api.services.data_service import get_data_service
        service = get_data_service()
        data = service.get_enrollment_timeseries_v5(
            parent_org=parent_org,
            plan_types=plan_types.split(",") if plan_types else None,
            product_types=product_types.split(",") if product_types else None,
            snp_types=snp_types.split(",") if snp_types else None,
            group_types=group_types.split(",") if group_types else None,
            states=states.split(",") if states else None,
            start_year=2013,
            end_year=2026
        )
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Year", "Enrollment"])
        for i, year in enumerate(data.get("years", [])):
            enrollment = data.get("enrollment", [])[i] if i < len(data.get("enrollment", [])) else 0
            writer.writerow([year, enrollment])
        
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=enrollment_audit.csv"}
        )
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
