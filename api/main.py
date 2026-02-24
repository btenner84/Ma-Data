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

app = FastAPI(
    title="MA Intelligence Platform API",
    description="API for Medicare Advantage data intelligence",
    version="1.0.0"
)

# CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


# === Data Loading Utilities ===

@lru_cache(maxsize=32)
def load_parquet(s3_key: str) -> pd.DataFrame:
    """Load parquet file from S3 with caching."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def get_enrollment_data():
    """Load unified enrollment data."""
    return load_parquet('processed/unified/enrollment_by_parent_annual.parquet')


def get_enrollment_unified():
    """Load unified enrollment with all dimensions (plan_type, product_type, group_type)."""
    try:
        # Use v6 (correct classifications from CPSC files)
        df = load_parquet('processed/unified/fact_enrollment_v6.parquet')
        return consolidate_parent_org_names(df)
    except:
        try:
            df = load_parquet('processed/unified/fact_enrollment_v4.parquet')
            return consolidate_parent_org_names(df)
        except:
            return pd.DataFrame()


def get_enrollment_by_state_data():
    """Load enrollment data with state dimension."""
    try:
        return load_parquet('processed/unified/fact_enrollment_by_state.parquet')
    except:
        return pd.DataFrame()


def get_enrollment_by_geography():
    """Load enrollment data with state and county dimensions."""
    try:
        df = load_parquet('processed/unified/fact_enrollment_by_geography.parquet')
        return consolidate_parent_org_names(df)
    except:
        return pd.DataFrame()


def get_county_lookup():
    """Load county lookup table."""
    try:
        return load_parquet('processed/unified/dim_county.parquet')
    except:
        return pd.DataFrame()


def get_enrollment_by_year():
    """Load enrollment aggregated by year."""
    try:
        return load_parquet('processed/unified/agg_enrollment_by_year_v2.parquet')
    except:
        return pd.DataFrame()


def get_enrollment_by_plantype():
    """Load enrollment by year and plan type."""
    try:
        return load_parquet('processed/unified/agg_enrollment_by_plantype_v2.parquet')
    except:
        return pd.DataFrame()


def get_enrollment_by_product():
    """Load enrollment by year and product type (derived from plan type)."""
    df = get_enrollment_unified()
    if df.empty:
        return pd.DataFrame()
    return df.groupby(['year', 'product_type']).agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index().rename(columns={'parent_org': 'parent_count'})


def get_enrollment_detail():
    """Load detailed enrollment by contract/plan/county."""
    # Get latest year's January data for detailed view
    try:
        return load_parquet('processed/fact_enrollment/2025/1/enrollment.parquet')
    except:
        return load_parquet('processed/fact_enrollment/2024/1/enrollment.parquet')


def get_stars_summary():
    """Load unified stars summary."""
    return load_parquet('processed/unified/stars_summary.parquet')


def get_measure_data():
    """Load complete measure-level data."""
    return load_parquet('processed/unified/measure_data_complete.parquet')


def get_parent_summary():
    """Load parent organization summary."""
    return load_parquet('processed/unified/parent_org_summary.parquet')


def get_risk_scores():
    """Load risk scores summary."""
    try:
        return load_parquet('processed/unified/risk_scores_summary.parquet')
    except:
        return pd.DataFrame()


def get_risk_scores_unified():
    """Load unified risk scores with enrollment for v2 endpoints."""
    try:
        df = load_parquet('processed/unified/fact_risk_scores_unified.parquet')
        return consolidate_parent_org_names(df)
    except:
        return pd.DataFrame()


def get_risk_scores_by_parent_year():
    """Load risk scores aggregated by parent org and year."""
    try:
        df = load_parquet('processed/unified/risk_scores_by_parent_year.parquet')
        return consolidate_parent_org_names(df)
    except:
        return pd.DataFrame()


def get_risk_scores_by_parent_dims():
    """Load risk scores with plan_type, snp_type, group_type dimensions."""
    try:
        df = load_parquet('processed/unified/risk_scores_by_parent_dims.parquet')
        return consolidate_parent_org_names(df)
    except:
        return pd.DataFrame()


def get_risk_scores_summary_v2():
    """Load risk scores summary v2."""
    try:
        return load_parquet('processed/unified/risk_scores_summary_v2.parquet')
    except:
        return pd.DataFrame()


def get_snp_enrollment():
    """Load SNP enrollment data (legacy)."""
    try:
        return load_parquet('processed/unified/fact_snp_enrollment.parquet')
    except:
        return pd.DataFrame()


def get_snp_by_parent():
    """Load SNP enrollment by parent org with full dimensions."""
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
    'PPO': ['Local PPO', 'PPO'],
    'RPPO': ['Regional PPO'],
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

    # Create simplified version
    simplified_plan_types = []
    for pt in plan_types:
        if pt in ['HMO', 'HMOPOS', 'HMO-POS', 'HMO/HMOPOS']:
            if 'HMO' not in simplified_plan_types:
                simplified_plan_types.append('HMO')
        elif pt == 'Local PPO':
            if 'PPO' not in simplified_plan_types:
                simplified_plan_types.append('PPO')
        elif pt == 'Regional PPO':
            if 'RPPO' not in simplified_plan_types:
                simplified_plan_types.append('RPPO')
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
    try:
        df = load_parquet('processed/unified/stars_enrollment_unified.parquet')
        return consolidate_parent_org_names(df)
    except:
        return pd.DataFrame()


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

    return {
        "years": sorted(years),
        "measures": list(measures.values())
    }


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

    plan_types_simplified = ['HMO', 'PPO', 'RPPO', 'PFFS', 'MSA']

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


def get_measure_performance_aggregates():
    """Load pre-computed measure performance aggregates."""
    try:
        return load_parquet('processed/stars/measure_performance/aggregates.parquet')
    except:
        return pd.DataFrame()


@app.get("/api/stars/measure-performance")
async def get_measure_performance_table(
    parent_org: Optional[str] = None,  # None or "_INDUSTRY_" = industry, else specific payer
    avg_type: str = "weighted",  # "simple" or "weighted"
):
    """
    Get measure performance table data.
    Returns average performance % for each measure by year.

    Args:
        parent_org: Filter to specific payer, or None/"_INDUSTRY_" for industry
        avg_type: "simple" (mean) or "weighted" (enrollment-weighted)
    """
    df = get_measure_performance_aggregates()

    if df.empty:
        return {"error": "Measure performance data not available - run build_measure_performance.py"}

    # Filter to industry or specific payer
    if not parent_org or parent_org == "_INDUSTRY_" or parent_org == "Industry":
        df = df[df['parent_org'] == '_INDUSTRY_']
        display_org = "Industry"
    else:
        df = df[df['parent_org'] == parent_org]
        display_org = parent_org

    if df.empty:
        return {"error": f"No data for {parent_org}"}

    # Get value column based on avg_type
    value_col = 'weighted_avg' if avg_type == 'weighted' else 'simple_avg'

    # Get all years
    years = sorted(df['year'].unique())

    # Build measure order: 2026 measures first (by measure_key), then older measures
    # Load 2026 cutpoints for ordering and metadata
    try:
        cutpoints_2026 = load_parquet('processed/stars/cutpoints/2026/data.parquet')
        measures_2026 = cutpoints_2026[['measure_id', 'measure_key', 'measure_name', 'part', 'lower_is_better']].drop_duplicates()
        measures_2026_keys = measures_2026['measure_key'].tolist()
    except:
        measures_2026 = pd.DataFrame()
        measures_2026_keys = []

    # Load weights for all years (by measure_key since IDs change)
    weights_by_year = {}  # {year: {measure_key: weight}}
    for weight_year in years:
        try:
            cutpoints = load_parquet(f'processed/stars/cutpoints/{weight_year}/data.parquet')
            if not cutpoints.empty:
                weights_by_year[int(weight_year)] = {}
                for _, row in cutpoints[['measure_key', 'weight']].drop_duplicates().iterrows():
                    weights_by_year[int(weight_year)][row['measure_key']] = float(row['weight']) if pd.notna(row['weight']) else 1.0
        except:
            pass

    # Build response with measures as rows, years as columns
    # Group by measure_key (stable across years), NOT measure_id
    result_measures = []
    processed_keys = set()

    # First: measures in 2026 (in order by 2026 measure_id)
    for _, m2026 in measures_2026.iterrows():
        mkey = m2026['measure_key']
        if mkey in processed_keys:
            continue
        processed_keys.add(mkey)

        measure_df = df[df['measure_key'] == mkey]
        if measure_df.empty:
            continue

        yearly_data = {}
        for year in years:
            year_row = measure_df[measure_df['year'] == year]
            if not year_row.empty:
                row = year_row.iloc[0]
                yearly_data[int(year)] = {
                    'value': float(row[value_col]) if pd.notna(row[value_col]) else None,
                    'contract_count': int(row['contract_count']),
                    'enrollment': int(row['total_enrollment']) if pd.notna(row['total_enrollment']) else 0,
                    'measure_id': str(row['measure_id']),  # Year-specific ID
                }
            else:
                yearly_data[int(year)] = None

        # Get weights for this measure across years
        measure_weights = {}
        for wy in weights_by_year:
            if mkey in weights_by_year[wy]:
                measure_weights[wy] = weights_by_year[wy][mkey]

        result_measures.append({
            'measure_id': str(m2026['measure_id']),  # 2026 ID for display
            'measure_key': mkey,
            'measure_name': str(m2026['measure_name']),
            'part': str(m2026['part']) if pd.notna(m2026['part']) else 'C',
            'lower_is_better': bool(m2026['lower_is_better']) if pd.notna(m2026['lower_is_better']) else False,
            'in_2026': True,
            'yearly': yearly_data,
            'weights': measure_weights,
        })

    # Second: measures NOT in 2026 (discontinued)
    all_measure_keys = df['measure_key'].dropna().unique()
    discontinued_keys = [k for k in all_measure_keys if k not in processed_keys]

    for mkey in sorted(discontinued_keys):
        measure_df = df[df['measure_key'] == mkey]
        if measure_df.empty:
            continue

        # Get measure info from most recent year
        sample_row = measure_df.sort_values('year', ascending=False).iloc[0]

        yearly_data = {}
        for year in years:
            year_row = measure_df[measure_df['year'] == year]
            if not year_row.empty:
                row = year_row.iloc[0]
                yearly_data[int(year)] = {
                    'value': float(row[value_col]) if pd.notna(row[value_col]) else None,
                    'contract_count': int(row['contract_count']),
                    'enrollment': int(row['total_enrollment']) if pd.notna(row['total_enrollment']) else 0,
                    'measure_id': str(row['measure_id']),
                }
            else:
                yearly_data[int(year)] = None

        # Get weights for this measure across years
        measure_weights = {}
        for wy in weights_by_year:
            if mkey in weights_by_year[wy]:
                measure_weights[wy] = weights_by_year[wy][mkey]

        result_measures.append({
            'measure_id': str(sample_row['measure_id']),
            'measure_key': mkey,
            'measure_name': str(sample_row.get('measure_name', mkey)) if pd.notna(sample_row.get('measure_name')) else mkey,
            'part': str(sample_row.get('part', 'C')) if pd.notna(sample_row.get('part')) else 'C',
            'lower_is_better': bool(sample_row.get('lower_is_better', False)) if pd.notna(sample_row.get('lower_is_better')) else False,
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


@lru_cache(maxsize=1)
def get_measure_performance_contracts():
    """Load contract-level measure performance data."""
    return load_parquet('processed/stars/measure_performance/contract_level.parquet')


@app.get("/api/stars/measure-performance/detail")
async def get_measure_performance_detail(
    measure_key: str,
    year: int,
    parent_org: Optional[str] = None,  # None or "_INDUSTRY_" = all, else specific payer
):
    """
    Get contract-level detail for a specific measure/year.
    Used for auditing/drilling down into aggregate numbers.
    """
    df = get_measure_performance_contracts()

    if df.empty:
        return {"error": "Contract-level data not available"}

    # Filter by measure_key and year
    filtered = df[(df['measure_key'] == measure_key) & (df['year'] == year)]

    # Filter by parent_org if specified
    if parent_org and parent_org != "_INDUSTRY_" and parent_org != "Industry":
        filtered = filtered[filtered['parent_org'] == parent_org]
        display_org = parent_org
    else:
        display_org = "Industry"

    if filtered.empty:
        return {"error": f"No data for {measure_key} in {year}"}

    # Get measure info
    sample = filtered.iloc[0]
    measure_name = str(sample.get('measure_name', measure_key)) if pd.notna(sample.get('measure_name')) else measure_key
    measure_id = str(sample.get('measure_id', ''))

    # Build contract list sorted by performance
    contracts = []
    for _, row in filtered.sort_values('performance_pct', ascending=False).iterrows():
        contracts.append({
            'contract_id': str(row['contract_id']),
            'parent_org': str(row['parent_org']) if pd.notna(row['parent_org']) else None,
            'performance_pct': float(row['performance_pct']) if pd.notna(row['performance_pct']) else None,
            'enrollment': int(row['enrollment']) if pd.notna(row.get('enrollment')) else None,
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


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
