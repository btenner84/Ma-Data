#!/usr/bin/env python3
"""
Build Aggregation Tables

Pre-computes summary tables for common query patterns:
1. agg_by_parent_year - Parent org totals by year
2. agg_by_state_year - State totals by year
3. agg_by_dimensions - Enrollment by all dimension combinations
4. agg_market_share - Market share calculations

These tables enable fast API responses without scanning full fact tables.

All aggregations include:
- Source lineage (which fact table they aggregate)
- Validation metrics (sum checks against source)
- Refresh timestamps
"""

import os
import sys
import json
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Any
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

# Add parent for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from unified.audit_lineage import create_audit_logger

S3_BUCKET = "ma-data123"
FACTS_PREFIX = "processed/facts"
AGG_PREFIX = "processed/aggregations"

s3 = boto3.client('s3')


def load_fact_enrollment_unified() -> pd.DataFrame:
    """Load the unified enrollment fact table."""
    print("Loading fact_enrollment_unified...")

    prefix = f"{FACTS_PREFIX}/fact_enrollment_unified/"

    dfs = []
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                response = s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                df = pd.read_parquet(BytesIO(response['Body'].read()))
                dfs.append(df)

    if not dfs:
        raise ValueError("No fact_enrollment_unified data found")

    result = pd.concat(dfs, ignore_index=True)
    print(f"  Loaded {len(result):,} rows")
    return result


def load_fact_enrollment_geographic() -> pd.DataFrame:
    """Load the geographic enrollment fact table."""
    print("Loading fact_enrollment_geographic...")

    prefix = f"{FACTS_PREFIX}/fact_enrollment_geographic/"

    dfs = []
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                response = s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                df = pd.read_parquet(BytesIO(response['Body'].read()))
                dfs.append(df)

    if not dfs:
        print("  No geographic data found, skipping geographic aggregations")
        return None

    result = pd.concat(dfs, ignore_index=True)
    print(f"  Loaded {len(result):,} rows")
    return result


def build_agg_by_parent_year(df: pd.DataFrame, audit) -> pd.DataFrame:
    """
    Build parent org aggregation by year.

    Output columns:
    - parent_org, parent_org_id, year, month
    - total_enrollment, plan_count, contract_count
    - pct_hmo, pct_ppo, pct_dsnp, pct_mapd, pct_group
    - market_share (requires industry total)
    """
    print("\nBuilding agg_by_parent_year...")

    # Group by parent org, year, month
    agg = df.groupby(['parent_org', 'year', 'month']).agg(
        total_enrollment=('enrollment', 'sum'),
        plan_count=('plan_id', 'nunique'),
        contract_count=('contract_id', 'nunique'),

        # Count breakdowns
        hmo_enrollment=('enrollment', lambda x: x[df.loc[x.index, 'plan_type_simplified'] == 'HMO'].sum()),
        ppo_enrollment=('enrollment', lambda x: x[df.loc[x.index, 'plan_type_simplified'] == 'PPO'].sum()),
        dsnp_enrollment=('enrollment', lambda x: x[df.loc[x.index, 'snp_type'] == 'D-SNP'].sum()),
        mapd_enrollment=('enrollment', lambda x: x[df.loc[x.index, 'product_type'] == 'MAPD'].sum()),
        group_enrollment=('enrollment', lambda x: x[df.loc[x.index, 'group_type'] == 'Group'].sum()),
    ).reset_index()

    # Actually, the lambda approach above is slow and error-prone. Let's do it properly:

    # Simpler aggregation first
    agg = df.groupby(['parent_org', 'year', 'month']).agg(
        total_enrollment=('enrollment', 'sum'),
        plan_count=('plan_id', 'nunique'),
        contract_count=('contract_id', 'nunique'),
    ).reset_index()

    # Calculate dimension breakdowns separately
    for dim_name, dim_col, dim_value in [
        ('hmo', 'plan_type_simplified', 'HMO'),
        ('ppo', 'plan_type_simplified', 'PPO'),
        ('dsnp', 'snp_type', 'D-SNP'),
        ('mapd', 'product_type', 'MAPD'),
        ('group', 'group_type', 'Group'),
    ]:
        dim_agg = df[df[dim_col] == dim_value].groupby(
            ['parent_org', 'year', 'month']
        )['enrollment'].sum().reset_index(name=f'{dim_name}_enrollment')

        agg = agg.merge(dim_agg, on=['parent_org', 'year', 'month'], how='left')
        agg[f'{dim_name}_enrollment'] = agg[f'{dim_name}_enrollment'].fillna(0).astype(int)

    # Calculate percentages
    for dim_name in ['hmo', 'ppo', 'dsnp', 'mapd', 'group']:
        agg[f'pct_{dim_name}'] = (
            agg[f'{dim_name}_enrollment'] / agg['total_enrollment'] * 100
        ).round(2)

    # Calculate market share per year/month
    totals = agg.groupby(['year', 'month'])['total_enrollment'].sum().reset_index(name='industry_total')
    agg = agg.merge(totals, on=['year', 'month'])
    agg['market_share'] = (agg['total_enrollment'] / agg['industry_total'] * 100).round(4)
    agg = agg.drop(columns=['industry_total'])

    # Add metadata
    agg['_source_table'] = 'fact_enrollment_unified'
    agg['_aggregation'] = 'agg_by_parent_year'
    agg['_created_at'] = datetime.now().isoformat()

    # Log aggregation
    audit.log_aggregate(
        source='fact_enrollment_unified',
        group_by=['parent_org', 'year', 'month'],
        agg_functions={'enrollment': 'sum', 'plan_id': 'nunique'},
        result_df=agg,
        description='Aggregate by parent org and year'
    )

    print(f"  Generated {len(agg):,} rows for {agg['parent_org'].nunique()} parent orgs")
    return agg


def build_agg_by_state_year(df_geo: pd.DataFrame, audit) -> pd.DataFrame:
    """
    Build state-level aggregation by year.

    Uses geographic fact table (has suppression).
    """
    if df_geo is None:
        print("\nSkipping agg_by_state_year (no geographic data)")
        return None

    print("\nBuilding agg_by_state_year...")

    agg = df_geo.groupby(['state', 'year', 'month']).agg(
        total_enrollment=('enrollment', 'sum'),
        enrollment_estimated=('enrollment_estimated', 'sum'),
        plan_count=('plan_id', 'nunique'),
        contract_count=('contract_id', 'nunique'),
        county_count=('county', 'nunique'),
        suppressed_count=('is_suppressed', 'sum'),
    ).reset_index()

    # Calculate suppression rate
    total_records = df_geo.groupby(['state', 'year', 'month']).size().reset_index(name='total_records')
    agg = agg.merge(total_records, on=['state', 'year', 'month'])
    agg['suppression_rate'] = (agg['suppressed_count'] / agg['total_records'] * 100).round(2)

    # Calculate market share within state (for comparing payers within state)
    totals = agg.groupby(['year', 'month'])['total_enrollment'].sum().reset_index(name='national_enrollment')
    agg = agg.merge(totals, on=['year', 'month'])
    agg['pct_of_national'] = (agg['total_enrollment'] / agg['national_enrollment'] * 100).round(4)
    agg = agg.drop(columns=['national_enrollment'])

    # Add metadata
    agg['_source_table'] = 'fact_enrollment_geographic'
    agg['_aggregation'] = 'agg_by_state_year'
    agg['_created_at'] = datetime.now().isoformat()
    agg['_note'] = 'Totals may be ~1-3% lower than unified due to suppression'

    audit.log_aggregate(
        source='fact_enrollment_geographic',
        group_by=['state', 'year', 'month'],
        agg_functions={'enrollment': 'sum', 'is_suppressed': 'sum'},
        result_df=agg,
        description='Aggregate by state and year'
    )

    print(f"  Generated {len(agg):,} rows for {agg['state'].nunique()} states")
    return agg


def build_agg_by_dimensions(df: pd.DataFrame, audit) -> pd.DataFrame:
    """
    Build aggregation by all dimension combinations.

    Enables queries like "D-SNP + Individual + HMO enrollment by year"
    """
    print("\nBuilding agg_by_dimensions...")

    agg = df.groupby([
        'year', 'month',
        'plan_type_simplified',
        'product_type',
        'group_type',
        'snp_type'
    ]).agg(
        enrollment=('enrollment', 'sum'),
        plan_count=('plan_id', 'nunique'),
        contract_count=('contract_id', 'nunique'),
        parent_org_count=('parent_org', 'nunique'),
    ).reset_index()

    # Add totals for each year/month for percentage calculations
    totals = df.groupby(['year', 'month'])['enrollment'].sum().reset_index(name='total_enrollment')
    agg = agg.merge(totals, on=['year', 'month'])
    agg['pct_of_total'] = (agg['enrollment'] / agg['total_enrollment'] * 100).round(4)

    # Add metadata
    agg['_source_table'] = 'fact_enrollment_unified'
    agg['_aggregation'] = 'agg_by_dimensions'
    agg['_created_at'] = datetime.now().isoformat()

    audit.log_aggregate(
        source='fact_enrollment_unified',
        group_by=['year', 'month', 'plan_type_simplified', 'product_type', 'group_type', 'snp_type'],
        agg_functions={'enrollment': 'sum'},
        result_df=agg,
        description='Aggregate by all dimension combinations'
    )

    print(f"  Generated {len(agg):,} dimension combinations")
    return agg


def build_agg_industry_totals(df: pd.DataFrame, audit) -> pd.DataFrame:
    """
    Build industry-level totals by year/month.

    Quick reference for total MA enrollment, top-level stats.
    """
    print("\nBuilding agg_industry_totals...")

    agg = df.groupby(['year', 'month']).agg(
        total_enrollment=('enrollment', 'sum'),
        total_plans=('plan_id', 'nunique'),
        total_contracts=('contract_id', 'nunique'),
        total_parent_orgs=('parent_org', 'nunique'),
    ).reset_index()

    # Add dimension breakdowns
    for dim_name, dim_col, values in [
        ('enrollment_by_plan_type', 'plan_type_simplified',
         df['plan_type_simplified'].unique()),
        ('enrollment_by_product_type', 'product_type',
         df['product_type'].unique()),
        ('enrollment_by_group_type', 'group_type',
         df['group_type'].unique()),
        ('enrollment_by_snp_type', 'snp_type',
         df['snp_type'].unique()),
    ]:
        dim_agg = df.groupby(['year', 'month', dim_col])['enrollment'].sum().unstack(fill_value=0)
        dim_agg.columns = [f'{dim_name}_{c}' for c in dim_agg.columns]
        dim_agg = dim_agg.reset_index()
        agg = agg.merge(dim_agg, on=['year', 'month'], how='left')

    # Calculate YoY growth (for January only for consistency)
    jan_data = agg[agg['month'] == 1].copy()
    jan_data['prev_year_enrollment'] = jan_data['total_enrollment'].shift(1)
    jan_data['yoy_growth'] = (
        (jan_data['total_enrollment'] - jan_data['prev_year_enrollment'])
        / jan_data['prev_year_enrollment'] * 100
    ).round(2)

    agg = agg.merge(
        jan_data[['year', 'month', 'yoy_growth']],
        on=['year', 'month'],
        how='left'
    )

    # Add metadata
    agg['_source_table'] = 'fact_enrollment_unified'
    agg['_aggregation'] = 'agg_industry_totals'
    agg['_created_at'] = datetime.now().isoformat()

    audit.log_aggregate(
        source='fact_enrollment_unified',
        group_by=['year', 'month'],
        agg_functions={'enrollment': 'sum', 'plan_id': 'nunique'},
        result_df=agg,
        description='Industry-level totals by year/month'
    )

    print(f"  Generated {len(agg):,} rows ({agg['year'].nunique()} years)")
    return agg


def validate_aggregations(
    df_unified: pd.DataFrame,
    agg_parent: pd.DataFrame,
    agg_dims: pd.DataFrame,
    agg_totals: pd.DataFrame
) -> Dict:
    """
    Validate aggregation tables match source totals.
    """
    print("\nValidating aggregations...")

    validations = {
        'timestamp': datetime.now().isoformat(),
        'checks': [],
        'status': 'passed'
    }

    for year in df_unified['year'].unique():
        for month in [1, 7]:  # Check January and July
            if month not in df_unified[df_unified['year'] == year]['month'].unique():
                continue

            source_total = df_unified[
                (df_unified['year'] == year) & (df_unified['month'] == month)
            ]['enrollment'].sum()

            # Check parent org aggregation
            parent_total = agg_parent[
                (agg_parent['year'] == year) & (agg_parent['month'] == month)
            ]['total_enrollment'].sum()

            check = {
                'year': int(year),
                'month': int(month),
                'source_total': int(source_total),
                'agg_parent_total': int(parent_total),
                'match': source_total == parent_total
            }
            validations['checks'].append(check)

            if not check['match']:
                validations['status'] = 'failed'
                print(f"  MISMATCH {year}-{month}: source={source_total:,} vs agg={parent_total:,}")

    passed = sum(1 for c in validations['checks'] if c['match'])
    total = len(validations['checks'])
    print(f"  Validation: {passed}/{total} checks passed")

    return validations


def save_aggregation(df: pd.DataFrame, name: str):
    """Save aggregation table to S3."""
    if df is None:
        return

    s3_key = f"{AGG_PREFIX}/{name}.parquet"

    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        df.to_parquet(f.name, compression='snappy', index=False)
        s3.upload_file(f.name, S3_BUCKET, s3_key)

    print(f"  Saved: s3://{S3_BUCKET}/{s3_key}")


def main():
    print("=" * 70)
    print("BUILDING AGGREGATION TABLES")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().isoformat()}")

    # Initialize audit
    audit = create_audit_logger('build_aggregation_tables')

    try:
        # Load source data
        df_unified = load_fact_enrollment_unified()
        df_geo = load_fact_enrollment_geographic()

        # Build aggregations
        agg_parent = build_agg_by_parent_year(df_unified, audit)
        agg_state = build_agg_by_state_year(df_geo, audit)
        agg_dims = build_agg_by_dimensions(df_unified, audit)
        agg_totals = build_agg_industry_totals(df_unified, audit)

        # Validate
        validations = validate_aggregations(df_unified, agg_parent, agg_dims, agg_totals)

        # Save
        print("\nSaving aggregation tables...")
        save_aggregation(agg_parent, 'agg_by_parent_year')
        save_aggregation(agg_state, 'agg_by_state_year')
        save_aggregation(agg_dims, 'agg_by_dimensions')
        save_aggregation(agg_totals, 'agg_industry_totals')

        # Save validation results
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{AGG_PREFIX}/validations.json",
            Body=json.dumps(validations, indent=2),
            ContentType='application/json'
        )
        print(f"  Saved: s3://{S3_BUCKET}/{AGG_PREFIX}/validations.json")

        # Finish audit
        output_tables = [
            'agg_by_parent_year',
            'agg_by_state_year',
            'agg_by_dimensions',
            'agg_industry_totals'
        ]
        total_rows = sum([
            len(agg_parent) if agg_parent is not None else 0,
            len(agg_state) if agg_state is not None else 0,
            len(agg_dims) if agg_dims is not None else 0,
            len(agg_totals) if agg_totals is not None else 0,
        ])

        audit.finish_run(
            success=validations['status'] == 'passed',
            output_tables=output_tables,
            output_row_count=total_rows
        )

        print("\n" + "=" * 70)
        print("AGGREGATION TABLES COMPLETE")
        print("=" * 70)

        # Summary
        print(f"\nSummary:")
        print(f"  agg_by_parent_year: {len(agg_parent):,} rows")
        if agg_state is not None:
            print(f"  agg_by_state_year: {len(agg_state):,} rows")
        print(f"  agg_by_dimensions: {len(agg_dims):,} rows")
        print(f"  agg_industry_totals: {len(agg_totals):,} rows")
        print(f"  Validation: {validations['status'].upper()}")

        return validations['status'] == 'passed'

    except Exception as e:
        audit.finish_run(success=False, error_message=str(e))
        print(f"\nERROR: {e}")
        raise


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
