#!/usr/bin/env python3
"""
Rebuild Gold Layer from Legacy Tables
======================================

Rebuilds the Gold layer fact tables from the validated legacy tables
in processed/unified/ which have all dimension columns populated.

This ensures all filtering works: plan_type, product_type, snp_type, group_type, state.
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
import sys

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
PIPELINE_RUN_ID = f"gold_rebuild_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

s3 = boto3.client('s3')


def load_parquet(key: str) -> pd.DataFrame:
    """Load parquet file from S3."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"  Error loading {key}: {e}")
        return pd.DataFrame()


def save_parquet(df: pd.DataFrame, key: str):
    """Save dataframe to S3 as parquet."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    print(f"  Saved to s3://{S3_BUCKET}/{key} ({len(df):,} rows)")


def rebuild_fact_enrollment_national():
    """Rebuild gold/fact_enrollment_national from legacy table."""
    print("\n" + "=" * 70)
    print("REBUILDING: gold/fact_enrollment_national")
    print("=" * 70)
    
    source_key = "processed/unified/fact_enrollment_all_years.parquet"
    output_key = "gold/fact_enrollment_national.parquet"
    
    print(f"\n1. Loading source: {source_key}")
    df = load_parquet(source_key)
    
    if df.empty:
        print("  ERROR: Source table is empty")
        return False
    
    print(f"   Loaded {len(df):,} rows")
    print(f"   Columns: {df.columns.tolist()}")
    
    print("\n2. Adding Gold layer columns...")
    
    df['time_key'] = df['year'] * 100 + df['month']
    df['entity_id'] = df['contract_id']
    df['plan_id'] = '000'
    df['_source_row'] = range(len(df))
    df['_pipeline_run_id'] = PIPELINE_RUN_ID
    df['_loaded_at'] = datetime.now()
    
    final_cols = [
        'time_key', 'entity_id', 'contract_id', 'plan_id', 'year', 'month',
        'enrollment', 'plan_count', 'parent_org', 'state',
        'plan_type', 'product_type', 'snp_type', 'group_type',
        '_source_file', '_source_row', '_pipeline_run_id', '_loaded_at'
    ]
    
    for col in final_cols:
        if col not in df.columns:
            df[col] = None
    
    result = df[final_cols].copy()
    
    print("\n3. Validating dimension columns...")
    for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
        non_null = result[col].notna().sum()
        pct = non_null / len(result) * 100
        print(f"   {col}: {non_null:,} / {len(result):,} ({pct:.1f}% populated)")
    
    print("\n4. Saving to Gold layer...")
    save_parquet(result, output_key)
    
    return True


def rebuild_dim_geography():
    """Rebuild gold/dim_geography from fact table."""
    print("\n" + "=" * 70)
    print("REBUILDING: gold/dim_geography")
    print("=" * 70)
    
    source_key = "processed/unified/fact_enrollment_all_years.parquet"
    output_key = "gold/dim_geography.parquet"
    
    print(f"\n1. Loading source: {source_key}")
    df = load_parquet(source_key)
    
    if df.empty or 'state' not in df.columns:
        print("  ERROR: No state data in source")
        return False
    
    print("\n2. Building geography dimension...")
    geo_df = df[['state']].drop_duplicates()
    geo_df['county'] = None
    geo_df['fips_code'] = None
    geo_df = geo_df[geo_df['state'].notna()]
    
    print(f"   Found {len(geo_df)} unique states")
    
    print("\n3. Saving to Gold layer...")
    save_parquet(geo_df, output_key)
    
    return True


def rebuild_dim_plan():
    """Rebuild gold/dim_plan from fact table."""
    print("\n" + "=" * 70)
    print("REBUILDING: gold/dim_plan")
    print("=" * 70)
    
    source_key = "processed/unified/fact_enrollment_all_years.parquet"
    output_key = "gold/dim_plan.parquet"
    
    print(f"\n1. Loading source: {source_key}")
    df = load_parquet(source_key)
    
    if df.empty:
        print("  ERROR: Source table is empty")
        return False
    
    print("\n2. Building plan dimension...")
    
    plan_cols = ['contract_id', 'year', 'plan_type', 'product_type', 'snp_type', 'group_type']
    plan_cols = [c for c in plan_cols if c in df.columns]
    
    plan_df = df[plan_cols].drop_duplicates()
    plan_df['plan_id'] = '000'
    plan_df['plan_key'] = plan_df['contract_id'] + '_000_' + plan_df['year'].astype(str)
    plan_df['plan_name'] = None
    plan_df['plan_type_category'] = plan_df['plan_type']
    plan_df['is_snp'] = plan_df['snp_type'].apply(lambda x: x not in ['Non-SNP', None] if pd.notna(x) else False)
    plan_df['is_eghp'] = plan_df['group_type'] == 'Group'
    plan_df['offers_part_d'] = plan_df['product_type'] != 'MA-only'
    
    final_cols = [
        'plan_key', 'contract_id', 'plan_id', 'year',
        'plan_name', 'plan_type', 'plan_type_category', 'product_type',
        'snp_type', 'group_type', 'is_snp', 'is_eghp', 'offers_part_d'
    ]
    
    result = plan_df[[c for c in final_cols if c in plan_df.columns]]
    
    print(f"   Generated {len(result):,} plan dimension rows")
    
    print("\n3. Saving to Gold layer...")
    save_parquet(result, output_key)
    
    return True


def validate_gold_layer():
    """Validate the rebuilt Gold layer."""
    print("\n" + "=" * 70)
    print("VALIDATING GOLD LAYER")
    print("=" * 70)
    
    print("\n1. Checking fact_enrollment_national...")
    df = load_parquet("gold/fact_enrollment_national.parquet")
    if df.empty:
        print("  FAILED: Table is empty")
        return False
    
    dec_2024 = df[(df['year'] == 2024) & (df['month'] == 12)]
    print(f"   Dec 2024 rows: {len(dec_2024):,}")
    print(f"   Dec 2024 enrollment: {dec_2024['enrollment'].sum():,.0f}")
    
    print("\n2. Checking dimension column coverage...")
    for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
        non_null = dec_2024[col].notna().sum()
        pct = non_null / len(dec_2024) * 100 if len(dec_2024) > 0 else 0
        status = "OK" if pct > 50 else "WARN"
        print(f"   [{status}] {col}: {pct:.1f}% populated")
    
    print("\n3. Checking dim_geography...")
    geo_df = load_parquet("gold/dim_geography.parquet")
    print(f"   States: {len(geo_df)}")
    
    print("\n4. Checking dim_plan...")
    plan_df = load_parquet("gold/dim_plan.parquet")
    print(f"   Plan dimension rows: {len(plan_df):,}")
    
    return True


def main():
    print("=" * 70)
    print("REBUILD GOLD LAYER FROM LEGACY TABLES")
    print("=" * 70)
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    success = True
    
    if not rebuild_fact_enrollment_national():
        success = False
    
    if not rebuild_dim_geography():
        success = False
    
    if not rebuild_dim_plan():
        success = False
    
    if success:
        validate_gold_layer()
    
    print("\n" + "=" * 70)
    print(f"REBUILD {'COMPLETED' if success else 'FAILED'}")
    print(f"Finished: {datetime.now()}")
    print("=" * 70)
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
