#!/usr/bin/env python3
"""
Build Gold Layer: fact_enrollment_national
==========================================

Creates the national enrollment fact table from by_plan data.
This provides EXACT enrollment counts without geographic suppression.

Sources:
- s3://ma-data123/silver/enrollment/by_plan/*/enrollment.parquet
- s3://ma-data123/gold/dim_entity.parquet (for entity_id lookup)
- s3://ma-data123/gold/dim_plan.parquet (for plan attributes)

Output: s3://ma-data123/gold/fact_enrollment_national.parquet

Grain: contract_id + plan_id + year + month

Columns:
- time_key: INT (YYYYMM)
- entity_id: STRING (stable entity reference)
- contract_id: STRING
- plan_id: STRING
- year: INT
- month: INT
- enrollment: INT (exact count, no suppression)
- plan_count: INT (always 1 at this grain)
- parent_org: STRING
- plan_type: STRING
- product_type: STRING
- snp_type: STRING
- group_type: STRING
- _source_file: STRING
- _source_row: INT
- _pipeline_run_id: STRING
- _loaded_at: TIMESTAMP
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
import sys

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
SILVER_PREFIX = "silver/enrollment/by_plan"
DIM_ENTITY_KEY = "gold/dim_entity.parquet"
DIM_PLAN_KEY = "gold/dim_plan.parquet"
OUTPUT_KEY = "gold/fact_enrollment_national.parquet"

PIPELINE_RUN_ID = f"fact_enrollment_national_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

s3 = boto3.client('s3')


def list_files(prefix: str, suffix: str = '.parquet') -> list:
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith(suffix):
                files.append(obj['Key'])
    return sorted(files)


def load_parquet(key: str) -> pd.DataFrame:
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"  Warning: Could not load {key}: {e}")
        return pd.DataFrame()


def main():
    print("=" * 70)
    print("BUILD GOLD LAYER: fact_enrollment_national")
    print("=" * 70)
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    print("\n1. Loading dimension tables...")
    
    dim_entity = load_parquet(DIM_ENTITY_KEY)
    if not dim_entity.empty:
        entity_lookup = dim_entity[['contract_id', 'year', 'entity_id']].drop_duplicates()
        print(f"   dim_entity: {len(entity_lookup):,} rows")
    else:
        print("   dim_entity: not found, will use contract_id as entity_id")
        entity_lookup = None
    
    dim_plan = load_parquet(DIM_PLAN_KEY)
    if not dim_plan.empty:
        plan_cols = ['contract_id', 'plan_id', 'year', 'plan_type', 'product_type', 'snp_type', 'group_type']
        plan_cols = [c for c in plan_cols if c in dim_plan.columns]
        plan_lookup = dim_plan[plan_cols].drop_duplicates()
        print(f"   dim_plan: {len(plan_lookup):,} rows")
    else:
        print("   dim_plan: not found")
        plan_lookup = None
    sys.stdout.flush()
    
    print("\n2. Loading silver by_plan enrollment...")
    silver_files = list_files(SILVER_PREFIX, '/enrollment.parquet')
    print(f"   Found {len(silver_files)} silver files")
    
    if not silver_files:
        print("   ERROR: No silver files found. Run build_silver_by_plan.py first.")
        return
    
    all_chunks = []
    total_rows = 0
    
    for i, f in enumerate(silver_files):
        df = load_parquet(f)
        if df.empty:
            continue
        
        total_rows += len(df)
        all_chunks.append(df)
        
        if (i + 1) % 50 == 0:
            print(f"   Loaded {i+1}/{len(silver_files)} files ({total_rows:,} rows)")
            sys.stdout.flush()
    
    if not all_chunks:
        print("   ERROR: No enrollment data loaded")
        return
    
    fact_df = pd.concat(all_chunks, ignore_index=True)
    print(f"   Total rows: {len(fact_df):,}")
    sys.stdout.flush()
    
    print("\n3. Enriching with dimension data...")
    
    if entity_lookup is not None:
        fact_df = fact_df.merge(
            entity_lookup,
            on=['contract_id', 'year'],
            how='left'
        )
        fact_df['entity_id'] = fact_df['entity_id'].fillna(fact_df['contract_id'])
    else:
        fact_df['entity_id'] = fact_df['contract_id']
    
    if plan_lookup is not None:
        existing_plan_cols = [c for c in ['plan_type', 'product_type', 'snp_type', 'group_type'] if c in fact_df.columns]
        for col in existing_plan_cols:
            if col in plan_lookup.columns:
                fact_df = fact_df.drop(columns=[col], errors='ignore')
        
        fact_df = fact_df.merge(
            plan_lookup,
            on=['contract_id', 'plan_id', 'year'],
            how='left'
        )
    
    for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
        if col not in fact_df.columns:
            fact_df[col] = None
    sys.stdout.flush()
    
    print("\n4. Adding computed columns...")
    
    fact_df['time_key'] = fact_df['year'] * 100 + fact_df['month']
    
    fact_df['plan_count'] = 1
    
    fact_df['_pipeline_run_id'] = PIPELINE_RUN_ID
    fact_df['_loaded_at'] = datetime.now()
    
    final_cols = [
        'time_key', 'entity_id', 'contract_id', 'plan_id', 'year', 'month',
        'enrollment', 'plan_count', 'parent_org',
        'plan_type', 'product_type', 'snp_type', 'group_type',
        '_source_file', '_source_row', '_pipeline_run_id', '_loaded_at'
    ]
    for col in final_cols:
        if col not in fact_df.columns:
            fact_df[col] = None
    
    result = fact_df[final_cols].copy()
    
    print(f"   Final fact table: {len(result):,} rows")
    print(f"   Year range: {result['year'].min()} - {result['year'].max()}")
    print(f"   Total enrollment: {result['enrollment'].sum():,.0f}")
    sys.stdout.flush()
    
    print("\n5. Saving to S3...")
    buffer = BytesIO()
    result.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
    
    print(f"Saved to s3://{S3_BUCKET}/{OUTPUT_KEY}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
