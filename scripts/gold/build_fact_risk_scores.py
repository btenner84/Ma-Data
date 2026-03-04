#!/usr/bin/env python3
"""
Build Gold Layer: fact_risk_scores
===================================

Creates the risk scores fact table.

Sources:
- s3://ma-data123/silver/risk_scores/*/risk_scores.parquet
- s3://ma-data123/gold/dim_entity.parquet
- s3://ma-data123/gold/dim_plan.parquet

Output: s3://ma-data123/gold/fact_risk_scores.parquet

Grain: contract_id + plan_id + year

Columns:
- entity_id: STRING
- contract_id: STRING
- plan_id: STRING
- year: INT
- risk_score: FLOAT (1.0 = average, >1 = higher expected costs)
- enrollment: INT (for weighting)
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
SILVER_PREFIX = "silver/risk_scores"
DIM_ENTITY_KEY = "gold/dim_entity.parquet"
DIM_PLAN_KEY = "gold/dim_plan.parquet"
OUTPUT_KEY = "gold/fact_risk_scores.parquet"

PIPELINE_RUN_ID = f"fact_risk_scores_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

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
        return pd.DataFrame()


def main():
    print("=" * 70)
    print("BUILD GOLD LAYER: fact_risk_scores")
    print("=" * 70)
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    print("\n1. Loading dimension tables...")
    
    dim_entity = load_parquet(DIM_ENTITY_KEY)
    entity_lookup = None
    if not dim_entity.empty:
        entity_lookup = dim_entity[['contract_id', 'year', 'entity_id', 'parent_org']].drop_duplicates()
        print(f"   dim_entity: {len(entity_lookup):,} rows")
    
    dim_plan = load_parquet(DIM_PLAN_KEY)
    plan_lookup = None
    if not dim_plan.empty:
        plan_cols = ['contract_id', 'plan_id', 'year', 'plan_type', 'product_type', 'snp_type', 'group_type']
        plan_cols = [c for c in plan_cols if c in dim_plan.columns]
        plan_lookup = dim_plan[plan_cols].drop_duplicates()
        print(f"   dim_plan: {len(plan_lookup):,} rows")
    sys.stdout.flush()
    
    print("\n2. Loading silver risk scores...")
    silver_files = list_files(SILVER_PREFIX, '/risk_scores.parquet')
    print(f"   Found {len(silver_files)} silver files")
    
    if not silver_files:
        print("   Checking for existing risk scores data...")
        existing = load_parquet('processed/unified/fact_risk_scores_unified.parquet')
        if not existing.empty:
            print(f"   Using existing fact_risk_scores_unified: {len(existing)} rows")
            all_chunks = [existing]
        else:
            print("   ERROR: No risk scores data found")
            return
    else:
        all_chunks = []
        for f in silver_files:
            df = load_parquet(f)
            if not df.empty:
                all_chunks.append(df)
    
    if not all_chunks:
        print("   ERROR: No risk scores data loaded")
        return
    
    fact_df = pd.concat(all_chunks, ignore_index=True)
    print(f"   Total rows: {len(fact_df):,}")
    sys.stdout.flush()
    
    print("\n3. Standardizing columns...")
    
    risk_col_candidates = [c for c in fact_df.columns if 'risk' in c.lower() and 'score' in c.lower()]
    if risk_col_candidates and 'risk_score' not in fact_df.columns:
        fact_df['risk_score'] = fact_df[risk_col_candidates[0]]
    
    if 'risk_score' in fact_df.columns:
        fact_df['risk_score'] = pd.to_numeric(fact_df['risk_score'], errors='coerce')
    sys.stdout.flush()
    
    print("\n4. Enriching with dimension data...")
    
    if entity_lookup is not None:
        fact_df = fact_df.merge(
            entity_lookup,
            on=['contract_id', 'year'],
            how='left',
            suffixes=('', '_entity')
        )
        fact_df['entity_id'] = fact_df['entity_id'].fillna(fact_df['contract_id'])
        if 'parent_org_entity' in fact_df.columns:
            fact_df['parent_org'] = fact_df.get('parent_org', fact_df['parent_org_entity'])
    else:
        fact_df['entity_id'] = fact_df['contract_id']
    
    if plan_lookup is not None:
        for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
            fact_df = fact_df.drop(columns=[col], errors='ignore')
        
        fact_df = fact_df.merge(
            plan_lookup,
            on=['contract_id', 'plan_id', 'year'],
            how='left'
        )
    
    fact_df['_pipeline_run_id'] = PIPELINE_RUN_ID
    fact_df['_loaded_at'] = datetime.now()
    
    final_cols = [
        'entity_id', 'contract_id', 'plan_id', 'year',
        'risk_score', 'enrollment', 'parent_org',
        'plan_type', 'product_type', 'snp_type', 'group_type',
        '_source_file', '_source_row', '_pipeline_run_id', '_loaded_at'
    ]
    for col in final_cols:
        if col not in fact_df.columns:
            fact_df[col] = None
    
    result = fact_df[final_cols].drop_duplicates(subset=['contract_id', 'plan_id', 'year'])
    
    print(f"   Final fact table: {len(result):,} rows")
    print(f"   Year range: {result['year'].min()} - {result['year'].max()}")
    if 'risk_score' in result.columns and result['risk_score'].notna().any():
        print(f"   Avg risk score: {result['risk_score'].mean():.3f}")
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
