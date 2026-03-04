#!/usr/bin/env python3
"""
Build Gold Layer: fact_enrollment_geographic
=============================================

Creates the geographic enrollment fact table from CPSC data.
This has county-level detail but may have suppressed values (<10 enrollees).

Sources:
- s3://ma-data123/silver/enrollment/cpsc/*/enrollment.parquet
- s3://ma-data123/silver/enrollment/cpsc/*/contracts.parquet
- s3://ma-data123/gold/dim_entity.parquet
- s3://ma-data123/gold/dim_plan.parquet

Output: s3://ma-data123/gold/fact_enrollment_geographic.parquet

Grain: contract_id + plan_id + year + month + state + county

Columns:
- time_key: INT (YYYYMM)
- entity_id: STRING
- contract_id: STRING
- plan_id: STRING
- geo_key: STRING (FIPS code)
- year: INT
- month: INT
- state: STRING
- county: STRING
- fips_code: STRING
- enrollment: INT (NULL if suppressed)
- is_suppressed: BOOL (True if <10 enrollees, enrollment shown as "*")
- plan_count: INT
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
SILVER_ENROLLMENT_PREFIX = "silver/enrollment/cpsc"
SILVER_CONTRACTS_PREFIX = "silver/enrollment/cpsc"
DIM_ENTITY_KEY = "gold/dim_entity.parquet"
DIM_PLAN_KEY = "gold/dim_plan.parquet"
OUTPUT_KEY = "gold/fact_enrollment_geographic.parquet"

PIPELINE_RUN_ID = f"fact_enrollment_geo_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

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
    print("BUILD GOLD LAYER: fact_enrollment_geographic")
    print("=" * 70)
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    print("\n1. Loading dimension tables...")
    
    dim_entity = load_parquet(DIM_ENTITY_KEY)
    entity_lookup = None
    if not dim_entity.empty:
        entity_lookup = dim_entity[['contract_id', 'year', 'entity_id']].drop_duplicates()
        print(f"   dim_entity: {len(entity_lookup):,} rows")
    else:
        print("   dim_entity: not found")
    
    dim_plan = load_parquet(DIM_PLAN_KEY)
    plan_lookup = None
    if not dim_plan.empty:
        plan_cols = ['contract_id', 'plan_id', 'year', 'plan_type', 'product_type', 'snp_type', 'group_type']
        plan_cols = [c for c in plan_cols if c in dim_plan.columns]
        plan_lookup = dim_plan[plan_cols].drop_duplicates()
        print(f"   dim_plan: {len(plan_lookup):,} rows")
    sys.stdout.flush()
    
    print("\n2. Loading contract info for parent_org...")
    contract_files = list_files(SILVER_CONTRACTS_PREFIX, '/contracts.parquet')
    print(f"   Found {len(contract_files)} contract files")
    
    contract_lookup = None
    if contract_files:
        contracts = []
        for f in contract_files:
            df = load_parquet(f)
            if not df.empty and 'parent_org' in df.columns:
                contracts.append(df[['contract_id', 'plan_id', 'year', 'parent_org']].drop_duplicates())
        if contracts:
            contract_lookup = pd.concat(contracts, ignore_index=True).drop_duplicates()
            print(f"   Contract lookup: {len(contract_lookup):,} rows")
    sys.stdout.flush()
    
    print("\n3. Processing silver CPSC enrollment in chunks...")
    enrollment_files = list_files(SILVER_ENROLLMENT_PREFIX, '/enrollment.parquet')
    print(f"   Found {len(enrollment_files)} enrollment files")
    
    if not enrollment_files:
        print("   ERROR: No enrollment files found")
        return
    
    # Process in chunks to avoid OOM - group by year-month
    from collections import defaultdict
    files_by_period = defaultdict(list)
    for f in enrollment_files:
        parts = f.split('/')
        for part in parts:
            if part.isdigit() and len(part) == 4:  # year
                year = part
                break
        for part in parts:
            if part.isdigit() and len(part) == 2:  # month
                month = part
                break
        period = f"{year}-{month}"
        files_by_period[period].append(f)
    
    print(f"   Processing {len(files_by_period)} year-month periods")
    sys.stdout.flush()
    
    final_cols = [
        'time_key', 'entity_id', 'contract_id', 'plan_id', 'geo_key',
        'year', 'month', 'state', 'county', 'fips_code',
        'enrollment', 'is_suppressed', 'plan_count', 'parent_org',
        'plan_type', 'product_type', 'snp_type', 'group_type',
        '_source_file', '_source_row', '_pipeline_run_id', '_loaded_at'
    ]
    
    total_rows = 0
    total_suppressed = 0
    all_years = set()
    all_states = set()
    output_buffers = []
    
    for period_idx, (period, files) in enumerate(sorted(files_by_period.items())):
        # Load this period's data
        period_chunks = []
        for f in files:
            df = load_parquet(f)
            if not df.empty:
                period_chunks.append(df)
        
        if not period_chunks:
            continue
        
        fact_df = pd.concat(period_chunks, ignore_index=True)
        del period_chunks  # Free memory
        
        # Enrich with dimension data
        if entity_lookup is not None:
            fact_df = fact_df.merge(entity_lookup, on=['contract_id', 'year'], how='left')
            fact_df['entity_id'] = fact_df['entity_id'].fillna(fact_df['contract_id'])
        else:
            fact_df['entity_id'] = fact_df['contract_id']
        
        if contract_lookup is not None:
            fact_df = fact_df.merge(
                contract_lookup,
                on=['contract_id', 'plan_id', 'year'],
                how='left',
                suffixes=('', '_contract')
            )
        
        if plan_lookup is not None:
            for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
                fact_df = fact_df.drop(columns=[col], errors='ignore')
            
            fact_df = fact_df.merge(
                plan_lookup,
                on=['contract_id', 'plan_id', 'year'],
                how='left'
            )
        
        # Add computed columns
        fact_df['time_key'] = fact_df['year'] * 100 + fact_df['month']
        fact_df['geo_key'] = fact_df['fips_code'] if 'fips_code' in fact_df.columns else None
        fact_df['plan_count'] = 1
        fact_df['_pipeline_run_id'] = PIPELINE_RUN_ID
        fact_df['_loaded_at'] = datetime.now()
        
        # Ensure all columns exist
        for col in final_cols:
            if col not in fact_df.columns:
                fact_df[col] = None
        
        result = fact_df[final_cols].copy()
        del fact_df  # Free memory
        
        # Track stats
        total_rows += len(result)
        if 'is_suppressed' in result.columns:
            total_suppressed += result['is_suppressed'].sum()
        if 'year' in result.columns:
            all_years.update(result['year'].dropna().unique())
        if 'state' in result.columns:
            all_states.update(result['state'].dropna().unique())
        
        # Save to temporary buffer
        buffer = BytesIO()
        result.to_parquet(buffer, index=False, compression='snappy')
        output_buffers.append(buffer.getvalue())
        del result, buffer  # Free memory
        
        if (period_idx + 1) % 12 == 0:
            print(f"   Processed {period_idx + 1}/{len(files_by_period)} periods ({total_rows:,} rows)")
            sys.stdout.flush()
    
    print(f"\n4. Summary:")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Suppressed rows: {total_suppressed:,}")
    print(f"   Year range: {min(all_years) if all_years else 'N/A'} - {max(all_years) if all_years else 'N/A'}")
    print(f"   States: {len(all_states)}")
    if total_rows > 0:
        print(f"   Suppression rate: {total_suppressed / total_rows * 100:.1f}%")
    sys.stdout.flush()
    
    # Write partitioned files to avoid OOM on concat
    print(f"\n5. Saving partitioned files to S3...")
    partition_count = 0
    
    for i, buf in enumerate(output_buffers):
        partition_key = f"gold/fact_enrollment_geographic/part_{i:04d}.parquet"
        s3.put_object(Bucket=S3_BUCKET, Key=partition_key, Body=buf)
        partition_count += 1
        if (i + 1) % 20 == 0:
            print(f"   Saved {i+1}/{len(output_buffers)} partitions")
            sys.stdout.flush()
    
    del output_buffers
    
    print(f"   Saved {partition_count} partitions to s3://{S3_BUCKET}/gold/fact_enrollment_geographic/")
    
    print(f"Saved to s3://{S3_BUCKET}/{OUTPUT_KEY}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
