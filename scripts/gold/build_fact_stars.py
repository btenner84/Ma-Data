#!/usr/bin/env python3
"""
Build Gold Layer: fact_stars
=============================

Creates the Stars ratings fact table.

Sources:
- s3://ma-data123/silver/stars/*/summary.parquet
- s3://ma-data123/gold/dim_entity.parquet

Output: s3://ma-data123/gold/fact_stars.parquet

Grain: contract_id + year

Columns:
- entity_id: STRING
- contract_id: STRING
- year: INT
- overall_rating: FLOAT (1-5 stars)
- part_c_rating: FLOAT
- part_d_rating: FLOAT
- improvement_rating: FLOAT
- cai_eligible: BOOL (Contract Adjustment Indicator eligible)
- low_enrollment: BOOL
- is_new_contract: BOOL
- parent_org: STRING
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
SILVER_PREFIX = "silver/stars"
DIM_ENTITY_KEY = "gold/dim_entity.parquet"
OUTPUT_KEY = "gold/fact_stars.parquet"

PIPELINE_RUN_ID = f"fact_stars_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

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
    print("BUILD GOLD LAYER: fact_stars")
    print("=" * 70)
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    print("\n1. Loading dim_entity...")
    dim_entity = load_parquet(DIM_ENTITY_KEY)
    entity_lookup = None
    if not dim_entity.empty:
        entity_lookup = dim_entity[['contract_id', 'year', 'entity_id', 'parent_org']].drop_duplicates()
        print(f"   dim_entity: {len(entity_lookup):,} rows")
    sys.stdout.flush()
    
    print("\n2. Loading silver stars summary...")
    silver_files = list_files(SILVER_PREFIX, '/summary.parquet')
    print(f"   Found {len(silver_files)} silver files")
    
    if not silver_files:
        print("   Checking for existing stars data...")
        existing = load_parquet('processed/unified/summary_all_years.parquet')
        if not existing.empty:
            print(f"   Using existing summary_all_years: {len(existing)} rows")
            silver_files = []
            all_chunks = [existing]
        else:
            print("   ERROR: No stars data found")
            return
    else:
        all_chunks = []
        for f in silver_files:
            df = load_parquet(f)
            if not df.empty:
                all_chunks.append(df)
    
    if not all_chunks:
        print("   ERROR: No stars data loaded")
        return
    
    fact_df = pd.concat(all_chunks, ignore_index=True)
    print(f"   Total rows: {len(fact_df):,}")
    sys.stdout.flush()
    
    print("\n3. Standardizing columns...")
    
    # Handle different source formats
    rating_col_map = {
        'overall_star_rating': 'overall_rating',
        'overall_stars': 'overall_rating',
        'overall': 'overall_rating',
        'part_c_summary_star_rating': 'part_c_rating',
        'part_c_stars': 'part_c_rating',
        'part_d_summary_star_rating': 'part_d_rating',
        'part_d_stars': 'part_d_rating',
        'summary_rating': 'overall_rating',  # From processed data
        'parent_organization': 'parent_org',  # From processed data
    }
    
    for old_col, new_col in rating_col_map.items():
        if old_col in fact_df.columns and new_col not in fact_df.columns:
            fact_df[new_col] = fact_df[old_col]
    
    # If data has 'part' column (Part C / Part D rows), pivot to get overall rating
    if 'part' in fact_df.columns and 'summary_rating' in fact_df.columns:
        print("   Pivoting part-level data to contract level...")
        # Group by contract and year, get the overall rating (usually Part C for MA contracts)
        pivot_df = fact_df.pivot_table(
            index=['contract_id', 'year'],
            columns='part',
            values='summary_rating',
            aggfunc='first'
        ).reset_index()
        pivot_df.columns.name = None
        
        # Use Part C rating as overall for MA contracts, Part D for PDP
        if 'C' in pivot_df.columns:
            pivot_df['part_c_rating'] = pd.to_numeric(pivot_df['C'], errors='coerce')
        if 'D' in pivot_df.columns:
            pivot_df['part_d_rating'] = pd.to_numeric(pivot_df['D'], errors='coerce')
        
        # Overall is typically Part C rating for MA contracts
        pivot_df['overall_rating'] = pivot_df.get('part_c_rating', pivot_df.get('part_d_rating'))
        
        # Merge back parent_org
        parent_org_lookup = fact_df[['contract_id', 'year', 'parent_org']].drop_duplicates() if 'parent_org' in fact_df.columns else None
        if parent_org_lookup is not None:
            pivot_df = pivot_df.merge(parent_org_lookup, on=['contract_id', 'year'], how='left')
        
        fact_df = pivot_df
    
    # Parse text ratings like "3 out of 5 stars" or "2.5 out of 5 stars" to numeric
    def parse_rating(val):
        if pd.isna(val):
            return None
        val_str = str(val).strip().lower()
        # Handle "not enough data", "plan too new", etc.
        if 'not enough' in val_str or 'plan too new' in val_str or 'n/a' in val_str:
            return None
        # Handle "X out of 5 stars" format
        import re
        match = re.match(r'^([\d.]+)\s*(out of 5)?\s*stars?', val_str)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        # Try direct numeric conversion
        try:
            return float(val_str)
        except ValueError:
            return None
    
    rating_cols = ['overall_rating', 'part_c_rating', 'part_d_rating']
    for col in rating_cols:
        if col in fact_df.columns:
            fact_df[col] = fact_df[col].apply(parse_rating)
    sys.stdout.flush()
    
    print("\n4. Enriching with entity data...")
    if entity_lookup is not None:
        fact_df = fact_df.merge(
            entity_lookup,
            on=['contract_id', 'year'],
            how='left',
            suffixes=('', '_entity')
        )
        fact_df['entity_id'] = fact_df['entity_id'].fillna(fact_df['contract_id'])
        if 'parent_org_entity' in fact_df.columns:
            fact_df['parent_org'] = fact_df['parent_org'].fillna(fact_df['parent_org_entity'])
    else:
        fact_df['entity_id'] = fact_df['contract_id']
    
    fact_df['_pipeline_run_id'] = PIPELINE_RUN_ID
    fact_df['_loaded_at'] = datetime.now()
    
    final_cols = [
        'entity_id', 'contract_id', 'year',
        'overall_rating', 'part_c_rating', 'part_d_rating',
        'parent_org',
        '_source_file', '_source_row', '_pipeline_run_id', '_loaded_at'
    ]
    for col in final_cols:
        if col not in fact_df.columns:
            fact_df[col] = None
    
    result = fact_df[final_cols].drop_duplicates(subset=['contract_id', 'year'])
    
    print(f"   Final fact table: {len(result):,} rows")
    print(f"   Year range: {result['year'].min()} - {result['year'].max()}")
    print(f"   Contracts with 4+ stars: {(result['overall_rating'] >= 4).sum():,}")
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
