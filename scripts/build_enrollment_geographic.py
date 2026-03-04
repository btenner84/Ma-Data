#!/usr/bin/env python3
"""
Build Geographic Enrollment Table

Creates fact_enrollment_by_geography with ALL filter dimensions:
- year, month
- state, county
- parent_org, plan_type, product_type
- snp_type
- enrollment, plan_count

This enables all filters to work with geographic queries.
"""

import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def load_snp_lookup():
    """Load SNP type lookup."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key='processed/unified/snp_lookup.parquet')
        df = pd.read_parquet(BytesIO(resp['Body'].read()))
        print(f"Loaded SNP lookup: {len(df):,} records")
        return df
    except Exception as e:
        print(f"Warning: Could not load SNP lookup: {e}")
        return pd.DataFrame()


def build_geographic_table():
    """Build complete geographic enrollment table from source files."""
    
    print("=" * 70)
    print("BUILD GEOGRAPHIC ENROLLMENT TABLE")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print()
    
    # Load SNP lookup
    snp_lookup = load_snp_lookup()
    
    # Find all processed enrollment files
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='processed/fact_enrollment/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                files.append(obj['Key'])
    
    print(f"Found {len(files)} source files")
    
    all_data = []
    
    for i, key in enumerate(sorted(files)):
        if (i + 1) % 20 == 0:
            print(f"  Processing {i+1}/{len(files)}...")
        
        try:
            resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_parquet(BytesIO(resp['Body'].read()))
            
            # Ensure required columns exist
            required = ['year', 'month', 'contract_id', 'state', 'county', 'enrollment']
            if not all(c in df.columns for c in required):
                continue
            
            all_data.append(df)
        except Exception as e:
            print(f"    Error reading {key}: {e}")
    
    if not all_data:
        print("No data loaded!")
        return
    
    print(f"\nCombining {len(all_data)} files...")
    df = pd.concat(all_data, ignore_index=True)
    print(f"Total rows: {len(df):,}")
    
    # Join with SNP lookup
    if not snp_lookup.empty:
        print("\nJoining with SNP lookup...")
        df['plan_id_str'] = df['plan_id'].astype(str) if 'plan_id' in df.columns else None
        
        snp_lookup['plan_id_str'] = snp_lookup['plan_id'].astype(str)
        snp_lookup_slim = snp_lookup[['contract_id', 'plan_id_str', 'year', 'snp_type']].drop_duplicates()
        
        df = df.merge(
            snp_lookup_slim,
            on=['contract_id', 'plan_id_str', 'year'],
            how='left',
            suffixes=('', '_snp')
        )
        
        # Handle missing SNP type
        df['snp_type'] = df['snp_type'].fillna('Non-SNP')
        df.loc[df['snp_type'] == '', 'snp_type'] = 'Non-SNP'
    else:
        df['snp_type'] = 'Non-SNP'
    
    # Derive product_type if missing
    if 'product_type' not in df.columns or df['product_type'].isna().all():
        print("Deriving product_type...")
        plan_type_str = df['plan_type'].astype(str).str.lower()
        contract_prefix = df['contract_id'].str[0].str.upper()
        
        is_pdp = (contract_prefix == 'S') | plan_type_str.str.contains('prescription drug|pdp', regex=True, na=False)
        df['product_type'] = np.where(is_pdp, 'PDP', 'MAPD')
    
    # Drop rows with no geography
    print("Filtering rows with geography...")
    df = df[df['state'].notna() & (df['state'] != '')]
    print(f"Rows with geography: {len(df):,}")
    
    # Aggregate by all dimensions
    print("\nAggregating...")
    agg_df = df.groupby([
        'year', 'month', 'state', 'county',
        'parent_org', 'plan_type', 'product_type', 'snp_type'
    ], dropna=False).agg({
        'enrollment': 'sum',
        'plan_id': 'nunique' if 'plan_id' in df.columns else 'count'
    }).reset_index()
    
    agg_df = agg_df.rename(columns={'plan_id': 'plan_count'})
    
    # Fill NaN in string columns
    for col in ['parent_org', 'plan_type', 'product_type', 'snp_type', 'county']:
        if col in agg_df.columns:
            agg_df[col] = agg_df[col].fillna('Unknown')
    
    print(f"Aggregated rows: {len(agg_df):,}")
    print()
    print("Coverage:")
    print(f"  Years: {agg_df['year'].min()} - {agg_df['year'].max()}")
    print(f"  States: {agg_df['state'].nunique()}")
    print(f"  SNP types: {agg_df['snp_type'].value_counts().to_dict()}")
    
    # Save
    print("\nSaving to S3...")
    output_key = 'processed/unified/fact_enrollment_by_geography.parquet'
    
    buffer = BytesIO()
    agg_df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    
    s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=buffer.getvalue())
    
    size_mb = len(buffer.getvalue()) / (1024 * 1024)
    print(f"Saved to s3://{S3_BUCKET}/{output_key}")
    print(f"Size: {size_mb:.1f} MB")
    
    print()
    print("=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Finished: {datetime.now()}")


if __name__ == "__main__":
    build_geographic_table()
