#!/usr/bin/env python3
"""
Build Geographic Enrollment Table - Memory Efficient

Processes files in batches to avoid memory issues.
"""

import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import gc

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def main():
    print("=" * 70)
    print("BUILD GEOGRAPHIC ENROLLMENT TABLE (EFFICIENT)")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print()
    
    # Step 1: Load SNP lookup (small)
    print("[1/5] Loading SNP lookup...")
    resp = s3.get_object(Bucket=S3_BUCKET, Key='processed/unified/snp_lookup.parquet')
    snp_lookup = pd.read_parquet(BytesIO(resp['Body'].read()))
    snp_lookup['plan_id_str'] = snp_lookup['plan_id'].astype(str)
    snp_slim = snp_lookup[['contract_id', 'plan_id_str', 'year', 'snp_type']].drop_duplicates()
    print(f"  Loaded {len(snp_slim):,} SNP records")
    del snp_lookup
    gc.collect()
    
    # Step 2: Find all files
    print()
    print("[2/5] Finding enrollment files...")
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='processed/fact_enrollment/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                files.append(obj['Key'])
    print(f"  Found {len(files)} files")
    
    # Step 3: Process in batches and aggregate
    print()
    print("[3/5] Processing in batches...")
    
    BATCH_SIZE = 20
    all_aggregates = []
    
    for batch_start in range(0, len(files), BATCH_SIZE):
        batch_files = files[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(files) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"  Batch {batch_num}/{total_batches}...")
        
        batch_data = []
        for key in batch_files:
            try:
                resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
                df = pd.read_parquet(BytesIO(resp['Body'].read()))
                
                # Only keep rows with geography
                if 'state' in df.columns:
                    df = df[df['state'].notna() & (df['state'] != '') & (df['state'] != 'None')]
                    if len(df) > 0:
                        batch_data.append(df)
            except:
                pass
        
        if not batch_data:
            continue
        
        # Combine batch
        batch_df = pd.concat(batch_data, ignore_index=True)
        del batch_data
        gc.collect()
        
        # Join with SNP
        batch_df['plan_id_str'] = batch_df['plan_id'].astype(str) if 'plan_id' in batch_df.columns else 'unknown'
        batch_df = batch_df.merge(
            snp_slim,
            on=['contract_id', 'plan_id_str', 'year'],
            how='left'
        )
        batch_df['snp_type'] = batch_df['snp_type'].fillna('Non-SNP')
        
        # Derive product_type
        if 'product_type' not in batch_df.columns or batch_df['product_type'].isna().all():
            plan_type_str = batch_df['plan_type'].astype(str).str.lower()
            contract_prefix = batch_df['contract_id'].str[0].str.upper()
            is_pdp = (contract_prefix == 'S') | plan_type_str.str.contains('prescription drug|pdp', regex=True, na=False)
            batch_df['product_type'] = np.where(is_pdp, 'PDP', 'MAPD')
        
        # Set SNP to None for PDP
        is_pdp = batch_df['product_type'] == 'PDP'
        batch_df.loc[is_pdp, 'snp_type'] = None
        
        # Aggregate batch
        agg_batch = batch_df.groupby([
            'year', 'month', 'state', 'county',
            'parent_org', 'plan_type', 'product_type', 'snp_type'
        ], dropna=False).agg({
            'enrollment': 'sum',
            'contract_id': 'nunique'
        }).reset_index()
        
        agg_batch = agg_batch.rename(columns={'contract_id': 'plan_count'})
        all_aggregates.append(agg_batch)
        
        del batch_df, agg_batch
        gc.collect()
    
    # Step 4: Final aggregation
    print()
    print("[4/5] Final aggregation...")
    final_df = pd.concat(all_aggregates, ignore_index=True)
    del all_aggregates
    gc.collect()
    
    # Re-aggregate (some groups may span batches)
    final_df = final_df.groupby([
        'year', 'month', 'state', 'county',
        'parent_org', 'plan_type', 'product_type', 'snp_type'
    ], dropna=False).agg({
        'enrollment': 'sum',
        'plan_count': 'sum'
    }).reset_index()
    
    # Fill NaN strings
    for col in ['parent_org', 'plan_type', 'product_type', 'county']:
        if col in final_df.columns:
            final_df[col] = final_df[col].fillna('Unknown')
    
    print(f"  Final rows: {len(final_df):,}")
    print()
    print("Coverage:")
    print(f"  Years: {final_df['year'].min()} - {final_df['year'].max()}")
    print(f"  States: {final_df['state'].nunique()}")
    print()
    print("SNP types:")
    print(final_df['snp_type'].value_counts(dropna=False).to_dict())
    
    # Step 5: Save
    print()
    print("[5/5] Saving...")
    output_key = 'processed/unified/fact_enrollment_by_geography.parquet'
    buffer = BytesIO()
    final_df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=buffer.getvalue())
    size_mb = len(buffer.getvalue()) / (1024 * 1024)
    print(f"Saved to s3://{S3_BUCKET}/{output_key}")
    print(f"Size: {size_mb:.1f} MB")
    print()
    print("COLUMNS:", list(final_df.columns))
    print()
    print("=" * 70)
    print("COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
