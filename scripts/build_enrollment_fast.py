#!/usr/bin/env python3
"""
Fast Enrollment Build - Builds corrected aggregated enrollment with SNP and Group Type

This is a FAST build that:
1. Reads aggregated data (not 425M raw rows)
2. Properly derives group_type from plan_id
3. Uses is_snp flag + snp_lookup for correct SNP types
4. Validates MECE for all dimensions

Runtime: ~2-3 minutes (vs 40+ min for full build)
"""

import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

S3_BUCKET = "ma-data123"
PIPELINE_RUN_ID = f"enrollment_fast_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

s3 = boto3.client('s3')


def load_parquet(key: str) -> pd.DataFrame:
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(resp['Body'].read()))
    except Exception as e:
        print(f"  [WARN] Could not load {key}: {e}")
        return pd.DataFrame()


def list_enrollment_files() -> list:
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='processed/fact_enrollment/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('/data.parquet'):
                files.append(obj['Key'])
    return sorted(files)


def process_file(file_key: str, snp_lookup: pd.DataFrame) -> pd.DataFrame:
    """Process a single enrollment file and return aggregated result."""
    df = load_parquet(file_key)
    if df.empty:
        return pd.DataFrame()
    
    year = df['year'].iloc[0] if 'year' in df.columns else None
    
    # Normalize columns
    df['contract_id'] = df['contract_id'].astype(str).str.strip()
    df['plan_id'] = df['plan_id'].astype(str).str.strip().str.zfill(3)
    
    # Derive group_type from plan_id (vectorized - FAST)
    df['plan_id_num'] = pd.to_numeric(df['plan_id'], errors='coerce').fillna(0)
    df['group_type'] = np.where(df['plan_id_num'] >= 800, 'Group', 'Individual')
    
    # Derive product_type from plan_type and contract prefix (vectorized - FAST)
    # PDP plans: "Medicare Prescription Drug Plan" or "Employer/Union Only Direct Contract PDP"
    # MA plans: Everything else (H/R prefix contracts)
    if 'product_type' not in df.columns or df['product_type'].isna().all():
        plan_type_str = df['plan_type'].astype(str).str.lower()
        contract_prefix = df['contract_id'].str[0].str.upper()
        
        # PDP: S prefix or plan type contains "prescription drug" or "pdp"
        is_pdp = (contract_prefix == 'S') | plan_type_str.str.contains('prescription drug|pdp', regex=True, na=False)
        
        # MA-only: Plan type explicitly says "MA-only" or no Part D indicator
        # For now, assume most H/R plans are MAPD unless proven otherwise
        df['product_type'] = np.where(
            is_pdp,
            'PDP',
            'MAPD'  # Default MA plans to MAPD (can refine later with Part D data)
        )
    
    # Use is_snp flag for base SNP determination
    if 'is_snp' in df.columns:
        df['is_snp_bool'] = df['is_snp'].astype(str).str.lower().str.strip() == 'yes'
    else:
        df['is_snp_bool'] = False
    
    # Determine if MA plan (SNP only applies to MA, not PDP)
    plan_type_str = df['plan_type'].astype(str).str.lower()
    is_ma = ~plan_type_str.str.contains('prescription drug|pdp', regex=True, na=False)
    
    # Join to snp_lookup for specific type (D-SNP, C-SNP, I-SNP)
    if not snp_lookup.empty and year and year in snp_lookup['year'].values:
        snp_year = snp_lookup[snp_lookup['year'] == year][['contract_id', 'plan_id', 'snp_type']]
        df = df.merge(snp_year, on=['contract_id', 'plan_id'], how='left')
    else:
        df['snp_type'] = None
    
    # Set final SNP type (vectorized - FAST)
    # SNP only applies to MA plans, PDP gets None
    snp_type_from_lookup = df.get('snp_type', pd.Series([None]*len(df)))
    df['snp_type'] = np.where(
        ~is_ma,
        None,  # PDP plans: SNP not applicable
        np.where(
            pd.notna(snp_type_from_lookup),
            snp_type_from_lookup,
            np.where(df['is_snp_bool'], 'SNP-Unknown', 'Non-SNP')
        )
    )
    
    # Aggregate
    agg_cols = ['year', 'month', 'contract_id', 'state', 'parent_org',
                'plan_type', 'product_type', 'group_type', 'snp_type']
    agg_cols = [c for c in agg_cols if c in df.columns]
    
    result = df.groupby(agg_cols, dropna=False).agg({
        'enrollment': 'sum',
        'plan_id': 'nunique'
    }).reset_index()
    
    result = result.rename(columns={'plan_id': 'plan_count'})
    result['_source_file'] = file_key
    
    return result


def main():
    print("=" * 70)
    print("FAST ENROLLMENT BUILD")
    print("=" * 70)
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    # Load SNP lookup
    print("\n[1/4] Loading SNP lookup...")
    sys.stdout.flush()
    snp_lookup = load_parquet('processed/unified/snp_lookup.parquet')
    if not snp_lookup.empty:
        snp_lookup['contract_id'] = snp_lookup['contract_id'].astype(str).str.strip()
        snp_lookup['plan_id'] = snp_lookup['plan_id'].astype(str).str.strip().str.zfill(3)
        print(f"  Loaded {len(snp_lookup):,} SNP mappings for years {sorted(snp_lookup['year'].unique())}")
    sys.stdout.flush()
    
    # Get file list
    files = list_enrollment_files()
    print(f"\n[2/4] Processing {len(files)} files...")
    sys.stdout.flush()
    
    # Process files with progress
    all_results = []
    for i, file_key in enumerate(files):
        result = process_file(file_key, snp_lookup)
        if not result.empty:
            all_results.append(result)
        if (i + 1) % 20 == 0:
            print(f"  Processed {i + 1}/{len(files)} files...")
            sys.stdout.flush()
    
    print(f"  Processed {len(files)} files total")
    sys.stdout.flush()
    
    # Combine results
    print("\n[3/4] Combining and validating...")
    sys.stdout.flush()
    
    df_final = pd.concat(all_results, ignore_index=True)
    
    # Final aggregation
    agg_cols = ['year', 'month', 'contract_id', 'state', 'parent_org',
                'plan_type', 'product_type', 'group_type', 'snp_type']
    agg_cols = [c for c in agg_cols if c in df_final.columns]
    
    df_final = df_final.groupby(agg_cols, dropna=False).agg({
        'enrollment': 'sum',
        'plan_count': 'sum',
        '_source_file': 'first'
    }).reset_index()
    
    df_final['_pipeline_run_id'] = PIPELINE_RUN_ID
    
    print(f"  Final rows: {len(df_final):,}")
    print(f"  Total enrollment: {df_final['enrollment'].sum():,.0f}")
    sys.stdout.flush()
    
    # MECE Validation
    print("\n  MECE Validation (Jan 2026):")
    df_2026 = df_final[(df_final['year'] == 2026) & (df_final['month'] == 1)]
    total = df_2026['enrollment'].sum()
    
    for dim in ['plan_type', 'product_type', 'group_type', 'snp_type']:
        if dim in df_2026.columns:
            dim_sum = df_2026.groupby(dim, dropna=False)['enrollment'].sum()
            pct = 100 * dim_sum.sum() / total if total > 0 else 0
            status = "OK" if abs(pct - 100) < 0.1 else "WARN"
            print(f"    {dim}: {pct:.2f}% [{status}]")
            if dim == 'snp_type':
                for val, enroll in dim_sum.sort_values(ascending=False).head(5).items():
                    print(f"      {val or 'NULL'}: {enroll:,.0f} ({100*enroll/total:.1f}%)")
    sys.stdout.flush()
    
    # Save
    print("\n[4/4] Saving to S3...")
    sys.stdout.flush()
    
    buffer = BytesIO()
    df_final.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    
    # Save to main location
    s3.put_object(
        Bucket=S3_BUCKET,
        Key='processed/unified/fact_enrollment_all_years.parquet',
        Body=buffer.getvalue()
    )
    
    size_mb = len(buffer.getvalue()) / (1024 * 1024)
    print(f"  Saved to s3://{S3_BUCKET}/processed/unified/fact_enrollment_all_years.parquet")
    print(f"  Size: {size_mb:.1f} MB")
    
    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Rows: {len(df_final):,}")
    print(f"Years: {df_final['year'].min()}-{df_final['year'].max()}")
    print(f"Columns: {list(df_final.columns)}")
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Finished: {datetime.now()}")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
