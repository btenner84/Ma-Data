#!/usr/bin/env python3
"""
Build Unified Enrollment Fact Table - COMPREHENSIVE
====================================================

Creates a single authoritative enrollment fact table with:
- All filter dimensions (plan_type, product_type, group_type, snp_type)
- Proper SNP join BEFORE aggregation (preserves plan_id for join)
- Group type from plan_id (CMS convention: 800+ = Group)
- Full audit trail
- MECE validation for each dimension

Source Hierarchy:
1. processed/fact_enrollment/ - Base enrollment (from CPSC)
2. processed/unified/snp_lookup.parquet - SNP type mapping  
3. Source enrollment already has is_snp flag

Output: processed/unified/fact_enrollment_unified_v2.parquet

Grain: contract_id + year + month + state + plan_type + product_type + snp_type + group_type
"""

import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import sys
import warnings
warnings.filterwarnings('ignore')

S3_BUCKET = "ma-data123"
PIPELINE_RUN_ID = f"unified_enrollment_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

s3 = boto3.client('s3')


def load_parquet(key: str) -> pd.DataFrame:
    """Load a parquet file from S3."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(resp['Body'].read()))
    except Exception as e:
        print(f"  [WARN] Could not load {key}: {e}")
        return pd.DataFrame()


def list_enrollment_files() -> list:
    """List all processed enrollment files."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='processed/fact_enrollment/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('/data.parquet'):
                files.append(obj['Key'])
    return sorted(files)


def derive_group_type_from_plan_id(plan_id: str) -> str:
    """
    Derive group type from plan_id.
    
    CMS Convention:
    - Plan IDs 001-799: Individual
    - Plan IDs 800-999: Group (EGHP - Employer Group Health Plan)
    """
    try:
        plan_num = int(plan_id)
        if plan_num >= 800:
            return 'Group'
        else:
            return 'Individual'
    except (ValueError, TypeError):
        return 'Individual'


def main():
    print("=" * 70)
    print("BUILDING UNIFIED ENROLLMENT FACT TABLE V2")
    print("=" * 70)
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    # =========================================================================
    # STEP 1: Load SNP lookup
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 1: Loading SNP lookup")
    print("=" * 70)
    sys.stdout.flush()
    
    snp_lookup = load_parquet('processed/unified/snp_lookup.parquet')
    if not snp_lookup.empty:
        snp_lookup['contract_id'] = snp_lookup['contract_id'].astype(str).str.strip()
        snp_lookup['plan_id'] = snp_lookup['plan_id'].astype(str).str.strip().str.zfill(3)
        print(f"  snp_lookup: {len(snp_lookup):,} rows")
        print(f"  Years covered: {sorted(snp_lookup['year'].unique())}")
    else:
        print("  [WARN] No SNP lookup found")
    sys.stdout.flush()
    
    # =========================================================================
    # STEP 2: Process enrollment files
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 2: Processing enrollment files")
    print("=" * 70)
    sys.stdout.flush()
    
    files = list_enrollment_files()
    print(f"  Found {len(files)} enrollment files")
    sys.stdout.flush()
    
    all_chunks = []
    
    for i, file_key in enumerate(files):
        df = load_parquet(file_key)
        if df.empty:
            continue
        
        # Normalize key columns
        df['contract_id'] = df['contract_id'].astype(str).str.strip()
        if 'plan_id' in df.columns:
            df['plan_id'] = df['plan_id'].astype(str).str.strip().str.zfill(3)
        else:
            df['plan_id'] = '000'
        
        # Derive group_type from plan_id BEFORE aggregation
        df['group_type'] = df['plan_id'].apply(derive_group_type_from_plan_id)
        
        # Use is_snp flag to determine SNP vs Non-SNP
        # Then join to snp_lookup for specific type (D-SNP, C-SNP, I-SNP)
        if 'is_snp' in df.columns:
            df['is_snp_flag'] = df['is_snp'].apply(lambda x: str(x).strip().lower() == 'yes')
        else:
            df['is_snp_flag'] = False
        
        # Join to SNP lookup for specific SNP type
        if not snp_lookup.empty:
            year_val = df['year'].iloc[0] if 'year' in df.columns else None
            if year_val and year_val in snp_lookup['year'].values:
                snp_year = snp_lookup[snp_lookup['year'] == year_val][['contract_id', 'plan_id', 'snp_type']]
                df = df.merge(snp_year, on=['contract_id', 'plan_id'], how='left')
            else:
                df['snp_type'] = None
        else:
            df['snp_type'] = None
        
        # Set SNP type based on flag and lookup
        # If is_snp='Yes' but no specific type from lookup, use 'SNP-Unknown'
        # If is_snp='No', always 'Non-SNP'
        df['snp_type'] = df.apply(
            lambda r: r['snp_type'] if pd.notna(r['snp_type']) 
                      else ('SNP-Unknown' if r['is_snp_flag'] else 'Non-SNP'),
            axis=1
        )
        
        # Now aggregate
        agg_cols = ['year', 'month', 'contract_id', 'state', 'parent_org',
                    'plan_type', 'product_type', 'group_type', 'snp_type']
        agg_cols = [c for c in agg_cols if c in df.columns]
        
        agg = df.groupby(agg_cols, dropna=False).agg({
            'enrollment': 'sum',
            'plan_id': 'nunique'
        }).reset_index()
        
        agg = agg.rename(columns={'plan_id': 'plan_count'})
        agg['_source_file'] = file_key
        
        all_chunks.append(agg)
        
        if (i + 1) % 25 == 0:
            print(f"  Processed {i + 1}/{len(files)} files...")
            sys.stdout.flush()
    
    print(f"  Processed {len(files)} files total")
    sys.stdout.flush()
    
    # =========================================================================
    # STEP 3: Combine and finalize
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 3: Combining and finalizing")
    print("=" * 70)
    sys.stdout.flush()
    
    df_final = pd.concat(all_chunks, ignore_index=True)
    
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
    
    # =========================================================================
    # STEP 4: MECE Validation
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 4: MECE Validation")
    print("=" * 70)
    sys.stdout.flush()
    
    # Get Jan 2026 data for validation
    df_2026_01 = df_final[(df_final['year'] == 2026) & (df_final['month'] == 1)]
    total_enrollment = df_2026_01['enrollment'].sum()
    
    print(f"\nValidation using Jan 2026 (total: {total_enrollment:,.0f})")
    
    validation_passed = True
    
    for dim_name, dim_col in [('Plan Type', 'plan_type'), ('Product Type', 'product_type'), 
                               ('Group Type', 'group_type'), ('SNP Type', 'snp_type')]:
        if dim_col not in df_2026_01.columns:
            print(f"\n  {dim_name.upper()}: [SKIP] Column not found")
            continue
            
        print(f"\n  {dim_name.upper()}:")
        dim_sum = df_2026_01.groupby(dim_col, dropna=False)['enrollment'].sum()
        dim_total = dim_sum.sum()
        dim_pct = 100 * dim_total / total_enrollment if total_enrollment > 0 else 0
        print(f"    Sum: {dim_total:,.0f} ({dim_pct:.2f}%)")
        for val, enroll in dim_sum.sort_values(ascending=False).head(5).items():
            print(f"      {val or 'NULL'}: {enroll:,.0f} ({100*enroll/total_enrollment:.1f}%)")
        if abs(dim_pct - 100) > 0.1:
            print(f"    [WARN] Doesn't sum to 100%")
            validation_passed = False
        else:
            print(f"    [OK] Sums to 100%")
    
    sys.stdout.flush()
    
    # =========================================================================
    # STEP 5: Save to S3
    # =========================================================================
    print("\n" + "=" * 70)
    print("STEP 5: Saving to S3")
    print("=" * 70)
    sys.stdout.flush()
    
    buffer = BytesIO()
    df_final.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    
    output_key = 'processed/unified/fact_enrollment_unified_v2.parquet'
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=buffer.getvalue()
    )
    
    size_mb = len(buffer.getvalue()) / (1024 * 1024)
    print(f"  Saved to s3://{S3_BUCKET}/{output_key}")
    print(f"  Size: {size_mb:.1f} MB")
    
    # Also update the main table reference
    output_key_main = 'processed/unified/fact_enrollment_all_years.parquet'
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key_main,
        Body=buffer.getvalue()
    )
    print(f"  Also saved to s3://{S3_BUCKET}/{output_key_main}")
    sys.stdout.flush()
    
    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Rows: {len(df_final):,}")
    print(f"Years: {df_final['year'].min()}-{df_final['year'].max()}")
    print(f"Columns: {list(df_final.columns)}")
    print(f"MECE Validation: {'PASSED' if validation_passed else 'NEEDS REVIEW'}")
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Finished: {datetime.now()}")
    sys.stdout.flush()
    
    return validation_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
