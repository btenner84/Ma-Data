#!/usr/bin/env python3
"""
Build Gold Layer: dim_plan
===========================

Creates the plan dimension table consolidating all plan-level attributes.

Sources:
- s3://ma-data123/silver/enrollment/cpsc/*/contracts.parquet (plan_type, org_type)
- s3://ma-data123/silver/snp/*/snp.parquet (SNP type)
- Plan ID convention for group_type (800+ = Group)

Output: s3://ma-data123/gold/dim_plan.parquet

Columns:
- plan_key: STRING (contract_id + plan_id + year)
- contract_id: STRING
- plan_id: STRING (3-digit)
- year: INT
- plan_name: STRING
- plan_type: STRING (HMO, PPO, PFFS, MSA, Cost, etc.)
- plan_type_category: STRING (HMO/HMOPOS, Local PPO, Regional PPO, etc.)
- product_type: STRING (MAPD, MA-only, PDP)
- snp_type: STRING (Non-SNP, D-SNP, C-SNP, I-SNP)
- group_type: STRING (Individual, Group)
- is_snp: BOOL
- is_eghp: BOOL (Employer Group Health Plan)
- offers_part_d: BOOL
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
import sys

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
CONTRACTS_PREFIX = "silver/enrollment/cpsc"
SNP_PREFIX = "silver/snp"
OUTPUT_KEY = "gold/dim_plan.parquet"

PLAN_TYPE_CATEGORIES = {
    'HMO': 'HMO/HMOPOS',
    'HMOPOS': 'HMO/HMOPOS',
    'HMO/HMOPOS': 'HMO/HMOPOS',
    'LOCAL PPO': 'Local PPO',
    'PPO': 'Local PPO',
    'REGIONAL PPO': 'Regional PPO',
    'RPPO': 'Regional PPO',
    'PFFS': 'PFFS',
    'MSA': 'MSA',
    'COST': 'Cost',
    'HCPP': 'Cost',
    'PACE': 'PACE',
    'PDP': 'PDP',
    'EMPLOYER PDP': 'Employer PDP',
}

s3 = boto3.client('s3')


def list_files(prefix: str, suffix: str = '.parquet') -> list:
    """List files with given prefix and suffix."""
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


def derive_group_type(plan_id: str) -> str:
    """CMS Convention: 800-999 = Group, 001-799 = Individual."""
    try:
        plan_num = int(plan_id)
        return 'Group' if plan_num >= 800 else 'Individual'
    except (ValueError, TypeError):
        return 'Individual'


def normalize_plan_type(val) -> str:
    """Normalize plan type to standard categories."""
    if pd.isna(val):
        return None
    val_str = str(val).strip().upper()
    return PLAN_TYPE_CATEGORIES.get(val_str, val_str)


def derive_product_type(plan_type: str, offers_part_d: str) -> str:
    """
    Derive product type:
    - PDP: Standalone prescription drug plan
    - MAPD: Medicare Advantage + Part D
    - MA-only: Medicare Advantage without Part D
    """
    if pd.isna(plan_type):
        return None
    
    pt_upper = str(plan_type).upper()
    
    if 'PDP' in pt_upper:
        return 'PDP'
    
    part_d = str(offers_part_d).upper() if pd.notna(offers_part_d) else ''
    if part_d == 'YES':
        return 'MAPD'
    else:
        return 'MA-only'


def main():
    print("=" * 70)
    print("BUILD GOLD LAYER: dim_plan")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    print("\n1. Loading contract/plan info...")
    contract_files = list_files(CONTRACTS_PREFIX, '/contracts.parquet')
    print(f"   Found {len(contract_files)} contract files")
    
    all_plans = []
    for f in contract_files[:20]:
        df = load_parquet(f)
        if not df.empty:
            all_plans.append(df)
    
    if not all_plans:
        print("No contract data found. Checking existing snp_lookup...")
        existing = load_parquet('processed/unified/snp_lookup.parquet')
        if not existing.empty:
            print(f"Using existing snp_lookup as base: {len(existing)} rows")
            all_plans = [existing]
    
    if not all_plans:
        print("ERROR: No source data found")
        return
    
    plans_df = pd.concat(all_plans, ignore_index=True)
    plans_df = plans_df.drop_duplicates(subset=['contract_id', 'plan_id', 'year'])
    print(f"   Loaded {len(plans_df):,} unique contract-plan-year combinations")
    sys.stdout.flush()
    
    print("\n2. Loading SNP data...")
    snp_files = list_files(SNP_PREFIX, '/snp.parquet')
    print(f"   Found {len(snp_files)} SNP files")
    
    snp_df = pd.DataFrame()
    if snp_files:
        snp_dfs = []
        for f in snp_files[:20]:
            df = load_parquet(f)
            if not df.empty and 'contract_id' in df.columns and 'snp_type' in df.columns:
                snp_dfs.append(df[['contract_id', 'plan_id', 'year', 'snp_type']].drop_duplicates())
        if snp_dfs:
            snp_df = pd.concat(snp_dfs, ignore_index=True).drop_duplicates()
            print(f"   Loaded {len(snp_df):,} SNP records")
    
    print("\n3. Building plan dimension...")
    
    if not snp_df.empty:
        plans_df = plans_df.merge(
            snp_df,
            on=['contract_id', 'plan_id', 'year'],
            how='left',
            suffixes=('', '_snp')
        )
        if 'snp_type_snp' in plans_df.columns:
            plans_df['snp_type'] = plans_df['snp_type'].fillna(plans_df['snp_type_snp'])
            plans_df = plans_df.drop(columns=['snp_type_snp'])
    
    if 'snp_type' not in plans_df.columns:
        plans_df['snp_type'] = None
    
    if 'is_snp' in plans_df.columns:
        is_snp_flag = plans_df['is_snp'].apply(lambda x: str(x).strip().upper() == 'YES')
        plans_df.loc[is_snp_flag & plans_df['snp_type'].isna(), 'snp_type'] = 'SNP-Unknown'
        plans_df.loc[~is_snp_flag & plans_df['snp_type'].isna(), 'snp_type'] = 'Non-SNP'
    else:
        plans_df['snp_type'] = plans_df['snp_type'].fillna('Non-SNP')
    
    plans_df['group_type'] = plans_df['plan_id'].apply(derive_group_type)
    
    plans_df['plan_type_category'] = plans_df['plan_type'].apply(normalize_plan_type)
    
    offers_part_d = plans_df.get('offers_part_d', pd.Series([None] * len(plans_df)))
    plans_df['product_type'] = plans_df.apply(
        lambda row: derive_product_type(row.get('plan_type', ''), row.get('offers_part_d', '')),
        axis=1
    )
    
    plans_df['plan_key'] = (
        plans_df['contract_id'].astype(str) + '_' +
        plans_df['plan_id'].astype(str) + '_' +
        plans_df['year'].astype(str)
    )
    
    plans_df['is_snp'] = plans_df['snp_type'] != 'Non-SNP'
    
    if 'is_eghp' in plans_df.columns:
        plans_df['is_eghp'] = plans_df['is_eghp'].apply(lambda x: str(x).strip().upper() == 'YES')
    else:
        plans_df['is_eghp'] = plans_df['group_type'] == 'Group'
    
    if 'offers_part_d' in plans_df.columns:
        plans_df['offers_part_d'] = plans_df['offers_part_d'].apply(lambda x: str(x).strip().upper() == 'YES')
    else:
        plans_df['offers_part_d'] = plans_df['product_type'] != 'MA-only'
    
    final_cols = [
        'plan_key', 'contract_id', 'plan_id', 'year',
        'plan_name', 'plan_type', 'plan_type_category', 'product_type',
        'snp_type', 'group_type', 'is_snp', 'is_eghp', 'offers_part_d'
    ]
    for col in final_cols:
        if col not in plans_df.columns:
            plans_df[col] = None
    
    result = plans_df[final_cols].drop_duplicates(subset=['plan_key'])
    
    print(f"   Generated {len(result):,} plan dimension rows")
    print(f"   SNP distribution: {result['snp_type'].value_counts().to_dict()}")
    print(f"   Group distribution: {result['group_type'].value_counts().to_dict()}")
    print(f"   Product distribution: {result['product_type'].value_counts().to_dict()}")
    sys.stdout.flush()
    
    print("\n4. Saving to S3...")
    buffer = BytesIO()
    result.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
    
    print(f"Saved to s3://{S3_BUCKET}/{OUTPUT_KEY}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
