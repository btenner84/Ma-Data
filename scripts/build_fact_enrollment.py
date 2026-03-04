#!/usr/bin/env python3
"""
Build fact_enrollment_all_years - Aggregated at contract+year grain

This creates a manageable fact table that can JOIN to stars/risk data.

Grain: contract_id + year + month + state + product_type
(Aggregates away plan_id and county to reduce size)

Output: ~500K rows instead of 400M
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import uuid

S3_BUCKET = "ma-data123"
PIPELINE_RUN_ID = f"enrollment_build_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

s3 = boto3.client('s3')


def load_parquet(key: str) -> pd.DataFrame:
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(resp['Body'].read()))
    except:
        return pd.DataFrame()


def list_enrollment_files() -> list:
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='processed/fact_enrollment/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('/data.parquet'):
                files.append(obj['Key'])
    return sorted(files)


def main():
    print("=" * 70)
    print("BUILDING fact_enrollment_all_years (aggregated)")
    print("=" * 70)

    # Load dimension tables
    print("\n[1/4] Loading dimensions...")
    dim_contract = load_parquet('processed/unified/dim_contract_v2.parquet')
    dim_contract = dim_contract[['contract_id', 'year', 'product_type', 'org_type']].drop_duplicates()
    print(f"  dim_contract: {len(dim_contract):,} rows")

    snp_lookup = load_parquet('processed/unified/snp_lookup.parquet')
    snp_lookup = snp_lookup[['contract_id', 'plan_id', 'year', 'snp_type']].drop_duplicates()
    # Ensure plan_id is string for joining
    snp_lookup['plan_id'] = snp_lookup['plan_id'].astype(str)
    print(f"  snp_lookup: {len(snp_lookup):,} rows")

    # Process files in batches, aggregating as we go
    print("\n[2/4] Processing monthly files...")
    files = list_enrollment_files()
    print(f"  Found {len(files)} files")

    aggregated_chunks = []

    for i, file_key in enumerate(files):
        df = load_parquet(file_key)
        if df.empty:
            continue

        # Ensure plan_id is string for joining
        df['plan_id'] = df['plan_id'].astype(str)

        # Join to dim_contract for product_type
        df = df.merge(dim_contract, on=['contract_id', 'year'], how='left')

        # Join to snp_lookup for snp_type (creates snp_type column)
        df = df.merge(
            snp_lookup,
            on=['contract_id', 'plan_id', 'year'],
            how='left'
        )

        # Aggregate to contract + year + month + state + product_type grain
        agg = df.groupby(
            ['year', 'month', 'contract_id', 'state', 'parent_org',
             'plan_type', 'product_type', 'org_type', 'snp_type'],
            dropna=False
        ).agg({
            'enrollment': 'sum',
            'plan_id': 'nunique',
            'fips_code': 'nunique'
        }).reset_index()

        agg = agg.rename(columns={
            'plan_id': 'plan_count',
            'fips_code': 'county_count'
        })

        agg['_source_file'] = file_key
        aggregated_chunks.append(agg)

        if (i + 1) % 25 == 0:
            print(f"  Processed {i + 1}/{len(files)} files")

    # Combine all aggregated chunks
    print("\n[3/4] Combining aggregated data...")
    df_final = pd.concat(aggregated_chunks, ignore_index=True)

    # Final aggregation (in case same contract appears in multiple source files)
    df_final = df_final.groupby(
        ['year', 'month', 'contract_id', 'state', 'parent_org',
         'plan_type', 'product_type', 'org_type', 'snp_type'],
        dropna=False
    ).agg({
        'enrollment': 'sum',
        'plan_count': 'sum',
        'county_count': 'sum',
        '_source_file': 'first'
    }).reset_index()

    df_final['_pipeline_run_id'] = PIPELINE_RUN_ID

    print(f"  Final rows: {len(df_final):,}")

    # Save
    print("\n[4/4] Saving to S3...")
    buffer = BytesIO()
    df_final.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key='processed/unified/fact_enrollment_all_years.parquet',
        Body=buffer.getvalue()
    )

    size_mb = len(buffer.getvalue()) / (1024 * 1024)
    print(f"  Saved: {size_mb:.1f} MB")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Rows: {len(df_final):,}")
    print(f"Years: {df_final['year'].min()}-{df_final['year'].max()}")
    print(f"Columns: {list(df_final.columns)}")

    print(f"\nEnrollment by product_type (2026):")
    df_2026 = df_final[df_final['year'] == 2026]
    print(df_2026.groupby('product_type', dropna=False)['enrollment'].sum().sort_values(ascending=False))

    print(f"\n✅ Pipeline Run ID: {PIPELINE_RUN_ID}")


if __name__ == "__main__":
    main()
