#!/usr/bin/env python3
"""
MA Data Platform - Full ETL Processing

Processes ALL CPSC enrollment data (2013-2026) into Parquet dimensional model.
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import zipfile
import tempfile
import os
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# Configuration
S3_BUCKET = "ma-data123"
LOCAL_PROCESSED = Path("/Users/bentenner/rate/ma-data-platform/processed")
LOCAL_PROCESSED.mkdir(parents=True, exist_ok=True)

# S3 client
s3 = boto3.client('s3')


def list_cpsc_files():
    """List all CPSC files in S3."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='raw/enrollment/cpsc/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                # Extract year-month from path
                parts = obj['Key'].split('/')
                year_month = parts[3]  # e.g., "2026-01"
                files.append((year_month, obj['Key']))
    return sorted(files)


def download_and_extract_zip(s3_key: str) -> dict:
    """Download ZIP from S3 and extract, return file paths."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    zip_bytes = BytesIO(response['Body'].read())

    temp_dir = tempfile.mkdtemp()
    with zipfile.ZipFile(zip_bytes, 'r') as zf:
        zf.extractall(temp_dir)

    files = {}
    for root, dirs, filenames in os.walk(temp_dir):
        for f in filenames:
            ext = Path(f).suffix.lower()
            if ext in ['.csv', '.xlsx', '.xls']:
                files[f] = os.path.join(root, f)

    return files, temp_dir


def process_cpsc_month(year_month: str, s3_key: str) -> dict:
    """Process a single CPSC month."""
    year, month = int(year_month[:4]), int(year_month[5:7])

    try:
        files, temp_dir = download_and_extract_zip(s3_key)
    except Exception as e:
        return {'year_month': year_month, 'status': 'error', 'error': str(e)}

    contract_df = None
    enrollment_df = None

    for filename, filepath in files.items():
        try:
            if 'Contract_Info' in filename:
                contract_df = pd.read_csv(filepath, encoding='latin-1')
            elif 'Enrollment_Info' in filename:
                enrollment_df = pd.read_csv(filepath, encoding='latin-1')
        except Exception as e:
            pass

    # Clean up temp dir
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)

    if contract_df is None or enrollment_df is None:
        return {'year_month': year_month, 'status': 'missing_files'}

    # Add year/month
    contract_df['year'] = year
    contract_df['month'] = month
    enrollment_df['year'] = year
    enrollment_df['month'] = month

    # Handle suppressed values
    enrollment_df['Enrollment'] = pd.to_numeric(
        enrollment_df['Enrollment'].replace('*', None), errors='coerce'
    )

    # Build fact table
    fact = enrollment_df.merge(
        contract_df[['Contract ID', 'Plan ID', 'Parent Organization', 'Plan Type', 'SNP Plan']],
        left_on=['Contract Number', 'Plan ID'],
        right_on=['Contract ID', 'Plan ID'],
        how='left'
    )

    fact = fact.rename(columns={
        'Contract Number': 'contract_id',
        'Plan ID': 'plan_id',
        'FIPS State County Code': 'fips_code',
        'Enrollment': 'enrollment',
        'Parent Organization': 'parent_org',
        'Plan Type': 'plan_type',
        'SNP Plan': 'is_snp',
        'State': 'state',
        'County': 'county'
    })

    fact['fips_code'] = fact['fips_code'].astype(str).replace('nan', '')

    fact = fact[['year', 'month', 'contract_id', 'plan_id', 'fips_code', 'state', 'county',
                 'enrollment', 'parent_org', 'plan_type', 'is_snp']]

    # Save to parquet
    output_path = LOCAL_PROCESSED / f'fact_enrollment/{year}/{month:02d}/data.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fact.to_parquet(output_path, index=False)

    return {
        'year_month': year_month,
        'status': 'success',
        'rows': len(fact),
        'enrollment': fact['enrollment'].sum()
    }


def main():
    print("=" * 60)
    print("MA DATA PLATFORM - FULL ETL PROCESSING")
    print("=" * 60)

    # List all CPSC files
    print("\nListing CPSC files in S3...")
    files = list_cpsc_files()
    print(f"Found {len(files)} CPSC files to process")
    print(f"Date range: {files[0][0]} to {files[-1][0]}")
    print("-" * 60)

    # Process all files with thread pool
    results = []
    success_count = 0
    error_count = 0
    total_rows = 0
    total_enrollment = 0

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(process_cpsc_month, year_month, s3_key): year_month
            for year_month, s3_key in files
        }

        for i, future in enumerate(as_completed(futures), 1):
            year_month = futures[future]
            try:
                result = future.result()
                results.append(result)

                if result['status'] == 'success':
                    success_count += 1
                    total_rows += result['rows']
                    total_enrollment += result.get('enrollment', 0) or 0
                    print(f"[{i:3d}/{len(files)}] {year_month}: {result['rows']:,} rows")
                else:
                    error_count += 1
                    print(f"[{i:3d}/{len(files)}] {year_month}: {result['status']}")

            except Exception as e:
                error_count += 1
                print(f"[{i:3d}/{len(files)}] {year_month}: ERROR - {e}")

    print("-" * 60)
    print(f"\nCOMPLETE!")
    print(f"  Successful: {success_count}/{len(files)}")
    print(f"  Errors: {error_count}")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Total enrollment: {total_enrollment:,.0f}")

    # Build unified dimensions from latest month
    print("\nBuilding dimension tables from latest data...")

    # Read latest month for dimensions
    latest = sorted([f for f in (LOCAL_PROCESSED / 'fact_enrollment').glob('**/*.parquet')])[-1]
    latest_fact = pd.read_parquet(latest)

    # dim_organization
    dim_org = latest_fact.groupby('parent_org').agg({
        'contract_id': lambda x: list(x.unique()),
        'enrollment': 'sum'
    }).reset_index()
    dim_org['org_id'] = range(1, len(dim_org) + 1)
    dim_org = dim_org.rename(columns={'parent_org': 'parent_org_name', 'contract_id': 'contracts'})
    dim_org['contract_count'] = dim_org['contracts'].apply(len)
    dim_org = dim_org[['org_id', 'parent_org_name', 'contract_count', 'enrollment', 'contracts']]
    dim_org.to_parquet(LOCAL_PROCESSED / 'dim_organization.parquet', index=False)
    print(f"  dim_organization: {len(dim_org)} orgs")

    # dim_geography
    geo_df = latest_fact[latest_fact['fips_code'].notna() & (latest_fact['fips_code'] != '')].copy()
    dim_geo = geo_df.groupby('fips_code').agg({
        'state': 'first',
        'county': 'first'
    }).reset_index()
    dim_geo.to_parquet(LOCAL_PROCESSED / 'dim_geography.parquet', index=False)
    print(f"  dim_geography: {len(dim_geo)} counties")

    print("\n" + "=" * 60)
    print("ETL COMPLETE - Data ready for analysis!")
    print("=" * 60)


if __name__ == '__main__':
    main()
