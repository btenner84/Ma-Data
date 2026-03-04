#!/usr/bin/env python3
"""
Build National Enrollment Table from By-Contract Files

This creates a non-suppressed enrollment table with exact totals.
Unlike CPSC data which suppresses counties with <10 enrollees,
the by-contract files have exact contract-level totals.

Columns: year, month, parent_org, plan_type, product_type, enrollment, contract_count
"""

import boto3
import zipfile
from io import BytesIO
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

s3 = boto3.client('s3')
BUCKET = 'ma-data123'

def list_by_contract_files():
    """List all by_contract ZIP files."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix='raw/enrollment/by_contract/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                files.append(obj['Key'])
    return sorted(files)

def parse_year_month(key):
    """Extract year and month from file path."""
    # Format: raw/enrollment/by_contract/2024-12/enrollment_contract_2024_12.zip
    parts = key.split('/')
    if len(parts) >= 4:
        ym = parts[3]  # 2024-12
        if '-' in ym:
            year, month = ym.split('-')
            return int(year), int(month)
    return None, None

def process_by_contract_file(key):
    """Process a single by_contract ZIP file."""
    year, month = parse_year_month(key)
    if not year:
        print(f"  Skipping {key} - couldn't parse year/month")
        return None
    
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        zf = zipfile.ZipFile(BytesIO(response['Body'].read()))
        
        for name in zf.namelist():
            if name.endswith('.csv'):
                with zf.open(name) as f:
                    df = pd.read_csv(f, encoding='latin-1')
                    
                    # Standardize column names
                    df.columns = df.columns.str.strip()
                    
                    # Convert enrollment to numeric
                    df['Enrollment'] = pd.to_numeric(df['Enrollment'], errors='coerce').fillna(0)
                    
                    # Determine product_type from MA-only vs Part D
                    if 'MAOnly' in df.columns and 'PartD' in df.columns:
                        df['MAOnly'] = pd.to_numeric(df['MAOnly'], errors='coerce').fillna(0)
                        df['PartD'] = pd.to_numeric(df['PartD'], errors='coerce').fillna(0)
                    
                    # Create result
                    result = pd.DataFrame({
                        'year': year,
                        'month': month,
                        'contract_id': df['Contract Number'],
                        'parent_org': df['Parent Organization'],
                        'plan_type': df['Plan Type'],
                        'enrollment': df['Enrollment'],
                        'ma_only': df.get('MAOnly', 0),
                        'part_d': df.get('PartD', 0),
                    })
                    
                    return result
    except Exception as e:
        print(f"  Error processing {key}: {e}")
        return None
    
    return None

def main():
    print("=" * 60)
    print("Building National Enrollment Table from By-Contract Files")
    print("=" * 60)
    
    files = list_by_contract_files()
    print(f"\nFound {len(files)} by_contract files")
    
    all_data = []
    
    for i, key in enumerate(files):
        if i % 20 == 0:
            print(f"\nProcessing {i+1}/{len(files)}...")
        
        df = process_by_contract_file(key)
        if df is not None:
            all_data.append(df)
    
    if not all_data:
        print("No data processed!")
        return
    
    # Combine all data
    print(f"\nCombining {len(all_data)} files...")
    combined = pd.concat(all_data, ignore_index=True)
    
    # Add product_type based on plan_type (PDP vs MA)
    PDP_TYPES = ['Medicare Prescription Drug Plan', 'Employer/Union Only Direct Contract PDP']
    combined['product_type'] = combined['plan_type'].apply(
        lambda x: 'PDP' if x in PDP_TYPES else 'MA'
    )
    
    # Aggregate by year, month, parent_org, plan_type, product_type
    print("Aggregating...")
    agg = combined.groupby(['year', 'month', 'parent_org', 'plan_type', 'product_type']).agg({
        'enrollment': 'sum',
        'ma_only': 'sum',
        'part_d': 'sum',
        'contract_id': 'nunique'
    }).reset_index()
    agg.columns = ['year', 'month', 'parent_org', 'plan_type', 'product_type', 'enrollment', 'ma_only', 'part_d', 'contract_count']
    
    print(f"\nResult: {len(agg):,} rows")
    print(f"Years: {agg['year'].min()} - {agg['year'].max()}")
    print(f"Total enrollment (latest): {agg[agg['year'] == agg['year'].max()]['enrollment'].sum():,.0f}")
    
    # Save to S3
    output_key = 'processed/unified/fact_enrollment_national.parquet'
    print(f"\nSaving to s3://{BUCKET}/{output_key}")
    
    table = pa.Table.from_pandas(agg)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    
    s3.put_object(Bucket=BUCKET, Key=output_key, Body=buf.getvalue())
    
    print("\nDone!")
    
    # Show comparison with CPSC
    print("\n" + "=" * 60)
    print("COMPARISON: National (exact) vs CPSC (suppressed)")
    print("=" * 60)
    
    for year in [2024, 2025, 2026]:
        year_data = agg[(agg['year'] == year) & (agg['month'] == 12 if year < 2026 else agg['month'] == agg[agg['year']==year]['month'].max())]
        if not year_data.empty:
            national_total = year_data['enrollment'].sum()
            month_used = year_data['month'].iloc[0]
            print(f"  {year} (month {month_used}): {national_total:,.0f}")

if __name__ == '__main__':
    main()
