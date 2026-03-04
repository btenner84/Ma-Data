#!/usr/bin/env python3
"""
Build National Enrollment Table V2 - With SNP/Group Dimensions

This creates a non-suppressed enrollment table with exact totals AND
SNP/Group dimensions derived from CPSC proportions.

Strategy:
1. Get exact contract-level totals from by-contract files
2. Get SNP/Group proportions from CPSC for each contract
3. Distribute by-contract totals according to CPSC proportions

Result: Exact national totals + SNP/Group filter support
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


def load_cpsc_proportions():
    """Load CPSC data and calculate SNP/Group proportions by contract."""
    print("\n1. Loading CPSC data for proportions...")
    
    response = s3.get_object(
        Bucket=BUCKET,
        Key='processed/unified/fact_enrollment_all_years.parquet'
    )
    cpsc = pd.read_parquet(BytesIO(response['Body'].read()))
    
    print(f"   CPSC rows: {len(cpsc):,}")
    
    # Calculate proportions by contract/year/month/snp_type/group_type
    # Group and sum at contract level
    cpsc_grouped = cpsc.groupby([
        'year', 'month', 'contract_id', 'snp_type', 'group_type', 'plan_type', 'product_type'
    ]).agg({
        'enrollment': 'sum'
    }).reset_index()
    
    # Calculate total per contract/year/month
    cpsc_totals = cpsc_grouped.groupby(['year', 'month', 'contract_id']).agg({
        'enrollment': 'sum'
    }).reset_index()
    cpsc_totals.columns = ['year', 'month', 'contract_id', 'cpsc_total']
    
    # Join back to get proportions
    cpsc_proportions = cpsc_grouped.merge(cpsc_totals, on=['year', 'month', 'contract_id'])
    cpsc_proportions['proportion'] = cpsc_proportions['enrollment'] / cpsc_proportions['cpsc_total']
    
    print(f"   Proportion rows: {len(cpsc_proportions):,}")
    
    return cpsc_proportions


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
    parts = key.split('/')
    if len(parts) >= 4:
        ym = parts[3]
        if '-' in ym:
            year, month = ym.split('-')
            return int(year), int(month)
    return None, None


def process_by_contract_file(key):
    """Process a single by_contract ZIP file."""
    year, month = parse_year_month(key)
    if not year:
        return None
    
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        content = response['Body'].read()
        
        # Check if valid ZIP
        if content[:4] != b'PK\x03\x04':
            return None
            
        zf = zipfile.ZipFile(BytesIO(content))
        
        for name in zf.namelist():
            if name.endswith('.csv'):
                with zf.open(name) as f:
                    df = pd.read_csv(f, encoding='latin-1')
                    df.columns = df.columns.str.strip()
                    
                    # Convert enrollment to numeric
                    df['Enrollment'] = pd.to_numeric(df['Enrollment'], errors='coerce').fillna(0)
                    
                    # Return contract-level data
                    result = pd.DataFrame({
                        'year': year,
                        'month': month,
                        'contract_id': df['Contract Number'],
                        'parent_org': df['Parent Organization'],
                        'plan_type': df['Plan Type'],
                        'enrollment': df['Enrollment'],
                    })
                    
                    return result
    except Exception as e:
        print(f"  Error: {key}: {e}")
        return None
    
    return None


def main():
    print("=" * 70)
    print("Building National Enrollment V2 - With SNP/Group Dimensions")
    print("=" * 70)
    
    # Load CPSC proportions
    cpsc_props = load_cpsc_proportions()
    
    # Process by-contract files
    print("\n2. Processing by-contract files...")
    files = list_by_contract_files()
    print(f"   Found {len(files)} files")
    
    all_data = []
    for i, key in enumerate(files):
        if i % 50 == 0:
            print(f"   Processing {i+1}/{len(files)}...")
        
        df = process_by_contract_file(key)
        if df is not None and len(df) > 0:
            all_data.append(df)
    
    if not all_data:
        print("No data processed!")
        return
    
    # Combine by-contract data
    print(f"\n3. Combining {len(all_data)} files...")
    by_contract = pd.concat(all_data, ignore_index=True)
    print(f"   By-contract rows: {len(by_contract):,}")
    print(f"   Years: {by_contract['year'].min()} - {by_contract['year'].max()}")
    
    # Join with CPSC proportions
    print("\n4. Joining with CPSC proportions...")
    
    # Join on year, month, contract_id
    merged = by_contract.merge(
        cpsc_props[['year', 'month', 'contract_id', 'snp_type', 'group_type', 'product_type', 'proportion']],
        on=['year', 'month', 'contract_id'],
        how='left'
    )
    
    print(f"   Merged rows: {len(merged):,}")
    
    # For contracts without CPSC data, use defaults
    merged['snp_type'] = merged['snp_type'].fillna('Unknown')
    merged['group_type'] = merged['group_type'].fillna('Unknown')
    merged['product_type'] = merged['product_type'].fillna('MA')
    merged['proportion'] = merged['proportion'].fillna(1.0)
    
    # Calculate distributed enrollment
    merged['enrollment_distributed'] = merged['enrollment'] * merged['proportion']
    
    # Aggregate by dimensions
    print("\n5. Aggregating...")
    result = merged.groupby([
        'year', 'month', 'parent_org', 'plan_type', 'product_type', 'snp_type', 'group_type'
    ]).agg({
        'enrollment_distributed': 'sum',
        'contract_id': 'nunique'
    }).reset_index()
    
    result.columns = [
        'year', 'month', 'parent_org', 'plan_type', 'product_type', 
        'snp_type', 'group_type', 'enrollment', 'contract_count'
    ]
    
    print(f"   Result rows: {len(result):,}")
    
    # Validate totals
    print("\n6. Validating...")
    for year in [2024, 2025]:
        orig = by_contract[(by_contract['year'] == year) & (by_contract['month'] == 12)]['enrollment'].sum()
        new = result[(result['year'] == year) & (result['month'] == 12)]['enrollment'].sum()
        diff_pct = abs(orig - new) / orig * 100 if orig > 0 else 0
        print(f"   {year}: Original={orig:,.0f}, Distributed={new:,.0f}, Diff={diff_pct:.2f}%")
    
    # Save to S3
    output_key = 'processed/unified/fact_enrollment_national_v2.parquet'
    print(f"\n7. Saving to s3://{BUCKET}/{output_key}")
    
    table = pa.Table.from_pandas(result)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    
    s3.put_object(Bucket=BUCKET, Key=output_key, Body=buf.getvalue())
    
    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)
    
    # Show sample
    print("\nSample data (2024):")
    sample = result[(result['year'] == 2024) & (result['month'] == 12)]
    print(f"  Unique SNP types: {sample['snp_type'].unique().tolist()}")
    print(f"  Unique Group types: {sample['group_type'].unique().tolist()}")
    
    # Totals by SNP type
    print("\n  Enrollment by SNP type (2024):")
    snp_totals = sample.groupby('snp_type')['enrollment'].sum().sort_values(ascending=False)
    for snp, val in snp_totals.items():
        print(f"    {snp}: {val:,.0f}")


if __name__ == '__main__':
    main()
