#!/usr/bin/env python3
"""
Process Missing CPSC Months

Fills gaps in processed/fact_enrollment/ from raw/enrollment/cpsc/
"""

import boto3
import pandas as pd
from io import BytesIO
import zipfile
import re
from datetime import datetime
import sys

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

def get_missing_months():
    """Find months that have raw CPSC but no processed data."""
    paginator = s3.get_paginator('list_objects_v2')
    
    # Get raw CPSC months
    raw_cpsc = {}
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='raw/enrollment/cpsc/'):
        for obj in page.get('Contents', []):
            match = re.search(r'(\d{4})-(\d{2})', obj['Key'])
            if match and obj['Key'].endswith('.zip'):
                ym = f"{match.group(1)}-{match.group(2)}"
                raw_cpsc[ym] = obj['Key']
    
    # Get processed months
    processed = set()
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='processed/fact_enrollment/'):
        for obj in page.get('Contents', []):
            match = re.search(r'fact_enrollment/(\d{4})/(\d{2})/', obj['Key'])
            if match:
                processed.add(f"{match.group(1)}-{match.group(2)}")
    
    # Return missing
    missing = {ym: key for ym, key in raw_cpsc.items() if ym not in processed}
    return missing


def process_cpsc_file(raw_key: str, year: int, month: int) -> pd.DataFrame:
    """Process a raw CPSC zip file into enrollment dataframe."""
    
    print(f"    Downloading {raw_key}...")
    resp = s3.get_object(Bucket=S3_BUCKET, Key=raw_key)
    
    with zipfile.ZipFile(BytesIO(resp['Body'].read())) as zf:
        # Find the two CSV files
        contract_file = None
        enrollment_file = None
        
        for name in zf.namelist():
            if 'Contract_Info' in name and name.endswith('.csv'):
                contract_file = name
            elif 'Enrollment_Info' in name and name.endswith('.csv'):
                enrollment_file = name
        
        if not contract_file or not enrollment_file:
            print(f"    ERROR: Missing files in {raw_key}")
            print(f"    Found: {zf.namelist()}")
            return pd.DataFrame()
        
        # Read contract info (metadata)
        print(f"    Reading contract info...")
        with zf.open(contract_file) as f:
            contracts = pd.read_csv(f, encoding='latin-1')
        
        # Normalize contract columns
        contracts = contracts.rename(columns={
            'Contract ID': 'contract_id',
            'Plan ID': 'plan_id',
            'Organization Type': 'org_type',
            'Plan Type': 'plan_type',
            'Offers Part D': 'offers_part_d',
            'SNP Plan': 'is_snp',
            'Parent Organization': 'parent_org',
            'Organization Marketing Name': 'marketing_name'
        })
        
        # Read enrollment info
        print(f"    Reading enrollment info (this may take a moment)...")
        with zf.open(enrollment_file) as f:
            enrollment = pd.read_csv(f, encoding='latin-1')
        
        # Normalize enrollment columns
        enrollment = enrollment.rename(columns={
            'Contract Number': 'contract_id',
            'Plan ID': 'plan_id',
            'State': 'state',
            'County': 'county',
            'FIPS State County Code': 'fips_code',
            'Enrollment': 'enrollment'
        })
        
        # Handle suppressed enrollment (*)
        enrollment['enrollment'] = pd.to_numeric(enrollment['enrollment'], errors='coerce')
        
        # Join with contract info
        print(f"    Joining data...")
        contracts['plan_id'] = contracts['plan_id'].astype(str)
        enrollment['plan_id'] = enrollment['plan_id'].astype(str)
        
        df = enrollment.merge(
            contracts[['contract_id', 'plan_id', 'parent_org', 'plan_type', 'is_snp', 'offers_part_d', 'org_type']],
            on=['contract_id', 'plan_id'],
            how='left'
        )
        
        # Add year/month
        df['year'] = year
        df['month'] = month
        
        # Select columns
        result = df[['year', 'month', 'contract_id', 'plan_id', 'fips_code', 'state', 'county', 
                     'enrollment', 'parent_org', 'plan_type', 'is_snp']].copy()
        
        print(f"    Processed {len(result):,} rows, {result['enrollment'].sum():,.0f} total enrollment")
        return result


def save_to_s3(df: pd.DataFrame, year: int, month: int):
    """Save processed dataframe to S3."""
    key = f"processed/fact_enrollment/{year}/{month:02d}/data.parquet"
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    size_mb = len(buffer.getvalue()) / (1024 * 1024)
    print(f"    Saved to s3://{S3_BUCKET}/{key} ({size_mb:.1f} MB)")


def main():
    print("=" * 70)
    print("PROCESS MISSING CPSC MONTHS")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print()
    
    missing = get_missing_months()
    print(f"Found {len(missing)} missing months to process")
    print()
    
    if not missing:
        print("Nothing to process!")
        return
    
    success = 0
    failed = []
    
    for i, (ym, raw_key) in enumerate(sorted(missing.items()), 1):
        year, month = int(ym[:4]), int(ym[5:7])
        print(f"[{i}/{len(missing)}] Processing {ym}...")
        
        try:
            df = process_cpsc_file(raw_key, year, month)
            if not df.empty:
                save_to_s3(df, year, month)
                success += 1
                print(f"    ✓ Complete")
            else:
                failed.append(ym)
                print(f"    ✗ No data")
        except Exception as e:
            failed.append(ym)
            print(f"    ✗ Error: {e}")
        
        print()
        sys.stdout.flush()
    
    print("=" * 70)
    print(f"COMPLETE: {success}/{len(missing)} processed")
    if failed:
        print(f"Failed: {failed}")
    print(f"Finished: {datetime.now()}")


if __name__ == "__main__":
    main()
