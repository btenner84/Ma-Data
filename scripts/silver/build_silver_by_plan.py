#!/usr/bin/env python3
"""
Build Silver Layer: By-Plan Enrollment Data (National)
=======================================================

Reads raw by_plan ZIP files from S3, cleans/standardizes, and outputs to Silver layer.

Source: s3://ma-data123/raw/enrollment/by_plan/{YYYY-MM}/enrollment_plan_{YYYY}_{MM}.zip
Output: s3://ma-data123/silver/enrollment/by_plan/{year}/{month}/enrollment.parquet

This is NATIONAL enrollment data:
- NO geographic dimension (no state/county)
- EXACT enrollment counts (no HIPAA suppression)
- Has parent_org and plan metadata built-in

Raw CSV Columns:
- Contract Number, Plan ID
- Organization Type, Plan Type
- Offers Part D
- Organization Name, Organization Marketing Name, Plan Name
- Parent Organization
- Contract Effective Date
- Enrollment

Use Cases:
- National totals without suppression
- Parent org analysis when geography doesn't matter
- Historical data back to 2007 (before CPSC existed)
"""

import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
import os
import sys
import argparse
import warnings
warnings.filterwarnings('ignore')

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
RAW_PREFIX = "raw/enrollment/by_plan"
SILVER_PREFIX = "silver/enrollment/by_plan"

s3 = boto3.client('s3')


def list_raw_files() -> list:
    """List all raw by_plan files in S3."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                files.append(obj['Key'])
    return sorted(files)


def parse_year_month_from_key(key: str) -> tuple:
    """Extract year and month from S3 key."""
    parts = key.split('/')
    for part in parts:
        if '-' in part and len(part) == 7:  # YYYY-MM format
            year, month = part.split('-')
            return int(year), int(month)
    return None, None


def clean_contract_id(val) -> str:
    """Normalize contract_id."""
    if pd.isna(val):
        return None
    return str(val).strip().upper()


def clean_plan_id(val) -> str:
    """Normalize plan_id to 3-digit string."""
    if pd.isna(val) or str(val).strip() == '':
        return '000'
    try:
        return str(int(float(str(val).strip()))).zfill(3)
    except (ValueError, TypeError):
        return str(val).strip().zfill(3)


def derive_group_type(plan_id: str) -> str:
    """
    Derive group type from plan_id.
    CMS Convention: 800-999 = Group (EGHP), 001-799 = Individual
    """
    try:
        plan_num = int(plan_id)
        return 'Group' if plan_num >= 800 else 'Individual'
    except (ValueError, TypeError):
        return 'Individual'


def process_file(s3_key: str, dry_run: bool = False) -> dict:
    """Process a single by_plan ZIP file."""
    year, month = parse_year_month_from_key(s3_key)
    if not year or not month:
        return {"status": "error", "message": f"Could not parse year/month from {s3_key}"}
    
    result = {
        "s3_key": s3_key,
        "year": year,
        "month": month,
        "status": "pending",
        "rows": 0,
        "total_enrollment": 0,
    }
    
    if dry_run:
        result["status"] = "dry_run"
        return result
    
    try:
        # Download ZIP from S3
        print(f"  Downloading {s3_key}...")
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zip_data = BytesIO(response['Body'].read())
        
        with ZipFile(zip_data, 'r') as zf:
            file_list = zf.namelist()
            
            # Find the CSV file
            csv_file = None
            for f in file_list:
                if f.endswith('.csv'):
                    csv_file = f
                    break
            
            if not csv_file:
                result["status"] = "error"
                result["message"] = f"No CSV found in ZIP. Files: {file_list}"
                return result
            
            # Read CSV with encoding fallback
            print(f"  Processing {csv_file}...")
            with zf.open(csv_file) as f:
                content = f.read()
                df = None
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(BytesIO(content), dtype=str, low_memory=False, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                if df is None:
                    raise ValueError("Could not decode CSV with any encoding")
            
            # Standardize column names
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            
            # Rename to standard schema
            rename_map = {
                'contract_number': 'contract_id',
                'plan_id': 'plan_id',
                'organization_type': 'organization_type',
                'plan_type': 'plan_type',
                'offers_part_d': 'offers_part_d',
                'organization_name': 'organization_name',
                'organization_marketing_name': 'marketing_name',
                'plan_name': 'plan_name',
                'parent_organization': 'parent_org',
                'contract_effective_date': 'contract_effective_date',
                'enrollment': 'enrollment',
            }
            df = df.rename(columns=rename_map)
            
            # Clean key columns
            df['contract_id'] = df['contract_id'].apply(clean_contract_id)
            df['plan_id'] = df['plan_id'].apply(clean_plan_id)
            
            # Parse enrollment as integer
            df['enrollment'] = pd.to_numeric(df['enrollment'], errors='coerce').astype('Int64')
            
            # Derive group_type from plan_id
            df['group_type'] = df['plan_id'].apply(derive_group_type)
            
            # Add metadata
            df['year'] = year
            df['month'] = month
            df['_source_file'] = csv_file
            df['_source_row'] = range(len(df))
            
            result["rows"] = len(df)
            result["total_enrollment"] = df['enrollment'].sum()
            
            # Upload to Silver layer
            output_key = f"{SILVER_PREFIX}/{year}/{month:02d}/enrollment.parquet"
            
            print(f"  Uploading to {output_key}...")
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, compression='snappy')
            buffer.seek(0)
            s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=buffer.getvalue())
            
            result["status"] = "success"
            result["output_key"] = output_key
            
    except Exception as e:
        result["status"] = "error"
        result["message"] = str(e)
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Build Silver layer from by_plan raw files")
    parser.add_argument("--dry-run", action="store_true", help="List files without processing")
    parser.add_argument("--year", type=int, help="Process specific year only")
    parser.add_argument("--start-year", type=int, default=2007, help="Start year")
    parser.add_argument("--end-year", type=int, default=2026, help="End year")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    args = parser.parse_args()
    
    print("=" * 70)
    print("BUILD SILVER LAYER: BY-PLAN ENROLLMENT (NATIONAL)")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'PROCESS'}")
    sys.stdout.flush()
    
    # List raw files
    print("\nListing raw by_plan files...")
    raw_files = list_raw_files()
    print(f"Found {len(raw_files)} raw files")
    
    # Filter by year
    if args.year:
        raw_files = [f for f in raw_files if f"/{args.year}-" in f]
    else:
        raw_files = [f for f in raw_files 
                     if any(f"/{y}-" in f for y in range(args.start_year, args.end_year + 1))]
    
    if args.limit:
        raw_files = raw_files[:args.limit]
    
    print(f"Processing {len(raw_files)} files")
    print("-" * 70)
    sys.stdout.flush()
    
    # Process files
    results = []
    success_count = 0
    total_rows = 0
    total_enrollment = 0
    
    for i, s3_key in enumerate(raw_files, 1):
        year, month = parse_year_month_from_key(s3_key)
        print(f"\n[{i}/{len(raw_files)}] {year}-{month:02d}")
        sys.stdout.flush()
        
        result = process_file(s3_key, dry_run=args.dry_run)
        results.append(result)
        
        if result["status"] == "success":
            success_count += 1
            total_rows += result.get("rows", 0)
            total_enrollment += result.get("total_enrollment", 0)
            print(f"  ✓ {result['rows']:,} rows, {result['total_enrollment']:,} total enrollment")
        elif result["status"] == "dry_run":
            print(f"  [DRY RUN] Would process")
        else:
            print(f"  ✗ {result['status']}: {result.get('message', 'unknown error')}")
        
        sys.stdout.flush()
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Files processed: {success_count}/{len(raw_files)}")
    print(f"Total rows: {total_rows:,}")
    print(f"Total enrollment: {total_enrollment:,}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
