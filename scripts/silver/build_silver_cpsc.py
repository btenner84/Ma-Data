#!/usr/bin/env python3
"""
Build Silver Layer: CPSC Enrollment Data
=========================================

Reads raw CPSC ZIP files from S3, cleans/standardizes, and outputs to Silver layer.

Source: s3://ma-data123/raw/enrollment/cpsc/{YYYY-MM}/cpsc_enrollment_{YYYY}_{MM}.zip
Output: s3://ma-data123/silver/enrollment/cpsc/{year}/{month}/enrollment.parquet
        s3://ma-data123/silver/enrollment/cpsc/{year}/{month}/contracts.parquet

Raw Files:
- CPSC_Contract_Info_{YYYY}_{MM}.csv: Contract metadata (plan_type, org_name, parent_org, etc.)
- CPSC_Enrollment_Info_{YYYY}_{MM}.csv: Enrollment by county (contract, plan, state, county, enrollment)

Cleaning:
- Normalize contract_id/plan_id (strip whitespace, pad plan_id to 3 digits)
- Handle suppressed enrollment ("*" -> NULL with is_suppressed flag)
- Standardize column names to snake_case
- Add lineage columns (_source_file, _source_row)
"""

import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
import tempfile
import os
import sys
import argparse
import warnings
warnings.filterwarnings('ignore')

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
RAW_PREFIX = "raw/enrollment/cpsc"
SILVER_PREFIX = "silver/enrollment/cpsc"

s3 = boto3.client('s3')


def list_raw_cpsc_files() -> list:
    """List all raw CPSC files in S3."""
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


def clean_enrollment(val) -> tuple:
    """
    Parse enrollment value.
    Returns (enrollment_int, is_suppressed_bool).
    "*" indicates HIPAA suppression (<10 enrollees).
    """
    if pd.isna(val):
        return None, False
    val_str = str(val).strip()
    if val_str == '*':
        return None, True
    try:
        return int(float(val_str)), False
    except (ValueError, TypeError):
        return None, False


def process_cpsc_file(s3_key: str, dry_run: bool = False) -> dict:
    """
    Process a single CPSC ZIP file.
    
    Returns dict with processing statistics.
    """
    year, month = parse_year_month_from_key(s3_key)
    if not year or not month:
        return {"status": "error", "message": f"Could not parse year/month from {s3_key}"}
    
    result = {
        "s3_key": s3_key,
        "year": year,
        "month": month,
        "status": "pending",
        "contracts_rows": 0,
        "enrollment_rows": 0,
        "suppressed_rows": 0,
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
            
            # Find the contract info and enrollment info files
            contract_file = None
            enrollment_file = None
            for f in file_list:
                if 'Contract_Info' in f and f.endswith('.csv'):
                    contract_file = f
                elif 'Enrollment_Info' in f and f.endswith('.csv'):
                    enrollment_file = f
            
            if not contract_file or not enrollment_file:
                result["status"] = "error"
                result["message"] = f"Missing required files in ZIP. Found: {file_list}"
                return result
            
            # =====================================================================
            # Process Contract Info
            # =====================================================================
            print(f"  Processing {contract_file}...")
            with zf.open(contract_file) as f:
                df_contracts = pd.read_csv(f, dtype=str, low_memory=False)
            
            # Standardize column names
            df_contracts.columns = [c.strip().lower().replace(' ', '_') for c in df_contracts.columns]
            
            # Rename to standard schema
            rename_map = {
                'contract_id': 'contract_id',
                'plan_id': 'plan_id',
                'organization_type': 'organization_type',
                'plan_type': 'plan_type',
                'offers_part_d': 'offers_part_d',
                'snp_plan': 'is_snp',
                'eghp': 'is_eghp',
                'organization_name': 'organization_name',
                'organization_marketing_name': 'marketing_name',
                'plan_name': 'plan_name',
                'parent_organization': 'parent_org',
                'contract_effective_date': 'contract_effective_date',
            }
            df_contracts = df_contracts.rename(columns=rename_map)
            
            # Clean key columns
            df_contracts['contract_id'] = df_contracts['contract_id'].apply(clean_contract_id)
            df_contracts['plan_id'] = df_contracts['plan_id'].apply(clean_plan_id)
            
            # Add metadata
            df_contracts['year'] = year
            df_contracts['month'] = month
            df_contracts['_source_file'] = contract_file
            df_contracts['_source_row'] = range(len(df_contracts))
            
            result["contracts_rows"] = len(df_contracts)
            
            # =====================================================================
            # Process Enrollment Info  
            # =====================================================================
            print(f"  Processing {enrollment_file}...")
            with zf.open(enrollment_file) as f:
                df_enrollment = pd.read_csv(f, dtype=str, low_memory=False)
            
            # Standardize column names
            df_enrollment.columns = [c.strip().lower().replace(' ', '_') for c in df_enrollment.columns]
            
            # Rename to standard schema
            enroll_rename = {
                'contract_number': 'contract_id',
                'plan_id': 'plan_id',
                'ssa_state_county_code': 'ssa_code',
                'fips_state_county_code': 'fips_code',
                'state': 'state',
                'county': 'county',
                'enrollment': 'enrollment',
            }
            df_enrollment = df_enrollment.rename(columns=enroll_rename)
            
            # Clean key columns
            df_enrollment['contract_id'] = df_enrollment['contract_id'].apply(clean_contract_id)
            df_enrollment['plan_id'] = df_enrollment['plan_id'].apply(clean_plan_id)
            
            # Parse enrollment with suppression handling
            enrollment_parsed = df_enrollment['enrollment'].apply(clean_enrollment)
            df_enrollment['enrollment'] = enrollment_parsed.apply(lambda x: x[0])
            df_enrollment['is_suppressed'] = enrollment_parsed.apply(lambda x: x[1])
            
            # Convert enrollment to int (nullable)
            df_enrollment['enrollment'] = pd.to_numeric(df_enrollment['enrollment'], errors='coerce').astype('Int64')
            
            # Clean state/county
            df_enrollment['state'] = df_enrollment['state'].str.strip()
            df_enrollment['county'] = df_enrollment['county'].str.strip()
            
            # Add metadata
            df_enrollment['year'] = year
            df_enrollment['month'] = month
            df_enrollment['_source_file'] = enrollment_file
            df_enrollment['_source_row'] = range(len(df_enrollment))
            
            result["enrollment_rows"] = len(df_enrollment)
            result["suppressed_rows"] = df_enrollment['is_suppressed'].sum()
            
            # =====================================================================
            # Upload to Silver layer
            # =====================================================================
            contracts_key = f"{SILVER_PREFIX}/{year}/{month:02d}/contracts.parquet"
            enrollment_key = f"{SILVER_PREFIX}/{year}/{month:02d}/enrollment.parquet"
            
            # Upload contracts
            print(f"  Uploading to {contracts_key}...")
            contracts_buffer = BytesIO()
            df_contracts.to_parquet(contracts_buffer, index=False, compression='snappy')
            contracts_buffer.seek(0)
            s3.put_object(Bucket=S3_BUCKET, Key=contracts_key, Body=contracts_buffer.getvalue())
            
            # Upload enrollment
            print(f"  Uploading to {enrollment_key}...")
            enrollment_buffer = BytesIO()
            df_enrollment.to_parquet(enrollment_buffer, index=False, compression='snappy')
            enrollment_buffer.seek(0)
            s3.put_object(Bucket=S3_BUCKET, Key=enrollment_key, Body=enrollment_buffer.getvalue())
            
            result["status"] = "success"
            result["contracts_key"] = contracts_key
            result["enrollment_key"] = enrollment_key
            
    except Exception as e:
        result["status"] = "error"
        result["message"] = str(e)
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Build Silver layer from CPSC raw files")
    parser.add_argument("--dry-run", action="store_true", help="List files without processing")
    parser.add_argument("--year", type=int, help="Process specific year only")
    parser.add_argument("--start-year", type=int, default=2006, help="Start year")
    parser.add_argument("--end-year", type=int, default=2026, help="End year")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    args = parser.parse_args()
    
    print("=" * 70)
    print("BUILD SILVER LAYER: CPSC ENROLLMENT")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'PROCESS'}")
    sys.stdout.flush()
    
    # List raw files
    print("\nListing raw CPSC files...")
    raw_files = list_raw_cpsc_files()
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
    total_enrollment_rows = 0
    total_suppressed = 0
    
    for i, s3_key in enumerate(raw_files, 1):
        year, month = parse_year_month_from_key(s3_key)
        print(f"\n[{i}/{len(raw_files)}] {year}-{month:02d}")
        sys.stdout.flush()
        
        result = process_cpsc_file(s3_key, dry_run=args.dry_run)
        results.append(result)
        
        if result["status"] == "success":
            success_count += 1
            total_enrollment_rows += result.get("enrollment_rows", 0)
            total_suppressed += result.get("suppressed_rows", 0)
            print(f"  ✓ {result['contracts_rows']:,} contracts, {result['enrollment_rows']:,} enrollment rows ({result['suppressed_rows']:,} suppressed)")
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
    print(f"Total enrollment rows: {total_enrollment_rows:,}")
    print(f"Total suppressed rows: {total_suppressed:,} ({total_suppressed/max(total_enrollment_rows,1)*100:.1f}%)")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
