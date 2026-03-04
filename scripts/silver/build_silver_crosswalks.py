#!/usr/bin/env python3
"""
Build Silver Layer: Contract Crosswalks
========================================

Reads raw crosswalk files from S3, cleans/standardizes, and outputs to Silver layer.

Source: s3://ma-data123/raw/crosswalks/crosswalk_{YYYY}.zip
Output: s3://ma-data123/silver/crosswalks/{year}/crosswalk.parquet

Crosswalks track contract ID changes over time, enabling entity tracking across:
- Contract renewals with new IDs
- Mergers and acquisitions
- Parent organization changes

Key columns:
- old_contract_id: Previous contract ID
- new_contract_id: New contract ID
- effective_year: Year the change took effect
"""

import boto3
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
import os
import sys
import argparse
import warnings
warnings.filterwarnings('ignore')

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
RAW_PREFIX = "raw/crosswalks"
SILVER_PREFIX = "silver/crosswalks"

s3 = boto3.client('s3')


def list_raw_files() -> list:
    """List all raw crosswalk files in S3."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.zip') and 'crosswalk' in key:
                files.append(key)
    return sorted(files)


def extract_year_from_key(key: str) -> int:
    """Extract year from filename like crosswalk_2024.zip."""
    import re
    match = re.search(r'crosswalk_(\d{4})', key)
    if match:
        return int(match.group(1))
    match = re.search(r'(\d{4})_to_(\d{4})', key)
    if match:
        return int(match.group(2))
    return None


def clean_contract_id(val) -> str:
    if pd.isna(val):
        return None
    return str(val).strip().upper()


def clean_plan_id(val) -> str:
    if pd.isna(val) or str(val).strip() == '':
        return '000'
    try:
        return str(int(float(str(val).strip()))).zfill(3)
    except (ValueError, TypeError):
        return str(val).strip().zfill(3)


def process_file(s3_key: str, dry_run: bool = False) -> dict:
    """Process a single crosswalk ZIP file."""
    year = extract_year_from_key(s3_key)
    if not year:
        return {"status": "error", "message": f"Could not parse year from {s3_key}"}
    
    result = {
        "s3_key": s3_key,
        "year": year,
        "status": "pending",
        "rows": 0,
    }
    
    if dry_run:
        result["status"] = "dry_run"
        return result
    
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zip_data = BytesIO(response['Body'].read())
        
        with ZipFile(zip_data, 'r') as zf:
            file_list = zf.namelist()
            csv_file = next((f for f in file_list if f.endswith('.csv')), None)
            
            if not csv_file:
                result["status"] = "error"
                result["message"] = f"No CSV found in ZIP. Files: {file_list}"
                return result
            
            with zf.open(csv_file) as f:
                df = pd.read_csv(f, dtype=str, low_memory=False)
            
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            
            rename_map = {}
            for col in df.columns:
                col_lower = col.lower()
                if 'old' in col_lower and 'contract' in col_lower:
                    rename_map[col] = 'old_contract_id'
                elif 'new' in col_lower and 'contract' in col_lower:
                    rename_map[col] = 'new_contract_id'
                elif 'prior' in col_lower and 'contract' in col_lower:
                    rename_map[col] = 'old_contract_id'
                elif 'current' in col_lower and 'contract' in col_lower:
                    rename_map[col] = 'new_contract_id'
                elif 'old' in col_lower and 'plan' in col_lower:
                    rename_map[col] = 'old_plan_id'
                elif 'new' in col_lower and 'plan' in col_lower:
                    rename_map[col] = 'new_plan_id'
            
            df = df.rename(columns=rename_map)
            
            if 'old_contract_id' in df.columns:
                df['old_contract_id'] = df['old_contract_id'].apply(clean_contract_id)
            if 'new_contract_id' in df.columns:
                df['new_contract_id'] = df['new_contract_id'].apply(clean_contract_id)
            if 'old_plan_id' in df.columns:
                df['old_plan_id'] = df['old_plan_id'].apply(clean_plan_id)
            if 'new_plan_id' in df.columns:
                df['new_plan_id'] = df['new_plan_id'].apply(clean_plan_id)
            
            df['effective_year'] = year
            df['_source_file'] = csv_file
            df['_source_row'] = range(len(df))
            
            result["rows"] = len(df)
            
            output_key = f"{SILVER_PREFIX}/{year}/crosswalk.parquet"
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
    parser = argparse.ArgumentParser(description="Build Silver layer from crosswalk raw files")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year", type=int)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    
    print("=" * 70)
    print("BUILD SILVER LAYER: CONTRACT CROSSWALKS")
    print("=" * 70)
    
    raw_files = list_raw_files()
    print(f"Found {len(raw_files)} raw files")
    
    if args.year:
        raw_files = [f for f in raw_files if str(args.year) in f]
    if args.limit:
        raw_files = raw_files[:args.limit]
    
    for i, s3_key in enumerate(raw_files, 1):
        print(f"[{i}/{len(raw_files)}] {s3_key}")
        result = process_file(s3_key, dry_run=args.dry_run)
        print(f"  -> {result['status']} ({result.get('rows', 0)} rows)")


if __name__ == "__main__":
    main()
