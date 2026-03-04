#!/usr/bin/env python3
"""
Build Silver Layer: SNP (Special Needs Plan) Data
==================================================

Reads raw SNP files from S3, cleans/standardizes, and outputs to Silver layer.

Source: s3://ma-data123/raw/snp/{YYYY-MM}/snp_{YYYY}_{MM}.zip
Output: s3://ma-data123/silver/snp/{year}/{month}/snp.parquet

SNP Type Values:
- D-SNP: Dual Eligible (Medicare + Medicaid)
- C-SNP: Chronic Condition
- I-SNP: Institutional
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
RAW_PREFIX = "raw/snp"
SILVER_PREFIX = "silver/snp"

s3 = boto3.client('s3')


def list_raw_files() -> list:
    """List all raw SNP files in S3."""
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
        if '-' in part and len(part) == 7:
            year, month = part.split('-')
            return int(year), int(month)
    return None, None


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


def normalize_snp_type(val) -> str:
    """Normalize SNP type to standard values: D-SNP, C-SNP, I-SNP."""
    if pd.isna(val):
        return None
    val_str = str(val).strip().upper()
    if 'DUAL' in val_str or 'D-SNP' in val_str:
        return 'D-SNP'
    elif 'CHRONIC' in val_str or 'C-SNP' in val_str:
        return 'C-SNP'
    elif 'INSTITUTIONAL' in val_str or 'I-SNP' in val_str:
        return 'I-SNP'
    return val_str


def process_file(s3_key: str, dry_run: bool = False) -> dict:
    """Process a single SNP ZIP file."""
    year, month = parse_year_month_from_key(s3_key)
    if not year or not month:
        return {"status": "error", "message": f"Could not parse year/month from {s3_key}"}
    
    result = {
        "s3_key": s3_key,
        "year": year,
        "month": month,
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
                result["message"] = f"No CSV found in ZIP"
                return result
            
            with zf.open(csv_file) as f:
                df = pd.read_csv(f, dtype=str, low_memory=False)
            
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            
            rename_map = {
                'contract_number': 'contract_id',
                'contract_id': 'contract_id',
                'plan_id': 'plan_id',
                'snp_type': 'snp_type',
                'plan_type': 'snp_type',
            }
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            
            if 'contract_id' in df.columns:
                df['contract_id'] = df['contract_id'].apply(clean_contract_id)
            if 'plan_id' in df.columns:
                df['plan_id'] = df['plan_id'].apply(clean_plan_id)
            if 'snp_type' in df.columns:
                df['snp_type'] = df['snp_type'].apply(normalize_snp_type)
            
            df['year'] = year
            df['month'] = month
            df['_source_file'] = csv_file
            df['_source_row'] = range(len(df))
            
            result["rows"] = len(df)
            
            output_key = f"{SILVER_PREFIX}/{year}/{month:02d}/snp.parquet"
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
    parser = argparse.ArgumentParser(description="Build Silver layer from SNP raw files")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year", type=int)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    
    print("=" * 70)
    print("BUILD SILVER LAYER: SNP DATA")
    print("=" * 70)
    
    raw_files = list_raw_files()
    print(f"Found {len(raw_files)} raw files")
    
    if args.year:
        raw_files = [f for f in raw_files if f"/{args.year}-" in f]
    if args.limit:
        raw_files = raw_files[:args.limit]
    
    for i, s3_key in enumerate(raw_files, 1):
        print(f"[{i}/{len(raw_files)}] {s3_key}")
        result = process_file(s3_key, dry_run=args.dry_run)
        print(f"  -> {result['status']} ({result.get('rows', 0)} rows)")


if __name__ == "__main__":
    main()
