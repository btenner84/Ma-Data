#!/usr/bin/env python3
"""
Build Silver Layer: Risk Scores (Plan Payment Data)
====================================================

Reads raw plan payment files from S3, extracts risk scores, and outputs to Silver layer.

Source: s3://ma-data123/raw/plan_payment/{YYYY}/plan_payment_{YYYY}.zip
Output: s3://ma-data123/silver/risk_scores/{year}/risk_scores.parquet

Risk Scores: Indicate expected cost relative to average beneficiary (1.0 = average)
- Values > 1.0 indicate higher expected costs (sicker population)
- Values < 1.0 indicate lower expected costs (healthier population)
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
RAW_PREFIX = "raw/plan_payment"
SILVER_PREFIX = "silver/risk_scores"

s3 = boto3.client('s3')


def list_raw_files() -> list:
    """List all raw plan payment files in S3."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.zip'):
                files.append(key)
    return sorted(files)


def extract_year_from_key(key: str) -> int:
    """Extract year from filename."""
    import re
    match = re.search(r'/(\d{4})/', key)
    return int(match.group(1)) if match else None


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
    """Process a single plan payment ZIP file."""
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
                result["message"] = f"No CSV found in ZIP"
                return result
            
            with zf.open(csv_file) as f:
                df = pd.read_csv(f, dtype=str, low_memory=False)
            
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
            
            if 'contract_number' in df.columns:
                df = df.rename(columns={'contract_number': 'contract_id'})
            if 'contract_id' in df.columns:
                df['contract_id'] = df['contract_id'].apply(clean_contract_id)
            if 'plan_id' in df.columns:
                df['plan_id'] = df['plan_id'].apply(clean_plan_id)
            
            risk_cols = [c for c in df.columns if 'risk' in c.lower() or 'score' in c.lower()]
            for col in risk_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['year'] = year
            df['_source_file'] = csv_file
            df['_source_row'] = range(len(df))
            
            result["rows"] = len(df)
            
            output_key = f"{SILVER_PREFIX}/{year}/risk_scores.parquet"
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
    parser = argparse.ArgumentParser(description="Build Silver layer from plan payment raw files")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year", type=int)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    
    print("=" * 70)
    print("BUILD SILVER LAYER: RISK SCORES")
    print("=" * 70)
    
    raw_files = list_raw_files()
    print(f"Found {len(raw_files)} raw files")
    
    if args.year:
        raw_files = [f for f in raw_files if f"/{args.year}/" in f]
    if args.limit:
        raw_files = raw_files[:args.limit]
    
    for i, s3_key in enumerate(raw_files, 1):
        print(f"[{i}/{len(raw_files)}] {s3_key}")
        result = process_file(s3_key, dry_run=args.dry_run)
        print(f"  -> {result['status']} ({result.get('rows', 0)} rows)")


if __name__ == "__main__":
    main()
