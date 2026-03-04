#!/usr/bin/env python3
"""
Build Silver Layer: Stars Ratings Data
======================================

Reads raw Stars ratings files from S3, cleans/standardizes, and outputs to Silver layer.

Source: s3://ma-data123/raw/stars/{YYYY}_combined.zip (or _ratings.zip, _display.zip)
Output: s3://ma-data123/silver/stars/{year}/summary.parquet
        s3://ma-data123/silver/stars/{year}/measures.parquet

Stars Data Structure:
- Summary: Overall star ratings by contract (1-5 stars)
- Measures: Individual measure scores (e.g., diabetes care, med adherence)
- Cutpoints: Thresholds for each star level by measure
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
RAW_PREFIX = "raw/stars"
SILVER_PREFIX = "silver/stars"

s3 = boto3.client('s3')


def list_raw_files() -> list:
    """List all raw stars files in S3."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.zip') and ('combined' in key or 'ratings' in key):
                files.append(key)
    return sorted(files)


def extract_year_from_key(key: str) -> int:
    """Extract year from filename like 2024_combined.zip."""
    import re
    match = re.search(r'/(\d{4})_', key)
    return int(match.group(1)) if match else None


def clean_contract_id(val) -> str:
    if pd.isna(val):
        return None
    return str(val).strip().upper()


def process_file(s3_key: str, dry_run: bool = False) -> dict:
    """Process a single stars ZIP file."""
    year = extract_year_from_key(s3_key)
    if not year:
        return {"status": "error", "message": f"Could not parse year from {s3_key}"}
    
    result = {
        "s3_key": s3_key,
        "year": year,
        "status": "pending",
        "summary_rows": 0,
        "measure_rows": 0,
    }
    
    if dry_run:
        result["status"] = "dry_run"
        return result
    
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zip_data = BytesIO(response['Body'].read())
        
        with ZipFile(zip_data, 'r') as zf:
            file_list = zf.namelist()
            
            summary_df = None
            measures_df = None
            
            for f in file_list:
                if not f.endswith('.csv'):
                    continue
                
                fname_lower = f.lower()
                with zf.open(f) as csvf:
                    df = pd.read_csv(csvf, dtype=str, low_memory=False)
                
                df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
                
                if 'contract_id' in df.columns:
                    df['contract_id'] = df['contract_id'].apply(clean_contract_id)
                
                df['year'] = year
                df['_source_file'] = f
                df['_source_row'] = range(len(df))
                
                if 'summary' in fname_lower or 'overall' in fname_lower:
                    summary_df = df
                    result["summary_rows"] = len(df)
                elif 'measure' in fname_lower or 'domain' in fname_lower:
                    if measures_df is None:
                        measures_df = df
                    else:
                        measures_df = pd.concat([measures_df, df], ignore_index=True)
                    result["measure_rows"] = len(measures_df) if measures_df is not None else 0
            
            if summary_df is not None:
                output_key = f"{SILVER_PREFIX}/{year}/summary.parquet"
                buffer = BytesIO()
                summary_df.to_parquet(buffer, index=False, compression='snappy')
                buffer.seek(0)
                s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=buffer.getvalue())
                result["summary_key"] = output_key
            
            if measures_df is not None and len(measures_df) > 0:
                output_key = f"{SILVER_PREFIX}/{year}/measures.parquet"
                buffer = BytesIO()
                measures_df.to_parquet(buffer, index=False, compression='snappy')
                buffer.seek(0)
                s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=buffer.getvalue())
                result["measures_key"] = output_key
            
            result["status"] = "success"
            
    except Exception as e:
        result["status"] = "error"
        result["message"] = str(e)
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Build Silver layer from Stars raw files")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year", type=int)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    
    print("=" * 70)
    print("BUILD SILVER LAYER: STARS RATINGS")
    print("=" * 70)
    
    raw_files = list_raw_files()
    print(f"Found {len(raw_files)} raw files")
    
    if args.year:
        raw_files = [f for f in raw_files if f"/{args.year}_" in f]
    if args.limit:
        raw_files = raw_files[:args.limit]
    
    for i, s3_key in enumerate(raw_files, 1):
        print(f"[{i}/{len(raw_files)}] {s3_key}")
        result = process_file(s3_key, dry_run=args.dry_run)
        print(f"  -> {result['status']} (summary: {result.get('summary_rows', 0)}, measures: {result.get('measure_rows', 0)})")


if __name__ == "__main__":
    main()
