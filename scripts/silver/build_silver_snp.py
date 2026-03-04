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

try:
    import openpyxl
except ImportError:
    pass  # Will fail gracefully if needed

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
    """Process a single SNP ZIP file (may contain CSV or XLSX)."""
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
            
            # Try CSV first, then XLSX
            csv_file = next((f for f in file_list if f.endswith('.csv')), None)
            xlsx_file = next((f for f in file_list if f.endswith('.xlsx')), None)
            
            df = None
            source_file = None
            
            if csv_file:
                source_file = csv_file
                with zf.open(csv_file) as f:
                    content = f.read()
                    for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                        try:
                            df = pd.read_csv(BytesIO(content), dtype=str, low_memory=False, encoding=encoding)
                            break
                        except UnicodeDecodeError:
                            continue
            
            elif xlsx_file:
                source_file = xlsx_file
                with zf.open(xlsx_file) as f:
                    content = BytesIO(f.read())
                    xl = pd.ExcelFile(content, engine='openpyxl')
                    
                    # Priority order: PART_17 (most detailed), then PART_02, then any with contract+snp
                    # PART_17 has full plan-level SNP data with contract info
                    best_sheet = None
                    best_rows = 0
                    
                    for sheet in xl.sheet_names:
                        sheet_df = xl.parse(sheet, dtype=str, header=0)
                        cols_lower = [str(c).lower() for c in sheet_df.columns]
                        
                        # Look for sheets with contract data
                        has_contract = any('contract' in c for c in cols_lower)
                        has_plan_data = len(sheet_df) > 10 and has_contract
                        
                        # Prefer sheets with more rows that have contract data
                        if has_plan_data and len(sheet_df) > best_rows:
                            # Check it has useful SNP-related columns
                            if any('snp' in c or 'special' in c or 'enrollment' in c for c in cols_lower):
                                best_sheet = sheet
                                best_rows = len(sheet_df)
                                df = sheet_df
                                source_file = f"{xlsx_file}!{sheet}"
                    
                    # Fallback to specific sheets if nothing found
                    if df is None:
                        for sheet in ['SNP_REPORT_PART_17', 'SNP_REPORT_PART_02']:
                            if sheet in xl.sheet_names:
                                df = xl.parse(sheet, dtype=str, header=0)
                                source_file = f"{xlsx_file}!{sheet}"
                                break
            
            if df is None:
                result["status"] = "error"
                result["message"] = f"No CSV/XLSX with SNP data found. Files: {file_list}"
                return result
            
            # Clean column names
            df.columns = [str(c).strip().lower().replace(' ', '_').replace('(', '').replace(')', '') for c in df.columns]
            
            # Rename columns to standard names
            rename_map = {}
            for col in df.columns:
                col_str = col.lower()
                if 'contract' in col_str and ('id' in col_str or 'number' in col_str or col_str == 'contract_id'):
                    rename_map[col] = 'contract_id'
                elif col_str in ['plan_id', 'planid'] or (col_str == 'plan_id'):
                    rename_map[col] = 'plan_id'
                elif ('snp' in col_str or 'special_needs' in col_str or 'special needs' in col_str.replace('_', ' ')) and 'type' in col_str:
                    rename_map[col] = 'snp_type'
                elif col_str == 'snp_type':
                    rename_map[col] = 'snp_type'
                elif 'enrollment' in col_str and 'sub' not in col_str:  # Avoid 'Sub Total Enrollment'
                    rename_map[col] = 'enrollment'
                elif col_str in ['state', 'states', 'state(s)', 'states)']:
                    rename_map[col] = 'state'
                elif 'plan_type' in col_str or col_str == 'plan_type':
                    rename_map[col] = 'plan_type'
                elif 'organization' in col_str and 'type' in col_str:
                    rename_map[col] = 'organization_type'
                elif 'plan_name' in col_str or col_str == 'plan_name':
                    rename_map[col] = 'plan_name'
            
            df = df.rename(columns=rename_map)
            
            # Clean key columns
            if 'contract_id' in df.columns:
                df['contract_id'] = df['contract_id'].apply(clean_contract_id)
                # Filter valid contracts (exclude aggregated/placeholder rows like 'Under-11')
                df = df[df['contract_id'].notna() & (df['contract_id'].str.len() > 0)]
                df = df[~df['contract_id'].str.contains('UNDER', case=False, na=False)]
            if 'plan_id' in df.columns:
                df['plan_id'] = df['plan_id'].apply(clean_plan_id)
            if 'snp_type' in df.columns:
                df['snp_type'] = df['snp_type'].apply(normalize_snp_type)
            
            # Add metadata
            df['year'] = year
            df['month'] = month
            df['_source_file'] = source_file
            df['_source_row'] = range(len(df))
            
            result["rows"] = len(df)
            
            if len(df) == 0:
                result["status"] = "error"
                result["message"] = "No valid SNP records found after cleaning"
                return result
            
            # Save to Silver layer
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
        import traceback
        result["traceback"] = traceback.format_exc()
    
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
