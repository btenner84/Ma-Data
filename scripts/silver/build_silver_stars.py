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
            # Include combined, ratings, and display files (all contain star ratings)
            if key.endswith('.zip') and ('combined' in key or 'ratings' in key or 'display' in key):
                # Skip auxiliary files and duplicates in subfolders
                if 'cut_point' in key or 'historical' in key or 'tukey' in key:
                    continue
                # Skip the ratings/ subfolder which has duplicates
                if '/ratings/' in key:
                    continue
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


def read_csv_with_encoding(content: bytes) -> pd.DataFrame:
    """Read CSV with encoding fallback."""
    for enc in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
        try:
            df = pd.read_csv(BytesIO(content), dtype=str, low_memory=False, encoding=enc)
            return df
        except UnicodeDecodeError:
            continue
    raise ValueError("Could not decode CSV with any encoding")


def process_stars_csv(content: bytes, filename: str, year: int) -> pd.DataFrame:
    """Process a Stars CSV file, handling the CMS format with header rows."""
    df = read_csv_with_encoding(content)
    
    # CMS Stars files often have a title in row 0 with actual headers
    # Check if first column name looks like a title (contains year or "Star")
    first_col = df.columns[0]
    if str(year) in str(first_col) or 'star' in str(first_col).lower() or 'summary' in str(first_col).lower():
        # Row 0 contains actual column names
        new_columns = df.iloc[0].tolist()
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = new_columns
    
    # Clean column names
    df.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]
    
    # Find and rename key columns
    rename_map = {}
    for col in df.columns:
        col_str = str(col).lower()
        # Contract ID detection
        if 'contract' in col_str and ('number' in col_str or 'id' in col_str or col_str == 'contract'):
            rename_map[col] = 'contract_id'
        # Overall rating detection - multiple patterns for different years
        elif 'overall' in col_str and ('rating' in col_str or 'star' in col_str):
            rename_map[col] = 'overall_rating'
        elif f'{year}_overall' in col_str:
            rename_map[col] = 'overall_rating'
        # "Summary Score" pattern (older years like 2009)
        elif 'summary' in col_str and 'score' in col_str and ('health' in col_str or 'plan' in col_str or 'quality' in col_str):
            rename_map[col] = 'overall_rating'
        # "Summary Star" pattern
        elif 'summary' in col_str and 'star' in col_str:
            rename_map[col] = 'overall_rating'
        # Part C rating - handle "2013 Part C Summary Rating" pattern
        elif 'part_c' in col_str and ('summary' in col_str or 'rating' in col_str):
            rename_map[col] = 'part_c_rating'
        elif f'{year}_part_c' in col_str:
            rename_map[col] = 'part_c_rating'
        # Part D rating
        elif 'part_d' in col_str and ('summary' in col_str or 'rating' in col_str):
            rename_map[col] = 'part_d_rating'
        elif f'{year}_part_d' in col_str:
            rename_map[col] = 'part_d_rating'
        elif 'parent' in col_str and 'org' in col_str:
            rename_map[col] = 'parent_org'
        elif 'organization_type' in col_str or 'org_type' in col_str:
            rename_map[col] = 'organization_type'
        elif col_str == 'snp':
            rename_map[col] = 'is_snp'
    
    df = df.rename(columns=rename_map)
    
    # Clean contract_id
    if 'contract_id' in df.columns:
        df['contract_id'] = df['contract_id'].apply(clean_contract_id)
        # Filter out non-contract rows (headers, totals, etc.)
        df = df[df['contract_id'].notna() & (df['contract_id'].str.len() > 0)]
        df = df[df['contract_id'].str.match(r'^[A-Z][0-9]{4}$', na=False)]
    
    df['year'] = year
    df['_source_file'] = filename
    
    # Remove columns with nan names and duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    df = df[[c for c in df.columns if str(c).lower() not in ['nan', 'none', 'unnamed', '']]]
    
    return df


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
        
        summary_dfs = []
        measures_df = None
        
        with ZipFile(zip_data, 'r') as zf:
            file_list = zf.namelist()
            
            # First pass: look for summary CSV files (prefer Fall Release for final ratings)
            # Files are named like: 2017 Star Ratings Fall Release (10_2016)/2017_..._summary.csv
            # OR: 2013 Plan Ratings Fall Release (10_2012)/2013_..._Summary.csv
            summary_files = []
            for f in file_list:
                fname_lower = f.lower()
                # Look for summary files (ending in _summary.csv - case insensitive)
                if fname_lower.endswith('_summary.csv') and ('star' in fname_lower or 'rating' in fname_lower or 'plan' in fname_lower):
                    # Prefer Fall Release as it has final ratings
                    is_fall = 'fall' in fname_lower
                    # Prefer Part C (overall plan) over Part D (drug only)
                    is_part_c = 'part_c' in fname_lower or 'part c' in fname_lower
                    summary_files.append((f, is_fall, is_part_c))
            
            # Sort to prioritize: Fall Release, Part C, then alphabetically
            summary_files.sort(key=lambda x: (not x[1], not x[2], x[0]))  # Fall first, Part C second
            
            # Process the best summary file found (usually Fall Release)
            if summary_files:
                best_summary = summary_files[0][0]
                print(f"    Using summary: {best_summary}")
                with zf.open(best_summary) as csvf:
                    content = csvf.read()
                    df = process_stars_csv(content, best_summary, year)
                    if len(df) > 0:
                        summary_dfs.append(df)
            
            # Handle nested ZIPs (some combined files have nested star rating ZIPs)
            for f in file_list:
                fname_lower = f.lower()
                
                if f.endswith('.zip') and ('data table' in fname_lower or 'star' in fname_lower):
                    with zf.open(f) as nested_zip_file:
                        nested_data = BytesIO(nested_zip_file.read())
                        try:
                            with ZipFile(nested_data, 'r') as nested_zf:
                                for nf in nested_zf.namelist():
                                    if nf.endswith('.csv'):
                                        with nested_zf.open(nf) as csvf:
                                            content = csvf.read()
                                            nf_lower = nf.lower()
                                            if 'summary' in nf_lower or 'master' in nf_lower:
                                                df = process_stars_csv(content, nf, year)
                                                if len(df) > 0 and ('overall_rating' in df.columns or 'contract_id' in df.columns):
                                                    summary_dfs.append(df)
                                            elif 'measure' in nf_lower:
                                                df = process_stars_csv(content, nf, year)
                                                if len(df) > 0:
                                                    # Reset columns to avoid duplicate column names
                                                    df = df.loc[:, ~df.columns.duplicated()]
                                                    if measures_df is None:
                                                        measures_df = df
                                                    else:
                                                        # Align columns before concat
                                                        measures_df = pd.concat([measures_df, df], ignore_index=True, sort=False)
                        except Exception as e:
                            print(f"  Warning: Could not process nested ZIP {f}: {e}")
                            continue
                
                # Handle direct CSV files (not in subfolders with _summary suffix already handled)
                elif f.endswith('.csv') and not f.endswith('_summary.csv'):
                    fname_lower = f.lower()
                    with zf.open(f) as csvf:
                        content = csvf.read()
                        
                        # Determine file type from filename
                        if ('summary' in fname_lower or 'master' in fname_lower) and 'star' in fname_lower:
                            df = process_stars_csv(content, f, year)
                            if len(df) > 0:
                                summary_dfs.append(df)
                        elif 'measure' in fname_lower and 'data' in fname_lower:
                            df = process_stars_csv(content, f, year)
                            if len(df) > 0:
                                df = df.loc[:, ~df.columns.duplicated()]
                                if measures_df is None:
                                    measures_df = df
                                else:
                                    measures_df = pd.concat([measures_df, df], ignore_index=True, sort=False)
        
        # Combine all summary dataframes (prefer the one with most columns/data)
        summary_df = None
        if summary_dfs:
            # Pick the best summary: prefer one with overall_rating, most columns, most rows
            best_df = None
            best_score = 0
            for df in summary_dfs:
                score = 0
                if 'overall_rating' in df.columns:
                    score += 1000
                if 'part_c_rating' in df.columns:
                    score += 100
                if 'part_d_rating' in df.columns:
                    score += 100
                if 'parent_org' in df.columns:
                    score += 50
                score += len(df)  # More rows is better
                
                if score > best_score:
                    best_score = score
                    best_df = df
            
            summary_df = best_df
        
        # Save outputs
        if summary_df is not None and len(summary_df) > 0:
            result["summary_rows"] = len(summary_df)
            output_key = f"{SILVER_PREFIX}/{year}/summary.parquet"
            buffer = BytesIO()
            summary_df.to_parquet(buffer, index=False, compression='snappy')
            buffer.seek(0)
            s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=buffer.getvalue())
            result["summary_key"] = output_key
        
        if measures_df is not None and len(measures_df) > 0:
            result["measure_rows"] = len(measures_df)
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
        import traceback
        result["traceback"] = traceback.format_exc()
    
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
