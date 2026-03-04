#!/usr/bin/env python3
"""
Build SNP Lookup for ALL Years (2007-2026)

Processes all 219 months of raw SNP data to create complete snp_lookup table.
This fixes the gap where only 2023-2026 was processed.

Output: processed/unified/snp_lookup_complete.parquet
"""

import boto3
import pandas as pd
import zipfile
import tempfile
import os
from io import BytesIO
from datetime import datetime
import sys

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

def list_snp_files():
    """List all raw SNP files."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='raw/snp/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                files.append(obj['Key'])
    return sorted(files)

def extract_year_month(key):
    """Extract year and month from S3 key like raw/snp/2023-01/file.zip"""
    parts = key.split('/')
    for part in parts:
        if '-' in part and len(part) == 7:  # Format: YYYY-MM
            try:
                year, month = part.split('-')
                return int(year), int(month)
            except:
                pass
    return None, None

def process_snp_file(s3_key):
    """Process a single SNP file and extract contract/plan/snp_type mapping."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        data = resp['Body'].read()
        
        year, month = extract_year_month(s3_key)
        if not year:
            return pd.DataFrame()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, 'snp.zip')
            with open(zip_path, 'wb') as f:
                f.write(data)
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    # Extract all files
                    zf.extractall(tmpdir)
                    
                    # Find data file (may be in subdirectory)
                    import glob
                    data_files = glob.glob(os.path.join(tmpdir, '**/*.xls'), recursive=True)
                    data_files += glob.glob(os.path.join(tmpdir, '**/*.xlsx'), recursive=True)
                    data_files += glob.glob(os.path.join(tmpdir, '**/*.csv'), recursive=True)
                    # Exclude temp files
                    data_files = [f for f in data_files if not os.path.basename(f).startswith(('~', '.'))]
                    
                    if not data_files:
                        return pd.DataFrame()
                    
                    file_path = data_files[0]
            except zipfile.BadZipFile:
                return pd.DataFrame()
        
            # Read file - SNP files have headers that need to be found
            try:
                if file_path.endswith('.csv'):
                    df_raw = pd.read_csv(file_path, dtype=str, encoding='latin-1', header=None)
                else:
                    df_raw = pd.read_excel(file_path, dtype=str, header=None, engine='xlrd')
            except Exception:
                try:
                    df_raw = pd.read_excel(file_path, dtype=str, header=None, engine='openpyxl')
                except:
                    return pd.DataFrame()
            
            # Find header row - look for "Contract Number" as first cell
            header_row = None
            for i in range(min(50, len(df_raw))):
                first_val = str(df_raw.iloc[i, 0]).strip() if pd.notna(df_raw.iloc[i, 0]) else ''
                # Must be exactly "Contract Number" or similar header pattern
                if first_val.lower() in ['contract number', 'contract_number', 'contractnumber', 'contract']:
                    header_row = i
                    break
                # Or check if row looks like a header (multiple column names)
                if 'contract' in first_val.lower() and len(first_val) < 30:
                    row_vals = [str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v)]
                    # Header should have Plan ID somewhere
                    if any('plan' in str(v).lower() and 'id' in str(v).lower() for v in row_vals):
                        header_row = i
                        break
            
            if header_row is None:
                return pd.DataFrame()
            
            # Re-read with proper header
            df = df_raw.iloc[header_row:].copy()
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)
        
        # Find relevant columns
        col_map = {}
        for col in df.columns:
            if pd.isna(col):
                continue
            col_lower = str(col).lower().strip()
            if 'contract' in col_lower and 'number' in col_lower:
                col_map[col] = 'contract_id'
            elif col_lower == 'plan id' or (col_lower.startswith('plan') and 'id' in col_lower and 'segment' not in col_lower):
                col_map[col] = 'plan_id'
            elif 'special needs plan type' in col_lower or 'snp type' in col_lower:
                col_map[col] = 'snp_type'
        
        df = df.rename(columns=col_map)
        
        if 'contract_id' not in df.columns:
            return pd.DataFrame()
        
        # Filter out non-data rows
        df = df[df['contract_id'].notna()]
        df = df[df['contract_id'].astype(str).str.match(r'^[HSR]\d+$', na=False)]
        
        # Normalize
        df['contract_id'] = df['contract_id'].astype(str).str.strip()
        if 'plan_id' in df.columns:
            df['plan_id'] = df['plan_id'].astype(str).str.strip().str.zfill(3)
        else:
            df['plan_id'] = '000'
        
        # Normalize SNP type
        if 'snp_type' in df.columns:
            df['snp_type'] = df['snp_type'].astype(str).str.upper().str.strip()
            df['snp_type'] = df['snp_type'].apply(lambda x:
                'D-SNP' if any(s in x for s in ['DUAL', 'D-SNP', 'DSNP']) else
                'C-SNP' if any(s in x for s in ['CHRONIC', 'C-SNP', 'CSNP', 'DISABLING']) else
                'I-SNP' if any(s in x for s in ['INSTITUTIONAL', 'I-SNP', 'ISNP']) else
                'SNP-Other'
            )
        else:
            df['snp_type'] = 'SNP-Unknown'
        
        df['year'] = year
        df['month'] = month
        df['_source_file'] = s3_key
        
        result = df[['contract_id', 'plan_id', 'year', 'month', 'snp_type', '_source_file']].drop_duplicates()
        return result
        
    except Exception as e:
        print(f"  [ERROR] {s3_key}: {e}")
        return pd.DataFrame()


def main():
    print("=" * 70)
    print("BUILDING COMPLETE SNP LOOKUP (2007-2026)")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    # Get all SNP files
    print("\n[1/3] Finding SNP files...")
    sys.stdout.flush()
    
    files = list_snp_files()
    print(f"  Found {len(files)} SNP files")
    sys.stdout.flush()
    
    # Process each file
    print("\n[2/3] Processing files...")
    sys.stdout.flush()
    
    all_results = []
    for i, s3_key in enumerate(files):
        result = process_snp_file(s3_key)
        if not result.empty:
            all_results.append(result)
        
        if (i + 1) % 20 == 0:
            print(f"  Processed {i + 1}/{len(files)} files...")
            sys.stdout.flush()
    
    print(f"  Processed {len(files)} files total")
    sys.stdout.flush()
    
    # Combine
    print("\n[3/3] Combining and saving...")
    sys.stdout.flush()
    
    if not all_results:
        print("  [ERROR] No data extracted!")
        return
    
    df_final = pd.concat(all_results, ignore_index=True)
    
    # Keep only the latest record per contract/plan/year (use January as canonical)
    df_final = df_final.sort_values(['year', 'month']).drop_duplicates(
        subset=['contract_id', 'plan_id', 'year'], 
        keep='last'
    )
    
    print(f"  Total records: {len(df_final):,}")
    print(f"  Years covered: {df_final['year'].min()}-{df_final['year'].max()}")
    print(f"  SNP types: {df_final['snp_type'].value_counts().to_dict()}")
    sys.stdout.flush()
    
    # Save
    buffer = BytesIO()
    df_final.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    
    # Save as complete lookup
    s3.put_object(
        Bucket=S3_BUCKET,
        Key='processed/unified/snp_lookup_complete.parquet',
        Body=buffer.getvalue()
    )
    
    # Also replace the old snp_lookup
    s3.put_object(
        Bucket=S3_BUCKET,
        Key='processed/unified/snp_lookup.parquet',
        Body=buffer.getvalue()
    )
    
    size_mb = len(buffer.getvalue()) / (1024 * 1024)
    print(f"\n  Saved to s3://{S3_BUCKET}/processed/unified/snp_lookup.parquet")
    print(f"  Size: {size_mb:.2f} MB")
    
    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Finished: {datetime.now()}")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
