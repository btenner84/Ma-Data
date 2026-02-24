#!/usr/bin/env python3
"""
Extract ALL measure-level Stars data across all years (2007-2026).
Handles different file formats and structures across years.
"""

import boto3
import pandas as pd
import numpy as np
import zipfile
import tempfile
import shutil
import os
import re
from io import BytesIO

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"  Uploaded: {s3_key} ({len(df):,} rows)")


def find_header_row(df):
    """Find the row containing column headers."""
    for idx in range(min(10, len(df))):
        row = df.iloc[idx]
        row_str = ' '.join(str(v) for v in row.values if pd.notna(v))
        if 'Contract' in row_str and ('Number' in row_str or 'ID' in row_str):
            return idx
    return 0


def extract_measures_from_df(df, year, source_file):
    """Extract measure data from a dataframe."""
    measures = []

    # Find header row
    header_idx = find_header_row(df)
    if header_idx > 0:
        # Use this row as headers
        new_headers = df.iloc[header_idx].values
        df = df.iloc[header_idx + 1:].copy()
        df.columns = new_headers

    # Find contract column
    contract_col = None
    for col in df.columns:
        col_str = str(col).lower()
        if 'contract' in col_str and ('number' in col_str or 'id' in col_str):
            contract_col = col
            break
        elif col_str == 'contract_id':
            contract_col = col
            break

    if contract_col is None:
        # Try first column
        contract_col = df.columns[0]

    # Find measure columns (look for C01, D01, etc. or measure names)
    measure_cols = []
    for col in df.columns:
        col_str = str(col)
        # Match patterns like "C01:", "D01:", or measure names with codes
        if re.match(r'^[CD]\d{2}', col_str):
            measure_cols.append(col)
        elif re.search(r'[CD]\d{2}$', col_str):  # Code at end like "Breast Cancer - C01"
            measure_cols.append(col)
        elif re.search(r'DM[CD]\d{2}', col_str):  # Display measures
            measure_cols.append(col)

    if not measure_cols:
        # Look for columns with star ratings (1-5)
        for col in df.columns:
            if col == contract_col:
                continue
            vals = df[col].dropna().head(20)
            star_vals = [v for v in vals if str(v).strip() in ['1', '2', '3', '4', '5', '1.0', '2.0', '3.0', '4.0', '5.0']]
            if len(star_vals) >= 5:
                measure_cols.append(col)

    # Extract data
    for idx, row in df.iterrows():
        contract_id = str(row[contract_col]) if pd.notna(row[contract_col]) else ''

        # Validate contract ID format
        if not re.match(r'^[HERS]\d{4}', contract_id):
            continue

        for col in measure_cols:
            value = row[col]

            # Extract measure ID from column name
            col_str = str(col)
            match = re.search(r'([CD]\d{2})', col_str)
            if match:
                measure_id = match.group(1)
            else:
                measure_id = col_str[:15]

            # Parse value
            star_rating = None
            raw_value = str(value) if pd.notna(value) else None

            if pd.notna(value):
                val_str = str(value).strip()
                # Check if it's a star rating
                if val_str in ['1', '2', '3', '4', '5']:
                    star_rating = int(val_str)
                elif val_str in ['1.0', '2.0', '3.0', '4.0', '5.0']:
                    star_rating = int(float(val_str))
                # Check for percentage
                elif '%' in val_str:
                    raw_value = val_str

            measures.append({
                'year': year,
                'contract_id': contract_id,
                'measure_id': measure_id,
                'measure_name': col_str,
                'star_rating': star_rating,
                'raw_value': raw_value,
                'source': source_file
            })

    return measures


def process_nested_zip(zip_bytes, year):
    """Process nested ZIP files from CMS data tables."""
    measures = []

    temp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(zip_bytes, 'r') as outer_zf:
            outer_zf.extractall(temp_dir)

        # Find and process all CSVs and nested ZIPs
        for root, dirs, files in os.walk(temp_dir):
            for f in files:
                filepath = os.path.join(root, f)

                if f.endswith('.zip'):
                    # Extract nested ZIP
                    try:
                        with zipfile.ZipFile(filepath, 'r') as inner_zf:
                            inner_dir = filepath.replace('.zip', '_extracted')
                            os.makedirs(inner_dir, exist_ok=True)
                            inner_zf.extractall(inner_dir)
                    except:
                        pass

        # Now process all CSVs
        for root, dirs, files in os.walk(temp_dir):
            for f in files:
                filepath = os.path.join(root, f)
                fname_lower = f.lower()

                if f.endswith('.csv') and ('measure' in fname_lower or 'star' in fname_lower or 'data' in fname_lower):
                    try:
                        # Try different encodings
                        for encoding in ['utf-8-sig', 'latin-1', 'utf-8']:
                            try:
                                df = pd.read_csv(filepath, encoding=encoding)
                                if len(df) > 10:
                                    break
                            except:
                                continue

                        if len(df) > 10:
                            file_measures = extract_measures_from_df(df, year, f)
                            measures.extend(file_measures)
                            if file_measures:
                                print(f"    {f}: {len(file_measures)} measure records")
                    except Exception as e:
                        pass

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return measures


def main():
    print("=" * 70)
    print("EXTRACTING ALL MEASURE DATA (2007-2026)")
    print("=" * 70)

    all_measures = []

    # 1. Process CMS data table ZIPs (2019-2023)
    print("\n=== PROCESSING CMS DATA TABLE ZIPS ===")

    data_table_zips = [
        ('docs/stars/data_tables/2019_star_ratings.zip', 2019),
        ('docs/stars/data_tables/2020_star_ratings.zip', 2020),
        ('docs/stars/data_tables/2021_star_ratings.zip', 2021),
        ('docs/stars/data_tables/2022_star_ratings.zip', 2022),
        ('docs/stars/data_tables/2023_star_ratings.zip', 2023),
    ]

    for s3_key, year in data_table_zips:
        try:
            print(f"\n  Processing {year}...")
            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            zip_bytes = BytesIO(response['Body'].read())

            measures = process_nested_zip(zip_bytes, year)
            all_measures.extend(measures)
            print(f"  {year}: {len(measures)} total measure records")
        except Exception as e:
            print(f"  {year}: Error - {e}")

    # 2. Process existing parquet files
    print("\n=== PROCESSING EXISTING PARQUET FILES ===")

    paginator = s3.get_paginator('list_objects_v2')

    # Find all measure/display files
    parquet_files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='processed/stars/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if any(x in key.lower() for x in ['measure', 'display']):
                parquet_files.append(key)

    for s3_key in sorted(parquet_files):
        try:
            parts = s3_key.split('/')
            year = int(parts[2])

            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))

            measures = extract_measures_from_df(df, year, s3_key.split('/')[-1])
            all_measures.extend(measures)

            if measures:
                print(f"  {year} {s3_key.split('/')[-1]}: {len(measures)} records")
        except Exception as e:
            pass

    # 3. Build unified table
    print("\n=== BUILDING UNIFIED MEASURE TABLE ===")

    if all_measures:
        unified = pd.DataFrame(all_measures)

        # Remove duplicates
        unified = unified.drop_duplicates(subset=['year', 'contract_id', 'measure_id'])

        # Filter to valid records
        unified = unified[unified['contract_id'].str.match(r'^[HERS]\d{4}', na=False)]
        unified = unified[unified['star_rating'].notna() | unified['raw_value'].notna()]

        print(f"\n  Total records: {len(unified):,}")
        print(f"  Years: {unified['year'].min()} - {unified['year'].max()}")
        print(f"  Unique contracts: {unified['contract_id'].nunique():,}")
        print(f"  Unique measures: {unified['measure_id'].nunique():,}")

        # Year distribution
        print("\n  Records by year:")
        for year in sorted(unified['year'].unique()):
            count = len(unified[unified['year'] == year])
            contracts = unified[unified['year'] == year]['contract_id'].nunique()
            print(f"    {year}: {count:,} records ({contracts} contracts)")

        upload_parquet_to_s3(unified, 'processed/unified/measure_data_complete.parquet')

        # Also create summary
        summary = unified.groupby(['year', 'measure_id']).agg({
            'star_rating': ['mean', 'count'],
            'contract_id': 'nunique'
        }).reset_index()
        summary.columns = ['year', 'measure_id', 'avg_stars', 'rating_count', 'contract_count']

        upload_parquet_to_s3(summary, 'processed/unified/measure_summary_complete.parquet')

    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    main()
