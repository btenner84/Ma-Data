#!/usr/bin/env python3
"""
Build Unified Stars Detail Tables:
- Cut Points by measure and year
- Measure Performance by contract
- Domain scores standardized
"""

import boto3
import pandas as pd
import numpy as np
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


def download_parquet(key):
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def list_parquet_files(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                files.append(obj['Key'])
    return files


def parse_cut_point_value(val):
    """Extract numeric value from cut point string like '< 48 %' or '>= 71 % to < 79 %'"""
    if pd.isna(val) or val == '':
        return None
    val_str = str(val)

    # Extract numbers
    numbers = re.findall(r'[\d.]+', val_str)
    if numbers:
        return float(numbers[0])
    return None


def build_unified_cut_points():
    """Build unified cut points table across all years."""
    print("\n=== BUILDING UNIFIED CUT POINTS ===")

    all_cut_points = []

    # Find all cut_points files
    files = list_parquet_files('processed/stars/')
    cut_point_files = [f for f in files if 'cut_points' in f.lower()]

    for s3_key in cut_point_files:
        try:
            parts = s3_key.split('/')
            year = int(parts[2])
            cut_type = 'part_c' if 'cut_points_c' in s3_key else 'part_d'

            df = download_parquet(s3_key)

            # Find measure columns (skip first few metadata rows)
            # The structure varies by year, so we need to handle different formats

            # Remove completely empty columns
            df = df.dropna(axis=1, how='all')

            # Find the row with star ratings (1star, 2star, etc)
            star_row = None
            for idx, row in df.iterrows():
                row_str = ' '.join(str(v).lower() for v in row.values if pd.notna(v))
                if '1star' in row_str or '1 star' in row_str or 'stars' in row_str:
                    star_row = idx
                    break

            if star_row is not None:
                # Use this row as indicator and extract cut points from subsequent rows
                header_row = df.iloc[0] if star_row > 0 else df.columns

                # Get measure names from first row
                measures = []
                for col in df.columns:
                    val = df.iloc[0][col] if star_row > 0 else col
                    if pd.notna(val) and 'C0' in str(val) or 'D0' in str(val):
                        measures.append((col, str(val)))

                # Extract cut point values for each star level
                for col, measure_id in measures:
                    for star_level in range(1, 6):
                        # Find the row for this star level
                        for idx, row in df.iterrows():
                            if idx <= star_row:
                                continue
                            star_str = str(row.iloc[0]).lower()
                            if f'{star_level}star' in star_str or f'{star_level} star' in star_str:
                                cut_value = row[col]
                                if pd.notna(cut_value):
                                    threshold = parse_cut_point_value(cut_value)
                                    all_cut_points.append({
                                        'year': year,
                                        'cut_type': cut_type,
                                        'measure_id': measure_id,
                                        'star_level': star_level,
                                        'threshold_text': str(cut_value),
                                        'threshold_value': threshold
                                    })

            print(f"  {year} {cut_type}: processed")

        except Exception as e:
            print(f"  Error {s3_key}: {e}")

    # Also load 2024 cutpoints from Excel
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key='docs/stars/cut_points/2024_cutpoints.xlsx')
        df = pd.read_excel(BytesIO(response['Body'].read()))

        for _, row in df.iterrows():
            measure_id = row.get('Measure ID*')
            if pd.notna(measure_id):
                for star_level, col in [(1, '1 Star'), (2, '2 Stars'), (3, '3 Stars'), (4, '4 Stars'), (5, '5 Stars')]:
                    if col in row.index:
                        cut_value = row[col]
                        if pd.notna(cut_value):
                            threshold = parse_cut_point_value(cut_value)
                            all_cut_points.append({
                                'year': 2024,
                                'cut_type': 'part_c' if measure_id.startswith('C') else 'part_d',
                                'measure_id': measure_id,
                                'star_level': star_level,
                                'threshold_text': str(cut_value),
                                'threshold_value': threshold
                            })
        print("  2024 Excel cutpoints: processed")
    except Exception as e:
        print(f"  Error loading 2024 Excel: {e}")

    if all_cut_points:
        unified = pd.DataFrame(all_cut_points)
        unified = unified.drop_duplicates()
        upload_parquet_to_s3(unified, 'processed/unified/cut_points_all_years.parquet')
        return unified

    return None


def build_unified_measure_stars():
    """Build unified measure stars table across all years."""
    print("\n=== BUILDING UNIFIED MEASURE STARS ===")

    all_measures = []

    files = list_parquet_files('processed/stars/')
    measure_files = [f for f in files if 'measure_stars' in f.lower() or 'measure_data' in f.lower()]

    for s3_key in measure_files:
        try:
            parts = s3_key.split('/')
            year = int(parts[2])

            df = download_parquet(s3_key)

            # Find the contract ID column
            contract_col = None
            for col in df.columns:
                if 'contract' in str(col).lower() and 'id' in str(col).lower():
                    contract_col = col
                    break
                elif col == 'CONTRACT_ID':
                    contract_col = col
                    break

            if contract_col is None:
                contract_col = df.columns[0]

            # Find measure columns (C01, C02, D01, etc.)
            measure_cols = []
            for col in df.columns:
                col_str = str(col)
                if re.match(r'^[CD]\d{2}:', col_str) or re.match(r'^[CD]\d{2}\s', col_str):
                    measure_cols.append(col)

            if not measure_cols:
                # Try to find them in header row
                for col in df.columns:
                    first_val = df.iloc[0][col] if len(df) > 0 else None
                    if pd.notna(first_val) and re.match(r'^[CD]\d{2}', str(first_val)):
                        measure_cols.append(col)

            # Skip header rows and process data
            data_start = 0
            for idx, row in df.iterrows():
                contract_val = str(row[contract_col])
                if re.match(r'^[HER]\d{4}', contract_val):
                    data_start = idx
                    break

            for idx, row in df.iloc[data_start:].iterrows():
                contract_id = str(row[contract_col])
                if not re.match(r'^[HER]\d{4}', contract_id):
                    continue

                for col in measure_cols:
                    value = row[col]
                    measure_name = str(col)

                    # Extract measure ID from column name
                    match = re.match(r'^([CD]\d{2})', measure_name)
                    if match:
                        measure_id = match.group(1)
                    else:
                        measure_id = measure_name[:10]

                    # Parse star rating
                    star_rating = None
                    if pd.notna(value):
                        val_str = str(value)
                        if val_str.isdigit() and 1 <= int(val_str) <= 5:
                            star_rating = int(val_str)
                        elif '%' in val_str:
                            # This is a percentage, not a star rating
                            star_rating = None

                    all_measures.append({
                        'year': year,
                        'contract_id': contract_id,
                        'measure_id': measure_id,
                        'measure_name': measure_name,
                        'star_rating': star_rating,
                        'raw_value': str(value) if pd.notna(value) else None
                    })

            print(f"  {year}: {len(df) - data_start} contracts")

        except Exception as e:
            print(f"  Error {s3_key}: {e}")

    if all_measures:
        unified = pd.DataFrame(all_measures)
        unified = unified.drop_duplicates()

        # Clean up
        unified = unified[unified['contract_id'].notna()]
        unified = unified[unified['star_rating'].notna() | unified['raw_value'].notna()]

        upload_parquet_to_s3(unified, 'processed/unified/measure_performance_all_years.parquet')

        # Create summary by measure
        measure_summary = unified.groupby(['year', 'measure_id']).agg({
            'star_rating': ['mean', 'count'],
            'contract_id': 'nunique'
        }).reset_index()
        measure_summary.columns = ['year', 'measure_id', 'avg_stars', 'rating_count', 'contract_count']

        upload_parquet_to_s3(measure_summary, 'processed/unified/measure_summary_by_year.parquet')

        return unified

    return None


def build_unified_domains():
    """Build unified domain scores across all years."""
    print("\n=== BUILDING UNIFIED DOMAINS ===")

    all_domains = []

    files = list_parquet_files('processed/stars/')
    domain_files = [f for f in files if '/domain.parquet' in f.lower()]

    for s3_key in domain_files:
        try:
            parts = s3_key.split('/')
            year = int(parts[2])

            df = download_parquet(s3_key)

            # Find contract ID column
            contract_col = None
            for col in df.columns:
                if 'contract' in str(col).lower():
                    contract_col = col
                    break

            if contract_col is None:
                continue

            # Find domain columns (HD1-HD5, DD1-DD4)
            domain_cols = [col for col in df.columns if re.match(r'^[HD]D\d:', str(col))]

            for idx, row in df.iterrows():
                contract_id = str(row[contract_col])
                if not re.match(r'^[HER]\d{4}', contract_id):
                    continue

                for col in domain_cols:
                    value = row[col]
                    domain_id = str(col).split(':')[0] if ':' in str(col) else str(col)[:3]

                    # Parse star rating
                    star_rating = None
                    if pd.notna(value):
                        try:
                            star_rating = float(value)
                            if star_rating < 1 or star_rating > 5:
                                star_rating = None
                        except:
                            pass

                    all_domains.append({
                        'year': year,
                        'contract_id': contract_id,
                        'domain_id': domain_id,
                        'domain_name': str(col),
                        'star_rating': star_rating
                    })

            print(f"  {year}: {len(df)} contracts")

        except Exception as e:
            print(f"  Error {s3_key}: {e}")

    if all_domains:
        unified = pd.DataFrame(all_domains)
        unified = unified.drop_duplicates()
        unified = unified[unified['star_rating'].notna()]

        upload_parquet_to_s3(unified, 'processed/unified/domain_scores_all_years.parquet')
        return unified

    return None


def build_unified_cai():
    """Build unified CAI data across all years."""
    print("\n=== BUILDING UNIFIED CAI ===")

    all_cai = []

    files = list_parquet_files('processed/stars/')
    cai_files = [f for f in files if '/cai.parquet' in f.lower()]

    for s3_key in cai_files:
        try:
            parts = s3_key.split('/')
            year = int(parts[2])

            df = download_parquet(s3_key)

            # Find contract column
            contract_col = None
            for col in df.columns:
                if 'contract' in str(col).lower():
                    contract_col = col
                    break

            if contract_col is None:
                continue

            # Find FAC columns
            fac_cols = [col for col in df.columns if 'fac' in str(col).lower()]

            for idx, row in df.iterrows():
                contract_id = str(row[contract_col])
                if not re.match(r'^[HER]\d{4}', contract_id):
                    continue

                record = {
                    'year': year,
                    'contract_id': contract_id
                }

                for col in fac_cols:
                    col_name = str(col).lower().replace(' ', '_')
                    try:
                        record[col_name] = float(row[col])
                    except:
                        record[col_name] = None

                all_cai.append(record)

            print(f"  {year}: {len(df)} contracts")

        except Exception as e:
            print(f"  Error {s3_key}: {e}")

    if all_cai:
        unified = pd.DataFrame(all_cai)
        unified = unified.drop_duplicates()
        upload_parquet_to_s3(unified, 'processed/unified/cai_all_years.parquet')
        return unified

    return None


def main():
    print("=" * 70)
    print("BUILDING UNIFIED STARS DETAIL TABLES")
    print("=" * 70)

    cut_points = build_unified_cut_points()
    measures = build_unified_measure_stars()
    domains = build_unified_domains()
    cai = build_unified_cai()

    print("\n" + "=" * 70)
    print("COMPLETE - Unified Stars Detail Tables:")
    print("  s3://ma-data123/processed/unified/cut_points_all_years.parquet")
    print("  s3://ma-data123/processed/unified/measure_performance_all_years.parquet")
    print("  s3://ma-data123/processed/unified/measure_summary_by_year.parquet")
    print("  s3://ma-data123/processed/unified/domain_scores_all_years.parquet")
    print("  s3://ma-data123/processed/unified/cai_all_years.parquet")
    print("=" * 70)


if __name__ == '__main__':
    main()
