#!/usr/bin/env python3
"""
MA Data Platform - ETL to S3

All processing outputs go directly to S3, nothing stored locally.
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import zipfile
import tempfile
import os
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str):
    """Upload DataFrame as Parquet directly to S3."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())


def download_and_extract_zip(s3_key: str) -> dict:
    """Download ZIP from S3 and extract to temp directory."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    zip_bytes = BytesIO(response['Body'].read())

    temp_dir = tempfile.mkdtemp()
    with zipfile.ZipFile(zip_bytes, 'r') as zf:
        zf.extractall(temp_dir)

    files = {}
    for root, dirs, filenames in os.walk(temp_dir):
        for f in filenames:
            if f.endswith(('.csv', '.xlsx', '.xls')):
                files[f] = os.path.join(root, f)

    return files, temp_dir


def process_cpsc_to_s3(year: int, month: int) -> dict:
    """Process CPSC month and upload directly to S3."""
    s3_key = f"raw/enrollment/cpsc/{year}-{month:02d}/cpsc_enrollment_{year}_{month:02d}.zip"

    try:
        files, temp_dir = download_and_extract_zip(s3_key)
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

    contract_df = enrollment_df = None

    for filename, filepath in files.items():
        try:
            if 'Contract_Info' in filename:
                contract_df = pd.read_csv(filepath, encoding='latin-1')
            elif 'Enrollment_Info' in filename:
                enrollment_df = pd.read_csv(filepath, encoding='latin-1')
        except:
            pass

    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)

    if contract_df is None or enrollment_df is None:
        return {'status': 'missing_files'}

    enrollment_df['year'] = year
    enrollment_df['month'] = month
    enrollment_df['Enrollment'] = pd.to_numeric(
        enrollment_df['Enrollment'].replace('*', None), errors='coerce'
    )

    fact = enrollment_df.merge(
        contract_df[['Contract ID', 'Plan ID', 'Parent Organization', 'Plan Type', 'SNP Plan']],
        left_on=['Contract Number', 'Plan ID'],
        right_on=['Contract ID', 'Plan ID'],
        how='left'
    )

    fact = fact.rename(columns={
        'Contract Number': 'contract_id', 'Plan ID': 'plan_id',
        'FIPS State County Code': 'fips_code', 'Enrollment': 'enrollment',
        'Parent Organization': 'parent_org', 'Plan Type': 'plan_type',
        'SNP Plan': 'is_snp', 'State': 'state', 'County': 'county'
    })

    fact['fips_code'] = fact['fips_code'].astype(str).replace('nan', '')
    fact = fact[['year', 'month', 'contract_id', 'plan_id', 'fips_code', 'state', 'county',
                 'enrollment', 'parent_org', 'plan_type', 'is_snp']]

    # Upload to S3
    s3_output = f"processed/fact_enrollment/{year}/{month:02d}/data.parquet"
    upload_parquet_to_s3(fact, s3_output)

    return {'status': 'success', 'rows': len(fact), 's3_key': s3_output}


def process_stars_to_s3(year: int) -> dict:
    """Process Stars data and upload to S3."""
    # Handle different naming conventions
    if year >= 2024:
        s3_keys = [f"raw/stars/{year}_ratings.zip", f"raw/stars/{year}_display.zip"]
    elif year >= 2010:
        s3_keys = [f"raw/stars/{year}_combined.zip"]
    else:
        s3_keys = [f"raw/stars/{year}_ratings.zip"]

    all_results = {}
    for s3_key in s3_keys:
        try:
            files, temp_dir = download_and_extract_zip(s3_key)
        except Exception as e:
            continue

        for filename, filepath in files.items():
            if filepath.endswith('.csv'):
                try:
                    # Try different read strategies
                    try:
                        df = pd.read_csv(filepath, encoding='utf-8-sig', skiprows=1)
                    except:
                        df = pd.read_csv(filepath, encoding='latin-1', skiprows=1)

                    df['rating_year'] = year

                    fname_lower = filename.lower()
                    if 'summary' in fname_lower:
                        table_name = 'summary'
                    elif 'measure' in fname_lower and 'star' in fname_lower:
                        table_name = 'measure_stars'
                    elif 'measure' in fname_lower and 'data' in fname_lower:
                        table_name = 'measure_data'
                    elif 'domain' in fname_lower:
                        table_name = 'domain'
                    elif 'cut' in fname_lower and 'part_c' in fname_lower:
                        table_name = 'cut_points_c'
                    elif 'cut' in fname_lower and 'part_d' in fname_lower:
                        table_name = 'cut_points_d'
                    elif 'cai' in fname_lower:
                        table_name = 'cai'
                    elif 'master' in fname_lower or 'report_card' in fname_lower:
                        table_name = 'master'
                    else:
                        # Use filename as table name
                        table_name = filename.replace('.csv', '').replace(' ', '_').lower()[:30]

                    s3_output = f"processed/stars/{year}/{table_name}.parquet"
                    upload_parquet_to_s3(df, s3_output)
                    all_results[table_name] = len(df)
                except Exception as e:
                    pass

        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    return {'status': 'success', 'tables': all_results}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', choices=['cpsc', 'stars'], required=True)
    parser.add_argument('--year', type=int)
    parser.add_argument('--month', type=int)
    parser.add_argument('--all', action='store_true')
    args = parser.parse_args()

    if args.type == 'stars':
        if args.all:
            for year in range(2007, 2027):
                print(f"Processing Stars {year}...")
                result = process_stars_to_s3(year)
                print(f"  {result}")
        elif args.year:
            result = process_stars_to_s3(args.year)
            print(result)

    elif args.type == 'cpsc':
        if args.year and args.month:
            result = process_cpsc_to_s3(args.year, args.month)
            print(result)
