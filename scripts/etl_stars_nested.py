#!/usr/bin/env python3
"""
Process Stars 2019-2023 nested ZIPs to S3.
These years have ZIP files containing other ZIP files.
"""

import boto3
import pandas as pd
import zipfile
import tempfile
import os
import shutil
from io import BytesIO

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())


def extract_nested_zips(zip_path: str, output_dir: str):
    """Extract ZIP and any nested ZIPs."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(output_dir)

    # Extract any nested zips
    for root, dirs, files in os.walk(output_dir):
        for f in files:
            if f.endswith('.zip'):
                nested_path = os.path.join(root, f)
                nested_dir = os.path.join(root, f.replace('.zip', ''))
                os.makedirs(nested_dir, exist_ok=True)
                try:
                    with zipfile.ZipFile(nested_path, 'r') as nzf:
                        nzf.extractall(nested_dir)
                except:
                    pass


def process_stars_year(year: int) -> dict:
    """Process a Stars year with nested ZIPs."""
    s3_key = f"raw/stars/{year}_combined.zip"

    print(f"Processing Stars {year}...")

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zip_bytes = response['Body'].read()
    except Exception as e:
        return {'year': year, 'status': 'error', 'error': str(e)}

    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, 'data.zip')

    with open(zip_path, 'wb') as f:
        f.write(zip_bytes)

    extract_nested_zips(zip_path, temp_dir)

    results = {}

    # Find all CSV files
    for root, dirs, files in os.walk(temp_dir):
        for f in files:
            if f.endswith('.csv'):
                filepath = os.path.join(root, f)
                fname_lower = f.lower()

                try:
                    # Try different encodings
                    try:
                        df = pd.read_csv(filepath, encoding='utf-8-sig', skiprows=1)
                    except:
                        try:
                            df = pd.read_csv(filepath, encoding='latin-1', skiprows=1)
                        except:
                            df = pd.read_csv(filepath, encoding='utf-8', skiprows=1)

                    df['rating_year'] = year

                    # Determine table name
                    if 'summary' in fname_lower:
                        table_name = 'summary'
                    elif 'measure' in fname_lower and 'star' in fname_lower:
                        table_name = 'measure_stars'
                    elif 'measure' in fname_lower and 'data' in fname_lower:
                        table_name = 'measure_data'
                    elif 'domain' in fname_lower:
                        table_name = 'domain'
                    elif 'cut' in fname_lower and 'part_c' in fname_lower.replace(' ', '_'):
                        table_name = 'cut_points_c'
                    elif 'cut' in fname_lower and 'part_d' in fname_lower.replace(' ', '_'):
                        table_name = 'cut_points_d'
                    elif 'cai' in fname_lower:
                        table_name = 'cai'
                    elif 'master' in fname_lower:
                        table_name = 'master'
                    elif 'display' in fname_lower:
                        table_name = 'display_measures'
                    else:
                        # Clean filename for table name
                        table_name = f.replace('.csv', '').replace(' ', '_').lower()[:30]

                    s3_output = f"processed/stars/{year}/{table_name}.parquet"
                    upload_parquet_to_s3(df, s3_output)
                    results[table_name] = len(df)
                    print(f"  {table_name}: {len(df)} rows")

                except Exception as e:
                    print(f"  Error reading {f}: {e}")

    shutil.rmtree(temp_dir, ignore_errors=True)

    return {'year': year, 'status': 'success', 'tables': results}


def main():
    print("=" * 60)
    print("STARS 2019-2023 NESTED ZIP ETL")
    print("=" * 60)

    for year in [2019, 2020, 2021, 2022, 2023]:
        result = process_stars_year(year)
        print(f"  Result: {len(result.get('tables', {}))} tables")
        print()


if __name__ == '__main__':
    main()
