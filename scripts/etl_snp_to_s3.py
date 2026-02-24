#!/usr/bin/env python3
"""
Process all SNP data to S3 as Parquet.
"""

import boto3
import pandas as pd
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


def list_snp_files():
    """List all SNP files in S3."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='raw/snp/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip') and obj['Size'] > 50000:
                # Extract year-month from key
                parts = obj['Key'].split('/')
                year_month = parts[2]  # e.g., "2026-01"
                files.append((year_month, obj['Key']))
    return sorted(files)


def process_snp_month(year_month: str, s3_key: str) -> dict:
    """Process a single SNP month."""
    year, month = int(year_month[:4]), int(year_month[5:7])

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zip_bytes = BytesIO(response['Body'].read())

        temp_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(zip_bytes, 'r') as zf:
            zf.extractall(temp_dir)

        # Find XLSX or XLS file
        excel_path = None
        for root, dirs, files in os.walk(temp_dir):
            for f in files:
                if f.endswith('.xlsx') or f.endswith('.xls'):
                    excel_path = os.path.join(root, f)
                    break

        if not excel_path:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
            return {'year_month': year_month, 'status': 'no_excel'}

        # Determine engine based on file extension
        engine = 'xlrd' if excel_path.endswith('.xls') else 'openpyxl'

        df = None

        if excel_path.endswith('.xls'):
            # Old XLS format - need to find header row
            raw_df = pd.read_excel(excel_path, engine='xlrd', header=None)
            header_row = None
            for i, row in raw_df.iterrows():
                if 'Contract Number' in str(row.values):
                    header_row = i
                    break

            if header_row is not None:
                df = pd.read_excel(excel_path, engine='xlrd', skiprows=header_row)
        else:
            # XLSX format - try different sheets
            xl = pd.ExcelFile(excel_path)

            # Try to find the main data sheet
            for sheet in xl.sheet_names:
                if 'PART_17' in sheet or 'Report' in sheet:
                    try:
                        test_df = pd.read_excel(xl, sheet_name=sheet)
                        if 'Contract Number' in test_df.columns:
                            df = test_df
                            break
                    except:
                        pass

            # If no named sheet found, try first sheet with data
            if df is None:
                for sheet in xl.sheet_names:
                    try:
                        test_df = pd.read_excel(xl, sheet_name=sheet)
                        if len(test_df) > 100 and len(test_df.columns) > 5:
                            df = test_df
                            break
                    except:
                        pass

        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

        if df is None:
            return {'year_month': year_month, 'status': 'no_data'}

        # Add year/month
        df['year'] = year
        df['month'] = month

        # Standardize column names
        col_map = {
            'Contract Number': 'contract_id',
            'Contract Name': 'contract_name',
            'Plan ID': 'plan_id',
            'Plan Name': 'plan_name',
            'Plan Type': 'plan_type',
            'Organization Type': 'org_type',
            'State(s)': 'states',
            'Enrollment': 'enrollment',
            'Special Needs Plan Type': 'snp_type',
            'Specialty Diseases': 'specialty_diseases',
            'Integration Status': 'integration_status',
            'Geographic Name': 'geographic_name'
        }

        for old, new in col_map.items():
            if old in df.columns:
                df = df.rename(columns={old: new})

        # Upload to S3
        s3_output = f"processed/snp/{year}/{month:02d}/data.parquet"
        upload_parquet_to_s3(df, s3_output)

        return {
            'year_month': year_month,
            'status': 'success',
            'rows': len(df)
        }

    except Exception as e:
        return {'year_month': year_month, 'status': 'error', 'error': str(e)}


def main():
    print("=" * 60)
    print("SNP DATA ETL TO S3")
    print("=" * 60)

    files = list_snp_files()
    print(f"Found {len(files)} SNP files to process")
    print(f"Date range: {files[0][0]} to {files[-1][0]}")
    print("-" * 60)

    success = 0
    errors = 0
    total_rows = 0

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(process_snp_month, ym, key): ym
            for ym, key in files
        }

        for i, future in enumerate(as_completed(futures), 1):
            ym = futures[future]
            try:
                result = future.result()
                if result['status'] == 'success':
                    success += 1
                    total_rows += result['rows']
                    print(f"[{i:3d}/{len(files)}] {ym}: {result['rows']:,} rows")
                else:
                    errors += 1
                    print(f"[{i:3d}/{len(files)}] {ym}: {result['status']}")
            except Exception as e:
                errors += 1
                print(f"[{i:3d}/{len(files)}] {ym}: ERROR - {e}")

    print("-" * 60)
    print(f"COMPLETE: {success}/{len(files)} successful, {errors} errors")
    print(f"Total rows: {total_rows:,}")


if __name__ == '__main__':
    main()
