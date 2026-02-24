#!/usr/bin/env python3
"""
ETL Plan Payment Data to S3.
Extracts risk scores, rebates, payments by plan/contract.
"""

import boto3
import pandas as pd
import zipfile
import tempfile
import os
import shutil
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())


def find_header_row(df, keywords=['Contract Number', 'Contract', 'Plan ID']):
    """Find the header row in a dataframe."""
    for i, row in df.iterrows():
        row_values = [str(v) for v in row.values if pd.notna(v)]
        row_str = ' '.join(row_values)
        # Check if this looks like a header row (has multiple keywords or specific patterns)
        if 'Contract Number' in row_str or 'Contract' in row_values:
            return i
    return 0


def process_plan_payment_year(year: int) -> dict:
    """Process plan payment data for a single year."""
    s3_key = f"raw/plan_payment/{year}/plan_payment_{year}.zip"

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zip_bytes = BytesIO(response['Body'].read())
    except:
        return {'year': year, 'status': 'not_found'}

    temp_dir = tempfile.mkdtemp()
    results = {}

    try:
        with zipfile.ZipFile(zip_bytes, 'r') as zf:
            zf.extractall(temp_dir)

        # Process all Excel files
        for root, dirs, files in os.walk(temp_dir):
            for f in files:
                if f.endswith(('.xlsx', '.xls')) and not f.startswith('~'):
                    filepath = os.path.join(root, f)
                    fname_lower = f.lower()

                    try:
                        # Determine engine
                        engine = 'xlrd' if f.endswith('.xls') else 'openpyxl'

                        # Read first to find header
                        df_raw = pd.read_excel(filepath, header=None, nrows=10, engine=engine)
                        header_row = find_header_row(df_raw)

                        # Read with correct header
                        df = pd.read_excel(filepath, header=header_row, engine=engine)

                        # Add year
                        df['year'] = year

                        # Standardize column names
                        col_map = {
                            'Contract Number': 'contract_id',
                            'Contract': 'contract_id',
                            'Plan Benefit Package': 'plan_id',
                            'Plan ID': 'plan_id',
                            'Contract Name': 'contract_name',
                            'Plan Type': 'plan_type',
                            'Average Part C Risk Score': 'avg_risk_score',
                            'Average A/B PM/PM Payment': 'avg_ab_payment',
                            'Average Rebate PM/PM Payment': 'avg_rebate',
                            'Rebate': 'rebate',
                            'Benchmark': 'benchmark',
                            'Payment': 'payment'
                        }

                        for old, new in col_map.items():
                            if old in df.columns:
                                df = df.rename(columns={old: new})

                        # Determine table type
                        if 'planlevel' in fname_lower or 'plan level' in fname_lower.replace('_', ' '):
                            table_name = 'plan_level'
                        elif 'countylevel' in fname_lower or 'county level' in fname_lower.replace('_', ' '):
                            table_name = 'county_level'
                        elif 'partd' in fname_lower:
                            table_name = 'part_d'
                        elif 'reconcil' in fname_lower:
                            table_name = 'reconciliation'
                        else:
                            # Clean filename for table name
                            table_name = f.replace('.xlsx', '').replace('.xls', '').replace(' ', '_').lower()[:30]

                        # Upload
                        s3_output = f"processed/plan_payment/{year}/{table_name}.parquet"
                        upload_parquet_to_s3(df, s3_output)
                        results[table_name] = len(df)

                    except Exception as e:
                        results[f"error_{f}"] = str(e)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return {'year': year, 'status': 'success', 'tables': results}


def build_unified_risk_scores():
    """Build unified risk scores across all years."""
    print("\n=== BUILDING UNIFIED RISK SCORES ===")

    # List all plan_level files
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='processed/plan_payment/'):
        for obj in page.get('Contents', []):
            if 'plan_level' in obj['Key'].lower() and obj['Key'].endswith('.parquet'):
                files.append(obj['Key'])

    all_data = []

    for s3_key in sorted(files):
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))

            # Keep risk score columns
            keep_cols = ['year', 'contract_id', 'plan_id', 'contract_name', 'plan_type',
                         'avg_risk_score', 'avg_ab_payment', 'avg_rebate']

            available = [c for c in keep_cols if c in df.columns]
            if 'avg_risk_score' in available or 'contract_id' in available:
                df_subset = df[available].copy()

                # Convert numeric columns
                for col in ['avg_risk_score', 'avg_ab_payment', 'avg_rebate']:
                    if col in df_subset.columns:
                        df_subset[col] = pd.to_numeric(df_subset[col], errors='coerce')

                all_data.append(df_subset)
                print(f"  {s3_key.split('/')[2]}: {len(df_subset)} plans")

        except Exception as e:
            print(f"  Error: {s3_key}: {e}")

    if all_data:
        unified = pd.concat(all_data, ignore_index=True)

        # Clean contract_id
        if 'contract_id' in unified.columns:
            unified['contract_id'] = unified['contract_id'].astype(str).str.strip()

        upload_parquet_to_s3(unified, 'processed/unified/risk_scores_by_plan.parquet')
        print(f"\n  Unified risk scores: {len(unified):,} records")

        # Aggregate by contract
        if 'avg_risk_score' in unified.columns:
            by_contract = unified.groupby(['year', 'contract_id']).agg({
                'avg_risk_score': 'mean',
                'plan_id': 'nunique'
            }).reset_index()
            by_contract = by_contract.rename(columns={
                'avg_risk_score': 'contract_avg_risk_score',
                'plan_id': 'plan_count'
            })
            by_contract['contract_avg_risk_score'] = by_contract['contract_avg_risk_score'].round(4)

            upload_parquet_to_s3(by_contract, 'processed/unified/risk_scores_by_contract.parquet')
            print(f"  Risk scores by contract: {len(by_contract):,} records")

        return unified

    return None


def main():
    print("=" * 60)
    print("PLAN PAYMENT DATA ETL")
    print("=" * 60)

    # Process each year
    years = list(range(2006, 2025))
    success = 0

    for year in years:
        print(f"\nProcessing {year}...")
        result = process_plan_payment_year(year)
        if result['status'] == 'success':
            success += 1
            tables = result.get('tables', {})
            for table, rows in tables.items():
                if not table.startswith('error'):
                    print(f"  {table}: {rows} rows")

    print(f"\n{success}/{len(years)} years processed")

    # Build unified tables
    build_unified_risk_scores()

    print("\n" + "=" * 60)
    print("PLAN PAYMENT ETL COMPLETE")
    print("=" * 60)


if __name__ == '__main__':
    main()
