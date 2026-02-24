#!/usr/bin/env python3
"""
MA Data Platform - ETL Processing Script

Extracts raw CMS data from S3, transforms to unified schema, loads to Parquet.

Usage:
    python etl_process.py --data-type cpsc --year 2026 --month 01
    python etl_process.py --data-type cpsc --all
    python etl_process.py --build-dims  # Build dimension tables
"""

import argparse
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import zipfile
import tempfile
import os
from io import BytesIO

# Configuration
S3_BUCKET = "ma-data123"
LOCAL_PROCESSED = Path("/Users/bentenner/rate/ma-data-platform/processed")

# S3 client
s3 = boto3.client('s3')


def download_and_extract_zip(s3_key: str) -> dict:
    """Download ZIP from S3 and extract to temp directory, return file paths."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    zip_bytes = BytesIO(response['Body'].read())

    temp_dir = tempfile.mkdtemp()
    with zipfile.ZipFile(zip_bytes, 'r') as zf:
        zf.extractall(temp_dir)

    # Find extracted files
    files = {}
    for root, dirs, filenames in os.walk(temp_dir):
        for f in filenames:
            ext = Path(f).suffix.lower()
            if ext in ['.csv', '.xlsx', '.xls']:
                files[f] = os.path.join(root, f)

    return files


def process_cpsc(year: int, month: int) -> tuple:
    """Process CPSC enrollment data for a given month."""
    s3_key = f"raw/enrollment/cpsc/{year}-{month:02d}/cpsc_enrollment_{year}_{month:02d}.zip"

    print(f"Processing CPSC {year}-{month:02d}...")

    try:
        files = download_and_extract_zip(s3_key)
    except Exception as e:
        print(f"  Error downloading: {e}")
        return None, None

    # Find contract and enrollment files
    contract_df = None
    enrollment_df = None

    for filename, filepath in files.items():
        if 'Contract_Info' in filename:
            contract_df = pd.read_csv(filepath, encoding='latin-1')
        elif 'Enrollment_Info' in filename:
            enrollment_df = pd.read_csv(filepath, encoding='latin-1')

    if contract_df is not None:
        contract_df['year'] = year
        contract_df['month'] = month

    if enrollment_df is not None:
        enrollment_df['year'] = year
        enrollment_df['month'] = month
        # Handle suppressed values
        enrollment_df['Enrollment'] = pd.to_numeric(
            enrollment_df['Enrollment'].replace('*', None), errors='coerce'
        )

    return contract_df, enrollment_df


def process_stars(year: int) -> dict:
    """Process Star Ratings data for a given year."""
    # Try different file patterns based on year
    if year >= 2024:
        s3_key = f"raw/stars/{year}_ratings.zip"
    elif year >= 2019:
        s3_key = f"raw/stars/{year}_combined.zip"
    else:
        s3_key = f"raw/stars/{year}_combined.zip"

    print(f"Processing Stars {year}...")

    try:
        files = download_and_extract_zip(s3_key)
    except Exception as e:
        print(f"  Error: {e}")
        return {}

    results = {}
    for filename, filepath in files.items():
        if filepath.endswith('.csv'):
            try:
                df = pd.read_csv(filepath, encoding='utf-8-sig', skiprows=1)
                df['rating_year'] = year

                # Categorize by file type
                fname_lower = filename.lower()
                if 'summary' in fname_lower:
                    results['summary'] = df
                elif 'measure' in fname_lower and 'star' in fname_lower:
                    results['measure_stars'] = df
                elif 'measure' in fname_lower and 'data' in fname_lower:
                    results['measure_data'] = df
                elif 'domain' in fname_lower:
                    results['domain'] = df
                elif 'cut' in fname_lower:
                    results['cut_points'] = df
                elif 'cai' in fname_lower:
                    results['cai'] = df
            except Exception as e:
                print(f"  Error reading {filename}: {e}")

    return results


def build_dim_organization(cpsc_contract_df: pd.DataFrame) -> pd.DataFrame:
    """Build organization dimension from CPSC contract data."""
    dim = cpsc_contract_df.groupby('Parent Organization').agg({
        'Contract ID': lambda x: list(x.unique()),
        'Organization Marketing Name': 'first',
        'Organization Type': lambda x: list(x.unique())
    }).reset_index()

    dim['org_id'] = range(1, len(dim) + 1)
    dim = dim.rename(columns={
        'Parent Organization': 'parent_org_name',
        'Organization Marketing Name': 'marketing_name',
        'Organization Type': 'org_types',
        'Contract ID': 'contracts'
    })

    dim['contract_count'] = dim['contracts'].apply(len)

    return dim[['org_id', 'parent_org_name', 'marketing_name', 'org_types', 'contract_count', 'contracts']]


def build_dim_contract(cpsc_contract_df: pd.DataFrame) -> pd.DataFrame:
    """Build contract dimension."""
    dim = cpsc_contract_df.groupby('Contract ID').agg({
        'Parent Organization': 'first',
        'Organization Name': 'first',
        'Organization Marketing Name': 'first',
        'Organization Type': 'first',
        'Contract Effective Date': 'first'
    }).reset_index()

    dim = dim.rename(columns={
        'Contract ID': 'contract_id',
        'Parent Organization': 'parent_org',
        'Organization Name': 'org_name',
        'Organization Marketing Name': 'marketing_name',
        'Organization Type': 'org_type',
        'Contract Effective Date': 'effective_date'
    })

    return dim


def build_dim_geography(cpsc_enrollment_df: pd.DataFrame) -> pd.DataFrame:
    """Build geography dimension from CPSC enrollment data."""
    # Filter out rows without FIPS codes
    df = cpsc_enrollment_df[cpsc_enrollment_df['FIPS State County Code'].notna()].copy()

    dim = df.groupby('FIPS State County Code').agg({
        'State': 'first',
        'County': 'first',
        'SSA State County Code': 'first'
    }).reset_index()

    dim = dim.rename(columns={
        'FIPS State County Code': 'fips_code',
        'State': 'state',
        'County': 'county',
        'SSA State County Code': 'ssa_code'
    })

    # Ensure FIPS code is string and add state/county splits
    dim['fips_code'] = dim['fips_code'].astype(str).str.zfill(5)
    dim['state_fips'] = dim['fips_code'].str[:2]
    dim['county_fips'] = dim['fips_code'].str[2:]

    return dim


def build_fact_enrollment(cpsc_enrollment_df: pd.DataFrame, cpsc_contract_df: pd.DataFrame) -> pd.DataFrame:
    """Build enrollment fact table."""
    # Merge contract info with enrollment
    fact = cpsc_enrollment_df.merge(
        cpsc_contract_df[['Contract ID', 'Plan ID', 'Parent Organization', 'Plan Type', 'SNP Plan']],
        left_on=['Contract Number', 'Plan ID'],
        right_on=['Contract ID', 'Plan ID'],
        how='left'
    )

    fact = fact.rename(columns={
        'Contract Number': 'contract_id',
        'Plan ID': 'plan_id',
        'FIPS State County Code': 'fips_code',
        'Enrollment': 'enrollment',
        'Parent Organization': 'parent_org',
        'Plan Type': 'plan_type',
        'SNP Plan': 'is_snp',
        'State': 'state',
        'County': 'county'
    })

    # Clean up FIPS code
    fact['fips_code'] = fact['fips_code'].astype(str).replace('nan', '')

    return fact[['year', 'month', 'contract_id', 'plan_id', 'fips_code', 'state', 'county',
                 'enrollment', 'parent_org', 'plan_type', 'is_snp']]


def save_to_parquet(df: pd.DataFrame, output_path: Path, partition_cols: list = None):
    """Save DataFrame to Parquet format."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if partition_cols:
        pq.write_to_dataset(
            pa.Table.from_pandas(df),
            root_path=str(output_path),
            partition_cols=partition_cols
        )
    else:
        df.to_parquet(output_path, index=False)

    print(f"  Saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='MA Data Platform ETL')
    parser.add_argument('--data-type', choices=['cpsc', 'stars', 'snp', 'enrollment'])
    parser.add_argument('--year', type=int)
    parser.add_argument('--month', type=int)
    parser.add_argument('--all', action='store_true', help='Process all available data')
    parser.add_argument('--build-dims', action='store_true', help='Build dimension tables')
    parser.add_argument('--sample', action='store_true', help='Process just one month as sample')

    args = parser.parse_args()

    LOCAL_PROCESSED.mkdir(parents=True, exist_ok=True)

    if args.sample or (args.data_type == 'cpsc' and args.year and args.month):
        # Process single CPSC month
        year = args.year or 2026
        month = args.month or 1

        contract_df, enrollment_df = process_cpsc(year, month)

        if contract_df is not None and enrollment_df is not None:
            print(f"\nContract Info: {len(contract_df)} rows")
            print(f"Enrollment Info: {len(enrollment_df)} rows")

            # Build dimensions
            dim_org = build_dim_organization(contract_df)
            dim_contract = build_dim_contract(contract_df)
            dim_geo = build_dim_geography(enrollment_df)
            fact_enrollment = build_fact_enrollment(enrollment_df, contract_df)

            # Save
            save_to_parquet(dim_org, LOCAL_PROCESSED / 'dim_organization.parquet')
            save_to_parquet(dim_contract, LOCAL_PROCESSED / 'dim_contract.parquet')
            save_to_parquet(dim_geo, LOCAL_PROCESSED / 'dim_geography.parquet')
            save_to_parquet(fact_enrollment, LOCAL_PROCESSED / f'fact_enrollment/{year}/{month:02d}/data.parquet')

            print(f"\nDimensions created:")
            print(f"  Organizations: {len(dim_org)}")
            print(f"  Contracts: {len(dim_contract)}")
            print(f"  Geographies: {len(dim_geo)}")
            print(f"  Enrollment Facts: {len(fact_enrollment)}")

    elif args.data_type == 'stars' and args.year:
        stars = process_stars(args.year)
        for table_name, df in stars.items():
            save_to_parquet(df, LOCAL_PROCESSED / f'stars/{args.year}/{table_name}.parquet')

    else:
        print("Usage examples:")
        print("  python etl_process.py --sample  # Process Jan 2026 as sample")
        print("  python etl_process.py --data-type cpsc --year 2026 --month 1")
        print("  python etl_process.py --data-type stars --year 2026")


if __name__ == '__main__':
    main()
