#!/usr/bin/env python3
"""
Build Geographic Enrollment Fact Table

Creates county-level enrollment with suppression tracking.

Source: CPSC Enrollment_Info (has county-level detail)
Note: Values < 11 are suppressed as "*" for HIPAA compliance

Output: fact_enrollment_geographic/ (partitioned by year/month/state)

Grain: (contract_id, plan_id, year, month, state, county)
"""

import os
import sys
import json
import zipfile
import tempfile
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

# Configuration
S3_BUCKET = "ma-data123"
S3_PREFIX_CPSC = "raw/enrollment/cpsc"
S3_PREFIX_UNIFIED = "processed/facts/fact_enrollment_unified"
OUTPUT_PREFIX = "processed/facts/fact_enrollment_geographic"

s3 = boto3.client('s3')


def download_from_s3(s3_key: str) -> Optional[bytes]:
    """Download file from S3."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        return response['Body'].read()
    except Exception as e:
        return None


def load_cpsc_enrollment(year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Load county-level enrollment from CPSC Enrollment_Info file.

    Tracks suppressed values ("*") separately.
    """
    key = f"{S3_PREFIX_CPSC}/{year}-{month:02d}/CPSC_Enrollment_Info_{year}_{month:02d}.zip"
    data = download_from_s3(key)

    if data is None:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "cpsc.zip")
        with open(zip_path, 'wb') as f:
            f.write(data)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            files = zf.namelist()

            # Find Enrollment_Info file (not Contract_Info)
            enrollment_file = None
            for f in files:
                if 'enrollment' in f.lower() and 'info' in f.lower() and f.endswith('.csv'):
                    if 'contract' not in f.lower():
                        enrollment_file = f
                        break

            if not enrollment_file:
                # Fallback - any enrollment file
                for f in files:
                    if 'enrollment' in f.lower() and f.endswith('.csv'):
                        enrollment_file = f
                        break

            if not enrollment_file:
                return None

            zf.extract(enrollment_file, tmpdir)
            file_path = os.path.join(tmpdir, enrollment_file)

            df = pd.read_csv(file_path, dtype=str, encoding='latin-1')

    # Map columns
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'contract' in col_lower and ('number' in col_lower or col_lower.endswith('contract')):
            col_map[col] = 'contract_id'
        elif 'plan' in col_lower and 'id' in col_lower:
            col_map[col] = 'plan_id'
        elif 'state' in col_lower and 'code' not in col_lower and 'fips' not in col_lower:
            col_map[col] = 'state'
        elif 'county' in col_lower and 'code' not in col_lower and 'fips' not in col_lower:
            col_map[col] = 'county'
        elif 'fips' in col_lower and 'state' in col_lower:
            col_map[col] = 'fips_code'
        elif 'fips' in col_lower and 'county' in col_lower:
            col_map[col] = 'fips_code'
        elif 'fips' in col_lower:
            col_map[col] = 'fips_code'
        elif 'ssa' in col_lower:
            col_map[col] = 'ssa_code'
        elif col_lower == 'enrollment' or (col_lower.endswith('enrollment') and 'total' not in col_lower):
            col_map[col] = 'enrollment_raw'

    df = df.rename(columns=col_map)

    required = ['contract_id', 'enrollment_raw']
    if not all(c in df.columns for c in required):
        return None

    # Clean contract_id and plan_id
    df['contract_id'] = df['contract_id'].astype(str).str.strip()
    if 'plan_id' in df.columns:
        df['plan_id'] = df['plan_id'].astype(str).str.strip().str.zfill(3)
    else:
        df['plan_id'] = '000'

    # Track suppression
    df['is_suppressed'] = df['enrollment_raw'].astype(str).str.strip() == '*'

    # Convert enrollment (suppressed = NULL)
    df['enrollment'] = pd.to_numeric(
        df['enrollment_raw'].astype(str).str.replace(',', '').str.replace('*', ''),
        errors='coerce'
    )

    # For suppressed, estimate as 5.5 (midpoint of 1-10)
    df['enrollment_estimated'] = df.apply(
        lambda x: 5.5 if x['is_suppressed'] else x['enrollment'],
        axis=1
    )

    # Clean geography
    if 'state' in df.columns:
        df['state'] = df['state'].astype(str).str.strip()
    else:
        df['state'] = 'Unknown'

    if 'county' in df.columns:
        df['county'] = df['county'].astype(str).str.strip()
    else:
        df['county'] = 'Unknown'

    if 'fips_code' in df.columns:
        df['fips_code'] = df['fips_code'].astype(str).str.strip().str.zfill(5)
    else:
        df['fips_code'] = None

    return df


def load_unified_dimensions(year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Load dimensions from unified fact table to join to geographic data.
    """
    s3_key = f"{S3_PREFIX_UNIFIED}/year={year}/month={month:02d}/data.parquet"

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        return df[[
            'contract_id', 'plan_id',
            'parent_org', 'plan_type', 'plan_type_simplified',
            'product_type', 'group_type', 'snp_type'
        ]].drop_duplicates()
    except Exception as e:
        return None


def process_month(year: int, month: int) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Process a single month of geographic enrollment data.

    Returns dict of DataFrames keyed by state for partitioning.
    """
    print(f"  Processing {year}-{month:02d}...")

    # Load CPSC enrollment
    cpsc = load_cpsc_enrollment(year, month)
    if cpsc is None:
        print(f"    [SKIP] No CPSC data for {year}-{month:02d}")
        return None

    suppressed_count = cpsc['is_suppressed'].sum()
    total_count = len(cpsc)
    suppression_pct = (suppressed_count / total_count * 100) if total_count > 0 else 0

    print(f"    CPSC records: {total_count:,}")
    print(f"    Suppressed: {suppressed_count:,} ({suppression_pct:.1f}%)")

    # Load unified dimensions
    dims = load_unified_dimensions(year, month)
    if dims is not None:
        print(f"    Unified dimensions: {len(dims):,} records")
        cpsc = cpsc.merge(dims, on=['contract_id', 'plan_id'], how='left')
    else:
        print(f"    [WARN] No unified dimensions for {year}-{month:02d}")
        cpsc['parent_org'] = None
        cpsc['plan_type'] = None
        cpsc['plan_type_simplified'] = None
        cpsc['product_type'] = None
        cpsc['group_type'] = None
        cpsc['snp_type'] = None

    # Add time columns
    cpsc['year'] = year
    cpsc['month'] = month

    # Select final columns
    final_cols = [
        'contract_id', 'plan_id', 'year', 'month',
        'state', 'county', 'fips_code',
        'parent_org', 'plan_type', 'plan_type_simplified',
        'product_type', 'group_type', 'snp_type',
        'enrollment', 'enrollment_estimated', 'is_suppressed'
    ]

    for col in final_cols:
        if col not in cpsc.columns:
            cpsc[col] = None

    cpsc = cpsc[final_cols]

    # Partition by state
    states = cpsc['state'].unique()
    result = {}
    for state in states:
        if pd.notna(state) and state != 'Unknown':
            state_df = cpsc[cpsc['state'] == state]
            if len(state_df) > 0:
                result[state] = state_df

    # Unknown/null state
    unknown = cpsc[cpsc['state'].isna() | (cpsc['state'] == 'Unknown')]
    if len(unknown) > 0:
        result['_UNKNOWN'] = unknown

    return result


def upload_parquet(df: pd.DataFrame, year: int, month: int, state: str):
    """Upload DataFrame as partitioned Parquet to S3."""
    state_safe = state.replace(' ', '_').replace('/', '_')
    s3_key = f"{OUTPUT_PREFIX}/year={year}/month={month:02d}/state={state_safe}/data.parquet"

    table = pa.Table.from_pandas(df)

    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        pq.write_table(table, f.name, compression='snappy')
        s3.upload_file(f.name, S3_BUCKET, s3_key)


def main():
    print("=" * 70)
    print("BUILD GEOGRAPHIC ENROLLMENT FACT TABLE")
    print("=" * 70)
    print(f"Started: {datetime.now()}")

    # Get current date to avoid processing future months
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month

    total_records = 0
    total_suppressed = 0
    months_processed = 0
    errors = []

    for year in range(2013, current_year + 1):  # CPSC starts 2013
        print(f"\n=== Year {year} ===")

        for month in range(1, 13):
            # Skip future months dynamically
            if year == current_year and month > current_month:
                continue

            try:
                result = process_month(year, month)

                if result:
                    for state, df in result.items():
                        upload_parquet(df, year, month, state)
                        total_records += len(df)
                        total_suppressed += df['is_suppressed'].sum()

                    months_processed += 1
                    print(f"    Uploaded {len(result)} state partitions")
            except Exception as e:
                print(f"    [ERROR] {year}-{month:02d}: {e}")
                errors.append({'year': year, 'month': month, 'error': str(e)})

    # Summary
    suppression_pct = (total_suppressed / total_records * 100) if total_records > 0 else 0

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Months processed: {months_processed}")
    print(f"Total records: {total_records:,}")
    print(f"Total suppressed: {total_suppressed:,} ({suppression_pct:.1f}%)")
    if errors:
        print(f"Errors: {len(errors)}")
    print(f"Finished: {datetime.now()}")

    return len(errors) == 0


if __name__ == '__main__':
    main()
