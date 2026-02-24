#!/usr/bin/env python3
"""
Build Parent Organization Dimension Table

Creates a canonical parent organization reference with:
- Normalized names (handling variations, trailing whitespace)
- M&A history tracking (who acquired whom, when)
- Year-specific name mapping (what was the org called in year X)

Sources:
1. Stars Contract_Info (primary - has Parent Organization field)
2. CPSC Contract_Info (secondary)
3. Manual M&A mapping

Output: dim_parent_org.parquet
"""

import os
import sys
import uuid
import json
import zipfile
import tempfile
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

# Configuration
S3_BUCKET = "ma-data123"
S3_PREFIX_STARS = "raw/stars"
S3_PREFIX_CPSC = "raw/enrollment/cpsc"
OUTPUT_KEY = "processed/dimensions/dim_parent_org.parquet"

s3 = boto3.client('s3')


# Known M&A events and rebrands
MA_EVENTS = [
    {
        'year': 2018,
        'event_type': 'acquisition',
        'acquirer': 'CVS Health Corporation',
        'acquired': ['Aetna Inc.', 'Aetna, Inc.', 'Aetna'],
        'notes': 'CVS acquired Aetna for $69B'
    },
    {
        'year': 2020,
        'event_type': 'acquisition',
        'acquirer': 'Centene Corporation',
        'acquired': ['WellCare Health Plans, Inc.', 'WellCare Health Plans'],
        'notes': 'Centene acquired WellCare for $17.3B'
    },
    {
        'year': 2022,
        'event_type': 'acquisition',
        'acquirer': 'Centene Corporation',
        'acquired': ['Magellan Health, Inc.', 'Magellan Health'],
        'notes': 'Centene acquired Magellan for $2.2B'
    },
    {
        'year': 2022,
        'event_type': 'rebrand',
        'acquirer': 'Elevance Health, Inc.',
        'acquired': ['Anthem Inc.', 'Anthem, Inc.', 'Anthem Blue Cross'],
        'notes': 'Anthem rebranded to Elevance Health'
    },
    {
        'year': 2023,
        'event_type': 'rebrand',
        'acquirer': 'The Cigna Group',
        'acquired': ['CIGNA', 'Cigna Corporation', 'CIGNA Corporation'],
        'notes': 'CIGNA rebranded to The Cigna Group'
    },
]

# Name variations mapping to canonical names
NAME_VARIATIONS = {
    # UnitedHealth
    'UnitedHealth Group': 'UnitedHealth Group, Inc.',
    'UnitedHealthcare': 'UnitedHealth Group, Inc.',
    'United Healthcare': 'UnitedHealth Group, Inc.',
    'UnitedHealthcare Insurance Company': 'UnitedHealth Group, Inc.',
    'AARP': 'UnitedHealth Group, Inc.',  # AARP plans are UHC

    # Humana
    'Humana': 'Humana Inc.',
    'Humana Insurance Company': 'Humana Inc.',
    'Humana Health Plan': 'Humana Inc.',

    # Kaiser
    'Kaiser Foundation Health Plan': 'Kaiser Permanente',
    'Kaiser Foundation Health Plan, Inc.': 'Kaiser Permanente',
    'Kaiser Foundation Health Plan of Colorado': 'Kaiser Permanente',
    'Kaiser Foundation Health Plan of Georgia': 'Kaiser Permanente',
    'Kaiser Foundation Health Plan of the Mid-Atlantic': 'Kaiser Permanente',
    'Kaiser Foundation Health Plan of the Northwest': 'Kaiser Permanente',

    # BCBS variations
    'Blue Cross Blue Shield': 'Blue Cross Blue Shield',
    'Blue Cross and Blue Shield': 'Blue Cross Blue Shield',
    'BCBS': 'Blue Cross Blue Shield',

    # CVS/Aetna
    'Aetna Inc.': 'CVS Health Corporation',
    'Aetna, Inc.': 'CVS Health Corporation',
    'Aetna Health': 'CVS Health Corporation',
    'Aetna Life Insurance Company': 'CVS Health Corporation',

    # Elevance/Anthem
    'Anthem Inc.': 'Elevance Health, Inc.',
    'Anthem, Inc.': 'Elevance Health, Inc.',
    'Anthem Blue Cross': 'Elevance Health, Inc.',
    'Anthem Blue Cross and Blue Shield': 'Elevance Health, Inc.',

    # Cigna
    'CIGNA': 'The Cigna Group',
    'Cigna Corporation': 'The Cigna Group',
    'CIGNA Corporation': 'The Cigna Group',
    'Cigna Health and Life Insurance Company': 'The Cigna Group',

    # Centene
    'WellCare Health Plans, Inc.': 'Centene Corporation',
    'WellCare Health Plans': 'Centene Corporation',
    'Magellan Health, Inc.': 'Centene Corporation',
    'Magellan Health': 'Centene Corporation',

    # Molina
    'Molina Healthcare': 'Molina Healthcare, Inc.',
    'Molina Healthcare of California': 'Molina Healthcare, Inc.',
    'Molina Healthcare of Texas': 'Molina Healthcare, Inc.',
}


def normalize_name(name: str) -> str:
    """
    Normalize a parent organization name.

    - Strip whitespace
    - Remove extra spaces
    - Standardize punctuation
    """
    if pd.isna(name) or name is None:
        return None

    name = str(name).strip()
    name = re.sub(r'\s+', ' ', name)  # Multiple spaces to single
    name = name.replace('  ', ' ')

    return name


def get_canonical_name(name: str, year: int = None) -> str:
    """
    Get the canonical (current) name for an organization.

    Applies:
    1. Basic normalization
    2. Known variation mapping
    3. M&A-aware mapping (if year provided)
    """
    if pd.isna(name) or name is None:
        return None

    name = normalize_name(name)

    # Check direct variation mapping
    if name in NAME_VARIATIONS:
        return NAME_VARIATIONS[name]

    # Check M&A events
    for event in MA_EVENTS:
        if name in event['acquired']:
            # If year provided and is before M&A, return original name
            if year and year < event['year']:
                return name
            return event['acquirer']

    return name


def list_s3_files(prefix: str) -> List[str]:
    """List all files under an S3 prefix."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    return files


def download_from_s3(s3_key: str) -> bytes:
    """Download file from S3."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return response['Body'].read()


def extract_parent_orgs_from_stars(year: int) -> Optional[pd.DataFrame]:
    """
    Extract parent org names from Stars files.

    Stars files have 'Parent Organization' column which is the best source.
    """
    # Find stars file for this year
    files = list_s3_files(f"{S3_PREFIX_STARS}/")
    star_files = [f for f in files if str(year) in f and f.endswith('.zip')]

    if not star_files:
        return None

    # Prefer 'ratings' or 'combined' file
    target_file = None
    for f in star_files:
        if 'rating' in f.lower() or 'combined' in f.lower():
            target_file = f
            break
    if not target_file:
        target_file = star_files[0]

    try:
        data = download_from_s3(target_file)

        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "stars.zip")
            with open(zip_path, 'wb') as f:
                f.write(data)

            with zipfile.ZipFile(zip_path, 'r') as zf:
                files = zf.namelist()

                # Look for contract info or summary file
                data_file = None
                for f in files:
                    f_lower = f.lower()
                    if ('contract' in f_lower or 'summary' in f_lower) and (f.endswith('.csv') or f.endswith('.xlsx')):
                        data_file = f
                        break
                    elif f.endswith('.csv') or f.endswith('.xlsx'):
                        if not data_file:
                            data_file = f

                if not data_file:
                    return None

                zf.extract(data_file, tmpdir)
                file_path = os.path.join(tmpdir, data_file)

                if file_path.endswith('.csv'):
                    df = pd.read_csv(file_path, dtype=str, encoding='latin-1')
                else:
                    df = pd.read_excel(file_path, dtype=str)

        # Find parent org column
        parent_col = None
        contract_col = None
        for col in df.columns:
            col_lower = col.lower()
            if 'parent' in col_lower and 'org' in col_lower:
                parent_col = col
            elif 'contract' in col_lower and ('number' in col_lower or 'id' in col_lower):
                contract_col = col

        if parent_col:
            result = df[[contract_col, parent_col]].copy() if contract_col else df[[parent_col]].copy()
            result.columns = ['contract_id', 'parent_org'] if contract_col else ['parent_org']
            result['parent_org'] = result['parent_org'].apply(normalize_name)
            result['year'] = year
            result['source'] = 'stars'
            return result.drop_duplicates()

    except Exception as e:
        print(f"  [WARN] Error processing Stars {year}: {e}")

    return None


def extract_parent_orgs_from_cpsc(year: int, month: int = 1) -> Optional[pd.DataFrame]:
    """
    Extract parent org names from CPSC Contract_Info files.
    """
    key = f"{S3_PREFIX_CPSC}/{year}-{month:02d}/CPSC_Enrollment_Info_{year}_{month:02d}.zip"

    try:
        data = download_from_s3(key)

        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "cpsc.zip")
            with open(zip_path, 'wb') as f:
                f.write(data)

            with zipfile.ZipFile(zip_path, 'r') as zf:
                files = zf.namelist()

                # Look for Contract_Info file
                data_file = None
                for f in files:
                    if 'contract' in f.lower() and 'info' in f.lower():
                        data_file = f
                        break

                if not data_file:
                    return None

                zf.extract(data_file, tmpdir)
                file_path = os.path.join(tmpdir, data_file)

                df = pd.read_csv(file_path, dtype=str, encoding='latin-1')

        # Find parent org column
        parent_col = None
        contract_col = None
        for col in df.columns:
            col_lower = col.lower()
            if 'parent' in col_lower and 'org' in col_lower:
                parent_col = col
            elif 'contract' in col_lower and ('id' in col_lower or col_lower == 'contract id'):
                contract_col = col

        if parent_col:
            result = df[[contract_col, parent_col]].copy() if contract_col else df[[parent_col]].copy()
            result.columns = ['contract_id', 'parent_org'] if contract_col else ['parent_org']
            result['parent_org'] = result['parent_org'].apply(normalize_name)
            result['year'] = year
            result['source'] = 'cpsc'
            return result.drop_duplicates()

    except Exception as e:
        pass

    return None


def build_parent_org_dimension() -> pd.DataFrame:
    """
    Build the parent organization dimension table.

    1. Collect all unique parent org names from all years
    2. Normalize and map to canonical names
    3. Build name history per org
    4. Track M&A events
    """
    print("\n=== Collecting Parent Org Names ===")

    all_orgs = []

    # Collect from Stars (primary source)
    print("\nFrom Stars files:")
    for year in range(2007, 2027):
        df = extract_parent_orgs_from_stars(year)
        if df is not None:
            print(f"  {year}: {len(df):,} records")
            all_orgs.append(df)

    # Collect from CPSC (secondary)
    print("\nFrom CPSC files:")
    for year in range(2013, 2027):
        df = extract_parent_orgs_from_cpsc(year, 1)
        if df is not None:
            print(f"  {year}: {len(df):,} records")
            all_orgs.append(df)

    # Combine all
    combined = pd.concat(all_orgs, ignore_index=True)
    print(f"\nTotal raw records: {len(combined):,}")

    # Get unique org names per year
    name_year = combined.groupby(['parent_org', 'year']).size().reset_index(name='count')
    print(f"Unique (name, year) pairs: {len(name_year):,}")

    # Build canonical mapping
    print("\n=== Building Canonical Mapping ===")

    # Group all historical names by canonical name
    canonical_groups = defaultdict(lambda: {'names': set(), 'years': set(), 'contracts': set()})

    for _, row in combined.iterrows():
        if pd.isna(row['parent_org']):
            continue

        original_name = row['parent_org']
        year = row['year']
        canonical = get_canonical_name(original_name, year)

        canonical_groups[canonical]['names'].add(original_name)
        canonical_groups[canonical]['years'].add(year)
        if 'contract_id' in row and pd.notna(row.get('contract_id')):
            canonical_groups[canonical]['contracts'].add(row['contract_id'])

    print(f"Unique canonical orgs: {len(canonical_groups):,}")

    # Build dimension table
    dimension_records = []

    for canonical_name, data in canonical_groups.items():
        if canonical_name is None:
            continue

        parent_org_id = str(uuid.uuid4())

        # Build name history
        name_history = []
        for name in data['names']:
            # Find years this name was used
            years_for_name = combined[combined['parent_org'] == name]['year'].unique().tolist()
            name_history.append({
                'name': name,
                'years': sorted(years_for_name)
            })

        # Build M&A history
        ma_history = []
        for event in MA_EVENTS:
            if canonical_name == event['acquirer']:
                for acquired in event['acquired']:
                    if acquired in data['names'] or acquired == canonical_name:
                        continue
                    ma_history.append({
                        'year': event['year'],
                        'event_type': event['event_type'],
                        'acquired_org': acquired,
                        'notes': event['notes']
                    })

        dimension_records.append({
            'parent_org_id': parent_org_id,
            'canonical_name': canonical_name,
            'name_variations': list(data['names']),
            'name_history': json.dumps(name_history),
            'ma_history': json.dumps(ma_history),
            'first_year': min(data['years']) if data['years'] else None,
            'last_year': max(data['years']) if data['years'] else None,
            'contract_count': len(data['contracts']),
            'is_active': max(data['years']) >= 2025 if data['years'] else False,
            'created_at': datetime.now().isoformat()
        })

    return pd.DataFrame(dimension_records)


def upload_to_s3(df: pd.DataFrame, s3_key: str):
    """Upload DataFrame as Parquet to S3."""
    print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_key} ===")

    table = pa.Table.from_pandas(df)

    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        pq.write_table(table, f.name, compression='snappy')
        s3.upload_file(f.name, S3_BUCKET, s3_key)

    print(f"  Uploaded {len(df):,} parent orgs")


def main():
    print("=" * 70)
    print("BUILD PARENT ORGANIZATION DIMENSION")
    print("=" * 70)
    print(f"Started: {datetime.now()}")

    # Build dimension
    dim_parent_org = build_parent_org_dimension()

    # Summary
    print("\n=== Summary ===")
    print(f"Total parent organizations: {len(dim_parent_org):,}")
    print(f"Active (seen in 2025+): {dim_parent_org['is_active'].sum():,}")
    print(f"With M&A history: {(dim_parent_org['ma_history'] != '[]').sum():,}")

    # Top orgs by contract count
    print("\nTop 10 by contract count:")
    top10 = dim_parent_org.nlargest(10, 'contract_count')
    for _, row in top10.iterrows():
        print(f"  {row['canonical_name']}: {row['contract_count']:,} contracts")

    # Upload
    upload_to_s3(dim_parent_org, OUTPUT_KEY)

    print("\n" + "=" * 70)
    print("COMPLETE")
    print(f"Finished: {datetime.now()}")
    print("=" * 70)


if __name__ == '__main__':
    main()
