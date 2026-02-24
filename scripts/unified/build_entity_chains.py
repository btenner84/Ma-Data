#!/usr/bin/env python3
"""
Build Entity Chains from Crosswalk Files

Creates stable entity IDs that track contract+plan combinations across years,
even when IDs change due to benefit changes, mergers, or CMS reassignments.

Crosswalk coverage: 2006-2026 (21 years)

Output: dim_entity.parquet
"""

import os
import sys
import uuid
import json
import zipfile
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

# Configuration
S3_BUCKET = "ma-data123"
S3_PREFIX_CROSSWALKS = "raw/crosswalks"
S3_PREFIX_ENROLLMENT = "raw/enrollment/by_plan"
OUTPUT_KEY = "processed/dimensions/dim_entity.parquet"

s3 = boto3.client('s3')


# Schema mapping for different crosswalk eras
CROSSWALK_SCHEMAS = {
    # 2022-2026: New format
    'new': {
        'prev_contract': 'PREVIOUS_CONTRACT_NUMBER',
        'prev_plan': 'PREVIOUS_PLAN_ID',
        'curr_contract': 'CURRENT_CONTRACT_NUMBER',
        'curr_plan': 'CURRENT_PLAN_ID',
        'snp_type': 'CURRENT_SNP_TYPE',
    },
    # 2013-2021: Intermediate format
    'intermediate': {
        'prev_contract': 'Previous Contract ID',
        'prev_plan': 'Previous Plan ID',
        'curr_contract': 'New Contract ID',
        'curr_plan': 'New Plan ID',
        'snp_type': None,
    },
    # 2006-2012: Old format
    'old': {
        'prev_contract': 'Old Contract Number',
        'prev_plan': 'Old Plan ID',
        'curr_contract': 'New Contract Number',
        'curr_plan': 'New Plan ID',
        'snp_type': None,
    }
}


def get_schema_era(year: int) -> str:
    """Determine which schema era a crosswalk year belongs to."""
    if year >= 2022:
        return 'new'
    elif year >= 2013:
        return 'intermediate'
    else:
        return 'old'


def download_from_s3(s3_key: str) -> bytes:
    """Download file from S3."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return response['Body'].read()


def load_crosswalk(year: int) -> Optional[pd.DataFrame]:
    """
    Load and normalize a crosswalk file for a given year.

    The crosswalk for year N maps plans from year N-1 to year N.
    """
    # Try different key patterns
    key_patterns = [
        f"{S3_PREFIX_CROSSWALKS}/crosswalk_{year}.zip",
        f"{S3_PREFIX_CROSSWALKS}/crosswalk_{year-1}_to_{year}.zip",
    ]

    data = None
    for key in key_patterns:
        try:
            data = download_from_s3(key)
            break
        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            continue

    if data is None:
        print(f"  [WARN] No crosswalk found for {year}")
        return None

    # Extract ZIP and read CSV/Excel
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, f"crosswalk_{year}.zip")
        with open(zip_path, 'wb') as f:
            f.write(data)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find the data file inside
            files = zf.namelist()
            data_file = None
            for f in files:
                if f.endswith('.csv') or f.endswith('.xlsx') or f.endswith('.xls'):
                    if 'crosswalk' in f.lower() or 'xwalk' in f.lower():
                        data_file = f
                        break
                    elif not data_file:
                        data_file = f

            if not data_file:
                print(f"  [WARN] No data file found in crosswalk ZIP for {year}")
                return None

            zf.extract(data_file, tmpdir)
            file_path = os.path.join(tmpdir, data_file)

            # Read based on file type
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str, encoding='latin-1')
            else:
                df = pd.read_excel(file_path, dtype=str)

    # Normalize column names
    era = get_schema_era(year)
    schema = CROSSWALK_SCHEMAS[era]

    # Try to find columns (case-insensitive, partial match)
    col_map = {}
    for target, source in schema.items():
        if source is None:
            continue
        for col in df.columns:
            if source.lower() in col.lower() or col.lower() in source.lower():
                col_map[col] = target
                break

    if 'prev_contract' not in col_map.values() or 'curr_contract' not in col_map.values():
        # Try alternate column patterns
        for col in df.columns:
            col_lower = col.lower()
            if 'prev' in col_lower and 'contract' in col_lower and 'prev_contract' not in col_map.values():
                col_map[col] = 'prev_contract'
            elif 'old' in col_lower and 'contract' in col_lower and 'prev_contract' not in col_map.values():
                col_map[col] = 'prev_contract'
            elif ('new' in col_lower or 'curr' in col_lower) and 'contract' in col_lower and 'curr_contract' not in col_map.values():
                col_map[col] = 'curr_contract'
            elif 'prev' in col_lower and 'plan' in col_lower and 'id' in col_lower and 'prev_plan' not in col_map.values():
                col_map[col] = 'prev_plan'
            elif 'old' in col_lower and 'plan' in col_lower and 'prev_plan' not in col_map.values():
                col_map[col] = 'prev_plan'
            elif ('new' in col_lower or 'curr' in col_lower) and 'plan' in col_lower and 'id' in col_lower and 'curr_plan' not in col_map.values():
                col_map[col] = 'curr_plan'

    df = df.rename(columns=col_map)

    # Ensure required columns exist
    required = ['prev_contract', 'prev_plan', 'curr_contract', 'curr_plan']
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"  [WARN] Missing columns {missing} in crosswalk {year}")
        print(f"         Available columns: {list(df.columns)}")
        return None

    # Clean up values
    for col in ['prev_contract', 'prev_plan', 'curr_contract', 'curr_plan']:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(['nan', 'None', ''], pd.NA)

    # Remove rows with missing keys
    df = df.dropna(subset=['prev_contract', 'curr_contract'])

    # Add year
    df['crosswalk_year'] = year

    print(f"  [OK] Loaded crosswalk {year}: {len(df):,} mappings")
    return df[['prev_contract', 'prev_plan', 'curr_contract', 'curr_plan', 'crosswalk_year']]


def get_current_year_plans() -> pd.DataFrame:
    """
    Get all contract+plan combinations from the most recent enrollment data.
    """
    print("\n=== Loading Current Year Plans ===")

    # Try to find most recent enrollment by plan file
    for year in range(2026, 2020, -1):
        for month in range(12, 0, -1):
            key = f"{S3_PREFIX_ENROLLMENT}/{year}-{month:02d}/enrollment_plan_{year}_{month:02d}.zip"
            try:
                data = download_from_s3(key)
                print(f"  Found enrollment data for {year}-{month:02d}")

                with tempfile.TemporaryDirectory() as tmpdir:
                    zip_path = os.path.join(tmpdir, "enrollment.zip")
                    with open(zip_path, 'wb') as f:
                        f.write(data)

                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        files = zf.namelist()
                        data_file = [f for f in files if f.endswith('.csv') or f.endswith('.xlsx')][0]
                        zf.extract(data_file, tmpdir)
                        file_path = os.path.join(tmpdir, data_file)

                        if file_path.endswith('.csv'):
                            df = pd.read_csv(file_path, dtype=str, encoding='latin-1')
                        else:
                            df = pd.read_excel(file_path, dtype=str)

                # Find contract and plan columns
                contract_col = None
                plan_col = None
                for col in df.columns:
                    col_lower = col.lower()
                    if 'contract' in col_lower and ('number' in col_lower or 'id' in col_lower):
                        contract_col = col
                    elif 'plan' in col_lower and 'id' in col_lower:
                        plan_col = col

                if contract_col and plan_col:
                    plans = df[[contract_col, plan_col]].drop_duplicates()
                    plans.columns = ['contract_id', 'plan_id']
                    plans['contract_id'] = plans['contract_id'].astype(str).str.strip()
                    plans['plan_id'] = plans['plan_id'].astype(str).str.strip()
                    plans = plans.dropna()
                    plans['year'] = year
                    print(f"  Found {len(plans):,} unique contract+plan combinations")
                    return plans

            except Exception as e:
                continue

    raise RuntimeError("Could not load current year plans from any enrollment file")


def build_entity_chains(crosswalks: Dict[int, pd.DataFrame], current_plans: pd.DataFrame) -> pd.DataFrame:
    """
    Build entity chains by walking backwards through crosswalks.

    For each current plan, trace its history back through crosswalk mappings.
    """
    print("\n=== Building Entity Chains ===")

    # Index crosswalks by (curr_contract, curr_plan) for fast lookup
    crosswalk_index = {}
    for year, cw in crosswalks.items():
        for _, row in cw.iterrows():
            key = (row['curr_contract'], str(row['curr_plan']).zfill(3) if pd.notna(row['curr_plan']) else None)
            if key not in crosswalk_index:
                crosswalk_index[(year, key)] = (row['prev_contract'], row['prev_plan'])

    entities = []

    for idx, row in current_plans.iterrows():
        if idx % 10000 == 0:
            print(f"  Processing plan {idx:,} / {len(current_plans):,}")

        contract_id = row['contract_id']
        plan_id = str(row['plan_id']).zfill(3) if pd.notna(row['plan_id']) else None
        current_year = row['year']

        # Build chain
        chain = [{
            'year': current_year,
            'contract_id': contract_id,
            'plan_id': plan_id,
            'source': 'current'
        }]

        curr_contract, curr_plan = contract_id, plan_id

        # Walk backwards through years
        for year in range(current_year, 2005, -1):
            key = (year, (curr_contract, curr_plan))

            if key in crosswalk_index:
                prev_contract, prev_plan = crosswalk_index[key]
                prev_plan = str(prev_plan).zfill(3) if pd.notna(prev_plan) else curr_plan

                chain.append({
                    'year': year - 1,
                    'contract_id': prev_contract,
                    'plan_id': prev_plan,
                    'source': 'crosswalk'
                })

                curr_contract, curr_plan = prev_contract, prev_plan
            else:
                # No crosswalk mapping - assume stable ID or plan didn't exist
                # Check if we have any crosswalk for this year at all
                has_crosswalk_for_year = any(y == year for (y, _) in crosswalk_index.keys())

                if has_crosswalk_for_year:
                    # Crosswalk exists but plan not in it - might be new plan or terminated
                    # Try assuming stable ID
                    chain.append({
                        'year': year - 1,
                        'contract_id': curr_contract,
                        'plan_id': curr_plan,
                        'source': 'assumed_stable'
                    })
                else:
                    # No crosswalk for this year - just assume stable
                    if year > 2006:
                        chain.append({
                            'year': year - 1,
                            'contract_id': curr_contract,
                            'plan_id': curr_plan,
                            'source': 'no_crosswalk'
                        })

        # Create entity record
        entity_id = str(uuid.uuid4())
        first_year = min(c['year'] for c in chain)

        entities.append({
            'entity_id': entity_id,
            'entity_type': 'plan',
            'current_contract_id': contract_id,
            'current_plan_id': plan_id,
            'first_year': first_year,
            'last_year': current_year,
            'is_active': True,
            'identity_chain': json.dumps(chain),
            'chain_length': len(chain),
            'crosswalk_links': sum(1 for c in chain if c['source'] == 'crosswalk'),
            'created_at': datetime.now().isoformat()
        })

    return pd.DataFrame(entities)


def upload_to_s3(df: pd.DataFrame, s3_key: str):
    """Upload DataFrame as Parquet to S3."""
    print(f"\n=== Uploading to s3://{S3_BUCKET}/{s3_key} ===")

    # Convert to Parquet bytes
    table = pa.Table.from_pandas(df)

    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        pq.write_table(table, f.name, compression='snappy')
        f.seek(0)

        s3.upload_file(f.name, S3_BUCKET, s3_key)

    print(f"  Uploaded {len(df):,} entities")


def main():
    print("=" * 70)
    print("BUILD ENTITY CHAINS FROM CROSSWALKS")
    print("=" * 70)
    print(f"Started: {datetime.now()}")

    # Load all crosswalks
    print("\n=== Loading Crosswalks (2006-2026) ===")
    crosswalks = {}
    for year in range(2007, 2027):  # Crosswalk for year N maps N-1 to N
        cw = load_crosswalk(year)
        if cw is not None:
            crosswalks[year] = cw

    print(f"\nLoaded {len(crosswalks)} crosswalk files")
    total_mappings = sum(len(cw) for cw in crosswalks.values())
    print(f"Total mappings: {total_mappings:,}")

    # Get current year plans
    current_plans = get_current_year_plans()

    # Build entity chains
    entities = build_entity_chains(crosswalks, current_plans)

    # Summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total entities: {len(entities):,}")
    print(f"Avg chain length: {entities['chain_length'].mean():.1f} years")
    print(f"Avg crosswalk links: {entities['crosswalk_links'].mean():.1f}")
    print(f"Entities with full 20-year history: {(entities['chain_length'] >= 20).sum():,}")

    # Upload to S3
    upload_to_s3(entities, OUTPUT_KEY)

    print("\n" + "=" * 70)
    print("COMPLETE")
    print(f"Finished: {datetime.now()}")
    print("=" * 70)


if __name__ == '__main__':
    main()
