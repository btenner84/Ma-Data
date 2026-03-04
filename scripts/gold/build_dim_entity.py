#!/usr/bin/env python3
"""
Build Gold Layer: dim_entity
=============================

Creates the entity dimension table that tracks contracts across time.
Uses crosswalk data to build entity chains - stable IDs that persist
across contract ID changes, mergers, and acquisitions.

Sources:
- s3://ma-data123/silver/crosswalks/*/crosswalk.parquet
- s3://ma-data123/silver/enrollment/cpsc/*/contracts.parquet

Output: s3://ma-data123/gold/dim_entity.parquet

Columns:
- entity_id: STRING (stable ID across contract changes)
- contract_id: STRING (current contract ID for this year)
- year: INT (year this row represents)
- parent_org: STRING (parent organization name)
- organization_name: STRING
- organization_type: STRING
- is_ma: BOOL (Medicare Advantage)
- is_pdp: BOOL (Prescription Drug Plan)
- contract_effective_date: DATE
- predecessor_contract_id: STRING (previous contract ID, if changed)
- is_contract_change: BOOL (contract ID changed from prior year)
- is_ma_exit: BOOL (contract exited MA market)

SCD Type 2 style - one row per entity per year.
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
import sys

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
CROSSWALK_PREFIX = "silver/crosswalks"
CONTRACTS_PREFIX = "silver/enrollment/cpsc"
OUTPUT_KEY = "gold/dim_entity.parquet"

s3 = boto3.client('s3')


def list_files(prefix: str, suffix: str = '.parquet') -> list:
    """List files with given prefix and suffix."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith(suffix):
                files.append(obj['Key'])
    return sorted(files)


def load_parquet(key: str) -> pd.DataFrame:
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"  Warning: Could not load {key}: {e}")
        return pd.DataFrame()


def build_crosswalk_map(crosswalk_files: list) -> dict:
    """
    Build mapping of old_contract_id -> new_contract_id across all years.
    Returns dict: {old_contract_id: [(new_contract_id, effective_year), ...]}
    """
    all_crosswalks = []
    for f in crosswalk_files:
        df = load_parquet(f)
        if not df.empty and 'old_contract_id' in df.columns and 'new_contract_id' in df.columns:
            all_crosswalks.append(df[['old_contract_id', 'new_contract_id', 'effective_year']].drop_duplicates())
    
    if not all_crosswalks:
        return {}
    
    combined = pd.concat(all_crosswalks, ignore_index=True).drop_duplicates()
    
    crosswalk_map = {}
    for _, row in combined.iterrows():
        old_id = row['old_contract_id']
        new_id = row['new_contract_id']
        year = row['effective_year']
        if old_id and new_id and old_id != new_id:
            if old_id not in crosswalk_map:
                crosswalk_map[old_id] = []
            crosswalk_map[old_id].append((new_id, year))
    
    return crosswalk_map


def find_entity_root(contract_id: str, crosswalk_map: dict, visited: set = None) -> str:
    """
    Find the root entity ID by following crosswalk chain backwards.
    The root is the earliest known contract ID.
    """
    if visited is None:
        visited = set()
    
    if contract_id in visited:
        return contract_id
    
    visited.add(contract_id)
    
    for old_id, mappings in crosswalk_map.items():
        for new_id, _ in mappings:
            if new_id == contract_id:
                return find_entity_root(old_id, crosswalk_map, visited)
    
    return contract_id


def build_entity_chains(contract_info: pd.DataFrame, crosswalk_map: dict) -> pd.DataFrame:
    """
    Build entity chains by:
    1. Finding all unique contracts per year
    2. Assigning stable entity_id (using crosswalk to find chain root)
    3. Tracking predecessor relationships
    """
    all_entities = []
    
    contracts_by_year = {}
    for year in sorted(contract_info['year'].unique()):
        year_df = contract_info[contract_info['year'] == year]
        contracts_by_year[year] = set(year_df['contract_id'].dropna().unique())
    
    contract_to_entity = {}
    for contract_id in contract_info['contract_id'].dropna().unique():
        entity_id = find_entity_root(contract_id, crosswalk_map)
        contract_to_entity[contract_id] = entity_id
    
    years = sorted(contracts_by_year.keys())
    
    for year in years:
        year_contracts = contract_info[contract_info['year'] == year].copy()
        
        for _, row in year_contracts.iterrows():
            contract_id = row['contract_id']
            if pd.isna(contract_id):
                continue
            
            entity_id = contract_to_entity.get(contract_id, contract_id)
            
            predecessor = None
            is_contract_change = False
            
            for old_id, mappings in crosswalk_map.items():
                for new_id, eff_year in mappings:
                    if new_id == contract_id and eff_year == year:
                        predecessor = old_id
                        is_contract_change = True
                        break
                if predecessor:
                    break
            
            prev_year = year - 1
            is_ma_exit = False
            if prev_year in contracts_by_year:
                prev_contracts = contracts_by_year[prev_year]
                if entity_id in prev_contracts and contract_id not in contracts_by_year.get(year, set()):
                    is_ma_exit = True
            
            entity_row = {
                'entity_id': entity_id,
                'contract_id': contract_id,
                'year': year,
                'parent_org': row.get('parent_org'),
                'organization_name': row.get('organization_name'),
                'organization_type': row.get('organization_type'),
                'is_ma': str(row.get('offers_part_d', '')).upper() != 'YES' or True,
                'is_pdp': str(row.get('offers_part_d', '')).upper() == 'YES',
                'contract_effective_date': row.get('contract_effective_date'),
                'predecessor_contract_id': predecessor,
                'is_contract_change': is_contract_change,
                'is_ma_exit': is_ma_exit,
            }
            all_entities.append(entity_row)
    
    return pd.DataFrame(all_entities).drop_duplicates(subset=['entity_id', 'contract_id', 'year'])


def main():
    print("=" * 70)
    print("BUILD GOLD LAYER: dim_entity")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    print("\n1. Loading crosswalk data...")
    crosswalk_files = list_files(CROSSWALK_PREFIX, '/crosswalk.parquet')
    print(f"   Found {len(crosswalk_files)} crosswalk files")
    crosswalk_map = build_crosswalk_map(crosswalk_files)
    print(f"   Built crosswalk map with {len(crosswalk_map)} contract ID changes")
    sys.stdout.flush()
    
    print("\n2. Loading contract info...")
    contract_files = list_files(CONTRACTS_PREFIX, '/contracts.parquet')
    print(f"   Found {len(contract_files)} contract files")
    
    if not contract_files:
        print("No contract files found. Checking existing dim_entity...")
        existing = load_parquet('processed/unified/dim_entity.parquet')
        if not existing.empty:
            print(f"Using existing dim_entity: {len(existing)} rows")
            buffer = BytesIO()
            existing.to_parquet(buffer, index=False, compression='snappy')
            buffer.seek(0)
            s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
            return
        print("ERROR: No source data found")
        return
    
    all_contracts = []
    for f in contract_files[:20]:
        df = load_parquet(f)
        if not df.empty:
            all_contracts.append(df)
    
    contract_info = pd.concat(all_contracts, ignore_index=True) if all_contracts else pd.DataFrame()
    
    if contract_info.empty:
        print("ERROR: No contract data loaded")
        return
    
    contract_info = contract_info.drop_duplicates(subset=['contract_id', 'year'])
    print(f"   Loaded {len(contract_info):,} unique contract-year combinations")
    print(f"   Years: {sorted(contract_info['year'].unique())}")
    sys.stdout.flush()
    
    print("\n3. Building entity chains...")
    entity_df = build_entity_chains(contract_info, crosswalk_map)
    print(f"   Generated {len(entity_df):,} entity dimension rows")
    print(f"   Unique entities: {entity_df['entity_id'].nunique():,}")
    print(f"   Contract changes tracked: {entity_df['is_contract_change'].sum()}")
    sys.stdout.flush()
    
    print("\n4. Saving to S3...")
    buffer = BytesIO()
    entity_df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
    
    print(f"Saved to s3://{S3_BUCKET}/{OUTPUT_KEY}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
