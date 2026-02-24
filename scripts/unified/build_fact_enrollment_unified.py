#!/usr/bin/env python3
"""
Build Unified Enrollment Fact Table

Creates the master enrollment fact table that supports ALL filter combinations:
- plan_type, product_type, group_type, snp_type
- parent_org
- All with confidence scores for derived fields

Source Hierarchy:
1. Enrollment by Plan (authoritative for totals - no suppression)
2. CPSC Contract_Info (dimensions: parent_org, plan_type, EGHP, SNP flag)
3. SNP Report (specific SNP types: D-SNP, C-SNP, I-SNP)

Output: fact_enrollment_unified/ (partitioned by year/month)

Grain: (contract_id, plan_id, year, month)
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
from functools import lru_cache

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import audit lineage system
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from audit_lineage import AuditLogger, create_audit_logger
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    print("[WARN] Audit lineage module not available")

# Configuration
S3_BUCKET = "ma-data123"
S3_PREFIX_ENROLLMENT_BY_PLAN = "raw/enrollment/by_plan"
S3_PREFIX_CPSC = "raw/enrollment/cpsc"
S3_PREFIX_SNP = "raw/snp"
OUTPUT_PREFIX = "processed/facts/fact_enrollment_unified"

s3 = boto3.client('s3')

# Plan type simplification mapping
PLAN_TYPE_MAP = {
    'HMO': 'HMO',
    'HMOPOS': 'HMO',
    'HMO-POS': 'HMO',
    'HMO/HMOPOS': 'HMO',
    'LOCAL HMO': 'HMO',

    'PPO': 'PPO',
    'LOCAL PPO': 'PPO',
    'LPPO': 'PPO',

    'REGIONAL PPO': 'RPPO',
    'RPPO': 'RPPO',

    'PFFS': 'PFFS',
    'PRIVATE FEE-FOR-SERVICE': 'PFFS',

    'MSA': 'MSA',
    'MEDICAL SAVINGS ACCOUNT': 'MSA',

    'PACE': 'PACE',
    'NATIONAL PACE': 'PACE',

    '1876 COST': 'Cost',
    'HCPP': 'Cost',
    'COST': 'Cost',

    'PDP': 'PDP',
    'PRESCRIPTION DRUG PLAN': 'PDP',

    'EMPLOYER DIRECT': 'Employer',
    'EMPLOYER/UNION ONLY DIRECT CONTRACT PDP': 'Employer',
}


def download_from_s3(s3_key: str) -> Optional[bytes]:
    """Download file from S3."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        return response['Body'].read()
    except Exception as e:
        return None


def list_s3_files(prefix: str) -> List[str]:
    """List all files under an S3 prefix."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    return files


def load_enrollment_by_plan(year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Load enrollment totals from Monthly Enrollment by Plan file.

    This is the authoritative source for enrollment (no suppression).
    """
    key = f"{S3_PREFIX_ENROLLMENT_BY_PLAN}/{year}-{month:02d}/enrollment_plan_{year}_{month:02d}.zip"
    data = download_from_s3(key)

    if data is None:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "enrollment.zip")
        with open(zip_path, 'wb') as f:
            f.write(data)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            files = [f for f in zf.namelist() if f.endswith('.csv') or f.endswith('.xlsx')]
            if not files:
                return None

            data_file = files[0]
            zf.extract(data_file, tmpdir)
            file_path = os.path.join(tmpdir, data_file)

            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str, encoding='latin-1')
            else:
                df = pd.read_excel(file_path, dtype=str)

    # Normalize column names
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'contract' in col_lower and ('number' in col_lower or 'id' in col_lower):
            col_map[col] = 'contract_id'
        elif 'plan' in col_lower and 'id' in col_lower:
            col_map[col] = 'plan_id'
        elif 'enrollment' in col_lower and 'total' not in col_lower:
            col_map[col] = 'enrollment'
        elif col_lower == 'enrollment':
            col_map[col] = 'enrollment'

    df = df.rename(columns=col_map)

    if 'contract_id' not in df.columns or 'enrollment' not in df.columns:
        return None

    # Clean data
    df['contract_id'] = df['contract_id'].astype(str).str.strip()
    if 'plan_id' in df.columns:
        df['plan_id'] = df['plan_id'].astype(str).str.strip().str.zfill(3)
    else:
        df['plan_id'] = '000'

    # Convert enrollment to numeric
    df['enrollment'] = pd.to_numeric(
        df['enrollment'].astype(str).str.replace(',', '').str.replace('*', ''),
        errors='coerce'
    ).fillna(0).astype(int)

    df = df[df['enrollment'] > 0]

    return df[['contract_id', 'plan_id', 'enrollment']].drop_duplicates()


def load_cpsc_contract_info(year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Load contract dimensions from CPSC Contract_Info file.

    Provides: parent_org, plan_type, offers_part_d, snp_plan, eghp
    """
    key = f"{S3_PREFIX_CPSC}/{year}-{month:02d}/CPSC_Enrollment_Info_{year}_{month:02d}.zip"
    data = download_from_s3(key)

    if data is None:
        # Try alternate key patterns
        for pattern in [
            f"{S3_PREFIX_CPSC}/CPSC_Enrollment_Info_{year}_{month:02d}.zip",
            f"{S3_PREFIX_CPSC}/{year}/CPSC_Enrollment_Info_{year}_{month:02d}.zip",
        ]:
            data = download_from_s3(pattern)
            if data:
                break

    if data is None:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "cpsc.zip")
        with open(zip_path, 'wb') as f:
            f.write(data)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            files = zf.namelist()

            # Find Contract_Info file
            contract_file = None
            for f in files:
                if 'contract' in f.lower() and 'info' in f.lower() and f.endswith('.csv'):
                    contract_file = f
                    break

            if not contract_file:
                return None

            zf.extract(contract_file, tmpdir)
            file_path = os.path.join(tmpdir, contract_file)

            df = pd.read_csv(file_path, dtype=str, encoding='latin-1')

    # Map columns
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'contract' in col_lower and 'id' in col_lower:
            col_map[col] = 'contract_id'
        elif 'plan' in col_lower and 'id' in col_lower:
            col_map[col] = 'plan_id'
        elif 'parent' in col_lower and 'org' in col_lower:
            col_map[col] = 'parent_org'
        elif 'plan' in col_lower and 'type' in col_lower:
            col_map[col] = 'plan_type'
        elif 'offers' in col_lower and 'part' in col_lower and 'd' in col_lower:
            col_map[col] = 'offers_part_d'
        elif 'snp' in col_lower and 'plan' in col_lower:
            col_map[col] = 'snp_plan'
        elif col_lower == 'eghp':
            col_map[col] = 'eghp'
        elif 'organization' in col_lower and 'type' in col_lower:
            col_map[col] = 'org_type'

    df = df.rename(columns=col_map)

    # Clean
    if 'contract_id' in df.columns:
        df['contract_id'] = df['contract_id'].astype(str).str.strip()
    if 'plan_id' in df.columns:
        df['plan_id'] = df['plan_id'].astype(str).str.strip().str.zfill(3)
    else:
        df['plan_id'] = '000'

    # Keep relevant columns
    keep_cols = ['contract_id', 'plan_id', 'parent_org', 'plan_type',
                 'offers_part_d', 'snp_plan', 'eghp', 'org_type']
    available = [c for c in keep_cols if c in df.columns]

    return df[available].drop_duplicates()


def load_snp_report(year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Load SNP Report for specific SNP types (D-SNP, C-SNP, I-SNP).
    """
    # Try different key patterns
    patterns = [
        f"{S3_PREFIX_SNP}/{year}-{month:02d}/snp_{year}_{month:02d}.zip",
        f"{S3_PREFIX_SNP}/snp_{year}_{month:02d}.zip",
        f"{S3_PREFIX_SNP}/{year}/snp_{year}_{month:02d}.zip",
    ]

    data = None
    for pattern in patterns:
        data = download_from_s3(pattern)
        if data:
            break

    if data is None:
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "snp.zip")
        with open(zip_path, 'wb') as f:
            f.write(data)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            files = zf.namelist()

            # Find data file
            data_file = None
            for f in files:
                if f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.csv'):
                    data_file = f
                    break

            if not data_file:
                return None

            zf.extract(data_file, tmpdir)
            file_path = os.path.join(tmpdir, data_file)

            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str, encoding='latin-1')
            else:
                # Try multiple sheet names
                try:
                    df = pd.read_excel(file_path, dtype=str, sheet_name=0)
                except:
                    return None

    # Map columns
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'contract' in col_lower and ('number' in col_lower or 'id' in col_lower):
            col_map[col] = 'contract_id'
        elif 'plan' in col_lower and 'id' in col_lower:
            col_map[col] = 'plan_id'
        elif 'special' in col_lower and 'needs' in col_lower and 'type' in col_lower:
            col_map[col] = 'snp_type'
        elif 'snp' in col_lower and 'type' in col_lower:
            col_map[col] = 'snp_type'

    df = df.rename(columns=col_map)

    if 'contract_id' not in df.columns:
        return None

    df['contract_id'] = df['contract_id'].astype(str).str.strip()
    if 'plan_id' in df.columns:
        df['plan_id'] = df['plan_id'].astype(str).str.strip().str.zfill(3)
    else:
        df['plan_id'] = '000'

    # Normalize SNP type
    if 'snp_type' in df.columns:
        df['snp_type'] = df['snp_type'].astype(str).str.upper()
        df['snp_type'] = df['snp_type'].apply(lambda x:
            'D-SNP' if 'DUAL' in x or 'D-SNP' in x else
            'C-SNP' if 'CHRONIC' in x or 'C-SNP' in x else
            'I-SNP' if 'INSTITUTIONAL' in x or 'I-SNP' in x else
            'SNP-Unknown'
        )

    return df[['contract_id', 'plan_id', 'snp_type']].drop_duplicates()


def derive_group_type(row) -> Tuple[str, str, float]:
    """
    Derive group_type with source tracking and confidence score.

    Returns: (group_type, source, confidence)
    """
    # Priority 1: EGHP field (explicit)
    eghp = row.get('eghp', '')
    if pd.notna(eghp) and str(eghp).strip():
        eghp_str = str(eghp).strip().lower()
        if eghp_str == 'yes':
            return ('Group', 'eghp_yes', 1.0)
        elif eghp_str == 'no':
            return ('Individual', 'eghp_no', 0.95)

    # Priority 2: Plan ID heuristic (plan_id >= 800 typically = Group)
    plan_id = row.get('plan_id', '000')
    try:
        plan_id_int = int(plan_id)
        if plan_id_int >= 800:
            return ('Group', 'plan_id_gte_800', 0.90)
        else:
            return ('Individual', 'plan_id_lt_800', 0.85)
    except (ValueError, TypeError):
        pass

    # Priority 3: Organization type heuristic
    org_type = row.get('org_type', '')
    if pd.notna(org_type):
        org_type_str = str(org_type).upper()
        if 'EMPLOYER' in org_type_str or 'UNION' in org_type_str:
            return ('Group', 'org_type', 0.85)

    return ('Unknown', 'unknown', 0.0)


def derive_product_type(row) -> str:
    """
    Derive product_type from contract prefix and Offers Part D.
    """
    contract_id = str(row.get('contract_id', ''))
    offers_part_d = str(row.get('offers_part_d', '')).strip().lower()

    # PDP contracts start with S
    if contract_id.startswith('S'):
        return 'PDP'

    # MA contracts start with H or R
    if contract_id.startswith(('H', 'R')):
        if offers_part_d == 'yes':
            return 'MAPD'
        else:
            return 'MA-only'

    # E contracts are employer
    if contract_id.startswith('E'):
        return 'Employer'

    return 'Other'


def simplify_plan_type(plan_type: str) -> str:
    """Simplify plan_type to standard categories."""
    if pd.isna(plan_type):
        return 'Unknown'

    plan_type = str(plan_type).strip().upper()

    # Check mapping
    for pattern, simplified in PLAN_TYPE_MAP.items():
        if pattern.upper() in plan_type or plan_type in pattern.upper():
            return simplified

    return 'Other'


def process_month(year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Process a single month of enrollment data.
    """
    print(f"  Processing {year}-{month:02d}...")

    # 1. Load enrollment totals (authoritative)
    enrollment = load_enrollment_by_plan(year, month)
    if enrollment is None:
        print(f"    [SKIP] No enrollment data for {year}-{month:02d}")
        return None

    print(f"    Enrollment by Plan: {len(enrollment):,} records, {enrollment['enrollment'].sum():,} total")

    # 2. Load CPSC Contract_Info for dimensions
    cpsc_info = load_cpsc_contract_info(year, month)
    if cpsc_info is not None:
        print(f"    CPSC Contract_Info: {len(cpsc_info):,} records")
    else:
        print(f"    [WARN] No CPSC Contract_Info for {year}-{month:02d}")

    # 3. Load SNP Report for specific SNP types
    snp = load_snp_report(year, month)
    if snp is not None:
        print(f"    SNP Report: {len(snp):,} records")

    # 4. Join dimensions to enrollment
    df = enrollment.copy()

    if cpsc_info is not None:
        df = df.merge(
            cpsc_info,
            on=['contract_id', 'plan_id'],
            how='left'
        )

    # 5. Derive group_type with confidence
    if 'eghp' in df.columns or 'plan_id' in df.columns:
        derived = df.apply(derive_group_type, axis=1, result_type='expand')
        df['group_type'] = derived[0]
        df['group_type_source'] = derived[1]
        df['group_type_confidence'] = derived[2]
    else:
        df['group_type'] = 'Unknown'
        df['group_type_source'] = 'no_data'
        df['group_type_confidence'] = 0.0

    # 6. Derive product_type
    df['product_type'] = df.apply(derive_product_type, axis=1)

    # 7. Simplify plan_type
    if 'plan_type' in df.columns:
        df['plan_type_simplified'] = df['plan_type'].apply(simplify_plan_type)
    else:
        df['plan_type'] = 'Unknown'
        df['plan_type_simplified'] = 'Unknown'

    # 8. Join SNP detail
    if snp is not None:
        df = df.merge(
            snp,
            on=['contract_id', 'plan_id'],
            how='left',
            suffixes=('', '_snp')
        )
        # Use SNP Report type if available, else fall back to CPSC flag
        if 'snp_type_snp' in df.columns:
            df['snp_type'] = df['snp_type_snp']
            df['snp_type_source'] = 'snp_report'
        if 'snp_plan' in df.columns:
            df['snp_type'] = df['snp_type'].fillna(
                df['snp_plan'].apply(lambda x: 'SNP-Unknown' if str(x).lower() == 'yes' else 'Non-SNP')
            )
            df['snp_type_source'] = df['snp_type_source'].fillna('cpsc_flag')
        else:
            df['snp_type'] = df['snp_type'].fillna('Non-SNP')
            df['snp_type_source'] = df['snp_type_source'].fillna('assumed')
    else:
        # No SNP Report - use CPSC flag if available
        if 'snp_plan' in df.columns:
            df['snp_type'] = df['snp_plan'].apply(
                lambda x: 'SNP-Unknown' if str(x).lower() == 'yes' else 'Non-SNP'
            )
            df['snp_type_source'] = 'cpsc_flag'
        else:
            df['snp_type'] = 'Non-SNP'
            df['snp_type_source'] = 'assumed'

    # 9. Add time columns
    df['year'] = year
    df['month'] = month
    df['enrollment_source'] = 'enrollment_by_plan'

    # 10. Select final columns
    final_cols = [
        'contract_id', 'plan_id', 'year', 'month',
        'parent_org', 'plan_type', 'plan_type_simplified',
        'product_type', 'group_type', 'group_type_source', 'group_type_confidence',
        'snp_type', 'snp_type_source',
        'enrollment', 'enrollment_source'
    ]

    # Add missing columns with None
    for col in final_cols:
        if col not in df.columns:
            df[col] = None

    df = df[final_cols]

    print(f"    Output: {len(df):,} records, {df['enrollment'].sum():,} total enrollment")

    return df


def upload_parquet(df: pd.DataFrame, year: int, month: int):
    """Upload DataFrame as partitioned Parquet to S3."""
    s3_key = f"{OUTPUT_PREFIX}/year={year}/month={month:02d}/data.parquet"

    table = pa.Table.from_pandas(df)

    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        pq.write_table(table, f.name, compression='snappy')
        s3.upload_file(f.name, S3_BUCKET, s3_key)

    print(f"    Uploaded to {s3_key}")


def main():
    print("=" * 70)
    print("BUILD UNIFIED ENROLLMENT FACT TABLE")
    print("=" * 70)
    print(f"Started: {datetime.now()}")

    # Initialize audit logger
    audit = None
    if AUDIT_AVAILABLE:
        try:
            audit = create_audit_logger('build_fact_enrollment_unified')
        except Exception as e:
            print(f"[WARN] Could not initialize audit: {e}")

    # Get current date to avoid processing future months
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month

    # Process all available months
    total_enrollment = 0
    total_records = 0
    months_processed = 0
    errors = []

    for year in range(2007, current_year + 1):
        print(f"\n=== Year {year} ===")

        for month in range(1, 13):
            # Skip future months dynamically
            if year == current_year and month > current_month:
                continue

            try:
                df = process_month(year, month)

                if df is not None and len(df) > 0:
                    upload_parquet(df, year, month)
                    total_enrollment += df['enrollment'].sum()
                    total_records += len(df)
                    months_processed += 1
            except Exception as e:
                print(f"    [ERROR] {year}-{month:02d}: {e}")
                errors.append({'year': year, 'month': month, 'error': str(e)})

    # Finish audit
    if audit:
        try:
            audit.finish_run(
                success=len(errors) == 0,
                output_tables=['fact_enrollment_unified'],
                output_row_count=total_records,
                error_message=str(errors) if errors else None
            )
        except Exception as e:
            print(f"[WARN] Could not finish audit: {e}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Months processed: {months_processed}")
    print(f"Total records: {total_records:,}")
    print(f"Total enrollment: {total_enrollment:,}")
    if errors:
        print(f"Errors: {len(errors)}")
    print(f"Finished: {datetime.now()}")

    return len(errors) == 0


if __name__ == '__main__':
    main()
