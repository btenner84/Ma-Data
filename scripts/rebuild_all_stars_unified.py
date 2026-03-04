#!/usr/bin/env python3
"""
Rebuild ALL unified stars tables with proper parsing for all year formats.
Handles the varying CMS file structures from 2007-2026.
"""

import boto3
import pandas as pd
import numpy as np
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def upload_parquet(df: pd.DataFrame, s3_key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"  Uploaded: {s3_key} ({len(df):,} rows)")


def load_parquet(key):
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def list_parquet_files(prefix):
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                files.append(obj['Key'])
    return files


def parse_star_rating(value):
    """Parse star rating from various formats."""
    if pd.isna(value):
        return None
    val_str = str(value).strip().lower()

    # Skip non-rating values
    skip_patterns = ['not enough', 'too new', 'not applicable', 'n/a', 'plan too new',
                     'not available', 'under review', 'nan', '']
    if any(p in val_str for p in skip_patterns):
        return None

    # Try to extract numeric rating
    # Handle "X out of 5 stars" format
    match = re.search(r'([\d.]+)\s*(?:out\s*of\s*5)?', val_str)
    if match:
        try:
            rating = float(match.group(1))
            if 1 <= rating <= 5:
                return rating
        except:
            pass

    # Try direct numeric
    try:
        rating = float(val_str)
        if 1 <= rating <= 5:
            return rating
    except:
        pass

    return None


def find_contract_column(df):
    """Find the contract ID column."""
    for col in df.columns:
        col_str = str(col).lower()
        if 'contract' in col_str and ('number' in col_str or 'id' in col_str or col_str == 'contract'):
            return col
    # Fallback: first column that starts with H/R/E patterns
    for col in df.columns:
        sample = df[col].dropna().head(5).astype(str)
        if sample.str.match(r'^[HRE]\d{4}').any():
            return col
    return df.columns[0]


# ============================================================
# DOMAIN SCORES
# ============================================================
def build_unified_domains():
    """Build unified domain scores for all years."""
    print("\n" + "=" * 60)
    print("BUILDING UNIFIED DOMAIN SCORES")
    print("=" * 60)

    all_domains = []
    files = [f for f in list_parquet_files('processed/stars/') if '/domain.parquet' in f]

    for s3_key in sorted(files):
        try:
            year = int(s3_key.split('/')[2])
            df = load_parquet(s3_key)

            contract_col = find_contract_column(df)

            # Find domain columns - various naming conventions
            domain_cols = []
            for col in df.columns:
                col_str = str(col)
                col_lower = col_str.lower()
                # Modern format: HD1:, HD2:, DD1:, etc.
                if re.match(r'^[HD]D\d:', col_str):
                    domain_cols.append((col, col_str.split(':')[0]))
                # Older format: descriptive names
                elif any(x in col_lower for x in ['staying healthy', 'chronic', 'member experience',
                                                   'complaints', 'customer service', 'drug plan',
                                                   'getting care', 'managing', 'health plan']):
                    # Create a domain ID based on the column
                    if 'staying healthy' in col_lower or 'screenings' in col_lower:
                        domain_id = 'HD1'
                    elif 'chronic' in col_lower or 'managing' in col_lower:
                        domain_id = 'HD2'
                    elif 'member experience' in col_lower and 'health' in col_lower:
                        domain_id = 'HD3'
                    elif 'complaints' in col_lower and 'health' in col_lower:
                        domain_id = 'HD4'
                    elif 'customer service' in col_lower or ('drug' in col_lower and 'service' in col_lower):
                        domain_id = 'DD1'
                    elif 'complaints' in col_lower and 'drug' in col_lower:
                        domain_id = 'DD2'
                    elif 'member experience' in col_lower and 'drug' in col_lower:
                        domain_id = 'DD3'
                    elif 'safety' in col_lower or 'pricing' in col_lower:
                        domain_id = 'DD4'
                    else:
                        domain_id = f'D{len(domain_cols)+1}'
                    domain_cols.append((col, domain_id))

            rows_added = 0
            for _, row in df.iterrows():
                contract_id = str(row[contract_col]).strip()
                if not re.match(r'^[HRE]\d{4}', contract_id):
                    continue

                for col, domain_id in domain_cols:
                    rating = parse_star_rating(row[col])
                    if rating is not None:
                        all_domains.append({
                            'year': year,
                            'contract_id': contract_id,
                            'domain_id': domain_id,
                            'domain_name': str(col),
                            'star_rating': rating
                        })
                        rows_added += 1

            print(f"  {year}: {len(df)} contracts, {rows_added} domain scores")

        except Exception as e:
            print(f"  {s3_key}: ERROR - {e}")

    if all_domains:
        unified = pd.DataFrame(all_domains)
        unified = unified.drop_duplicates()
        upload_parquet(unified, 'processed/unified/domain_scores_all_years.parquet')
        return unified
    return None


# ============================================================
# CUT POINTS
# ============================================================
def build_unified_cutpoints():
    """Build unified cut points for all years."""
    print("\n" + "=" * 60)
    print("BUILDING UNIFIED CUT POINTS")
    print("=" * 60)

    all_cutpoints = []
    files = [f for f in list_parquet_files('processed/stars/') if 'cut_point' in f.lower()]

    for s3_key in sorted(files):
        try:
            year = int(s3_key.split('/')[2])
            cut_type = 'part_c' if 'cut_points_c' in s3_key else 'part_d'
            df = load_parquet(s3_key)

            # Different parsing strategies based on year/format
            cutpoints_found = 0

            # Strategy 1: Modern format (2019+) - rows have "1star", "2star" etc.
            star_col = None
            for col in df.columns:
                sample = df[col].astype(str).str.lower()
                if sample.str.contains('1star|2star|1 star|2 star', regex=True).any():
                    star_col = col
                    break

            if star_col:
                # Find measure columns (C01:, C02:, D01:, etc.)
                measure_cols = []
                for col in df.columns:
                    col_str = str(col)
                    if re.match(r'^[CD]\d{2}:', col_str):
                        measure_cols.append((col, col_str.split(':')[0]))

                # Also check first row for measure IDs
                if not measure_cols and len(df) > 0:
                    for col in df.columns:
                        first_val = str(df.iloc[0][col]) if pd.notna(df.iloc[0][col]) else ''
                        if re.match(r'^[CD]\d{2}:', first_val):
                            measure_cols.append((col, first_val.split(':')[0]))

                for _, row in df.iterrows():
                    star_val = str(row[star_col]).lower().strip()
                    star_match = re.search(r'(\d)\s*star', star_val)
                    if star_match:
                        star_level = int(star_match.group(1))
                        for col, measure_id in measure_cols:
                            cut_value = row[col]
                            if pd.notna(cut_value) and str(cut_value).strip():
                                all_cutpoints.append({
                                    'year': year,
                                    'cut_type': cut_type,
                                    'measure_id': measure_id,
                                    'star_level': star_level,
                                    'threshold_text': str(cut_value).strip()
                                })
                                cutpoints_found += 1

            # Strategy 2: Older format - stars in first column as "5", "4", "3", etc.
            else:
                # Find the row with measure names
                measure_row = None
                star_rows = {}

                for idx, row in df.iterrows():
                    first_val = str(row.iloc[0]).strip()
                    # Check if this is the star level indicator
                    if first_val in ['5', '4', '3', '2', '1']:
                        star_rows[int(first_val)] = idx
                    # Check if this has "Number of Stars" or measure names
                    row_str = ' '.join(str(v) for v in row.values if pd.notna(v))
                    if 'number of stars' in row_str.lower() or 'stars displayed' in row_str.lower():
                        measure_row = idx

                if measure_row is not None and star_rows:
                    # Get measure names from the row after measure_row or from columns
                    measure_cols = []
                    for i, col in enumerate(df.columns[1:], 1):
                        col_str = str(col)
                        # Use column name or value from measure row
                        if measure_row is not None and measure_row < len(df):
                            val = df.iloc[measure_row].iloc[i] if i < len(df.iloc[measure_row]) else col_str
                            measure_name = str(val) if pd.notna(val) else col_str
                        else:
                            measure_name = col_str

                        if measure_name and 'unnamed' not in measure_name.lower():
                            # Create measure ID
                            measure_id = f"{'C' if cut_type == 'part_c' else 'D'}{i:02d}"
                            measure_cols.append((i, measure_id, measure_name))

                    for star_level, row_idx in star_rows.items():
                        row = df.iloc[row_idx]
                        for col_idx, measure_id, measure_name in measure_cols:
                            if col_idx < len(row):
                                cut_value = row.iloc[col_idx]
                                if pd.notna(cut_value) and str(cut_value).strip():
                                    all_cutpoints.append({
                                        'year': year,
                                        'cut_type': cut_type,
                                        'measure_id': measure_id,
                                        'star_level': star_level,
                                        'threshold_text': str(cut_value).strip()
                                    })
                                    cutpoints_found += 1

            print(f"  {year} {cut_type}: {cutpoints_found} cutpoints")

        except Exception as e:
            print(f"  {s3_key}: ERROR - {e}")

    if all_cutpoints:
        unified = pd.DataFrame(all_cutpoints)
        unified = unified.drop_duplicates()
        upload_parquet(unified, 'processed/unified/cut_points_all_years.parquet')
        return unified
    return None


# ============================================================
# MEASURE STARS / PERFORMANCE
# ============================================================
def build_unified_measure_performance():
    """Build unified measure performance for all years."""
    print("\n" + "=" * 60)
    print("BUILDING UNIFIED MEASURE PERFORMANCE")
    print("=" * 60)

    all_measures = []

    # Find all measure-related files
    files = list_parquet_files('processed/stars/')
    measure_files = [f for f in files if any(x in f.lower() for x in ['measure_star', 'measure_data', 'display'])]

    for s3_key in sorted(measure_files):
        try:
            parts = s3_key.split('/')
            year = int(parts[2])
            df = load_parquet(s3_key)

            contract_col = find_contract_column(df)

            # Find measure columns - look for C01, C02, D01, etc. patterns
            measure_cols = []
            for col in df.columns:
                col_str = str(col)
                # Direct measure ID in column name
                if re.match(r'^[CD]\d{2}:', col_str):
                    measure_id = col_str.split(':')[0]
                    measure_cols.append((col, measure_id))
                # Check first row for measure IDs
                elif len(df) > 0:
                    first_val = str(df.iloc[0][col]) if pd.notna(df.iloc[0][col]) else ''
                    if re.match(r'^[CD]\d{2}', first_val):
                        measure_id = first_val[:3]
                        measure_cols.append((col, measure_id))

            # If no measure columns found, try to identify them by content
            if not measure_cols:
                for col in df.columns:
                    col_lower = str(col).lower()
                    # Skip metadata columns
                    if any(x in col_lower for x in ['contract', 'organization', 'parent', 'type', 'name', 'snp']):
                        continue
                    # Check if column has star ratings (1-5)
                    try:
                        vals = df[col].dropna()
                        if len(vals) > 0:
                            numeric_vals = pd.to_numeric(vals, errors='coerce').dropna()
                            if len(numeric_vals) > 0 and numeric_vals.between(1, 5).all():
                                measure_cols.append((col, str(col)[:20]))
                    except:
                        pass

            rows_added = 0
            # Find where data rows start (skip header rows)
            data_start = 0
            for idx, row in df.iterrows():
                contract_val = str(row[contract_col])
                if re.match(r'^[HRE]\d{4}', contract_val):
                    data_start = idx
                    break

            for _, row in df.iloc[data_start:].iterrows():
                contract_id = str(row[contract_col]).strip()
                if not re.match(r'^[HRE]\d{4}', contract_id):
                    continue

                for col, measure_id in measure_cols:
                    value = row[col]
                    rating = parse_star_rating(value)

                    all_measures.append({
                        'year': year,
                        'contract_id': contract_id,
                        'measure_id': measure_id,
                        'star_rating': rating,
                        'raw_value': str(value) if pd.notna(value) else None
                    })
                    rows_added += 1

            if rows_added > 0:
                print(f"  {year} ({parts[3]}): {rows_added} measure records")

        except Exception as e:
            print(f"  {s3_key}: ERROR - {e}")

    if all_measures:
        unified = pd.DataFrame(all_measures)
        # Remove rows with no useful data
        unified = unified[unified['star_rating'].notna() | unified['raw_value'].notna()]
        unified = unified.drop_duplicates()
        upload_parquet(unified, 'processed/unified/measure_performance_all_years.parquet')

        # Also create summary by measure
        with_ratings = unified[unified['star_rating'].notna()]
        if len(with_ratings) > 0:
            summary = with_ratings.groupby(['year', 'measure_id']).agg({
                'star_rating': ['mean', 'count'],
                'contract_id': 'nunique'
            }).reset_index()
            summary.columns = ['year', 'measure_id', 'avg_stars', 'rating_count', 'contract_count']
            upload_parquet(summary, 'processed/unified/measure_summary_by_year.parquet')

        return unified
    return None


# ============================================================
# CAI (Categorical Adjustment Index)
# ============================================================
def build_unified_cai():
    """Build unified CAI data for all years."""
    print("\n" + "=" * 60)
    print("BUILDING UNIFIED CAI")
    print("=" * 60)

    all_cai = []
    files = [f for f in list_parquet_files('processed/stars/') if '/cai.parquet' in f]

    for s3_key in sorted(files):
        try:
            year = int(s3_key.split('/')[2])
            df = load_parquet(s3_key)

            contract_col = find_contract_column(df)

            # Find FAC (Factor) columns
            fac_cols = [col for col in df.columns if 'fac' in str(col).lower()]

            for _, row in df.iterrows():
                contract_id = str(row[contract_col]).strip()
                if not re.match(r'^[HRE]\d{4}', contract_id):
                    continue

                record = {'year': year, 'contract_id': contract_id}
                for col in fac_cols:
                    col_name = str(col).lower().replace(' ', '_')
                    try:
                        record[col_name] = float(row[col])
                    except:
                        record[col_name] = None

                all_cai.append(record)

            print(f"  {year}: {len(df)} contracts")

        except Exception as e:
            print(f"  {s3_key}: ERROR - {e}")

    if all_cai:
        unified = pd.DataFrame(all_cai)
        unified = unified.drop_duplicates()
        upload_parquet(unified, 'processed/unified/cai_all_years.parquet')
        return unified
    return None


def main():
    print("=" * 70)
    print("REBUILDING ALL UNIFIED STARS TABLES")
    print("=" * 70)

    domains = build_unified_domains()
    cutpoints = build_unified_cutpoints()
    measures = build_unified_measure_performance()
    cai = build_unified_cai()

    print("\n" + "=" * 70)
    print("REBUILD COMPLETE")
    print("=" * 70)

    # Summary
    print("\nFinal coverage:")
    for name, df in [('domain_scores', domains), ('cut_points', cutpoints),
                     ('measure_performance', measures), ('cai', cai)]:
        if df is not None:
            years = sorted(df['year'].unique())
            print(f"  {name}: {min(years)}-{max(years)} ({len(years)} years), {len(df):,} rows")


if __name__ == '__main__':
    main()
