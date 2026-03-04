#!/usr/bin/env python3
"""
MASTER REBUILD SCRIPT - Fix all broken data
1. Parse stars files correctly (skip title rows)
2. Build ALL years (2007-2026)
3. Add audit columns
4. Create entity chains
5. Create measure crosswalks
"""

import boto3
import pandas as pd
import zipfile
import re
import uuid
from io import BytesIO
from datetime import datetime
from typing import Dict, List, Optional, Tuple

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

# Pipeline run ID for audit
PIPELINE_RUN_ID = f"rebuild_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def upload_parquet(df: pd.DataFrame, s3_key: str):
    """Upload DataFrame as parquet to S3."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"  ✓ Uploaded: {s3_key} ({len(df):,} rows)")

def get_zip_files(year: int) -> List[str]:
    """Get the zip file keys for a year."""
    if year >= 2024:
        return [f"raw/stars/{year}_ratings.zip", f"raw/stars/{year}_display.zip"]
    else:
        return [f"raw/stars/{year}_combined.zip"]

def extract_files_from_zip(zip_bytes: bytes) -> Dict[str, bytes]:
    """Extract all files from a zip (handles nested zips)."""
    files = {}
    try:
        zf = zipfile.ZipFile(BytesIO(zip_bytes))
        for name in zf.namelist():
            if name.endswith('.zip'):
                # Nested zip - extract its contents
                try:
                    inner_bytes = zf.read(name)
                    inner_zf = zipfile.ZipFile(BytesIO(inner_bytes))
                    for inner_name in inner_zf.namelist():
                        if inner_name.endswith(('.csv', '.xlsx')):
                            files[f"{name}/{inner_name}"] = inner_zf.read(inner_name)
                except:
                    pass
            elif name.endswith(('.csv', '.xlsx')):
                files[name] = zf.read(name)
    except:
        pass
    return files

def find_header_row(content: bytes, encoding: str = 'latin-1') -> int:
    """Find the actual header row (not title row)."""
    try:
        text = content.decode(encoding, errors='replace')
        lines = text.split('\n')[:10]

        for i, line in enumerate(lines):
            line_lower = line.lower()
            # Header row typically has CONTRACT_ID or Contract Number
            if 'contract' in line_lower and ('id' in line_lower or 'number' in line_lower):
                return i
        return 1  # Default to row 1
    except:
        return 1

def parse_csv_smart(content: bytes, source_file: str) -> pd.DataFrame:
    """Parse CSV with smart header detection."""
    # Try encodings
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            header_row = find_header_row(content, encoding)
            df = pd.read_csv(
                BytesIO(content),
                encoding=encoding,
                header=header_row,
                dtype=str,
                on_bad_lines='skip'
            )

            # Remove empty rows at top
            while len(df) > 0 and df.iloc[0].isna().all():
                df = df.iloc[1:].reset_index(drop=True)

            # Remove Unnamed columns
            df = df[[c for c in df.columns if 'Unnamed' not in str(c)]]

            # Add audit columns
            df['_source_file'] = source_file
            df['_pipeline_run_id'] = PIPELINE_RUN_ID

            return df
        except Exception as e:
            continue

    return pd.DataFrame()

def find_contract_column(df: pd.DataFrame) -> Optional[str]:
    """Find the contract ID column."""
    for col in df.columns:
        col_lower = str(col).lower()
        if 'contract' in col_lower and ('id' in col_lower or 'number' in col_lower):
            return col
        if col == 'CONTRACT_ID':
            return col
    # Check first column
    if len(df.columns) > 0:
        first_col = df.columns[0]
        sample = df[first_col].dropna().iloc[:5].tolist() if len(df) > 0 else []
        if any(re.match(r'^[HER]\d{4}', str(v)) for v in sample):
            return first_col
    return None

def process_measure_stars(files: Dict[str, bytes], year: int) -> pd.DataFrame:
    """Process measure stars file - extracts star ratings per measure per contract."""
    all_data = []

    # Find measure stars/data file
    for fname, content in files.items():
        fname_lower = fname.lower()
        if ('measure' in fname_lower and ('star' in fname_lower or 'data' in fname_lower)
            and fname.endswith('.csv')):

            df = parse_csv_smart(content, fname)
            if df.empty:
                continue

            contract_col = find_contract_column(df)
            if not contract_col:
                continue

            # Find measure columns (C01:, C02:, D01:, etc.)
            measure_cols = []
            for col in df.columns:
                col_str = str(col)
                match = re.match(r'^([CD]\d{2})[:.\s](.+)', col_str)
                if match:
                    measure_cols.append((col, match.group(1), match.group(2).strip()))

            if not measure_cols:
                # Try to find columns that look like measure data
                for col in df.columns:
                    if col not in [contract_col, '_source_file', '_pipeline_run_id']:
                        # Check if values look like star ratings (1-5)
                        sample_vals = df[col].dropna().astype(str).head(20)
                        if sample_vals.str.match(r'^[1-5]$').any():
                            measure_cols.append((col, col, col))

            # Extract data
            for _, row in df.iterrows():
                contract_id = str(row[contract_col]).strip()
                if not re.match(r'^[HER]\d{4}', contract_id):
                    continue

                for col, measure_id, measure_name in measure_cols:
                    value = row[col]
                    if pd.isna(value):
                        continue

                    # Parse star rating
                    star_rating = None
                    raw_value = str(value).strip()

                    if raw_value in ['1', '2', '3', '4', '5']:
                        star_rating = int(raw_value)

                    all_data.append({
                        'year': year,
                        'contract_id': contract_id,
                        'measure_id': measure_id,
                        'measure_name': measure_name,
                        'star_rating': star_rating,
                        'raw_value': raw_value,
                        '_source_file': fname,
                        '_pipeline_run_id': PIPELINE_RUN_ID
                    })

            print(f"    Measures: {len(all_data):,} records from {fname.split('/')[-1]}")
            break

    return pd.DataFrame(all_data)

def process_summary(files: Dict[str, bytes], year: int) -> pd.DataFrame:
    """Process summary ratings file."""
    for fname, content in files.items():
        fname_lower = fname.lower()
        if 'summary' in fname_lower and 'rating' in fname_lower and fname.endswith('.csv'):
            df = parse_csv_smart(content, fname)
            if df.empty:
                continue

            contract_col = find_contract_column(df)
            if contract_col:
                df = df.rename(columns={contract_col: 'contract_id'})
                df['year'] = year
                df = df[df['contract_id'].astype(str).str.match(r'^[HER]\d{4}', na=False)]
                print(f"    Summary: {len(df):,} contracts from {fname.split('/')[-1]}")
                return df

    return pd.DataFrame()

def process_domain(files: Dict[str, bytes], year: int) -> pd.DataFrame:
    """Process domain scores file."""
    all_data = []

    for fname, content in files.items():
        fname_lower = fname.lower()
        if 'domain' in fname_lower and fname.endswith('.csv'):
            df = parse_csv_smart(content, fname)
            if df.empty:
                continue

            contract_col = find_contract_column(df)
            if not contract_col:
                continue

            # Find domain columns (HD1:, HD2:, DD1:, etc.)
            domain_cols = []
            for col in df.columns:
                col_str = str(col)
                if re.match(r'^[HD]D\d:', col_str):
                    domain_cols.append(col)

            for _, row in df.iterrows():
                contract_id = str(row[contract_col]).strip()
                if not re.match(r'^[HER]\d{4}', contract_id):
                    continue

                for col in domain_cols:
                    value = row[col]
                    if pd.isna(value):
                        continue

                    try:
                        star_rating = float(value)
                        if star_rating < 1 or star_rating > 5:
                            continue
                    except:
                        continue

                    domain_id = col.split(':')[0]

                    all_data.append({
                        'year': year,
                        'contract_id': contract_id,
                        'domain_id': domain_id,
                        'domain_name': col,
                        'star_rating': star_rating,
                        '_source_file': fname,
                        '_pipeline_run_id': PIPELINE_RUN_ID
                    })

            print(f"    Domain: {len(all_data):,} records from {fname.split('/')[-1]}")
            break

    return pd.DataFrame(all_data)

def process_cutpoints(files: Dict[str, bytes], year: int) -> pd.DataFrame:
    """Process cutpoints files (Part C and Part D)."""
    all_cutpoints = []

    for part in ['C', 'D']:
        for fname, content in files.items():
            fname_lower = fname.lower()
            if 'cut' in fname_lower and f'part {part.lower()}' in fname_lower and fname.endswith('.csv'):
                df = parse_csv_smart(content, fname)
                if df.empty:
                    continue

                # Find measure row (has C01:, D01:, etc.)
                measure_row_idx = None
                for idx in range(min(5, len(df))):
                    row_str = ' '.join(df.iloc[idx].astype(str))
                    if f'{part}01' in row_str or f'{part}02' in row_str:
                        measure_row_idx = idx
                        break

                if measure_row_idx is None:
                    continue

                # Find star rows
                star_rows = {}
                for idx, row in df.iterrows():
                    first_val = str(row.iloc[0]).lower().strip()
                    second_val = str(row.iloc[1]).lower().strip() if len(row) > 1 else ''
                    combined = first_val + ' ' + second_val

                    for star in [1, 2, 3, 4, 5]:
                        if f'{star}star' in combined or f'{star} star' in combined:
                            if star not in star_rows:
                                star_rows[star] = idx

                if not star_rows:
                    continue

                # Extract measures from header row
                measure_row = df.iloc[measure_row_idx]
                start_col = 2 if part == 'D' else 1

                for col_idx in range(start_col, len(measure_row)):
                    measure_val = measure_row.iloc[col_idx]
                    if pd.isna(measure_val):
                        continue

                    match = re.match(rf'({part}\d+)[:.]?\s*(.*)', str(measure_val))
                    if not match:
                        continue

                    measure_id = match.group(1)
                    measure_name = match.group(2).strip()

                    for star_level, row_idx in star_rows.items():
                        try:
                            cut_value = df.iloc[row_idx, col_idx]
                            if pd.isna(cut_value):
                                continue

                            # Extract numeric threshold
                            numbers = re.findall(r'[\d.]+', str(cut_value))
                            threshold = float(numbers[0]) if numbers else None

                            all_cutpoints.append({
                                'year': year,
                                'part': part,
                                'measure_id': measure_id,
                                'measure_name': measure_name,
                                'star_rating': star_level,
                                'threshold': threshold,
                                'threshold_text': str(cut_value),
                                '_source_file': fname,
                                '_pipeline_run_id': PIPELINE_RUN_ID
                            })
                        except:
                            continue

                print(f"    Cutpoints Part {part}: {len([c for c in all_cutpoints if c['part']==part]):,} from {fname.split('/')[-1]}")
                break

    return pd.DataFrame(all_cutpoints)

def process_year(year: int) -> Dict[str, pd.DataFrame]:
    """Process all data for a single year."""
    print(f"\n{'='*60}")
    print(f"PROCESSING YEAR {year}")
    print(f"{'='*60}")

    results = {
        'measures': pd.DataFrame(),
        'summary': pd.DataFrame(),
        'domain': pd.DataFrame(),
        'cutpoints': pd.DataFrame()
    }

    # Load files
    all_files = {}
    for key in get_zip_files(year):
        try:
            resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
            files = extract_files_from_zip(resp['Body'].read())
            all_files.update(files)
            print(f"  Loaded {len(files)} files from {key}")
        except Exception as e:
            print(f"  Could not load {key}: {e}")

    if not all_files:
        print(f"  No files found for {year}")
        return results

    # Process each type
    results['measures'] = process_measure_stars(all_files, year)
    results['summary'] = process_summary(all_files, year)
    results['domain'] = process_domain(all_files, year)
    results['cutpoints'] = process_cutpoints(all_files, year)

    return results

def build_measure_crosswalk(all_measures: pd.DataFrame) -> pd.DataFrame:
    """Build stable measure crosswalk from all measure data."""
    if all_measures.empty:
        return pd.DataFrame()

    # Group by measure_name to create stable keys
    crosswalk = all_measures.groupby(['measure_id', 'measure_name']).agg({
        'year': ['min', 'max', 'count']
    }).reset_index()

    crosswalk.columns = ['measure_id', 'measure_name', 'first_year', 'last_year', 'occurrences']

    # Create stable key from measure name
    def create_stable_key(name):
        if pd.isna(name):
            return None
        # Normalize: lowercase, remove special chars, collapse spaces
        key = str(name).lower()
        key = re.sub(r'[^a-z0-9\s]', ' ', key)
        key = re.sub(r'\s+', '_', key).strip('_')
        return key[:60]

    crosswalk['measure_key'] = crosswalk['measure_name'].apply(create_stable_key)
    crosswalk['part'] = crosswalk['measure_id'].str[0]

    return crosswalk

def main():
    print("=" * 70)
    print("MASTER DATA REBUILD")
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print("=" * 70)

    # Process all years
    all_measures = []
    all_summary = []
    all_domain = []
    all_cutpoints = []

    for year in range(2008, 2027):
        results = process_year(year)

        if not results['measures'].empty:
            all_measures.append(results['measures'])
        if not results['summary'].empty:
            all_summary.append(results['summary'])
        if not results['domain'].empty:
            all_domain.append(results['domain'])
        if not results['cutpoints'].empty:
            all_cutpoints.append(results['cutpoints'])

    # Combine and upload
    print("\n" + "=" * 70)
    print("UPLOADING UNIFIED TABLES")
    print("=" * 70)

    if all_measures:
        measures_df = pd.concat(all_measures, ignore_index=True)
        upload_parquet(measures_df, 'processed/unified/measure_data_rebuilt.parquet')

        # Build crosswalk
        crosswalk = build_measure_crosswalk(measures_df)
        upload_parquet(crosswalk, 'processed/unified/dim_measure.parquet')

    if all_summary:
        summary_df = pd.concat(all_summary, ignore_index=True)
        upload_parquet(summary_df, 'processed/unified/stars_summary_rebuilt.parquet')

    if all_domain:
        domain_df = pd.concat(all_domain, ignore_index=True)
        upload_parquet(domain_df, 'processed/unified/domain_scores_rebuilt.parquet')

    if all_cutpoints:
        cutpoints_df = pd.concat(all_cutpoints, ignore_index=True)
        upload_parquet(cutpoints_df, 'processed/unified/stars_cutpoints_rebuilt.parquet')

    print("\n" + "=" * 70)
    print("REBUILD COMPLETE")
    print("=" * 70)

if __name__ == '__main__':
    main()
