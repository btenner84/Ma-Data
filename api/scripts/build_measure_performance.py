"""
Build measure performance data from CMS star ratings data tables.
Extracts actual performance percentages (not just star ratings) for each contract/measure/year.

Output:
  - s3://ma-data123/processed/stars/measure_performance/data.parquet
  - Validation reports printed to console for manual verification

VALIDATION APPROACH:
  1. For each year, print sample contracts with their raw values
  2. Print aggregate stats that can be verified against CMS files
  3. Flag any data anomalies
"""

import boto3
import pandas as pd
import zipfile
import re
from io import BytesIO
from typing import Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def load_parquet(s3_key: str) -> pd.DataFrame:
    """Load parquet file from S3."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"Error loading {s3_key}: {e}")
        return pd.DataFrame()


def save_parquet(df: pd.DataFrame, s3_key: str):
    """Save dataframe to S3 as parquet."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"Saved {len(df)} rows to {s3_key}")


def extract_percentage(value) -> Optional[float]:
    """Extract numeric percentage from value like '76%' or '76'."""
    if pd.isna(value):
        return None
    val_str = str(value).strip()

    # Match patterns like "76%", "76.5%", "76"
    match = re.match(r'^(\d+\.?\d*)%?$', val_str)
    if match:
        return float(match.group(1))
    return None


def create_measure_key(measure_id: str, measure_name: str) -> str:
    """
    Create stable measure key from name (IDs change year over year).

    Uses same logic as build_cutpoints.py for consistency.
    """
    if pd.isna(measure_name) or not measure_name:
        return ""

    # Aggressive normalization - remove ALL special chars, lowercase
    def normalize(s):
        s = str(s).lower()
        # Remove any non-alphanumeric (keeps letters, numbers, spaces)
        s = re.sub(r'[^a-z0-9\s]', ' ', s)
        # Collapse multiple spaces
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    name_norm = normalize(measure_name)

    # Canonical name mappings - same as build_cutpoints.py
    canonical_mappings = {
        'controlling blood pressure': 'controlling_blood_pressure',
        'controlling high blood pressure': 'controlling_blood_pressure',
        'diabetes care blood sugar controlled': 'diabetes_blood_sugar_controlled',
        'diabetes care eye exam': 'diabetes_eye_exam',
        'diabetes care kidney disease monitoring': 'diabetes_kidney_disease_monitoring',
        'care for older adults medication review': 'care_for_older_adults_medication_review',
        'care for older adults pain assessment': 'care_for_older_adults_pain_assessment',
        'care for older adults functional status assessment': 'care_for_older_adults_functional_status',
        'call center foreign language interpreter and tty availability': 'call_center_foreign_language_tty',
    }

    # Check for canonical mapping
    if name_norm in canonical_mappings:
        return canonical_mappings[name_norm]

    # Default: create key from normalized name
    key = name_norm.replace(' ', '_')
    return key[:60]


def parse_measure_data_csv(zip_bytes: bytes, year: int) -> Tuple[pd.DataFrame, dict]:
    """
    Parse measure data from CMS zip file.
    Returns (dataframe, validation_stats).

    CMS files have a multi-row header:
    - Row 0: Title
    - Row 1: CONTRACT_ID, Org Type, etc., Domain names
    - Row 2: Measure IDs (C01, C02, etc.)
    - Row 3: Date ranges
    - Row 4+: Data
    """
    validation = {
        'year': year,
        'total_contracts': 0,
        'contracts_with_data': 0,
        'measures_found': [],
        'sample_contracts': [],  # For manual verification
    }

    with zipfile.ZipFile(BytesIO(zip_bytes)) as z:
        # Find measure data file
        measure_file = None
        for f in z.namelist():
            if 'measure data' in f.lower() and f.endswith('.csv'):
                measure_file = f
                break

        # Also check nested zips
        if not measure_file:
            for f in z.namelist():
                if f.endswith('.zip'):
                    with z.open(f) as nested_zip_file:
                        try:
                            nested_bytes = nested_zip_file.read()
                            with zipfile.ZipFile(BytesIO(nested_bytes)) as nested_z:
                                for nf in nested_z.namelist():
                                    if 'measure data' in nf.lower() and nf.endswith('.csv'):
                                        measure_file = nf
                                        # Re-open and read the nested file
                                        with nested_z.open(nf) as csvf:
                                            csv_bytes = csvf.read()
                                        return parse_measure_data_from_bytes(csv_bytes, year, nf, validation)
                        except:
                            continue

        if not measure_file:
            print(f"  WARNING: No measure data file found for {year}")
            return pd.DataFrame(), validation

        print(f"  Reading: {measure_file}")

        with z.open(measure_file) as csvf:
            csv_bytes = csvf.read()

        return parse_measure_data_from_bytes(csv_bytes, year, measure_file, validation)


def parse_measure_data_from_bytes(csv_bytes: bytes, year: int, filename: str, validation: dict) -> Tuple[pd.DataFrame, dict]:
    """Parse measure data from CSV bytes."""
    # Read raw to understand structure
    raw_df = pd.read_csv(BytesIO(csv_bytes), encoding='latin-1', header=None, nrows=10)

    # Find rows with key info
    contract_row = None  # Row with CONTRACT_ID
    measure_row = None   # Row with C01, C02, etc.

    for idx, row in raw_df.iterrows():
        row_str = ' '.join(str(v) for v in row.values if pd.notna(v))
        if 'CONTRACT_ID' in row_str.upper():
            contract_row = idx
        if re.search(r'C01[:\s]', row_str):
            measure_row = idx

    if contract_row is None:
        print(f"  WARNING: Could not find CONTRACT_ID row for {year}")
        return pd.DataFrame(), validation

    # Build column names from contract_row, but use measure_row for measure IDs
    df = pd.read_csv(BytesIO(csv_bytes), encoding='latin-1', header=None, skiprows=contract_row)

    # First row is now the header (CONTRACT_ID, etc.)
    base_header = df.iloc[0].fillna('')

    # If measure_row exists and is different, get measure IDs from there
    if measure_row is not None and measure_row != contract_row:
        measure_header = df.iloc[measure_row - contract_row].fillna('')
    else:
        measure_header = base_header

    # Build column names and extract measure names
    # The measure header row has format like "C01: Breast Cancer Screening"
    col_names = []
    measure_names_map = {}  # measure_id -> measure_name

    for i in range(len(base_header)):
        base = str(base_header.iloc[i]).strip()
        measure = str(measure_header.iloc[i]).strip() if i < len(measure_header) else ''

        # Check if measure header has a measure ID
        measure_match = re.match(r'([CD]\d{2})[:.]?\s*(.*)', measure)
        if measure_match:
            measure_id = measure_match.group(1)
            measure_name = measure_match.group(2).strip() if measure_match.group(2) else measure_id
            col_names.append(measure_id)
            measure_names_map[measure_id] = measure_name
        elif base:
            col_names.append(base)
        else:
            col_names.append(f'col_{i}')

    # Skip header rows and set column names
    data_start = 1
    if measure_row is not None and measure_row != contract_row:
        data_start = max(data_start, measure_row - contract_row + 1)
    # Also skip date range row if present
    data_start = max(data_start, 3)  # Usually 3 header rows

    df = df.iloc[data_start:].copy()
    df.columns = col_names

    # Find key columns
    contract_col = None
    for col in df.columns:
        if 'CONTRACT' in str(col).upper():
            contract_col = col
            break
    if contract_col is None:
        contract_col = df.columns[0]

    parent_col = None
    for col in df.columns:
        if 'PARENT' in str(col).upper():
            parent_col = col
            break

    org_type_col = None
    for col in df.columns:
        if 'ORGANIZATION' in str(col).upper() and 'TYPE' in str(col).upper():
            org_type_col = col
            break

    validation['total_contracts'] = len(df)

    # Identify measure columns (C01, C02, D01, etc.)
    # measure_names_map was populated earlier when building col_names
    measure_cols = {}
    for col in df.columns:
        if re.match(r'^[CD]\d{2}$', str(col)):
            measure_cols[col] = col
            # If we didn't capture the name earlier, use the measure_id
            if col not in measure_names_map:
                measure_names_map[col] = col

    validation['measures_found'] = sorted(measure_cols.keys())
    if validation['measures_found']:
        print(f"  Found {len(measure_cols)} measures: {validation['measures_found'][:5]}...{validation['measures_found'][-3:]}")
    else:
        print(f"  WARNING: No measure columns found. Columns: {df.columns.tolist()[:10]}")
        return pd.DataFrame(), validation

    # Extract data for each contract and measure
    records = []
    contracts_with_any_data = set()

    for idx, row in df.iterrows():
        contract_id = str(row[contract_col]).strip() if pd.notna(row[contract_col]) else None
        if not contract_id or contract_id == 'nan':
            continue

        parent_org = str(row[parent_col]).strip() if parent_col and pd.notna(row[parent_col]) else None
        org_type = str(row[org_type_col]).strip() if org_type_col and pd.notna(row[org_type_col]) else None

        for measure_id in measure_cols:
            raw_value = row[measure_id]
            pct_value = extract_percentage(raw_value)

            if pct_value is not None:
                contracts_with_any_data.add(contract_id)
                measure_name = measure_names_map.get(measure_id, measure_id)
                measure_key = create_measure_key(measure_id, measure_name)
                records.append({
                    'year': year,
                    'contract_id': contract_id,
                    'parent_org': parent_org,
                    'org_type': org_type,
                    'measure_id': measure_id,
                    'measure_name': measure_name,
                    'measure_key': measure_key,
                    'performance_pct': pct_value,
                    'raw_value': str(raw_value),
                })

    validation['contracts_with_data'] = len(contracts_with_any_data)

    # Get sample contracts for validation (first 5 with data)
    result_df = pd.DataFrame(records)
    if not result_df.empty:
        sample_contracts = result_df['contract_id'].unique()[:5]
        for cid in sample_contracts:
            contract_data = result_df[result_df['contract_id'] == cid]
            c01_data = contract_data[contract_data['measure_id'] == 'C01']
            if not c01_data.empty:
                validation['sample_contracts'].append({
                    'contract_id': cid,
                    'C01_value': c01_data.iloc[0]['raw_value'],
                    'C01_pct': c01_data.iloc[0]['performance_pct'],
                })

    return result_df, validation


def print_validation_report(all_validations: list):
    """Print validation report for manual verification."""
    print("\n" + "=" * 70)
    print("VALIDATION REPORT - Check these against raw CMS files")
    print("=" * 70)

    for v in all_validations:
        print(f"\n{v['year']}:")
        print(f"  Total contracts in file: {v['total_contracts']}")
        print(f"  Contracts with numeric data: {v['contracts_with_data']}")
        print(f"  Measures found: {len(v['measures_found'])}")

        if v['sample_contracts']:
            print(f"  SAMPLE CONTRACTS (verify in CMS file):")
            for s in v['sample_contracts'][:3]:
                print(f"    {s['contract_id']}: C01 = {s['C01_value']} -> {s['C01_pct']}%")


def add_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    """Join with enrollment data for weighted averages.

    Uses monthly fact_enrollment files which have ALL contracts,
    not just those with overall star ratings.

    For each star year, tries January of that year first, then falls back
    to December of previous year for contracts not found.
    """
    print("\nAdding enrollment data...")

    try:
        # Get unique years in the measure data
        years_needed = sorted(df['year'].unique())
        print(f"  Years needed: {years_needed}")

        all_enrollment = []

        for year in years_needed:
            # Primary: January of the star year
            primary_key = f'processed/fact_enrollment/{year}/01/data.parquet'

            year_contracts = {}

            # Load primary
            try:
                primary_enroll = load_parquet(primary_key)
                if not primary_enroll.empty:
                    for cid, grp in primary_enroll.groupby('contract_id'):
                        total = grp['enrollment'].sum()
                        if total > 0:
                            year_contracts[cid] = total
                    print(f"    {year}: {len(year_contracts)} contracts from {primary_key}")
            except Exception as e:
                print(f"    {year}: Primary failed ({e})")

            # Load fallbacks for any missing contracts (try Dec of prev year, then Dec of 2 years ago)
            for fallback_year in [year - 1, year - 2]:
                fallback_key = f'processed/fact_enrollment/{fallback_year}/12/data.parquet'
                try:
                    fallback_enroll = load_parquet(fallback_key)
                    if not fallback_enroll.empty:
                        fallback_count = 0
                        for cid, grp in fallback_enroll.groupby('contract_id'):
                            if cid not in year_contracts:
                                total = grp['enrollment'].sum()
                                if total > 0:
                                    year_contracts[cid] = total
                                    fallback_count += 1
                        if fallback_count > 0:
                            print(f"    {year}: +{fallback_count} contracts from fallback {fallback_key}")
                except Exception as e:
                    pass  # Fallback is optional

            # Convert to dataframe
            if year_contracts:
                contract_enroll = pd.DataFrame([
                    {'contract_id': cid, 'enrollment': enroll, 'year': year}
                    for cid, enroll in year_contracts.items()
                ])
                all_enrollment.append(contract_enroll)

        if not all_enrollment:
            print("  WARNING: No enrollment data found")
            return df

        enrollment_agg = pd.concat(all_enrollment, ignore_index=True)

        # Merge
        df = df.merge(enrollment_agg, on=['contract_id', 'year'], how='left')

        matched = df['enrollment'].notna().sum()
        total = len(df)
        print(f"  Matched enrollment for {matched:,} of {total:,} records ({matched/total*100:.1f}%)")

        return df
    except Exception as e:
        print(f"  ERROR loading enrollment: {e}")
        return df


def add_measure_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Add measure metadata (part, lower_is_better, etc.) from cutpoints data.

    Note: measure_key is already created during extraction (name-based, stable across years).
    We only add other metadata columns here, not measure_key.
    """
    print("\nAdding measure metadata...")

    # Load 2026 cutpoints for additional metadata
    try:
        cutpoints = load_parquet('processed/stars/cutpoints/2026/data.parquet')
        if cutpoints.empty:
            print("  WARNING: No cutpoints data found")
            return df

        # Create mapping of measure_id to metadata (excluding measure_key since we already have it)
        # Note: We match on measure_id which is year-specific, so this only works for 2026 measures
        measure_map = cutpoints[['measure_id', 'part', 'lower_is_better', 'data_source', 'cutpoint_method']].drop_duplicates()

        # Merge - don't include measure_key or measure_name since we already have them from extraction
        df = df.merge(measure_map, on='measure_id', how='left')

        matched = df['part'].notna().sum()
        total = len(df)
        print(f"  Matched measure metadata for {matched:,} of {total:,} records ({matched/total*100:.1f}%)")

        return df
    except Exception as e:
        print(f"  ERROR loading cutpoints: {e}")
        return df


def compute_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pre-compute aggregates for faster API queries.
    Creates summary stats by year/measure for industry and by payer.

    IMPORTANT: Group by measure_key (stable name-based key), NOT measure_id
    because measure IDs change year-over-year in CMS data.
    """
    print("\nComputing aggregates...")

    # Ensure we have measure_key
    if 'measure_key' not in df.columns or df['measure_key'].isna().all():
        print("  WARNING: No measure_key found, falling back to measure_id")
        df['measure_key'] = df['measure_id'].str.lower()

    records = []

    # Industry aggregates by year/measure_key
    for (year, measure_key), group in df.groupby(['year', 'measure_key']):
        if pd.isna(measure_key) or not measure_key:
            continue

        valid = group[group['performance_pct'].notna()]
        if len(valid) == 0:
            continue

        # Simple average
        simple_avg = valid['performance_pct'].mean()

        # Weighted average (by enrollment)
        valid_with_enrollment = valid[valid['enrollment'].notna() & (valid['enrollment'] > 0)]
        if len(valid_with_enrollment) > 0:
            weighted_avg = (valid_with_enrollment['performance_pct'] * valid_with_enrollment['enrollment']).sum() / valid_with_enrollment['enrollment'].sum()
            total_enrollment = valid_with_enrollment['enrollment'].sum()
        else:
            weighted_avg = simple_avg
            total_enrollment = 0

        # Get the measure_id for this year (for display purposes)
        measure_id = valid['measure_id'].iloc[0]

        records.append({
            'year': year,
            'measure_key': measure_key,
            'measure_id': measure_id,  # Year-specific ID for display
            'parent_org': '_INDUSTRY_',
            'contract_count': len(valid),
            'simple_avg': round(simple_avg, 2),
            'weighted_avg': round(weighted_avg, 2),
            'total_enrollment': int(total_enrollment),
            'min_pct': valid['performance_pct'].min(),
            'max_pct': valid['performance_pct'].max(),
        })

    # Payer aggregates by year/measure_key/parent_org
    for (year, measure_key, parent_org), group in df.groupby(['year', 'measure_key', 'parent_org']):
        if pd.isna(parent_org) or pd.isna(measure_key) or not measure_key:
            continue

        valid = group[group['performance_pct'].notna()]
        if len(valid) == 0:
            continue

        simple_avg = valid['performance_pct'].mean()

        valid_with_enrollment = valid[valid['enrollment'].notna() & (valid['enrollment'] > 0)]
        if len(valid_with_enrollment) > 0:
            weighted_avg = (valid_with_enrollment['performance_pct'] * valid_with_enrollment['enrollment']).sum() / valid_with_enrollment['enrollment'].sum()
            total_enrollment = valid_with_enrollment['enrollment'].sum()
        else:
            weighted_avg = simple_avg
            total_enrollment = 0

        measure_id = valid['measure_id'].iloc[0]

        records.append({
            'year': year,
            'measure_key': measure_key,
            'measure_id': measure_id,
            'parent_org': parent_org,
            'contract_count': len(valid),
            'simple_avg': round(simple_avg, 2),
            'weighted_avg': round(weighted_avg, 2),
            'total_enrollment': int(total_enrollment),
            'min_pct': valid['performance_pct'].min(),
            'max_pct': valid['performance_pct'].max(),
        })

    agg_df = pd.DataFrame(records)
    print(f"  Created {len(agg_df)} aggregate records")

    return agg_df


def main():
    print("=" * 70)
    print("BUILDING MEASURE PERFORMANCE DATA")
    print("=" * 70)

    # Map years to their data table zip files
    year_files = {
        2026: 'docs/stars/data_tables/2026_star_ratings.zip',
        2025: 'docs/stars/data_tables/2025_star_ratings.zip',
        2024: 'docs/stars/data_tables/2024_star_ratings_data.zip',
        2023: 'docs/stars/data_tables/2023_star_ratings.zip',
        2022: 'docs/stars/data_tables/2022_star_ratings.zip',
        2021: 'docs/stars/data_tables/2021_star_ratings.zip',
        2020: 'docs/stars/data_tables/2020_star_ratings.zip',
        2019: 'docs/stars/data_tables/2019_star_ratings.zip',
    }

    all_data = []
    all_validations = []

    for year, zip_key in sorted(year_files.items(), reverse=True):
        print(f"\nProcessing {year}...")

        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=zip_key)
            zip_bytes = response['Body'].read()
        except Exception as e:
            print(f"  ERROR: Could not load {zip_key}: {e}")
            continue

        df, validation = parse_measure_data_csv(zip_bytes, year)

        if not df.empty:
            all_data.append(df)
            all_validations.append(validation)
            print(f"  Extracted {len(df):,} records")

    if not all_data:
        print("\nERROR: No data extracted!")
        return

    # Combine all years
    combined = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal records: {len(combined):,}")

    # Print validation report
    print_validation_report(all_validations)

    # Add enrollment data
    combined = add_enrollment_data(combined)

    # Add measure metadata
    combined = add_measure_metadata(combined)

    # Save contract-level data
    save_parquet(combined, 'processed/stars/measure_performance/contract_level.parquet')

    # Compute and save aggregates
    aggregates = compute_aggregates(combined)

    # Add measure metadata to aggregates (measure_key already exists from compute_aggregates)
    cutpoints = load_parquet('processed/stars/cutpoints/2026/data.parquet')
    if not cutpoints.empty:
        # Only add columns we don't already have (measure_key is already in aggregates)
        measure_map = cutpoints[['measure_key', 'measure_name', 'part', 'lower_is_better']].drop_duplicates()
        aggregates = aggregates.merge(measure_map, on='measure_key', how='left')

    save_parquet(aggregates, 'processed/stars/measure_performance/aggregates.parquet')

    # Print summary validation
    print("\n" + "=" * 70)
    print("FINAL VALIDATION SUMMARY")
    print("=" * 70)

    print("\nIndustry averages by year (C01 - Breast Cancer Screening):")
    c01_industry = aggregates[(aggregates['measure_id'] == 'C01') & (aggregates['parent_org'] == '_INDUSTRY_')]
    for _, row in c01_industry.sort_values('year').iterrows():
        print(f"  {int(row['year'])}: Simple={row['simple_avg']:.1f}%, Weighted={row['weighted_avg']:.1f}%, N={int(row['contract_count'])}")

    print("\nTop payers by enrollment (2025, C01):")
    c01_2025 = aggregates[(aggregates['measure_id'] == 'C01') & (aggregates['year'] == 2025) & (aggregates['parent_org'] != '_INDUSTRY_')]
    c01_2025 = c01_2025.sort_values('total_enrollment', ascending=False).head(5)
    for _, row in c01_2025.iterrows():
        print(f"  {row['parent_org'][:40]}: {row['weighted_avg']:.1f}% (N={int(row['contract_count'])}, Enrollment={int(row['total_enrollment']):,})")

    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)


if __name__ == "__main__":
    main()
