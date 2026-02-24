"""
Build measure-level star ratings data from CMS star ratings data tables.
Processes Measure Stars files for each year to create a granular fact table.

Output: s3://ma-data123/processed/stars/measure_stars/{year}/data.parquet
Combined: s3://ma-data123/processed/unified/fact_measure_stars.parquet
"""

import boto3
import pandas as pd
import zipfile
import re
from io import BytesIO

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


def create_measure_key(measure_id: str, measure_name: str) -> str:
    """Create stable key from measure name for cross-year tracking.

    Measure IDs (C01, C10, etc.) change year over year as CMS renumbers.
    The measure NAME is the stable identifier across years.
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

    # Canonical name mappings - handle CMS renaming measures
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

    if name_norm in canonical_mappings:
        return canonical_mappings[name_norm]

    # Default: create key from normalized name
    key = name_norm.replace(' ', '_')
    return key[:60]


def parse_star_rating(value) -> int:
    """Parse star rating value to integer 1-5."""
    if pd.isna(value):
        return None

    val_str = str(value).strip()

    # Skip non-numeric values
    skip_patterns = ['not enough', 'too new', 'not applicable', 'n/a', 'plan not', 'nr', 'na']
    if any(p in val_str.lower() for p in skip_patterns):
        return None

    # Try to extract number
    try:
        # Handle "X stars" or "X out of 5" format
        match = re.search(r'(\d+)', val_str)
        if match:
            rating = int(match.group(1))
            if 1 <= rating <= 5:
                return rating
    except:
        pass

    return None


def find_measure_stars_in_zip(z: zipfile.ZipFile) -> str:
    """Find the Measure Stars CSV file in a zip (specifically 'Measure Stars', not 'Measure Data')."""
    for f in z.namelist():
        f_lower = f.lower()
        # Look for "measure stars" (not "measure data")
        # Files are named like "2025 Star Ratings Data Table - Measure Stars (Dec 2 2024).csv"
        if 'measure stars' in f_lower and f_lower.endswith('.csv'):
            return f
        # 2019 and earlier use different naming: "*_stars.csv" (not "_data.csv" or "_summary.csv")
        if f_lower.endswith('_stars.csv') and '_data' not in f_lower:
            return f
    return None


def get_measure_stars_file(zip_bytes: bytes, year: int):
    """Get the measure stars CSV content, handling nested zips."""
    with zipfile.ZipFile(BytesIO(zip_bytes)) as z:
        # First try direct file
        measure_file = find_measure_stars_in_zip(z)
        if measure_file:
            with z.open(measure_file) as f:
                return measure_file, f.read()

        # Check for nested zips (common in 2019-2023)
        for f in z.namelist():
            if f.endswith('.zip'):
                print(f"    Checking nested zip: {f}")
                with z.open(f) as nested_file:
                    nested_bytes = nested_file.read()
                    try:
                        with zipfile.ZipFile(BytesIO(nested_bytes)) as nested_z:
                            nested_measure_file = find_measure_stars_in_zip(nested_z)
                            if nested_measure_file:
                                with nested_z.open(nested_measure_file) as nmf:
                                    return nested_measure_file, nmf.read()
                    except zipfile.BadZipFile:
                        continue

    return None, None


def parse_measure_stars(zip_bytes: bytes, year: int) -> pd.DataFrame:
    """Parse measure stars data from CMS zip file."""

    # Get the measure stars file content (handles nested zips)
    measure_file, file_content = get_measure_stars_file(zip_bytes, year)

    if not measure_file or not file_content:
        print(f"  Could not find measure stars file for {year}")
        return pd.DataFrame()

    print(f"  Found: {measure_file}")

    # Read raw CSV without header
    raw_df = pd.read_csv(BytesIO(file_content), encoding='latin-1', header=None)

    # CMS files have complex multi-row headers:
    # Row 0: Title row
    # Row 1: CONTRACT_ID, Org Type, etc + Domain names
    # Row 2: Measure IDs (C01, C02, D01, etc.)
    # Row 3: Date ranges
    # Row 4+: Data

    # Find the row with measure IDs (C01, D01, etc.)
    measure_row_idx = None
    for idx in range(min(5, len(raw_df))):
        row_str = ' '.join(str(v) for v in raw_df.iloc[idx].values if pd.notna(v))
        if re.search(r'C01[:\s]|D01[:\s]', row_str):
            measure_row_idx = idx
            break

    if measure_row_idx is None:
        print(f"  Could not find measure ID row")
        return pd.DataFrame()

    print(f"  Measure IDs in row {measure_row_idx}")

    # Get measure info from the measure row
    measure_row = raw_df.iloc[measure_row_idx]

    # Find first row with CONTRACT_ID
    header_row_idx = None
    for idx in range(min(5, len(raw_df))):
        if 'CONTRACT' in str(raw_df.iloc[idx, 0]).upper():
            header_row_idx = idx
            break

    if header_row_idx is None:
        header_row_idx = 0

    # Data starts after the date range row (usually measure_row_idx + 2)
    data_start_idx = measure_row_idx + 2

    # Build column mapping: column index -> (measure_id, measure_name)
    measure_cols = {}
    for col_idx in range(5, len(measure_row)):
        val = measure_row.iloc[col_idx]
        if pd.isna(val):
            continue
        val_str = str(val).strip()

        # Parse "C01: Measure Name" format
        match = re.match(r'([CD]\d+)[:\s]+(.+)', val_str)
        if match:
            measure_id = match.group(1)
            measure_name = match.group(2).strip()
            measure_cols[col_idx] = (measure_id, measure_name)

    print(f"  Found {len(measure_cols)} measures")

    # Extract contract data
    records = []
    for row_idx in range(data_start_idx, len(raw_df)):
        row = raw_df.iloc[row_idx]

        # Get contract ID (first column)
        contract_id = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
        if not contract_id or contract_id.lower() in ['nan', 'none', '']:
            continue

        # Get parent org (column 4 or 5 typically)
        parent_org = None
        for col in [4, 5, 3]:
            if col < len(row) and pd.notna(row.iloc[col]):
                val = str(row.iloc[col]).strip()
                if len(val) > 3 and val.lower() not in ['nan', 'none']:
                    parent_org = val
                    break

        # Get org type (column 1)
        org_type = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else None

        # Get each measure's star rating
        for col_idx, (measure_id, measure_name) in measure_cols.items():
            if col_idx >= len(row):
                continue

            star_rating = parse_star_rating(row.iloc[col_idx])

            records.append({
                'star_year': year,
                'contract_id': contract_id,
                'parent_org': parent_org,
                'org_type': org_type,
                'measure_id': measure_id,
                'measure_name': measure_name,
                'measure_key': create_measure_key(measure_id, measure_name),
                'star_rating': star_rating,
            })

    df = pd.DataFrame(records)

    # Stats
    valid_ratings = df['star_rating'].notna().sum()
    print(f"  Parsed {len(df)} records, {valid_ratings} with valid star ratings")

    return df


def process_year(year: int, zip_key: str) -> pd.DataFrame:
    """Process measure stars for a specific year."""
    print(f"\nProcessing year {year}...")

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=zip_key)
        zip_bytes = response['Body'].read()
    except Exception as e:
        print(f"  Error loading {zip_key}: {e}")
        return pd.DataFrame()

    df = parse_measure_stars(zip_bytes, year)
    return df


def main():
    print("=" * 60)
    print("BUILDING MEASURE STARS DATA")
    print("=" * 60)

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

    # Check which files exist
    existing_files = {}
    for year, key in year_files.items():
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=key)
            existing_files[year] = key
        except:
            print(f"File not found: {key}")

    print(f"\nFound {len(existing_files)} year files")

    # Load enrollment data for joining
    print("\nLoading enrollment data...")
    try:
        enrollment_df = load_parquet('processed/unified/stars_enrollment_unified.parquet')
        enrollment_lookup = enrollment_df.groupby(['contract_id', 'star_year']).agg({
            'enrollment': 'sum',
            'plan_type_normalized': 'first',
            'group_type': 'first',
            'snp_type': 'first',
        }).reset_index()
        print(f"  Loaded {len(enrollment_lookup)} enrollment records")
    except Exception as e:
        print(f"  Warning: Could not load enrollment data: {e}")
        enrollment_lookup = pd.DataFrame()

    # Process each year
    all_years = []
    for year, key in sorted(existing_files.items(), reverse=True):
        df = process_year(year, key)
        if not df.empty:
            # Join with enrollment
            if not enrollment_lookup.empty:
                df = df.merge(
                    enrollment_lookup,
                    on=['contract_id', 'star_year'],
                    how='left'
                )

            # Save year file
            save_parquet(df, f'processed/stars/measure_stars/{year}/data.parquet')
            all_years.append(df)

    # Combine all years
    if all_years:
        combined = pd.concat(all_years, ignore_index=True)
        save_parquet(combined, 'processed/unified/fact_measure_stars.parquet')

        # Print summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total records: {len(combined):,}")
        print(f"Years: {sorted(combined['star_year'].unique())}")
        print(f"Unique measures: {combined['measure_key'].nunique()}")
        print(f"Contracts with ratings: {combined[combined['star_rating'].notna()]['contract_id'].nunique()}")

        # Star rating distribution
        print("\nStar rating distribution:")
        for rating in [1, 2, 3, 4, 5]:
            count = (combined['star_rating'] == rating).sum()
            pct = count / combined['star_rating'].notna().sum() * 100
            print(f"  {rating}★: {count:,} ({pct:.1f}%)")

    print("\n" + "=" * 60)
    print("DONE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
