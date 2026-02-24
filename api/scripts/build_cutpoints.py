"""
Build cutpoints data from CMS star ratings data tables.
Processes Part C and Part D cutpoints for each year.

Output: s3://ma-data123/processed/stars/cutpoints/{year}/data.parquet
"""

import boto3
import pandas as pd
import zipfile
import re
import json
import os
from io import BytesIO

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

# Load CMS weights from JSON file (extracted from official Technical Notes)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WEIGHTS_FILE = os.path.join(SCRIPT_DIR, 'cms_weights.json')

with open(WEIGHTS_FILE, 'r') as f:
    CMS_WEIGHTS = json.load(f)


def create_measure_key(measure_name: str) -> str:
    """Create a stable key from measure name for cross-year tracking.

    Measure IDs (C01, C10, etc.) change year over year as CMS renumbers.
    The measure NAME is the stable identifier across years.

    Also handles CMS renaming measures slightly between years.
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

    # Canonical name mappings - normalized form -> canonical key
    # This handles name variations across years
    canonical_mappings = {
        # Blood pressure (renamed 2024)
        'controlling blood pressure': 'controlling_blood_pressure',
        'controlling high blood pressure': 'controlling_blood_pressure',

        # Diabetes measures (special char variations between years)
        'diabetes care blood sugar controlled': 'diabetes_blood_sugar_controlled',
        'diabetes care eye exam': 'diabetes_eye_exam',
        'diabetes care kidney disease monitoring': 'diabetes_kidney_disease_monitoring',

        # Care for older adults (special char variations)
        'care for older adults medication review': 'care_for_older_adults_medication_review',
        'care for older adults pain assessment': 'care_for_older_adults_pain_assessment',
        'care for older adults functional status assessment': 'care_for_older_adults_functional_status',

        # Call center (special char variations)
        'call center foreign language interpreter and tty availability': 'call_center_foreign_language_tty',
    }

    # Check for canonical mapping
    if name_norm in canonical_mappings:
        return canonical_mappings[name_norm]

    # Default: create key from normalized name
    key = name_norm.replace(' ', '_')
    return key[:60]


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


def extract_threshold(value: str, star_level: int) -> str:
    """Extract and format threshold value."""
    if pd.isna(value) or not value:
        return None

    val = str(value).strip()

    # Handle special cases
    if val.lower() in ['nan', 'not applicable', 'n/a', '']:
        return None

    # For 5 star thresholds (highest), extract the lower bound
    if star_level == 5:
        # Format: ">= X" or ">= X %" or "= X"
        match = re.search(r'[><=]+\s*([0-9.]+)\s*(%)?', val)
        if match:
            num = match.group(1)
            pct = match.group(2) or ''
            return f"≥ {num}{pct}"
        # Just return as-is if no match
        return val

    # For lower star levels, extract the range or threshold
    return val


def is_lower_better(measure_id: str, measure_name: str, cutpoint_1star: str, cutpoint_5star: str) -> bool:
    """Determine if lower values are better for this measure.

    TRUE lower-is-better measures:
    - Readmissions (lower readmission rate = better)
    - Complaints (fewer complaints = better)
    - Members Choosing to Leave / Disenrollment (lower = better)
    """
    name_lower = measure_name.lower() if measure_name else ''

    # Known "lower is better" measure name patterns
    lower_better_patterns = [
        'readmission',
        'complaint',
        'choosing to leave',
        'disenrollment',
    ]

    for pattern in lower_better_patterns:
        if pattern in name_lower:
            return True

    # Check cutpoint direction as fallback
    # Lower-is-better measures use ">" for worse ratings and "<=" for better
    if cutpoint_1star and cutpoint_5star:
        c1 = str(cutpoint_1star).strip()
        c5 = str(cutpoint_5star).strip()
        # If 2-star or 1-star uses ">" (not ">=") it means higher values are worse
        if c1.startswith('>') and not c1.startswith('>='):
            # Double-check 5-star doesn't use ">=" (which would indicate higher is better)
            if not c5.startswith('>='):
                return True

    return False


def get_measure_weight(measure_id: str, year: int) -> float:
    """
    Return the CMS official weight for a measure based on year.

    Weights are loaded from cms_weights.json which contains exact weights
    extracted from each year's official CMS Technical Notes.

    Source: CMS Technical Notes Attachment G (or F in earlier years)
    """
    year_str = str(year)

    # Check if we have data for this year
    if year_str not in CMS_WEIGHTS:
        # Fall back to closest available year
        available_years = [int(y) for y in CMS_WEIGHTS.keys() if y != '_meta']
        if year < min(available_years):
            year_str = str(min(available_years))
        else:
            year_str = str(max(available_years))

    year_data = CMS_WEIGHTS.get(year_str, {})

    # Determine part (C or D)
    part = 'part_c' if measure_id.startswith('C') else 'part_d'

    # Look up the weight
    part_data = year_data.get(part, {})
    measure_data = part_data.get(measure_id, {})

    if isinstance(measure_data, dict):
        weight = measure_data.get('weight', 1)
    else:
        weight = 1  # Default if not found

    return float(weight)


def get_methodology(measure_key: str) -> dict:
    """
    Determine the data source and cutpoint methodology for a measure.

    Data sources:
    - CAHPS: Consumer Assessment of Healthcare Providers and Systems (survey)
    - HOS: Health Outcomes Survey
    - HEDIS: Healthcare Effectiveness Data and Information Set (claims/clinical)
    - Admin: Administrative data (complaints, appeals, etc.)

    Cutpoint methods:
    - Survey: Relative distribution with significance testing (CAHPS, HOS)
    - Clustering: Hierarchical clustering with mean resampling (HEDIS, clinical)
    - Admin: Administrative thresholds

    Returns dict with 'data_source' and 'cutpoint_method'.
    """
    # CAHPS measures - survey-based patient experience
    cahps_measures = {
        'getting_needed_care',
        'getting_appointments_and_care_quickly',
        'customer_service',
        'rating_of_health_care_quality',
        'rating_of_health_plan',
        'care_coordination',
        'rating_of_drug_plan',
        'getting_needed_prescription_drugs',
    }

    # HOS measures - Health Outcomes Survey
    hos_measures = {
        'improving_or_maintaining_physical_health',
        'improving_or_maintaining_mental_health',
        'monitoring_physical_activity',
        'reducing_the_risk_of_falling',
        'improving_bladder_control',
    }

    # Administrative measures (complaints, appeals, disenrollment)
    admin_measures = {
        'complaints_about_the_health_plan',
        'complaints_about_the_drug_plan',
        'members_choosing_to_leave_the_plan',
        'plan_makes_timely_decisions_about_appeals',
        'reviewing_appeals_decisions',
        'appeals_auto_forward',
        'appeals_upheld',
        'call_center_foreign_language_tty',
        'health_plan_quality_improvement',
        'drug_plan_quality_improvement',
    }

    if measure_key in cahps_measures:
        return {
            'data_source': 'CAHPS',
            'cutpoint_method': 'Survey'
        }
    elif measure_key in hos_measures:
        return {
            'data_source': 'HOS',
            'cutpoint_method': 'Survey'
        }
    elif measure_key in admin_measures:
        return {
            'data_source': 'Admin',
            'cutpoint_method': 'Admin'
        }
    else:
        # Default: HEDIS/clinical measures use clustering
        return {
            'data_source': 'HEDIS',
            'cutpoint_method': 'Clustering'
        }


def get_domain(measure_id: str) -> str:
    """Map measure ID to domain."""
    if measure_id.startswith('C'):
        num = int(re.search(r'\d+', measure_id).group())
        if num <= 4:
            return "Part C - Staying Healthy"
        elif num <= 8:
            return "Part C - Managing Conditions"
        elif num <= 18:
            return "Part C - Clinical Quality"
        elif num <= 24:
            return "Part C - Member Experience"
        elif num <= 27:
            return "Part C - Complaints & Performance"
        else:
            return "Part C - Customer Service"
    elif measure_id.startswith('D'):
        num = int(re.search(r'\d+', measure_id).group())
        if num <= 3:
            return "Part D - Complaints & Access"
        elif num <= 7:
            return "Part D - Drug Safety"
        elif num <= 11:
            return "Part D - Member Experience"
        else:
            return "Part D - Other"
    return "Other"


def parse_cutpoints_from_bytes(file_bytes: bytes, part: str, filename: str, year: int) -> pd.DataFrame:
    """Parse cutpoints from CSV or Excel file bytes."""
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        df = pd.read_excel(BytesIO(file_bytes), header=None)
    else:
        df = pd.read_csv(BytesIO(file_bytes), encoding='latin-1', header=None)

    if df.empty:
        return pd.DataFrame()

    # Find the row with measure IDs (contains C01, D01, etc.)
    measure_row_idx = None
    star_rows = {}

    for idx, row in df.iterrows():
        row_str = ' '.join(str(v) for v in row.values if pd.notna(v))

        # Find measure ID row
        if re.search(r'C01|D01', row_str):
            measure_row_idx = idx

        # Find star rating rows - check first two columns for star level
        first_val = str(row.iloc[0]).lower().strip() if pd.notna(row.iloc[0]) else ''
        second_val = str(row.iloc[1]).lower().strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
        combined = first_val + ' ' + second_val

        # Part D has format: "MA-PD" in col 0, "1star" in col 1
        # Part C has format: "1star" or "1 star" in col 0
        for star_level in [1, 2, 3, 4, 5]:
            patterns = [f'{star_level}star', f'{star_level} star']
            for p in patterns:
                if p in combined:
                    # For Part D, prefer MA-PD rows over PDP
                    if part == 'D':
                        if 'ma-pd' in first_val and star_level not in star_rows:
                            star_rows[star_level] = idx
                    else:
                        if star_level not in star_rows:
                            star_rows[star_level] = idx

    if measure_row_idx is None:
        print(f"    Could not find measure row in {filename}")
        return pd.DataFrame()

    if not star_rows:
        print(f"    Could not find star rows in {filename}")
        return pd.DataFrame()

    # Extract measures
    measures = []
    measure_row = df.iloc[measure_row_idx]

    # Start column depends on format (Part D has extra org type column)
    start_col = 2 if part == 'D' else 1

    for col_idx in range(start_col, len(measure_row)):
        measure_val = measure_row.iloc[col_idx]
        if pd.isna(measure_val):
            continue

        measure_str = str(measure_val).strip()
        # Extract measure ID (C01, D01, etc.)
        match = re.match(r'([CD]\d+)[:.]?\s*(.*)', measure_str)
        if not match:
            continue

        measure_id = match.group(1)
        measure_name = match.group(2).strip() if match.group(2) else measure_str

        # Get cutpoints for each star level
        cut_1 = df.iloc[star_rows.get(1, -1), col_idx] if 1 in star_rows else None
        cut_2 = df.iloc[star_rows.get(2, -1), col_idx] if 2 in star_rows else None
        cut_3 = df.iloc[star_rows.get(3, -1), col_idx] if 3 in star_rows else None
        cut_4 = df.iloc[star_rows.get(4, -1), col_idx] if 4 in star_rows else None
        cut_5 = df.iloc[star_rows.get(5, -1), col_idx] if 5 in star_rows else None

        # Format thresholds
        cut_5_fmt = extract_threshold(cut_5, 5)
        cut_4_fmt = extract_threshold(cut_4, 4)
        cut_3_fmt = extract_threshold(cut_3, 3)
        cut_2_fmt = extract_threshold(cut_2, 2)

        # Get methodology info
        m_key = create_measure_key(measure_name)
        methodology = get_methodology(m_key)

        measures.append({
            'measure_id': measure_id,
            'measure_key': m_key,  # Stable key for cross-year tracking
            'measure_name': measure_name,
            'part': part,
            'domain': get_domain(measure_id),
            'weight': get_measure_weight(measure_id, year),
            'lower_is_better': is_lower_better(measure_id, measure_name, cut_1, cut_5),
            'data_source': methodology['data_source'],
            'cutpoint_method': methodology['cutpoint_method'],
            'cut_5': cut_5_fmt,
            'cut_4': cut_4_fmt,
            'cut_3': cut_3_fmt,
            'cut_2': cut_2_fmt,
        })

    return pd.DataFrame(measures)


def matches_cutpoint_file(filename: str, part: str) -> bool:
    """Check if filename matches cutpoints pattern for given part."""
    f_lower = filename.lower()
    valid_extensions = ('.csv', '.xlsx', '.xls')

    if not f_lower.endswith(valid_extensions):
        return False

    # Check for cutpoint keywords
    has_cutpoint = ('cut point' in f_lower or 'cutpoint' in f_lower or 'cut_point' in f_lower)
    if not has_cutpoint:
        return False

    # Check for part identifier
    part_lower = part.lower()
    part_patterns = [
        f'part {part_lower}',
        f'part_{part_lower}',
        f'_{part_lower}_cutpoint',  # 2019 format: _C_cutpoints.csv
    ]
    return any(p in f_lower for p in part_patterns)


def find_and_parse_cutpoints(zip_bytes: bytes, part: str, year: int) -> pd.DataFrame:
    """Find and parse cutpoints file from zip (handles nested zips)."""

    with zipfile.ZipFile(BytesIO(zip_bytes)) as z:
        filenames = z.namelist()

        # First, look for direct cutpoints file
        for f in filenames:
            if matches_cutpoint_file(f, part):
                print(f"    Found: {f}")
                with z.open(f) as data_file:
                    return parse_cutpoints_from_bytes(data_file.read(), part, f, year)

        # Check nested zip files (try both data table and part c/d zips)
        for f in filenames:
            if f.endswith('.zip'):
                # Check both data table zips and other nested zips
                print(f"    Checking nested zip: {f}")
                with z.open(f) as nested_zip_file:
                    nested_bytes = nested_zip_file.read()
                    try:
                        with zipfile.ZipFile(BytesIO(nested_bytes)) as nested_z:
                            for nf in nested_z.namelist():
                                if matches_cutpoint_file(nf, part):
                                    print(f"    Found in nested: {nf}")
                                    with nested_z.open(nf) as data_file:
                                        return parse_cutpoints_from_bytes(data_file.read(), part, nf, year)
                    except zipfile.BadZipFile:
                        continue

    return pd.DataFrame()


def process_year(year: int, zip_key: str) -> pd.DataFrame:
    """Process cutpoints for a specific year."""
    print(f"\nProcessing year {year}...")

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=zip_key)
        zip_bytes = response['Body'].read()
    except Exception as e:
        print(f"  Error loading {zip_key}: {e}")
        return pd.DataFrame()

    all_measures = []

    # Parse Part C
    print(f"  Looking for Part C cutpoints...")
    df_c = find_and_parse_cutpoints(zip_bytes, 'C', year)
    if not df_c.empty:
        all_measures.append(df_c)
        print(f"    Parsed {len(df_c)} Part C measures")

    # Parse Part D
    print(f"  Looking for Part D cutpoints...")
    df_d = find_and_parse_cutpoints(zip_bytes, 'D', year)
    if not df_d.empty:
        all_measures.append(df_d)
        print(f"    Parsed {len(df_d)} Part D measures")

    if not all_measures:
        print(f"  No cutpoint data found for {year}")
        return pd.DataFrame()

    result = pd.concat(all_measures, ignore_index=True)
    result['year'] = year

    return result


def main():
    print("=" * 60)
    print("BUILDING CUTPOINTS DATA")
    print("=" * 60)

    # Map years to their data table zip files
    # CMS releases star ratings in October each year for the following payment year
    # e.g., 2025 Star Ratings released Oct 2024, applies to 2026 payment year
    year_files = {
        2026: 'docs/stars/data_tables/2026_star_ratings.zip',
        2025: 'docs/stars/data_tables/2025_star_ratings.zip',
        2024: 'docs/stars/data_tables/2024_star_ratings_data.zip',
        2023: 'docs/stars/data_tables/2023_star_ratings.zip',
        2022: 'docs/stars/data_tables/2022_star_ratings.zip',
        2021: 'docs/stars/data_tables/2021_star_ratings.zip',
        2020: 'docs/stars/data_tables/2020_star_ratings.zip',
        2019: 'docs/stars/data_tables/2019_star_ratings.zip',
        2018: 'docs/stars/data_tables/2018_star_ratings.zip',
        2017: 'docs/stars/data_tables/2017_star_ratings.zip',
        2016: 'docs/stars/data_tables/2016_star_ratings.zip',
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

    # Process each year
    for year, key in sorted(existing_files.items(), reverse=True):
        df = process_year(year, key)
        if not df.empty:
            save_parquet(df, f'processed/stars/cutpoints/{year}/data.parquet')

    print("\n" + "=" * 60)
    print("DONE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
