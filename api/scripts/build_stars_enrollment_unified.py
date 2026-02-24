"""
Build unified stars + enrollment table with all dimensions.
Single parquet file that supports instant filtering by any combination of:
- plan_type (HMO, PPO, etc.)
- group_type (Group, Individual)
- snp_type (D-SNP, C-SNP, I-SNP)
- parent_org

Schema:
  star_year, contract_id, parent_org, enrollment, overall_rating,
  plan_type, product_type, group_type, snp_type

Output: s3://ma-data123/processed/unified/stars_enrollment_unified.parquet
"""

import pandas as pd
import boto3
from io import BytesIO
import re

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


def parse_star_rating(value):
    """Parse star rating from various formats."""
    if pd.isna(value):
        return None
    val_str = str(value).strip()
    skip_patterns = ['not enough', 'too new', 'not applicable', 'n/a']
    if any(p in val_str.lower() for p in skip_patterns):
        return None
    match = re.search(r'([\d.]+)\s*(?:out\s*of\s*5)?', val_str, re.IGNORECASE)
    if match:
        try:
            rating = float(match.group(1))
            if 1 <= rating <= 5:
                return rating
        except:
            pass
    return None


def get_enrollment_detail(enrollment_year: int, month: int = 2):
    """Load monthly enrollment data with contract-level detail."""
    key = f'processed/fact_enrollment/{enrollment_year}/{month:02d}/data.parquet'
    df = load_parquet(key)
    if df.empty:
        key = f'processed/fact_enrollment/{enrollment_year}/{month}/data.parquet'
        df = load_parquet(key)

    if not df.empty:
        # Derive group_type from plan_id (>= 800 = Group, < 800 = Individual)
        if 'plan_id' in df.columns:
            df['group_type'] = df['plan_id'].apply(lambda x: 'Group' if x >= 800 else 'Individual')
        else:
            df['group_type'] = 'Unknown'

        # Derive product_type from plan_type
        if 'plan_type' in df.columns:
            df['product_type'] = df['plan_type'].apply(
                lambda x: 'PDP' if 'PDP' in str(x).upper() else 'MA'
            )
        else:
            df['product_type'] = 'Unknown'

        # Derive snp_type from is_snp
        if 'is_snp' in df.columns:
            df['snp_type'] = df['is_snp'].apply(lambda x: 'SNP' if x == 'Yes' else 'Non-SNP')
        else:
            df['snp_type'] = 'Unknown'

    return df


def build_unified_table():
    """Build unified stars + enrollment table with all dimensions."""
    print("Loading stars summary...")
    stars_df = load_parquet('processed/unified/stars_summary.parquet')
    if stars_df.empty:
        print("No stars data found!")
        return

    star_years = sorted(stars_df['rating_year'].unique().tolist())
    print(f"Processing {len(star_years)} star years")

    all_rows = []

    for star_year in star_years:
        print(f"\nProcessing star year {star_year}...")
        payment_year = star_year + 1

        # Find overall rating column
        rating_col = f"{star_year} Overall"
        if rating_col not in stars_df.columns:
            for col in stars_df.columns:
                if 'overall' in col.lower() and str(star_year) in col:
                    rating_col = col
                    break

        if rating_col not in stars_df.columns:
            print(f"  No rating column found")
            continue

        # Get stars for this year
        year_stars = stars_df[stars_df['rating_year'] == star_year].copy()
        year_stars['contract_id_clean'] = year_stars['contract_id'].astype(str).str.strip()
        year_stars['overall_rating'] = year_stars[rating_col].apply(parse_star_rating)
        year_stars = year_stars[year_stars['overall_rating'].notna()]

        if year_stars.empty:
            print(f"  No valid ratings")
            continue

        # Load monthly enrollment for payment year (with contract-level detail)
        enrollment_df = pd.DataFrame()
        enrollment_source = None
        for try_year in [payment_year] + list(range(payment_year - 1, 2013, -1)):
            enrollment_df = get_enrollment_detail(try_year, month=2)
            if not enrollment_df.empty:
                enrollment_source = f"{try_year}/02"
                break

        if enrollment_df.empty:
            print(f"  No enrollment data")
            continue

        print(f"  Using enrollment from {enrollment_source}")

        # Filter to MA/MAPD only (exclude PDP)
        enrollment_df = enrollment_df[enrollment_df['product_type'] != 'PDP']

        # Normalize plan_type for filtering
        def normalize_plan_type(pt):
            pt = str(pt).upper()
            if 'HMO' in pt or 'HMOPOS' in pt:
                return 'HMO/HMOPOS'
            elif 'LOCAL PPO' in pt:
                return 'PPO'
            elif 'REGIONAL PPO' in pt:
                return 'PPO'
            elif 'PPO' in pt:
                return 'PPO'
            elif 'PFFS' in pt:
                return 'PFFS'
            elif 'MSA' in pt:
                return 'MSA'
            elif 'COST' in pt:
                return '1876 Cost'
            else:
                return 'Other'

        enrollment_df['plan_type_normalized'] = enrollment_df['plan_type'].apply(normalize_plan_type)

        # Group by contract + dimensions
        group_cols = ['contract_id', 'parent_org', 'plan_type', 'plan_type_normalized',
                      'group_type', 'snp_type']
        enroll_agg = enrollment_df.groupby(group_cols).agg({
            'enrollment': 'sum'
        }).reset_index()

        # Filter out zero/null enrollment
        enroll_agg = enroll_agg[enroll_agg['enrollment'] > 0]

        # Join with stars
        merged = enroll_agg.merge(
            year_stars[['contract_id_clean', 'overall_rating']].rename(columns={'contract_id_clean': 'contract_id'}),
            on='contract_id',
            how='inner'
        )

        if merged.empty:
            print(f"  No matches after join")
            continue

        # Add star year
        merged['star_year'] = star_year
        merged['payment_year'] = payment_year

        all_rows.append(merged)
        print(f"  Added {len(merged)} rows (group_type: {merged['group_type'].unique()})")

    # Combine all years
    result_df = pd.concat(all_rows, ignore_index=True)

    # Ensure all dimension columns exist
    for col in ['plan_type', 'plan_type_normalized', 'group_type', 'snp_type']:
        if col not in result_df.columns:
            result_df[col] = 'Unknown'

    # Fill NaN in dimension columns
    result_df = result_df.fillna({
        'plan_type': 'Unknown',
        'plan_type_normalized': 'Other',
        'group_type': 'Unknown',
        'snp_type': 'Non-SNP'
    })

    # Calculate star band (round to 0.5)
    result_df['star_band'] = result_df['overall_rating'].apply(lambda x: round(x * 2) / 2 if pd.notna(x) else None)

    # Mark 4+ star
    result_df['is_fourplus'] = result_df['overall_rating'] >= 4

    print(f"\nTotal rows: {len(result_df)}")
    print(f"Years: {sorted(result_df['star_year'].unique())}")
    print(f"Columns: {list(result_df.columns)}")
    print(f"group_type values: {result_df['group_type'].unique()}")
    print(f"snp_type values: {result_df['snp_type'].unique()}")
    print(f"plan_type_normalized values: {result_df['plan_type_normalized'].unique()}")

    # Save
    save_parquet(result_df, 'processed/unified/stars_enrollment_unified.parquet')
    print("\nDone!")


if __name__ == "__main__":
    build_unified_table()
