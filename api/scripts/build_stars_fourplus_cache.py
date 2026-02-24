"""
Pre-compute 4+ star enrollment data for fast API queries.
Creates a unified parquet file with:
- 4+ star % by year for Industry
- 4+ star % by year for each major payer
- Pre-joined stars + enrollment data

Run this after stars/enrollment data is updated.
Output: s3://ma-data123/processed/unified/stars_fourplus_by_year.parquet
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
    """Load enrollment data for a specific year/month."""
    key = f'processed/fact_enrollment/{enrollment_year}/{month:02d}/data.parquet'
    df = load_parquet(key)
    if df.empty:
        key = f'processed/fact_enrollment/{enrollment_year}/{month}/data.parquet'
        df = load_parquet(key)
    return df


def build_fourplus_cache():
    """Build pre-computed 4+ star enrollment data."""
    print("Loading stars summary...")
    stars_df = load_parquet('processed/unified/stars_summary.parquet')
    if stars_df.empty:
        print("No stars data found!")
        return

    star_years = sorted(stars_df['rating_year'].unique().tolist())
    print(f"Processing {len(star_years)} star years: {star_years}")

    results = []

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
            print(f"  No rating column found for {star_year}")
            continue

        # Get stars for this year
        year_stars = stars_df[stars_df['rating_year'] == star_year].copy()
        year_stars['contract_id_clean'] = year_stars['contract_id'].astype(str).str.strip()
        year_stars['overall_rating'] = year_stars[rating_col].apply(parse_star_rating)
        year_stars = year_stars[year_stars['overall_rating'].notna()]

        if year_stars.empty:
            print(f"  No valid ratings for {star_year}")
            continue

        # Load enrollment - try payment year Feb, fall back
        enrollment_df = pd.DataFrame()
        enrollment_source = None
        for try_year in [payment_year] + list(range(payment_year - 1, 2013, -1)):
            enrollment_df = get_enrollment_detail(try_year, month=2)
            if not enrollment_df.empty:
                enrollment_source = f"{try_year}/02"
                break

        if enrollment_df.empty:
            print(f"  No enrollment data for star year {star_year}")
            continue

        print(f"  Using enrollment from {enrollment_source}")

        # Filter to MA/MAPD only
        if 'plan_type' in enrollment_df.columns:
            enrollment_df = enrollment_df[~enrollment_df['plan_type'].str.contains('PDP', case=False, na=False)]

        # Aggregate enrollment by contract
        enroll_by_contract = enrollment_df.groupby(['contract_id', 'parent_org']).agg({
            'enrollment': 'sum'
        }).reset_index()

        # Join stars with enrollment
        merged = enroll_by_contract.merge(
            year_stars[['contract_id_clean', 'overall_rating']].rename(columns={'contract_id_clean': 'contract_id'}),
            on='contract_id',
            how='inner'
        )

        if merged.empty:
            print(f"  No matches after join for {star_year}")
            continue

        # Mark 4+ star contracts
        merged['is_fourplus'] = merged['overall_rating'] >= 4

        # Calculate Industry total
        total_enrollment = merged['enrollment'].sum()
        fourplus_enrollment = merged[merged['is_fourplus']]['enrollment'].sum()
        industry_pct = round((fourplus_enrollment / total_enrollment * 100), 2) if total_enrollment > 0 else 0

        results.append({
            'star_year': star_year,
            'payment_year': payment_year,
            'enrollment_source': enrollment_source,
            'parent_org': 'Industry',
            'total_enrollment': int(total_enrollment),
            'fourplus_enrollment': int(fourplus_enrollment),
            'fourplus_pct': industry_pct
        })

        # Calculate per-payer stats for top payers
        payer_stats = merged.groupby('parent_org').agg({
            'enrollment': 'sum',
            'is_fourplus': lambda x: (merged.loc[x.index, 'enrollment'] * x).sum()
        }).reset_index()
        payer_stats.columns = ['parent_org', 'total_enrollment', 'fourplus_enrollment']
        payer_stats = payer_stats[payer_stats['total_enrollment'] >= 10000]  # Only include payers with 10K+ enrollment
        payer_stats['fourplus_pct'] = round((payer_stats['fourplus_enrollment'] / payer_stats['total_enrollment'] * 100), 2)

        for _, row in payer_stats.iterrows():
            results.append({
                'star_year': star_year,
                'payment_year': payment_year,
                'enrollment_source': enrollment_source,
                'parent_org': row['parent_org'],
                'total_enrollment': int(row['total_enrollment']),
                'fourplus_enrollment': int(row['fourplus_enrollment']),
                'fourplus_pct': float(row['fourplus_pct'])
            })

        print(f"  Industry: {industry_pct}% 4+ star ({len(payer_stats)} payers tracked)")

    # Create DataFrame and save
    result_df = pd.DataFrame(results)
    print(f"\nTotal rows: {len(result_df)}")
    print(f"Years covered: {sorted(result_df['star_year'].unique())}")
    print(f"Payers tracked: {result_df['parent_org'].nunique()}")

    save_parquet(result_df, 'processed/unified/stars_fourplus_by_year.parquet')
    print("\nDone!")


if __name__ == "__main__":
    build_fourplus_cache()
