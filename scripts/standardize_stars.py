#!/usr/bin/env python3
"""
Standardize Stars Data Across All Years (2009-2026)

Creates unified Stars tables with consistent column names regardless of year.
Handles the schema evolution from early years (simple scores) to modern (Part C/D/Overall).
"""

import boto3
import pandas as pd
from io import BytesIO
import re

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"  Uploaded: {s3_key} ({len(df):,} rows)")


def download_parquet(key):
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def list_s3_parquet_files(prefix: str) -> list:
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                files.append(obj['Key'])
    return files


def parse_star_rating(value):
    """Parse various star rating formats to numeric."""
    import re
    if pd.isna(value):
        return None
    val_str = str(value).strip()

    # Handle "X out of 5 stars" format
    match = re.search(r'([\d.]+)\s*out\s*of\s*5', val_str, re.IGNORECASE)
    if match:
        return float(match.group(1))

    # Handle "Not enough data" or similar
    if 'not enough' in val_str.lower() or 'too new' in val_str.lower():
        return None

    # Try direct numeric conversion
    try:
        return float(val_str)
    except:
        return None


def extract_rating(df, year, pattern_type):
    """Extract rating from year-specific column."""
    patterns = {
        'overall': [f'{year} Overall', 'Overall Rating', 'Overall Star Rating', 'Summary Score'],
        'part_c': [f'{year} Part C Summary', 'Part C Summary', 'Summary Score for Health Plan Quality', 'Summary Rating'],
        'part_d': [f'{year} Part D Summary', 'Part D Summary']
    }

    for pattern in patterns.get(pattern_type, []):
        for col in df.columns:
            if pattern.lower() in col.lower():
                return col
    return None


def standardize_stars_summary(df, year):
    """Standardize a Stars summary dataframe to consistent schema."""
    result = pd.DataFrame()

    # Standard columns
    col_map = {
        'Contract Number': 'contract_id',
        'Organization Type': 'org_type',
        'Organization Marketing Name': 'marketing_name',
        'Contract Name': 'contract_name',
        'Parent Organization': 'parent_org',
        'SNP': 'is_snp'
    }

    for old, new in col_map.items():
        if old in df.columns:
            result[new] = df[old]

    # Find and extract rating columns
    overall_col = extract_rating(df, year, 'overall')
    part_c_col = extract_rating(df, year, 'part_c')
    part_d_col = extract_rating(df, year, 'part_d')

    if overall_col:
        result['overall_rating'] = df[overall_col].apply(parse_star_rating)
    elif part_c_col:
        # Early years may only have Part C
        result['overall_rating'] = df[part_c_col].apply(parse_star_rating)

    if part_c_col:
        result['part_c_rating'] = df[part_c_col].apply(parse_star_rating)

    if part_d_col:
        result['part_d_rating'] = df[part_d_col].apply(parse_star_rating)

    # Look for disaster relief percentages
    for col in df.columns:
        if 'disaster' in col.lower() and '%' in col:
            # Extract year from column name
            match = re.search(r'(\d{4})', col)
            if match:
                disaster_year = match.group(1)
                result[f'disaster_pct_{disaster_year}'] = df[col]

    # Add year
    result['rating_year'] = year

    # Clean contract_id
    if 'contract_id' in result.columns:
        result['contract_id'] = result['contract_id'].astype(str).str.strip()

    return result


def build_standardized_stars():
    """Build fully standardized Stars dataset across all years."""
    print("\n=== BUILDING STANDARDIZED STARS ===")

    stars_files = list_s3_parquet_files('processed/stars/')
    summary_files = [f for f in stars_files if 'summary' in f.lower()]

    all_data = []

    for s3_key in summary_files:
        try:
            parts = s3_key.split('/')
            year = int(parts[2])

            df = download_parquet(s3_key)
            std_df = standardize_stars_summary(df, year)

            all_data.append(std_df)
            print(f"  {year}: {len(std_df)} contracts, ratings: {std_df['overall_rating'].notna().sum()}")

        except Exception as e:
            print(f"  Error processing {s3_key}: {e}")

    if all_data:
        # Concatenate all years
        unified = pd.concat(all_data, ignore_index=True)

        # Ensure consistent column order
        base_cols = ['rating_year', 'contract_id', 'parent_org', 'marketing_name',
                     'contract_name', 'org_type', 'is_snp',
                     'overall_rating', 'part_c_rating', 'part_d_rating']

        # Add any disaster columns
        disaster_cols = [c for c in unified.columns if c.startswith('disaster_pct')]

        available = [c for c in base_cols + sorted(disaster_cols) if c in unified.columns]
        unified = unified[available]

        upload_parquet_to_s3(unified, 'processed/unified/stars_standardized.parquet')

        # Create summary statistics
        print("\n  === STARS SUMMARY STATISTICS ===")
        print(f"  Total records: {len(unified):,}")
        print(f"  Years: {unified['rating_year'].min()} - {unified['rating_year'].max()}")
        print(f"  Unique contracts: {unified['contract_id'].nunique():,}")
        print(f"  Unique parent orgs: {unified['parent_org'].nunique():,}")

        # Rating distribution
        if 'overall_rating' in unified.columns:
            print("\n  Overall Rating Distribution:")
            rating_dist = unified.groupby('overall_rating').size()
            for rating, count in rating_dist.items():
                if pd.notna(rating):
                    print(f"    {rating:.1f} stars: {count:,} contracts")

        return unified

    return None


def build_stars_by_parent_org():
    """Create Stars trends by parent organization."""
    print("\n=== BUILDING STARS BY PARENT ORG ===")

    try:
        stars = download_parquet('processed/unified/stars_standardized.parquet')
    except:
        print("  Run build_standardized_stars first")
        return None

    # Aggregate by parent org and year
    by_parent = stars.groupby(['parent_org', 'rating_year']).agg({
        'contract_id': 'nunique',
        'overall_rating': 'mean',
        'part_c_rating': 'mean',
        'part_d_rating': 'mean'
    }).reset_index()

    by_parent = by_parent.rename(columns={
        'contract_id': 'contract_count',
        'overall_rating': 'avg_overall_rating',
        'part_c_rating': 'avg_part_c_rating',
        'part_d_rating': 'avg_part_d_rating'
    })

    # Round ratings
    for col in ['avg_overall_rating', 'avg_part_c_rating', 'avg_part_d_rating']:
        if col in by_parent.columns:
            by_parent[col] = by_parent[col].round(2)

    upload_parquet_to_s3(by_parent, 'processed/unified/stars_by_parent_org.parquet')

    # Pivot to wide format for trend analysis
    pivot = by_parent.pivot_table(
        index='parent_org',
        columns='rating_year',
        values='avg_overall_rating'
    ).reset_index()

    pivot.columns = ['parent_org'] + [f'rating_{y}' for y in pivot.columns[1:]]

    # Calculate trend (latest - earliest)
    rating_cols = [c for c in pivot.columns if c.startswith('rating_')]
    if len(rating_cols) >= 2:
        pivot['rating_trend'] = pivot[rating_cols[-1]] - pivot[rating_cols[0]]

    upload_parquet_to_s3(pivot, 'processed/unified/stars_parent_org_trends.parquet')

    return by_parent


def main():
    print("=" * 70)
    print("STANDARDIZING STARS DATA ACROSS ALL YEARS")
    print("=" * 70)

    # Build standardized Stars
    stars = build_standardized_stars()

    if stars is not None:
        # Build parent org aggregations
        by_parent = build_stars_by_parent_org()

    print("\n" + "=" * 70)
    print("STARS STANDARDIZATION COMPLETE")
    print("=" * 70)
    print("\nStandardized files:")
    print("  s3://ma-data123/processed/unified/stars_standardized.parquet")
    print("  s3://ma-data123/processed/unified/stars_by_parent_org.parquet")
    print("  s3://ma-data123/processed/unified/stars_parent_org_trends.parquet")


if __name__ == '__main__':
    main()
