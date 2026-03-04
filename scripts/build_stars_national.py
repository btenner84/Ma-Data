#!/usr/bin/env python3
"""
Build Stars + National Enrollment Table.

Processes RAW by_contract files (which have contract-level detail) and LEFT JOINs
with stars ratings. This ensures:
- 4+ star % is calculated using ALL MA enrollment, not just rated contracts
- No enrollment is lost due to missing star ratings
- Contract-level granularity is preserved

The key difference from stars_enrollment_unified:
- stars_enrollment_unified: INNER JOIN - only contracts with star ratings (~95% of MA)
- stars_enrollment_national: LEFT JOIN - ALL MA contracts, ratings where available

Schema:
  year, month, contract_id, parent_org, plan_type, enrollment,
  overall_rating (null if not rated), is_fourplus, is_rated, star_year

Output: s3://ma-data123/processed/unified/stars_enrollment_national.parquet
"""

import boto3
import pandas as pd
import zipfile
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

MA_PLAN_TYPES = [
    'HMO/HMOPOS', 'Local PPO', 'Regional PPO', 'PFFS', 'MSA',
    'National PACE', 'Medicare-Medicaid Plan HMO/HMOPOS', '1876 Cost',
    'HCPP - 1833 Cost'
]


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
    print(f"Saved {len(df):,} rows to s3://{S3_BUCKET}/{s3_key}")


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


def process_by_contract_file(s3_key: str) -> pd.DataFrame:
    """Process a single by_contract ZIP file."""
    try:
        # Extract year/month from path: raw/enrollment/by_contract/2024-12/...
        parts = s3_key.split('/')
        year_month = parts[3]  # e.g., "2024-12"
        year = int(year_month.split('-')[0])
        month = int(year_month.split('-')[1])
        
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zip_bytes = BytesIO(response['Body'].read())
        
        with zipfile.ZipFile(zip_bytes, 'r') as zf:
            for name in zf.namelist():
                if name.endswith('.csv'):
                    with zf.open(name) as f:
                        df = pd.read_csv(f)
                        
                        # Standardize columns
                        col_map = {
                            'Contract Number': 'contract_id',
                            'Plan Type': 'plan_type',
                            'Parent Organization': 'parent_org',
                            'Enrollment': 'enrollment'
                        }
                        df = df.rename(columns=col_map)
                        
                        # Filter to MA plan types
                        df = df[df['plan_type'].isin(MA_PLAN_TYPES)]
                        
                        # Convert enrollment to numeric
                        df['enrollment'] = pd.to_numeric(df['enrollment'].astype(str).str.replace('*', '0', regex=False), errors='coerce').fillna(0)
                        
                        df['year'] = year
                        df['month'] = month
                        df['contract_id'] = df['contract_id'].astype(str).str.strip()
                        
                        return df[['year', 'month', 'contract_id', 'parent_org', 'plan_type', 'enrollment']]
        
        return pd.DataFrame()
    except Exception as e:
        print(f"Error processing {s3_key}: {e}")
        return pd.DataFrame()


def main():
    print("=" * 70)
    print("BUILD STARS + NATIONAL ENROLLMENT (CONTRACT-LEVEL)")
    print("=" * 70)
    
    # 1. List all by_contract files
    print("\n1. Finding by_contract files...")
    paginator = s3.get_paginator('list_objects_v2')
    all_files = []
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='raw/enrollment/by_contract/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                all_files.append(obj['Key'])
    
    print(f"   Found {len(all_files)} files")
    
    # Filter to December files for each year (for year-end snapshot)
    december_files = [f for f in all_files if '-12/' in f]
    print(f"   December files: {len(december_files)}")
    
    # For 2026, use latest available month
    latest_2026_files = [f for f in all_files if '/2026-' in f]
    if latest_2026_files:
        latest_2026 = sorted(latest_2026_files)[-1]
        # Check if we already have December 2026
        if not any('/2026-12/' in f for f in december_files):
            december_files.append(latest_2026)
    
    print(f"   Processing {len(december_files)} year-end files")
    
    # 2. Process all December files
    print("\n2. Processing by_contract files...")
    all_enrollment = []
    
    for s3_key in sorted(december_files):
        df = process_by_contract_file(s3_key)
        if not df.empty:
            all_enrollment.append(df)
            print(f"   {s3_key.split('/')[3]}: {len(df):,} contracts, {df['enrollment'].sum():,.0f} enrollment")
    
    if not all_enrollment:
        print("ERROR: No enrollment data!")
        return
    
    enrollment_df = pd.concat(all_enrollment, ignore_index=True)
    print(f"\n   Total enrollment records: {len(enrollment_df):,}")
    
    # 3. Load stars ratings
    print("\n3. Loading stars ratings...")
    stars_df = load_parquet('processed/unified/stars_summary.parquet')
    if stars_df.empty:
        print("ERROR: stars_summary not found!")
        return
    
    # Build star lookup by year
    star_years = sorted(stars_df['rating_year'].dropna().unique())
    print(f"   Star years available: {[int(y) for y in star_years]}")
    
    star_lookups = []
    for year in star_years:
        year = int(year)
        year_stars = stars_df[stars_df['rating_year'] == year].copy()
        
        # Find overall rating column
        rating_col = f"{year} Overall"
        if rating_col not in year_stars.columns:
            for col in year_stars.columns:
                if 'overall' in col.lower() and str(year) in col:
                    rating_col = col
                    break
        
        if rating_col not in year_stars.columns:
            continue
        
        year_stars['overall_rating'] = year_stars[rating_col].apply(parse_star_rating)
        year_stars = year_stars[year_stars['overall_rating'].notna()]
        
        if year_stars.empty:
            continue
        
        lookup = year_stars[['contract_id', 'overall_rating']].copy()
        lookup['contract_id'] = lookup['contract_id'].astype(str).str.strip()
        lookup['star_year'] = year
        lookup = lookup.drop_duplicates(subset=['contract_id'])
        star_lookups.append(lookup)
        print(f"   {year}: {len(lookup)} contracts with ratings")
    
    stars_lookup = pd.concat(star_lookups, ignore_index=True)
    
    # 4. Join enrollment with stars (LEFT JOIN - keep all contracts)
    print("\n4. Joining enrollment with stars...")
    
    # Match enrollment year to star year
    enrollment_df['star_year'] = enrollment_df['year']
    
    result = enrollment_df.merge(
        stars_lookup,
        on=['contract_id', 'star_year'],
        how='left'
    )
    
    # Calculate metrics
    result['is_rated'] = result['overall_rating'].notna()
    result['is_fourplus'] = result['overall_rating'] >= 4.0
    result['star_band'] = result['overall_rating'].apply(
        lambda x: round(x * 2) / 2 if pd.notna(x) else None
    )
    
    print(f"   Joined records: {len(result):,}")
    
    # 5. Summary by year
    print("\n5. Coverage summary by year:")
    print("   " + "-" * 70)
    print(f"   {'Year':<6} {'Total Enroll':>14} {'Rated Enroll':>14} {'4+ Enroll':>14} {'4+ %':>8}")
    print("   " + "-" * 70)
    
    for year in sorted(result['year'].unique()):
        yr = result[result['year'] == year]
        total = yr['enrollment'].sum()
        rated = yr[yr['is_rated']]['enrollment'].sum()
        fourplus = yr[yr['is_fourplus'] == True]['enrollment'].sum()
        fourplus_pct = fourplus / total * 100 if total > 0 else 0
        print(f"   {int(year):<6} {total:>14,.0f} {rated:>14,.0f} {fourplus:>14,.0f} {fourplus_pct:>7.1f}%")
    
    print("   " + "-" * 70)
    
    # 6. Compare with stars_enrollment_unified to show the difference
    print("\n6. Comparing with stars_enrollment_unified (current data):")
    stars_unified = load_parquet('processed/unified/stars_enrollment_unified.parquet')
    
    if not stars_unified.empty:
        for year in [2024, 2025, 2026]:
            if year not in result['year'].values:
                continue
            
            national_yr = result[result['year'] == year]
            unified_yr = stars_unified[stars_unified['star_year'] == year]
            
            national_total = national_yr['enrollment'].sum()
            unified_total = unified_yr['enrollment'].sum() if not unified_yr.empty else 0
            
            national_fourplus = national_yr[national_yr['is_fourplus'] == True]['enrollment'].sum()
            unified_fourplus = unified_yr[unified_yr['is_fourplus'] == True]['enrollment'].sum() if not unified_yr.empty else 0
            
            print(f"\n   {year}:")
            print(f"      National total: {national_total:,.0f}")
            print(f"      Unified total:  {unified_total:,.0f} (missing {national_total - unified_total:,.0f})")
            print(f"      National 4+%:   {national_fourplus/national_total*100:.1f}% ({national_fourplus:,.0f})")
            if unified_total > 0:
                print(f"      Unified 4+%:    {unified_fourplus/unified_total*100:.1f}% ({unified_fourplus:,.0f})")
    
    # 7. Save
    save_parquet(result, 'processed/unified/stars_enrollment_national.parquet')
    
    # 8. Also create parent-level aggregation for the API
    print("\n7. Creating parent-level aggregation...")
    parent_agg = result.groupby(['year', 'month', 'star_year', 'parent_org']).agg({
        'enrollment': 'sum',
        'contract_id': 'nunique',
        'is_rated': lambda x: (result.loc[x.index, 'enrollment'] * x).sum() / result.loc[x.index, 'enrollment'].sum() if result.loc[x.index, 'enrollment'].sum() > 0 else 0,
    }).reset_index()
    
    # Calculate weighted average rating for rated contracts only
    rated_result = result[result['is_rated']].copy()
    if not rated_result.empty:
        wavg_by_parent = rated_result.groupby(['year', 'parent_org']).apply(
            lambda g: (g['overall_rating'] * g['enrollment']).sum() / g['enrollment'].sum() if g['enrollment'].sum() > 0 else None
        ).reset_index(name='wavg_rating')
        
        parent_agg = parent_agg.merge(wavg_by_parent, on=['year', 'parent_org'], how='left')
    else:
        parent_agg['wavg_rating'] = None
    
    parent_agg['is_fourplus'] = parent_agg['wavg_rating'] >= 4.0
    parent_agg = parent_agg.rename(columns={'contract_id': 'contract_count'})
    
    save_parquet(parent_agg, 'processed/unified/stars_by_parent_national.parquet')
    
    print("\nDone!")


if __name__ == "__main__":
    main()
