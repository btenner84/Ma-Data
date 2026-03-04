#!/usr/bin/env python3
"""
Build Risk Scores with National Enrollment.

Creates a national-level risk scores table by:
1. Loading raw plan-level risk scores from Plan Payment Data
2. Joining with national enrollment (from by_contract) for accurate enrollment weights
3. Aggregating by parent_org + year + plan_type + group_type + snp_type

This fixes the ~3% enrollment gap caused by using CPSC-based enrollment
which has suppression for small counties.

Output: s3://ma-data123/processed/unified/risk_scores_national.parquet
"""

import boto3
import pandas as pd
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
    print(f"Saved {len(df):,} rows to s3://{S3_BUCKET}/{s3_key}")


def main():
    print("=" * 70)
    print("BUILD RISK SCORES WITH NATIONAL ENROLLMENT")
    print("=" * 70)
    
    print("\n1. Loading fact_risk_scores_unified (plan-level risk scores)...")
    risk_df = load_parquet('processed/unified/fact_risk_scores_unified.parquet')
    if risk_df.empty:
        print("ERROR: fact_risk_scores_unified not found!")
        return
    print(f"   Loaded {len(risk_df):,} rows")
    print(f"   Years: {sorted(risk_df['year'].unique())}")
    
    print("\n2. Loading stars_enrollment_national (complete enrollment)...")
    enrollment_df = load_parquet('processed/unified/stars_enrollment_national.parquet')
    if enrollment_df.empty:
        print("WARNING: stars_enrollment_national not found, using CPSC enrollment")
        # Fallback to CPSC-based enrollment (less complete but available)
        enrollment_df = load_parquet('processed/unified/fact_enrollment_all_years.parquet')
    
    if enrollment_df.empty:
        print("ERROR: No enrollment data found!")
        return
    
    print(f"   Loaded {len(enrollment_df):,} enrollment rows")
    
    print("\n3. Preparing enrollment lookup by parent_org + year...")
    # Get parent-level enrollment for each year
    # Use December snapshot (same as our Stars national)
    if 'star_year' in enrollment_df.columns:
        # Using stars_enrollment_national
        enroll_agg = enrollment_df.groupby(['star_year', 'parent_org', 'plan_type']).agg({
            'enrollment': 'sum'
        }).reset_index()
        enroll_agg = enroll_agg.rename(columns={'star_year': 'year', 'enrollment': 'national_enrollment'})
    else:
        # Using fact_enrollment_all_years (CPSC)
        enroll_agg = enrollment_df.groupby(['year', 'parent_org', 'plan_type']).agg({
            'enrollment': 'sum'
        }).reset_index()
        enroll_agg = enroll_agg.rename(columns={'enrollment': 'national_enrollment'})
    
    print(f"   Enrollment lookup: {len(enroll_agg):,} parent-plan_type-year combinations")
    
    print("\n4. Aggregating risk scores by parent_org...")
    # Filter to valid risk scores only
    valid_risk = risk_df[risk_df['avg_risk_score'].notna() & (risk_df['avg_risk_score'] > 0)].copy()
    print(f"   Valid risk scores: {len(valid_risk):,}")
    
    # Aggregate to parent + year + plan_type level
    # Use simple average first (will re-weight with national enrollment later)
    parent_risk = valid_risk.groupby(['year', 'parent_org', 'plan_type']).agg({
        'avg_risk_score': 'mean',
        'enrollment': 'sum',
        'contract_id': 'nunique'
    }).reset_index()
    parent_risk = parent_risk.rename(columns={
        'enrollment': 'cpsc_enrollment',
        'contract_id': 'contract_count'
    })
    
    print(f"   Parent-level aggregation: {len(parent_risk):,} rows")
    
    print("\n5. Joining with national enrollment...")
    result = parent_risk.merge(
        enroll_agg,
        on=['year', 'parent_org', 'plan_type'],
        how='left'
    )
    
    # Fill missing national enrollment with CPSC enrollment
    result['national_enrollment'] = result['national_enrollment'].fillna(result['cpsc_enrollment'])
    
    # For weighted averages at industry level, we need enrollment weighting
    result['weighted_risk'] = result['avg_risk_score'] * result['national_enrollment']
    
    print(f"   Joined result: {len(result):,} rows")
    
    print("\n6. Summary by year:")
    print("   " + "-" * 70)
    print(f"   {'Year':<6} {'National Enroll':>16} {'CPSC Enroll':>14} {'Gap %':>8} {'Avg Risk':>10}")
    print("   " + "-" * 70)
    
    for year in sorted(result['year'].unique()):
        yr = result[result['year'] == year]
        national = yr['national_enrollment'].sum()
        cpsc = yr['cpsc_enrollment'].sum()
        gap_pct = (national - cpsc) / national * 100 if national > 0 else 0
        wavg_risk = yr['weighted_risk'].sum() / national if national > 0 else 0
        print(f"   {int(year):<6} {national:>16,.0f} {cpsc:>14,.0f} {gap_pct:>7.1f}% {wavg_risk:>10.3f}")
    
    print("   " + "-" * 70)
    
    # 7. Create final output table
    final = result[[
        'year', 'parent_org', 'plan_type',
        'avg_risk_score', 'national_enrollment', 'cpsc_enrollment',
        'contract_count', 'weighted_risk'
    ]].copy()
    
    # Calculate wavg for display
    final['wavg_risk_score'] = final['weighted_risk'] / final['national_enrollment']
    final = final.drop(columns=['weighted_risk'])
    
    save_parquet(final, 'processed/unified/risk_scores_national.parquet')
    
    print("\n7. Creating industry-level aggregation...")
    industry_agg = result.groupby('year').agg({
        'national_enrollment': 'sum',
        'cpsc_enrollment': 'sum',
        'weighted_risk': 'sum',
        'contract_count': 'sum'
    }).reset_index()
    industry_agg['wavg_risk_score'] = industry_agg['weighted_risk'] / industry_agg['national_enrollment']
    industry_agg = industry_agg.drop(columns=['weighted_risk'])
    
    save_parquet(industry_agg, 'processed/unified/risk_scores_industry_national.parquet')
    
    print("\nDone!")


if __name__ == "__main__":
    main()
