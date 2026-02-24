"""
Build unified enrollment dataset with all filter dimensions.
Links enrollment data with contract info, SNP data, and derives plan/product types.
"""

import boto3
import pandas as pd
from io import BytesIO

s3 = boto3.client('s3')
BUCKET = 'ma-data123'


def load_parquet(key):
    """Load parquet from S3."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def save_parquet(df, key):
    """Save dataframe as parquet to S3."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"  Saved {key}")


def main():
    print("Loading data sources...")

    # 1. Load stars summary for contract info
    print("  Loading stars summary...")
    stars = load_parquet('processed/unified/stars_summary.parquet')
    stars['org_type'] = stars['org_type'].str.strip()
    print(f"    {len(stars)} stars records")

    # 2. Load enrollment by parent
    print("  Loading enrollment by parent...")
    enrollment_parent = load_parquet('processed/unified/enrollment_by_parent_annual.parquet')
    print(f"    {len(enrollment_parent)} parent-year records")

    # 3. Load SNP data
    print("  Loading SNP data...")
    try:
        snp = load_parquet('processed/unified/snp_all.parquet')
        print(f"    {len(snp)} SNP records")
    except:
        snp = pd.DataFrame()
        print("    No SNP data")

    # ============================================
    # Build contract-level plan type breakdown
    # ============================================
    print("\nBuilding plan type breakdown by parent and year...")

    # Map org_type to cleaner plan_type
    org_type_map = {
        'Local CCP': 'HMO',
        'Regional CCP': 'Regional PPO',
        'PFFS': 'PFFS',
        'MSA': 'MSA',
        'PDP': 'PDP',
        '1876 Cost': '1876 Cost',
        'Demo': 'Demo',
        'National PACE': 'PACE',
    }
    stars['plan_type'] = stars['org_type'].map(org_type_map).fillna('Other')

    # Get contract counts by parent, year, and plan_type
    parent_plantype = stars.groupby(['rating_year', 'parent_org', 'plan_type']).agg({
        'contract_id': 'nunique'
    }).reset_index()
    parent_plantype = parent_plantype.rename(columns={
        'rating_year': 'year',
        'contract_id': 'contract_count'
    })

    print(f"  {len(parent_plantype)} parent-year-plantype combinations")

    # ============================================
    # Distribute enrollment across plan types
    # ============================================
    print("\nDistributing enrollment across plan types...")

    # Calculate total contracts per parent-year
    parent_totals = parent_plantype.groupby(['year', 'parent_org'])['contract_count'].sum().reset_index()
    parent_totals = parent_totals.rename(columns={'contract_count': 'total_contracts'})

    # Merge to get proportions
    parent_plantype = parent_plantype.merge(parent_totals, on=['year', 'parent_org'])
    parent_plantype['contract_pct'] = parent_plantype['contract_count'] / parent_plantype['total_contracts']

    # Merge with enrollment
    unified = parent_plantype.merge(
        enrollment_parent[['year', 'parent_org', 'total_enrollment']],
        on=['year', 'parent_org'],
        how='left'
    )

    # Fill missing enrollment with 0
    unified['total_enrollment'] = unified['total_enrollment'].fillna(0)

    # Distribute enrollment by contract proportion
    unified['enrollment'] = (unified['total_enrollment'] * unified['contract_pct']).round(0).astype(int)

    # Add product_type
    product_map = {
        'HMO': 'MAPD',
        'Regional PPO': 'MAPD',
        'PFFS': 'MA-only',
        'MSA': 'MA-only',
        'PDP': 'PDP',
        '1876 Cost': 'MA-only',
        'Demo': 'MAPD',
        'PACE': 'MAPD',
        'Other': 'Unknown',
    }
    unified['product_type'] = unified['plan_type'].map(product_map).fillna('Unknown')

    # Select final columns
    unified = unified[[
        'year', 'parent_org', 'plan_type', 'product_type',
        'enrollment', 'contract_count'
    ]]

    print(f"  Unified records: {len(unified)}")
    print(f"  Years: {sorted(unified['year'].unique())}")
    print(f"  Plan types: {unified.groupby('plan_type')['enrollment'].sum().sort_values(ascending=False).head().to_dict()}")

    # ============================================
    # Also add parents without stars data
    # ============================================
    print("\nAdding parents without stars data...")

    matched_parents = set(unified['parent_org'].unique())
    all_parents = set(enrollment_parent['parent_org'].unique())
    unmatched = all_parents - matched_parents

    print(f"  {len(unmatched)} parents without stars data")

    # Add them with 'Unknown' plan type
    unmatched_enrollment = enrollment_parent[enrollment_parent['parent_org'].isin(unmatched)].copy()
    unmatched_enrollment['plan_type'] = 'Unknown'
    unmatched_enrollment['product_type'] = 'Unknown'
    unmatched_enrollment['enrollment'] = unmatched_enrollment['total_enrollment'].fillna(0).astype(int)
    unmatched_enrollment['contract_count'] = unmatched_enrollment['contract_count'].fillna(1).astype(int)
    unmatched_enrollment = unmatched_enrollment[[
        'year', 'parent_org', 'plan_type', 'product_type',
        'enrollment', 'contract_count'
    ]]

    unified = pd.concat([unified, unmatched_enrollment], ignore_index=True)
    print(f"  Total unified records: {len(unified)}")

    # ============================================
    # Save unified fact table
    # ============================================
    print("\nSaving unified data...")
    save_parquet(unified, 'processed/unified/fact_enrollment_unified.parquet')

    # ============================================
    # Create aggregate tables for fast queries
    # ============================================
    print("\nCreating aggregate tables...")

    # Industry total by year
    by_year = unified.groupby('year').agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index().rename(columns={'parent_org': 'parent_count'})
    save_parquet(by_year, 'processed/unified/agg_enrollment_by_year.parquet')

    # By year and plan_type
    by_plantype = unified.groupby(['year', 'plan_type']).agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index().rename(columns={'parent_org': 'parent_count'})
    save_parquet(by_plantype, 'processed/unified/agg_enrollment_by_plantype.parquet')

    # By year and product_type
    by_product = unified.groupby(['year', 'product_type']).agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index().rename(columns={'parent_org': 'parent_count'})
    save_parquet(by_product, 'processed/unified/agg_enrollment_by_product.parquet')

    # By year and parent (for parent comparisons)
    by_parent = unified.groupby(['year', 'parent_org']).agg({
        'enrollment': 'sum',
        'contract_count': 'sum'
    }).reset_index()
    save_parquet(by_parent, 'processed/unified/agg_enrollment_by_parent_year.parquet')

    print("\nDone!")
    print(f"\nSample data:")
    print(unified.head(10).to_string())

    print(f"\nYear totals:")
    print(by_year.to_string())


if __name__ == '__main__':
    main()
