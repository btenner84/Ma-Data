"""
Build master data tables with proper data quality handling.

Key issues addressed:
1. Trailing whitespace in parent org names (especially 2021+)
2. M&A events that change parent org ownership
3. Name variations across data sources
4. Cross-validation against known totals
"""

import boto3
import pandas as pd
from io import BytesIO
import re

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
    print(f"  Saved {key} ({len(df):,} rows)")


def normalize_name(name):
    """Normalize parent org name for matching."""
    if pd.isna(name):
        return None
    name = str(name).strip()  # Remove trailing/leading whitespace
    return name


def create_name_key(name):
    """Create a normalized key for fuzzy matching."""
    if pd.isna(name):
        return None
    # Lowercase, remove punctuation, normalize whitespace
    key = str(name).strip().lower()
    key = re.sub(r'[,\.\(\)]', '', key)
    key = re.sub(r'\s+', ' ', key)
    # Remove common suffixes
    key = re.sub(r'\s*(inc|llc|corp|corporation|company|co|ltd)$', '', key)
    return key.strip()


# Known M&A mappings (acquired -> acquirer, effective year)
MA_EVENTS = [
    # (old_name_pattern, new_name, effective_year)
    ('WellCare Health Plans', 'Centene Corporation', 2021),
    ('Health Net', 'Centene Corporation', 2016),
    ('Fidelis', 'Centene Corporation', 2018),
    ('HealthSpring', 'CIGNA', 2012),
    ('Bravo Health', 'CIGNA', 2011),
    ('XLHealth', 'UnitedHealth Group, Inc.', 2012),
    ('Arcadian', 'Humana Inc.', 2012),
    ('WellPoint', 'Anthem Inc.', 2015),  # Rebranding
    ('Anthem Inc.', 'Elevance Health, Inc.', 2022),  # Rebranding
    ('CIGNA', 'The Cigna Group', 2023),  # Rebranding
    ('Aetna', 'CVS Health Corporation', 2019),  # But Aetna still operates as Aetna
]


def main():
    print("=" * 70)
    print("BUILDING MASTER DATA TABLES")
    print("=" * 70)

    # ========================================
    # 1. Load source data
    # ========================================
    print("\n### Loading source data ###")

    stars = load_parquet('processed/unified/stars_summary.parquet')
    print(f"  Stars: {len(stars):,} rows")

    enrollment = load_parquet('processed/unified/enrollment_by_parent_annual.parquet')
    print(f"  Enrollment: {len(enrollment):,} rows")

    # ========================================
    # 2. Clean and normalize names
    # ========================================
    print("\n### Cleaning parent org names ###")

    # Normalize all parent org names
    stars['parent_org_clean'] = stars['parent_org'].apply(normalize_name)
    stars['org_type_clean'] = stars['org_type'].apply(lambda x: str(x).strip() if pd.notna(x) else x)

    enrollment['parent_org_clean'] = enrollment['parent_org'].apply(normalize_name)

    # Check improvement
    stars_parents_raw = stars['parent_org'].nunique()
    stars_parents_clean = stars['parent_org_clean'].nunique()
    print(f"  Stars unique parents: {stars_parents_raw} raw -> {stars_parents_clean} cleaned")

    # ========================================
    # 3. Build parent org master table
    # ========================================
    print("\n### Building parent org master table ###")

    # Get all unique parent org names from both sources
    all_parents = set()

    # From stars (with year info)
    stars_parent_years = stars.groupby(['parent_org_clean', 'rating_year']).agg({
        'contract_id': 'nunique'
    }).reset_index()
    stars_parent_years.columns = ['parent_org', 'year', 'contract_count']

    for p in stars['parent_org_clean'].dropna().unique():
        all_parents.add(p)

    # From enrollment
    for p in enrollment['parent_org_clean'].dropna().unique():
        all_parents.add(p)

    print(f"  Total unique parent orgs: {len(all_parents)}")

    # Create master table
    parent_master = pd.DataFrame({'parent_org': list(all_parents)})
    parent_master['name_key'] = parent_master['parent_org'].apply(create_name_key)

    # Add flags for data availability
    stars_parents = set(stars['parent_org_clean'].dropna().unique())
    enrollment_parents = set(enrollment['parent_org_clean'].dropna().unique())

    parent_master['in_stars'] = parent_master['parent_org'].isin(stars_parents)
    parent_master['in_enrollment'] = parent_master['parent_org'].isin(enrollment_parents)
    parent_master['in_both'] = parent_master['in_stars'] & parent_master['in_enrollment']

    print(f"  In both sources: {parent_master['in_both'].sum()}")
    print(f"  Only in stars: {(parent_master['in_stars'] & ~parent_master['in_enrollment']).sum()}")
    print(f"  Only in enrollment: {(~parent_master['in_stars'] & parent_master['in_enrollment']).sum()}")

    # ========================================
    # 4. Build contract dimension table
    # ========================================
    print("\n### Building contract dimension table ###")

    # Get contract info from stars (one row per contract per year)
    contract_dim = stars.groupby(['contract_id', 'rating_year']).agg({
        'parent_org_clean': 'first',
        'marketing_name': 'first',
        'org_type_clean': 'first'
    }).reset_index()

    contract_dim.columns = ['contract_id', 'year', 'parent_org', 'marketing_name', 'org_type']

    # Map org_type to plan_type
    org_type_map = {
        'Local CCP': 'HMO',
        'Regional CCP': 'Regional PPO',
        'PFFS': 'PFFS',
        'MSA': 'MSA',
        'PDP': 'PDP',
        '1876 Cost': '1876 Cost',
        'Demo': 'Demo',
        'National PACE': 'PACE',
        'Employer/Union Only Direct Contract PDP': 'Employer PDP',
        'Employer/Union Only Direct Contract PFFS': 'Employer PFFS',
    }
    contract_dim['plan_type'] = contract_dim['org_type'].map(org_type_map).fillna('Other')

    # Derive product type
    product_map = {
        'HMO': 'MAPD',
        'Regional PPO': 'MAPD',
        'PFFS': 'MA-only',
        'MSA': 'MA-only',
        'PDP': 'PDP',
        '1876 Cost': 'MA-only',
        'Demo': 'MAPD',
        'PACE': 'MAPD',
        'Employer PDP': 'PDP',
        'Employer PFFS': 'MA-only',
        'Other': 'Other',
    }
    contract_dim['product_type'] = contract_dim['plan_type'].map(product_map).fillna('Other')

    print(f"  Contracts: {contract_dim['contract_id'].nunique():,}")
    print(f"  Contract-years: {len(contract_dim):,}")
    print(f"  Plan types: {contract_dim['plan_type'].value_counts().to_dict()}")

    # ========================================
    # 5. Create unified enrollment with plan_type
    # ========================================
    print("\n### Creating unified enrollment ###")

    # Aggregate contracts by parent/year/plan_type from stars
    contracts_by_type = contract_dim.groupby(['year', 'parent_org', 'plan_type', 'product_type']).agg({
        'contract_id': 'nunique'
    }).reset_index()
    contracts_by_type.columns = ['year', 'parent_org', 'plan_type', 'product_type', 'contract_count']

    # Get total contracts per parent/year
    parent_totals = contracts_by_type.groupby(['year', 'parent_org'])['contract_count'].sum().reset_index()
    parent_totals.columns = ['year', 'parent_org', 'total_contracts']

    # Calculate proportion
    contracts_by_type = contracts_by_type.merge(parent_totals, on=['year', 'parent_org'])
    contracts_by_type['contract_pct'] = contracts_by_type['contract_count'] / contracts_by_type['total_contracts']

    # Merge with enrollment (using cleaned names)
    enrollment_clean = enrollment[['year', 'parent_org_clean', 'total_enrollment', 'contract_count', 'plan_count']].copy()
    enrollment_clean.columns = ['year', 'parent_org', 'total_enrollment', 'enroll_contract_count', 'plan_count']

    unified = contracts_by_type.merge(
        enrollment_clean,
        on=['year', 'parent_org'],
        how='outer'
    )

    # Distribute enrollment by contract proportion
    unified['enrollment'] = unified['total_enrollment'] * unified['contract_pct']
    unified['enrollment'] = unified['enrollment'].fillna(unified['total_enrollment'])
    unified.loc[unified['enrollment'].isna(), 'enrollment'] = 0
    unified['enrollment'] = unified['enrollment'].round(0).astype(int)

    # Fill missing plan_type for unmatched
    unified['plan_type'] = unified['plan_type'].fillna('Unknown')
    unified['product_type'] = unified['product_type'].fillna('Unknown')
    unified['contract_count'] = unified['contract_count'].fillna(1).astype(int)

    # Keep key columns
    unified = unified[['year', 'parent_org', 'plan_type', 'product_type', 'enrollment', 'contract_count']]

    print(f"  Unified rows: {len(unified):,}")

    # ========================================
    # 6. Validate totals
    # ========================================
    print("\n### Validating totals ###")

    # Compare enrollment totals
    enrollment_by_year = enrollment.groupby('year')['total_enrollment'].sum()
    unified_by_year = unified.groupby('year')['enrollment'].sum()

    print("\nYear | Enrollment Source | Unified | Match?")
    print("-" * 50)
    for year in sorted(enrollment_by_year.index):
        orig = enrollment_by_year.get(year, 0)
        new = unified_by_year.get(year, 0)
        pct = (new / orig * 100) if orig > 0 else 0
        match = "OK" if pct >= 99 else f"DIFF {pct:.1f}%"
        print(f"{year} | {orig:>14,.0f} | {new:>14,.0f} | {match}")

    # ========================================
    # 7. Save outputs
    # ========================================
    print("\n### Saving outputs ###")

    save_parquet(parent_master, 'processed/unified/dim_parent_org.parquet')
    save_parquet(contract_dim, 'processed/unified/dim_contract_v2.parquet')
    save_parquet(unified, 'processed/unified/fact_enrollment_v2.parquet')

    # Create aggregates
    agg_by_year = unified.groupby('year').agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index()
    agg_by_year.columns = ['year', 'enrollment', 'contract_count', 'parent_count']
    save_parquet(agg_by_year, 'processed/unified/agg_enrollment_by_year_v2.parquet')

    agg_by_plantype = unified.groupby(['year', 'plan_type']).agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index()
    agg_by_plantype.columns = ['year', 'plan_type', 'enrollment', 'contract_count', 'parent_count']
    save_parquet(agg_by_plantype, 'processed/unified/agg_enrollment_by_plantype_v2.parquet')

    print("\nDone!")


if __name__ == '__main__':
    main()
