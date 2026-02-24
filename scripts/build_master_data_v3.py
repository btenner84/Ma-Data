"""
Build master data tables with group vs individual breakdown.

Uses raw CPSC enrollment data for accurate contract/plan level enrollment.
Plan ID 800+ = Group (EGWP), <800 = Individual
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


def load_csv(key):
    """Load CSV from S3."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(BytesIO(response['Body'].read()))


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
    return str(name).strip()


def get_available_cpsc_files():
    """List available CPSC enrollment files."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix='raw/enrollment/'):
        for obj in page.get('Contents', []):
            if 'CPSC_Enrollment' in obj['Key'] and obj['Key'].endswith('.csv'):
                files.append(obj['Key'])
    return sorted(files)


def load_cpsc_enrollment(key):
    """Load CPSC enrollment file and extract year/month."""
    df = load_csv(key)
    # Parse year from path like raw/enrollment/2026-02/CPSC_Enrollment_Info_2026_02.csv
    parts = key.split('/')
    year_month = parts[2]  # e.g., "2026-02"
    year = int(year_month.split('-')[0])
    month = int(year_month.split('-')[1])

    df['year'] = year
    df['month'] = month

    # Standardize columns
    df = df.rename(columns={
        'Contract Number': 'contract_id',
        'Plan ID': 'plan_id',
        'State': 'state',
        'County': 'county',
        'Enrollment': 'enrollment'
    })

    # Convert enrollment to numeric
    df['enrollment'] = pd.to_numeric(df['enrollment'], errors='coerce').fillna(0).astype(int)

    # Add group_type based on plan_id
    df['group_type'] = df['plan_id'].apply(lambda x: 'Group' if x >= 800 else 'Individual')

    return df[['contract_id', 'plan_id', 'state', 'enrollment', 'group_type', 'year', 'month']]


def main():
    print("=" * 70)
    print("BUILDING MASTER DATA V3 (with Group/Individual)")
    print("=" * 70)

    # ========================================
    # 1. Load stars data for contract metadata
    # ========================================
    print("\n### Loading stars data ###")
    stars = load_parquet('processed/unified/stars_summary.parquet')
    stars['parent_org'] = stars['parent_org'].apply(normalize_name)
    stars['org_type'] = stars['org_type'].apply(lambda x: str(x).strip() if pd.notna(x) else x)
    print(f"  Stars: {len(stars):,} rows")

    # Build contract lookup (contract -> parent_org, org_type)
    contract_lookup = stars.groupby(['contract_id', 'rating_year']).agg({
        'parent_org': 'first',
        'marketing_name': 'first',
        'org_type': 'first'
    }).reset_index()
    contract_lookup.columns = ['contract_id', 'year', 'parent_org', 'marketing_name', 'org_type']

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
    contract_lookup['plan_type'] = contract_lookup['org_type'].map(org_type_map).fillna('Other')

    # Product type
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
    contract_lookup['product_type'] = contract_lookup['plan_type'].map(product_map).fillna('Other')

    print(f"  Contract lookup: {len(contract_lookup):,} rows")

    # ========================================
    # 2. Load CPSC enrollment files
    # ========================================
    print("\n### Loading CPSC enrollment files ###")
    cpsc_files = get_available_cpsc_files()
    print(f"  Found {len(cpsc_files)} CPSC files")

    all_enrollment = []
    for f in cpsc_files:
        print(f"  Loading {f}...")
        df = load_cpsc_enrollment(f)
        all_enrollment.append(df)

    enrollment = pd.concat(all_enrollment, ignore_index=True)
    print(f"  Total CPSC rows: {len(enrollment):,}")

    # Use latest month per year for annual snapshot
    latest_months = enrollment.groupby('year')['month'].max().reset_index()
    latest_months.columns = ['year', 'latest_month']
    enrollment = enrollment.merge(latest_months, on='year')
    enrollment = enrollment[enrollment['month'] == enrollment['latest_month']]
    print(f"  After keeping latest month per year: {len(enrollment):,}")

    # ========================================
    # 3. Aggregate enrollment by contract/group_type
    # ========================================
    print("\n### Aggregating enrollment ###")

    # Sum enrollment by contract_id, group_type, year
    contract_enrollment = enrollment.groupby(['contract_id', 'group_type', 'year']).agg({
        'enrollment': 'sum',
        'plan_id': 'nunique'
    }).reset_index()
    contract_enrollment.columns = ['contract_id', 'group_type', 'year', 'enrollment', 'plan_count']
    print(f"  Contract-group-year rows: {len(contract_enrollment):,}")

    # ========================================
    # 4. Join with contract metadata
    # ========================================
    print("\n### Joining with contract metadata ###")

    # Join enrollment with contract lookup
    unified = contract_enrollment.merge(
        contract_lookup[['contract_id', 'year', 'parent_org', 'plan_type', 'product_type']],
        on=['contract_id', 'year'],
        how='left'
    )

    # Fill missing parent_org from contract prefix patterns
    # H/R/S contracts without stars data - try to get from any year
    missing_parent = unified[unified['parent_org'].isna()]['contract_id'].unique()
    print(f"  Contracts missing parent org: {len(missing_parent)}")

    # Try to fill from other years of same contract
    for contract in missing_parent:
        known = contract_lookup[contract_lookup['contract_id'] == contract]['parent_org'].dropna()
        if len(known) > 0:
            unified.loc[unified['contract_id'] == contract, 'parent_org'] = known.iloc[0]

    # Fill remaining missing
    unified['parent_org'] = unified['parent_org'].fillna('Unknown')
    unified['plan_type'] = unified['plan_type'].fillna('Unknown')
    unified['product_type'] = unified['product_type'].fillna('Unknown')

    print(f"  After join: {len(unified):,} rows")
    print(f"  Unknown parent orgs: {(unified['parent_org'] == 'Unknown').sum():,}")

    # ========================================
    # 5. Build fact table
    # ========================================
    print("\n### Building fact table ###")

    fact_enrollment = unified.groupby(['year', 'parent_org', 'plan_type', 'product_type', 'group_type']).agg({
        'enrollment': 'sum',
        'contract_id': 'nunique',
        'plan_count': 'sum'
    }).reset_index()
    fact_enrollment.columns = ['year', 'parent_org', 'plan_type', 'product_type', 'group_type',
                               'enrollment', 'contract_count', 'plan_count']

    print(f"  Fact rows: {len(fact_enrollment):,}")

    # ========================================
    # 6. Validate totals
    # ========================================
    print("\n### Validating totals ###")

    print("\nYear | Total | Individual | Group | Group%")
    print("-" * 55)
    for year in sorted(fact_enrollment['year'].unique()):
        year_data = fact_enrollment[fact_enrollment['year'] == year]
        total = year_data['enrollment'].sum()
        individual = year_data[year_data['group_type'] == 'Individual']['enrollment'].sum()
        group = year_data[year_data['group_type'] == 'Group']['enrollment'].sum()
        pct = (group / total * 100) if total > 0 else 0
        print(f"{year} | {total:>12,.0f} | {individual:>12,.0f} | {group:>10,.0f} | {pct:>5.1f}%")

    print("\n=== By Product Type (latest year) ===")
    latest_year = fact_enrollment['year'].max()
    latest = fact_enrollment[fact_enrollment['year'] == latest_year]
    by_product = latest.groupby(['product_type', 'group_type'])['enrollment'].sum().unstack(fill_value=0)
    print(by_product)

    # ========================================
    # 7. Save outputs
    # ========================================
    print("\n### Saving outputs ###")

    save_parquet(fact_enrollment, 'processed/unified/fact_enrollment_v3.parquet')

    # Aggregates
    agg_by_year = fact_enrollment.groupby(['year', 'group_type']).agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index()
    save_parquet(agg_by_year, 'processed/unified/agg_enrollment_by_year_v3.parquet')

    agg_by_plantype = fact_enrollment.groupby(['year', 'plan_type', 'group_type']).agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index()
    save_parquet(agg_by_plantype, 'processed/unified/agg_enrollment_by_plantype_v3.parquet')

    # Parent org list
    parent_list = fact_enrollment.groupby('parent_org').agg({
        'enrollment': 'sum',
        'year': 'max'
    }).reset_index()
    parent_list = parent_list.sort_values('enrollment', ascending=False)
    save_parquet(parent_list, 'processed/unified/dim_parent_org_v3.parquet')

    print("\nDone!")


if __name__ == '__main__':
    main()
