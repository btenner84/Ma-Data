"""
Build master data from ALL CPSC files (2013-2026) with CORRECT product types.

Uses Contract_Info file for:
- Offers Part D: Yes = MAPD, No = MA-only (for MA contracts)
- Plan Type: HMO, Local PPO, Regional PPO, PFFS, MSA, etc.
- EGHP: Yes = Group (or Plan ID >= 800)
- Parent Organization: payer name
"""

import boto3
import pandas as pd
from io import BytesIO
import zipfile

s3 = boto3.client('s3')
BUCKET = 'ma-data123'


def save_parquet(df, key):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"  Saved {key} ({len(df):,} rows)")


def list_cpsc_files():
    """List all CPSC enrollment files."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix='raw/enrollment/cpsc/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                files.append(obj['Key'])

    # Also add recent CSVs (these don't have the same structure)
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='raw/enrollment/202')
    for obj in response.get('Contents', []):
        if 'CPSC' in obj['Key'] and obj['Key'].endswith('.csv'):
            files.append(obj['Key'])

    return sorted(files)


def parse_year_month(key):
    """Extract year and month from file path."""
    parts = key.split('/')
    for part in parts:
        if '-' in part and len(part.split('-')[0]) == 4:
            year_month = part
            year = int(year_month.split('-')[0])
            month = int(year_month.split('-')[1])
            return year, month
    return None, None


def load_cpsc_zip(key):
    """Load CPSC data from zip file (has both contract info and enrollment)."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    zf = zipfile.ZipFile(BytesIO(response['Body'].read()))

    # Find contract info and enrollment files
    contract_file = None
    enroll_file = None
    for f in zf.namelist():
        if 'Contract_Info' in f and f.endswith('.csv'):
            contract_file = f
        elif 'Enrollment_Info' in f and f.endswith('.csv'):
            enroll_file = f

    if not contract_file or not enroll_file:
        return None, None

    # Load with latin-1 encoding
    with zf.open(contract_file) as f:
        contracts = pd.read_csv(f, encoding='latin-1')

    with zf.open(enroll_file) as f:
        enrollment = pd.read_csv(f, encoding='latin-1')

    return contracts, enrollment


def load_cpsc_csv(key):
    """Load recent CPSC CSV (enrollment only, no contract info)."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    enrollment = pd.read_csv(BytesIO(response['Body'].read()))
    return None, enrollment


def process_data(contracts, enrollment, year, month):
    """Process and join contract info with enrollment."""
    # Standardize enrollment columns
    enrollment = enrollment.rename(columns={
        'Contract Number': 'contract_id',
        'Plan ID': 'plan_id',
        'Enrollment': 'enrollment'
    })
    enrollment['enrollment'] = pd.to_numeric(enrollment['enrollment'], errors='coerce').fillna(0).astype(int)
    enrollment['year'] = year
    enrollment['month'] = month

    if contracts is not None:
        # Standardize contract columns
        contracts = contracts.rename(columns={
            'Contract ID': 'contract_id',
            'Plan ID': 'plan_id',
            'Plan Type': 'plan_type',
            'Offers Part D': 'offers_part_d',
            'SNP Plan': 'snp_plan',
            'EGHP': 'eghp',
            'Parent Organization': 'parent_org'
        })

        # Determine product type from contract info
        # H/R contracts with Offers Part D = Yes -> MAPD
        # H/R contracts with Offers Part D = No -> MA-only
        # S/E contracts -> PDP
        contracts['contract_id'] = contracts['contract_id'].astype(str)
        contracts['prefix'] = contracts['contract_id'].str[0]

        def get_product_type(row):
            prefix = row.get('prefix', '')
            offers_d = str(row.get('offers_part_d', '')).lower()
            if prefix in ['S', 'E']:
                return 'PDP'
            elif prefix in ['H', 'R']:
                if offers_d == 'yes':
                    return 'MAPD'
                else:
                    return 'MA-only'
            return 'Unknown'

        contracts['product_type'] = contracts.apply(get_product_type, axis=1)

        # Determine group type
        contracts['group_type'] = contracts.apply(
            lambda x: 'Group' if str(x.get('eghp', '')).lower() == 'yes' or x.get('plan_id', 0) >= 800 else 'Individual',
            axis=1
        )

        # Normalize plan type
        plan_type_map = {
            'HMO/HMOPOS': 'HMO',
            'Medicare-Medicaid Plan HMO/HMOPOS': 'HMO',
            'Local PPO': 'Local PPO',
            'Regional PPO': 'Regional PPO',
            'PFFS': 'PFFS',
            'MSA': 'MSA',
            '1876 Cost': '1876 Cost',
            'HCPP - 1833 Cost': '1876 Cost',
            'National PACE': 'PACE',
            'Medicare Prescription Drug Plan': 'PDP',
            'Employer/Union Only Direct Contract PDP': 'PDP',
        }
        contracts['plan_type'] = contracts['plan_type'].map(plan_type_map).fillna(contracts['plan_type'])

        # Clean parent org
        contracts['parent_org'] = contracts['parent_org'].apply(lambda x: str(x).strip() if pd.notna(x) else 'Unknown')

        # Create lookup for contract/plan -> metadata
        contract_lookup = contracts.groupby(['contract_id', 'plan_id']).agg({
            'plan_type': 'first',
            'product_type': 'first',
            'group_type': 'first',
            'parent_org': 'first'
        }).reset_index()

        # Join with enrollment
        enrollment['contract_id'] = enrollment['contract_id'].astype(str)
        merged = enrollment.merge(
            contract_lookup,
            on=['contract_id', 'plan_id'],
            how='left'
        )
    else:
        # For recent CSVs without contract info, use prefix-based classification
        enrollment['contract_id'] = enrollment['contract_id'].astype(str)
        enrollment['prefix'] = enrollment['contract_id'].str[0]
        enrollment['product_type'] = enrollment['prefix'].map({
            'H': 'MAPD',  # Assume MAPD for H contracts (most are)
            'R': 'MAPD',
            'S': 'PDP',
            'E': 'PDP'
        }).fillna('Unknown')
        enrollment['plan_type'] = enrollment['prefix'].map({
            'H': 'HMO',
            'R': 'Regional PPO',
            'S': 'PDP',
            'E': 'PDP'
        }).fillna('Unknown')
        enrollment['group_type'] = enrollment['plan_id'].apply(lambda x: 'Group' if x >= 800 else 'Individual')
        enrollment['parent_org'] = 'Unknown'
        merged = enrollment

    # Fill any missing values
    merged['plan_type'] = merged['plan_type'].fillna('Unknown')
    merged['product_type'] = merged['product_type'].fillna('Unknown')
    merged['group_type'] = merged['group_type'].fillna('Individual')
    merged['parent_org'] = merged['parent_org'].fillna('Unknown')

    return merged[['contract_id', 'plan_id', 'enrollment', 'year', 'month',
                   'plan_type', 'product_type', 'group_type', 'parent_org']]


def main():
    print("=" * 70)
    print("BUILDING MASTER DATA V6 (FULL DETAIL)")
    print("=" * 70)

    # ========================================
    # 1. Process all CPSC files
    # ========================================
    print("\n### Step 1: Process all CPSC files ###")

    files = list_cpsc_files()
    print(f"  Found {len(files)} files")

    all_data = []

    for key in files:
        year, month = parse_year_month(key)
        if year is None:
            continue

        # Only process December of each year (annual snapshot)
        # Exception: use latest month if December not available

        try:
            if key.endswith('.zip'):
                contracts, enrollment = load_cpsc_zip(key)
            else:
                contracts, enrollment = load_cpsc_csv(key)

            if enrollment is None or len(enrollment) == 0:
                continue

            data = process_data(contracts, enrollment, year, month)
            all_data.append(data)

            total = data['enrollment'].sum()
            ma = data[data['product_type'].isin(['MAPD', 'MA-only'])]['enrollment'].sum()
            print(f"  {year}-{month:02d}: {total:,.0f} total, {ma:,.0f} MA")

        except Exception as e:
            print(f"  ERROR {key}: {e}")

    print(f"\n  Total files processed: {len(all_data)}")

    # Combine all data
    combined = pd.concat(all_data, ignore_index=True)
    print(f"  Total rows: {len(combined):,}")

    # ========================================
    # 2. Keep latest month per year (annual snapshot)
    # ========================================
    print("\n### Step 2: Annual snapshots ###")

    latest_months = combined.groupby('year')['month'].max().reset_index()
    latest_months.columns = ['year', 'latest_month']

    combined = combined.merge(latest_months, on='year')
    combined = combined[combined['month'] == combined['latest_month']]

    print(f"  Rows after annual snapshot: {len(combined):,}")

    # ========================================
    # 3. Build fact table
    # ========================================
    print("\n### Step 3: Build fact table ###")

    fact = combined.groupby(['year', 'parent_org', 'product_type', 'plan_type', 'group_type']).agg({
        'enrollment': 'sum',
        'contract_id': 'nunique',
        'plan_id': 'nunique'
    }).reset_index()
    fact.columns = ['year', 'parent_org', 'product_type', 'plan_type', 'group_type',
                    'enrollment', 'contract_count', 'plan_count']

    print(f"  Fact table rows: {len(fact):,}")

    # ========================================
    # 4. Validate
    # ========================================
    print("\n### Step 4: Validate ###")

    print("\n=== By Year ===")
    by_year = fact.groupby('year').agg({
        'enrollment': 'sum'
    })
    for y, row in by_year.iterrows():
        print(f"  {y}: {row['enrollment']:,.0f}")

    print("\n=== Product Types (latest year) ===")
    latest = fact[fact['year'] == fact['year'].max()]
    by_product = latest.groupby('product_type')['enrollment'].sum().sort_values(ascending=False)
    for pt, e in by_product.items():
        print(f"  {pt}: {e:,.0f}")

    print("\n=== MAPD by Group Type (latest year) ===")
    mapd = latest[latest['product_type'] == 'MAPD']
    print(mapd.groupby('group_type')['enrollment'].sum())

    # ========================================
    # 5. Save
    # ========================================
    print("\n### Step 5: Save ###")

    save_parquet(fact, 'processed/unified/fact_enrollment_v6.parquet')

    # Parent org list (sorted by MA enrollment)
    ma_products = ['MAPD', 'MA-only']
    parent_enrollment = fact[fact['product_type'].isin(ma_products)].groupby('parent_org')['enrollment'].sum()
    parent_enrollment = parent_enrollment.sort_values(ascending=False).reset_index()
    parent_enrollment.columns = ['parent_org', 'ma_enrollment']
    save_parquet(parent_enrollment, 'processed/unified/dim_parent_org_v6.parquet')

    print("\nDone!")


if __name__ == '__main__':
    main()
