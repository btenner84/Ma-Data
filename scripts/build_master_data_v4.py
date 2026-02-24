"""
Build master data tables with CORRECT classifications.

Key findings from audit:
- Contract prefix determines product type:
  H, R = MA (Part C)
  S, E = PDP (Part D)
- Plan type comes from CMS plan payment data (HMO, Local PPO, Regional PPO, PFFS, MSA)
- Plan ID >= 800 = Group (EGWP), < 800 = Individual
"""

import boto3
import pandas as pd
from io import BytesIO
import zipfile

s3 = boto3.client('s3')
BUCKET = 'ma-data123'


def load_csv(key):
    """Load CSV from S3."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(BytesIO(response['Body'].read()))


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
    """Normalize parent org name."""
    if pd.isna(name):
        return None
    return str(name).strip()


def main():
    print("=" * 70)
    print("BUILDING MASTER DATA V4 (CORRECT CLASSIFICATIONS)")
    print("=" * 70)

    # ========================================
    # 1. Load plan type info from CMS plan payment data
    # ========================================
    print("\n### Step 1: Load CMS plan type data ###")

    response = s3.get_object(Bucket=BUCKET, Key='raw/plan_payment/2024/plan_payment_2024.zip')
    zf = zipfile.ZipFile(BytesIO(response['Body'].read()))

    # Part C plan types (MA)
    with zf.open('2024PartCPlanLevel.xlsx') as f:
        partc_plans = pd.read_excel(f, header=2)

    contract_plan_types = partc_plans.groupby('Contract Number')['Plan Type'].first().to_dict()
    print(f"  Part C contracts with plan types: {len(contract_plan_types)}")

    # ========================================
    # 2. Load stars data for parent org mapping
    # ========================================
    print("\n### Step 2: Load stars data for parent org mapping ###")

    stars = load_parquet('processed/unified/stars_summary.parquet')
    stars['parent_org'] = stars['parent_org'].apply(normalize_name)

    # Create contract -> parent_org lookup (use latest year for each contract)
    contract_parents = stars.sort_values('rating_year', ascending=False).groupby('contract_id')['parent_org'].first().to_dict()
    print(f"  Contracts with parent org: {len(contract_parents)}")

    # ========================================
    # 3. Load CPSC enrollment data
    # ========================================
    print("\n### Step 3: Load CPSC enrollment data ###")

    # Load all available CPSC files
    cpsc_files = [
        'raw/enrollment/2025-12/CPSC_Enrollment_Info_2025_12.csv',
        'raw/enrollment/2026-02/CPSC_Enrollment_Info_2026_02.csv'
    ]

    all_enrollment = []
    for key in cpsc_files:
        try:
            df = load_csv(key)
            # Parse year from path
            year_month = key.split('/')[2]
            year = int(year_month.split('-')[0])
            month = int(year_month.split('-')[1])
            df['year'] = year
            df['month'] = month
            all_enrollment.append(df)
            print(f"  Loaded {key}: {len(df):,} rows")
        except Exception as e:
            print(f"  Error loading {key}: {e}")

    cpsc = pd.concat(all_enrollment, ignore_index=True)

    # Standardize columns
    cpsc = cpsc.rename(columns={
        'Contract Number': 'contract_id',
        'Plan ID': 'plan_id',
        'Enrollment': 'enrollment'
    })
    cpsc['enrollment'] = pd.to_numeric(cpsc['enrollment'], errors='coerce').fillna(0).astype(int)

    # Keep latest month per year
    latest_months = cpsc.groupby('year')['month'].max().reset_index()
    cpsc = cpsc.merge(latest_months, on='year', suffixes=('', '_latest'))
    cpsc = cpsc[cpsc['month'] == cpsc['month_latest']]
    print(f"  Total rows after dedup: {len(cpsc):,}")

    # ========================================
    # 4. Apply correct classifications
    # ========================================
    print("\n### Step 4: Apply correct classifications ###")

    cpsc['prefix'] = cpsc['contract_id'].str[0]

    # Product type from contract prefix
    cpsc['product_type'] = cpsc['prefix'].map({
        'H': 'MA',
        'R': 'MA',
        'S': 'PDP',
        'E': 'PDP'
    }).fillna('Unknown')

    # Group type from plan ID
    cpsc['group_type'] = cpsc['plan_id'].apply(lambda x: 'Group' if x >= 800 else 'Individual')

    # Plan type from CMS data (MA contracts only)
    cpsc['plan_type'] = cpsc['contract_id'].map(contract_plan_types)
    # Override for Regional PPO
    cpsc.loc[cpsc['prefix'] == 'R', 'plan_type'] = 'Regional PPO'
    # Override for PDP contracts
    cpsc.loc[cpsc['product_type'] == 'PDP', 'plan_type'] = 'PDP'
    # Fill remaining Unknown
    cpsc['plan_type'] = cpsc['plan_type'].fillna('Unknown')

    # Parent org from stars data
    cpsc['parent_org'] = cpsc['contract_id'].map(contract_parents).fillna('Unknown')

    print(f"\n  Product type distribution:")
    print(cpsc.groupby('product_type')['enrollment'].sum())

    print(f"\n  Plan type distribution:")
    print(cpsc.groupby('plan_type')['enrollment'].sum().sort_values(ascending=False))

    # ========================================
    # 5. Build fact table
    # ========================================
    print("\n### Step 5: Build fact table ###")

    fact = cpsc.groupby(['year', 'parent_org', 'product_type', 'plan_type', 'group_type']).agg({
        'enrollment': 'sum',
        'contract_id': 'nunique',
        'plan_id': 'nunique'
    }).reset_index()
    fact.columns = ['year', 'parent_org', 'product_type', 'plan_type', 'group_type',
                    'enrollment', 'contract_count', 'plan_count']

    print(f"  Fact table rows: {len(fact):,}")

    # ========================================
    # 6. Validate totals
    # ========================================
    print("\n### Step 6: Validate totals ###")

    print("\n=== By Year ===")
    for year in sorted(fact['year'].unique()):
        year_data = fact[fact['year'] == year]
        total = year_data['enrollment'].sum()
        ma = year_data[year_data['product_type'] == 'MA']['enrollment'].sum()
        pdp = year_data[year_data['product_type'] == 'PDP']['enrollment'].sum()
        print(f"{year}: Total={total:,.0f}, MA={ma:,.0f}, PDP={pdp:,.0f}")

    print("\n=== MA by Group Type (latest year) ===")
    latest = fact[fact['year'] == fact['year'].max()]
    ma_latest = latest[latest['product_type'] == 'MA']
    print(ma_latest.groupby('group_type')['enrollment'].sum())

    print("\n=== MA by Plan Type (latest year) ===")
    print(ma_latest.groupby('plan_type')['enrollment'].sum().sort_values(ascending=False))

    # ========================================
    # 7. Save outputs
    # ========================================
    print("\n### Step 7: Save outputs ###")

    save_parquet(fact, 'processed/unified/fact_enrollment_v4.parquet')

    # Aggregates
    agg_by_year = fact.groupby(['year', 'product_type', 'group_type']).agg({
        'enrollment': 'sum',
        'contract_count': 'sum',
        'parent_org': 'nunique'
    }).reset_index()
    save_parquet(agg_by_year, 'processed/unified/agg_enrollment_by_year_v4.parquet')

    # Parent org list sorted by MA enrollment
    parent_enrollment = fact[fact['product_type'] == 'MA'].groupby('parent_org')['enrollment'].sum()
    parent_enrollment = parent_enrollment.sort_values(ascending=False).reset_index()
    parent_enrollment.columns = ['parent_org', 'ma_enrollment']
    save_parquet(parent_enrollment, 'processed/unified/dim_parent_org_v4.parquet')

    print("\nDone!")


if __name__ == '__main__':
    main()
