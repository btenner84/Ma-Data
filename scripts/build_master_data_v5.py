"""
Build master data from ALL CPSC files (2013-2026).

Uses contract prefix for product type (H,R = MA; S,E = PDP)
Uses plan ID >= 800 for group type
"""

import boto3
import pandas as pd
from io import BytesIO
import zipfile

s3 = boto3.client('s3')
BUCKET = 'ma-data123'


def load_parquet(key):
    response = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def save_parquet(df, key):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"  Saved {key} ({len(df):,} rows)")


def list_cpsc_files():
    """List all CPSC enrollment files."""
    files = []

    # Recent CSVs
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='raw/enrollment/202')
    for obj in response.get('Contents', []):
        if 'CPSC' in obj['Key'] and obj['Key'].endswith('.csv'):
            files.append(('csv', obj['Key']))

    # Historical zips
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix='raw/enrollment/cpsc/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                files.append(('zip', obj['Key']))

    return files


def load_cpsc_csv(key):
    """Load CPSC CSV file."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(BytesIO(response['Body'].read()))


def load_cpsc_zip(key):
    """Load CPSC from zip file."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    zf = zipfile.ZipFile(BytesIO(response['Body'].read()))

    # Find CSV inside zip
    csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
    if not csv_files:
        return None

    with zf.open(csv_files[0]) as f:
        return pd.read_csv(f)


def parse_year_month(key):
    """Extract year and month from file path."""
    # Paths like: raw/enrollment/2026-02/CPSC_...csv or raw/enrollment/cpsc/2013-01/cpsc_...zip
    parts = key.split('/')
    for part in parts:
        if '-' in part and len(part.split('-')[0]) == 4:
            year_month = part
            year = int(year_month.split('-')[0])
            month = int(year_month.split('-')[1])
            return year, month
    return None, None


def process_cpsc(df, year, month):
    """Process CPSC dataframe and add classifications."""
    # Standardize column names
    col_map = {
        'Contract Number': 'contract_id',
        'Contract_Number': 'contract_id',
        'Plan ID': 'plan_id',
        'Plan_ID': 'plan_id',
        'Enrollment': 'enrollment'
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Convert enrollment to numeric
    df['enrollment'] = pd.to_numeric(df['enrollment'], errors='coerce').fillna(0).astype(int)

    # Add year/month
    df['year'] = year
    df['month'] = month

    # Contract prefix -> product type
    df['contract_id'] = df['contract_id'].astype(str)
    df['prefix'] = df['contract_id'].str[0]
    df['product_type'] = df['prefix'].map({
        'H': 'MA', 'R': 'MA',
        'S': 'PDP', 'E': 'PDP'
    }).fillna('Unknown')

    # Plan ID -> group type
    df['group_type'] = df['plan_id'].apply(lambda x: 'Group' if x >= 800 else 'Individual')

    return df[['contract_id', 'plan_id', 'enrollment', 'year', 'month', 'product_type', 'group_type', 'prefix']]


def main():
    print("=" * 70)
    print("BUILDING MASTER DATA V5 (ALL YEARS)")
    print("=" * 70)

    # ========================================
    # 1. Load plan type info from CMS
    # ========================================
    print("\n### Step 1: Load CMS plan type data ###")

    response = s3.get_object(Bucket=BUCKET, Key='raw/plan_payment/2024/plan_payment_2024.zip')
    zf = zipfile.ZipFile(BytesIO(response['Body'].read()))

    with zf.open('2024PartCPlanLevel.xlsx') as f:
        partc_plans = pd.read_excel(f, header=2)

    contract_plan_types = partc_plans.groupby('Contract Number')['Plan Type'].first().to_dict()
    print(f"  Part C plan types: {len(contract_plan_types)}")

    # ========================================
    # 2. Load stars data for parent org mapping
    # ========================================
    print("\n### Step 2: Load stars data ###")

    stars = load_parquet('processed/unified/stars_summary.parquet')
    stars['parent_org'] = stars['parent_org'].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    contract_parents = stars.sort_values('rating_year', ascending=False).groupby('contract_id')['parent_org'].first().to_dict()
    print(f"  Contracts with parent org: {len(contract_parents)}")

    # ========================================
    # 3. Process all CPSC files
    # ========================================
    print("\n### Step 3: Load all CPSC files ###")

    files = list_cpsc_files()
    print(f"  Found {len(files)} CPSC files")

    all_data = []

    for file_type, key in files:
        year, month = parse_year_month(key)
        if year is None:
            continue

        try:
            if file_type == 'csv':
                df = load_cpsc_csv(key)
            else:
                df = load_cpsc_zip(key)

            if df is None or len(df) == 0:
                continue

            df = process_cpsc(df, year, month)
            all_data.append(df)
            print(f"  {year}-{month:02d}: {len(df):,} rows, {df['enrollment'].sum():,.0f} enrollment")

        except Exception as e:
            print(f"  ERROR {key}: {e}")

    print(f"\n  Total files processed: {len(all_data)}")

    # Combine all data
    cpsc = pd.concat(all_data, ignore_index=True)
    print(f"  Total rows: {len(cpsc):,}")

    # ========================================
    # 4. Keep latest month per year (annual snapshot)
    # ========================================
    print("\n### Step 4: Annual snapshots ###")

    latest_months = cpsc.groupby('year')['month'].max().reset_index()
    latest_months.columns = ['year', 'latest_month']

    cpsc = cpsc.merge(latest_months, on='year')
    cpsc = cpsc[cpsc['month'] == cpsc['latest_month']]

    print(f"  Rows after annual snapshot: {len(cpsc):,}")

    # ========================================
    # 5. Add plan type and parent org
    # ========================================
    print("\n### Step 5: Add metadata ###")

    # Plan type from CMS data
    cpsc['plan_type'] = cpsc['contract_id'].map(contract_plan_types)
    cpsc.loc[cpsc['prefix'] == 'R', 'plan_type'] = 'Regional PPO'
    cpsc.loc[cpsc['product_type'] == 'PDP', 'plan_type'] = 'PDP'
    cpsc['plan_type'] = cpsc['plan_type'].fillna('Unknown')

    # Parent org from stars
    cpsc['parent_org'] = cpsc['contract_id'].map(contract_parents).fillna('Unknown')

    # ========================================
    # 6. Build fact table
    # ========================================
    print("\n### Step 6: Build fact table ###")

    fact = cpsc.groupby(['year', 'parent_org', 'product_type', 'plan_type', 'group_type']).agg({
        'enrollment': 'sum',
        'contract_id': 'nunique',
        'plan_id': 'nunique'
    }).reset_index()
    fact.columns = ['year', 'parent_org', 'product_type', 'plan_type', 'group_type',
                    'enrollment', 'contract_count', 'plan_count']

    print(f"  Fact table rows: {len(fact):,}")

    # ========================================
    # 7. Validate
    # ========================================
    print("\n### Step 7: Validate ###")

    print("\n=== MA Enrollment by Year ===")
    ma_by_year = fact[fact['product_type'] == 'MA'].groupby('year')['enrollment'].sum()
    for y, e in ma_by_year.items():
        print(f"  {y}: {e:,.0f}")

    print("\n=== MA by Group Type (latest year) ===")
    latest = fact[fact['year'] == fact['year'].max()]
    ma_latest = latest[latest['product_type'] == 'MA']
    print(ma_latest.groupby('group_type')['enrollment'].sum())

    # ========================================
    # 8. Save
    # ========================================
    print("\n### Step 8: Save ###")

    save_parquet(fact, 'processed/unified/fact_enrollment_v5.parquet')

    # Parent org list
    parent_enrollment = fact[fact['product_type'] == 'MA'].groupby('parent_org')['enrollment'].sum()
    parent_enrollment = parent_enrollment.sort_values(ascending=False).reset_index()
    parent_enrollment.columns = ['parent_org', 'ma_enrollment']
    save_parquet(parent_enrollment, 'processed/unified/dim_parent_org_v5.parquet')

    print("\nDone!")


if __name__ == '__main__':
    main()
