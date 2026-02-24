#!/usr/bin/env python3
"""
Build Unified Risk Scores Fact Table.
Joins risk scores with enrollment data for weighted averages and filtering.
"""

import boto3
import pandas as pd
import zipfile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str):
    """Upload DataFrame as Parquet to S3."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"  Uploaded: {s3_key} ({len(df):,} rows)")


def load_parquet(s3_key: str) -> pd.DataFrame:
    """Load Parquet from S3."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def read_plan_level_excel(zf, year: int) -> pd.DataFrame:
    """Read plan level Excel file from zip, handling various formats."""
    # Try different filename patterns
    patterns = [
        f'{year}PartCPlanLevel.xlsx',
        f'{year}partcplanlevel.xlsx',
        f'PartCPlanLevel.xlsx',
        f'{year}_PartC_Plan_Level.xlsx',
    ]

    filename = None
    for pattern in patterns:
        for name in zf.namelist():
            if pattern.lower() in name.lower() and 'county' not in name.lower():
                filename = name
                break
        if filename:
            break

    if not filename:
        # Fallback: look for any xlsx with 'plan' and 'level' in name
        for name in zf.namelist():
            if 'plan' in name.lower() and 'level' in name.lower() and name.endswith('.xlsx'):
                if 'county' not in name.lower():
                    filename = name
                    break

    if not filename:
        return pd.DataFrame()

    with zf.open(filename) as f:
        xls = pd.ExcelFile(f, engine='openpyxl')

        # Try each sheet until we find data
        df = pd.DataFrame()
        for sheet_name in xls.sheet_names:
            try:
                # Try header at row 2 (common format)
                test_df = pd.read_excel(xls, sheet_name=sheet_name, header=2)
                if not test_df.empty and len(test_df.columns) >= 3:
                    # Check if it looks like plan data
                    cols_lower = [str(c).lower() for c in test_df.columns]
                    if any('contract' in c for c in cols_lower):
                        df = test_df
                        break
            except:
                continue

    return df


def process_year(year: int) -> pd.DataFrame:
    """Process plan payment data for a single year."""
    s3_key = f"raw/plan_payment/{year}/plan_payment_{year}.zip"

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zip_bytes = BytesIO(response['Body'].read())
    except Exception as e:
        print(f"  {year}: Not found in S3")
        return pd.DataFrame()

    try:
        with zipfile.ZipFile(zip_bytes, 'r') as zf:
            df = read_plan_level_excel(zf, year)

            if df.empty:
                print(f"  {year}: No plan level data found")
                return pd.DataFrame()

            # Standardize column names
            col_map = {
                'Contract Number': 'contract_id',
                'Contract': 'contract_id',
                'Plan Benefit Package': 'plan_id',
                'Plan ID': 'plan_id',
                'Contract Name': 'contract_name',
                'Plan Type': 'plan_type',
                'Average Part C Risk Score': 'avg_risk_score',
                'Average A/B PM/PM Payment': 'avg_ab_payment',
                'Average Rebate PM/PM Payment': 'avg_rebate',
            }

            for old, new in col_map.items():
                if old in df.columns:
                    df = df.rename(columns={old: new})

            # Add year
            df['year'] = year

            # Keep only relevant columns
            keep_cols = ['year', 'contract_id', 'plan_id', 'contract_name', 'plan_type',
                        'avg_risk_score', 'avg_ab_payment', 'avg_rebate']
            available = [c for c in keep_cols if c in df.columns]
            df = df[available].copy()

            # Clean data
            if 'contract_id' in df.columns:
                df['contract_id'] = df['contract_id'].astype(str).str.strip()
                df = df[df['contract_id'].str.len() > 0]
                df = df[~df['contract_id'].str.lower().isin(['nan', 'none', ''])]

            # Convert numeric columns, handling currency strings
            for col in ['avg_risk_score', 'avg_ab_payment', 'avg_rebate']:
                if col in df.columns:
                    # Remove $ and , from currency values
                    df[col] = df[col].astype(str).str.replace('$', '', regex=False)
                    df[col] = df[col].str.replace(',', '', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Fix 2024 CMS data where columns are swapped
            # Risk scores should be ~0.5-3.0, payments should be ~500-1200
            if year == 2024 and 'avg_risk_score' in df.columns and 'avg_ab_payment' in df.columns:
                # Check if risk scores look like payments (mean > 100)
                risk_mean = df['avg_risk_score'].mean()
                payment_mean = df['avg_ab_payment'].mean()
                if risk_mean > 100 and payment_mean < 10:
                    # Columns are swapped - fix them
                    print(f"  {year}: Detected column swap, correcting...")
                    df['avg_risk_score'], df['avg_ab_payment'] = df['avg_ab_payment'].copy(), df['avg_risk_score'].copy()

            if 'plan_id' in df.columns:
                df['plan_id'] = df['plan_id'].astype(str).str.strip()

            risk_count = df['avg_risk_score'].notna().sum() if 'avg_risk_score' in df.columns else 0
            print(f"  {year}: {len(df):,} plans, {risk_count:,} with risk scores")

            return df

    except Exception as e:
        print(f"  {year}: Error - {e}")
        return pd.DataFrame()


def load_contract_to_parent() -> pd.DataFrame:
    """Load contract to parent org mapping."""
    # Try the CSV file first
    try:
        import os
        csv_path = '/Users/bentenner/rate/ma-data-platform/data/contract_to_parent.csv'
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            df = df.rename(columns={
                'Contract ID': 'contract_id',
                'Parent Organization': 'parent_org'
            })
            return df[['contract_id', 'parent_org']].drop_duplicates()
    except:
        pass

    # Fallback to stars enrollment unified
    try:
        df = load_parquet('processed/unified/stars_enrollment_unified.parquet')
        # Get latest year's contract-parent mapping
        latest = df.groupby('contract_id').agg({
            'parent_org': 'first',
            'star_year': 'max'
        }).reset_index()
        return latest[['contract_id', 'parent_org']].drop_duplicates()
    except:
        pass

    return pd.DataFrame(columns=['contract_id', 'parent_org'])


def load_enrollment_by_contract() -> pd.DataFrame:
    """Load enrollment data at contract level for weighting."""
    try:
        df = load_parquet('processed/unified/stars_enrollment_unified.parquet')
        # Aggregate by contract, year
        agg = df.groupby(['contract_id', 'star_year']).agg({
            'enrollment': 'sum',
            'parent_org': 'first',
            'plan_type': 'first',
            'group_type': 'first',
            'snp_type': 'first'
        }).reset_index()
        agg = agg.rename(columns={'star_year': 'year'})
        return agg
    except Exception as e:
        print(f"Warning: Could not load enrollment data: {e}")
        return pd.DataFrame()


def main():
    print("=" * 60)
    print("BUILDING UNIFIED RISK SCORES")
    print("=" * 60)

    # Process all years
    print("\n=== PROCESSING RAW PLAN PAYMENT DATA ===")
    years = list(range(2006, 2025))
    all_data = []

    for year in years:
        df = process_year(year)
        if not df.empty:
            all_data.append(df)

    if not all_data:
        print("ERROR: No data processed")
        return

    # Combine all years
    risk_scores = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal risk scores: {len(risk_scores):,} rows")
    print(f"Years with data: {sorted(risk_scores['year'].unique())}")

    # Upload raw risk scores by plan
    upload_parquet_to_s3(risk_scores, 'processed/unified/risk_scores_by_plan_v2.parquet')

    # Load contract-to-parent mapping
    print("\n=== LOADING CONTRACT-TO-PARENT MAPPING ===")
    contract_parent = load_contract_to_parent()
    print(f"Contract-parent mappings: {len(contract_parent):,}")

    # Load enrollment data for weighting
    print("\n=== LOADING ENROLLMENT DATA ===")
    enrollment = load_enrollment_by_contract()
    print(f"Enrollment records: {len(enrollment):,}")

    # Join risk scores with parent org
    print("\n=== JOINING DATA ===")

    # First join with contract_parent
    if not contract_parent.empty:
        risk_scores = risk_scores.merge(
            contract_parent,
            on='contract_id',
            how='left'
        )
        matched = risk_scores['parent_org'].notna().sum()
        print(f"Matched to parent org: {matched:,} / {len(risk_scores):,}")

    # Join with enrollment to get enrollment weights and additional fields
    if not enrollment.empty:
        # Keep risk scores columns, add enrollment
        risk_with_enrollment = risk_scores.merge(
            enrollment[['contract_id', 'year', 'enrollment', 'plan_type', 'group_type', 'snp_type']],
            on=['contract_id', 'year'],
            how='left',
            suffixes=('', '_enroll')
        )

        # Use enrollment plan_type if risk score plan_type is missing
        if 'plan_type_enroll' in risk_with_enrollment.columns:
            mask = risk_with_enrollment['plan_type'].isna() | (risk_with_enrollment['plan_type'] == '')
            risk_with_enrollment.loc[mask, 'plan_type'] = risk_with_enrollment.loc[mask, 'plan_type_enroll']
            risk_with_enrollment = risk_with_enrollment.drop(columns=['plan_type_enroll'])

        risk_scores = risk_with_enrollment

        with_enrollment = risk_scores['enrollment'].notna().sum()
        print(f"Matched to enrollment: {with_enrollment:,} / {len(risk_scores):,}")

    # Create aggregated summary by year
    print("\n=== CREATING SUMMARIES ===")

    # Summary by year
    valid_risk = risk_scores[risk_scores['avg_risk_score'].notna()].copy()

    summary_by_year = valid_risk.groupby('year').agg({
        'avg_risk_score': ['mean', 'min', 'max', 'std'],
        'contract_id': 'nunique'
    }).reset_index()
    summary_by_year.columns = ['year', 'avg_risk_score', 'min_risk_score', 'max_risk_score', 'std_risk_score', 'contract_count']
    summary_by_year['record_count'] = valid_risk.groupby('year').size().values

    upload_parquet_to_s3(summary_by_year, 'processed/unified/risk_scores_summary_v2.parquet')

    # Summary by year and parent org (for payer comparison)
    # IMPORTANT: First aggregate to contract level to avoid duplicate enrollment
    if 'parent_org' in valid_risk.columns:
        # Step 1: Aggregate risk scores to contract level (average across plans)
        by_contract = valid_risk.groupby(['year', 'contract_id', 'parent_org']).agg({
            'avg_risk_score': 'mean',  # Average risk across plans in contract
        }).reset_index()

        # Step 2: Join enrollment at contract level WITH filter dimensions (no duplication)
        enrollment_with_dims = enrollment[['contract_id', 'year', 'enrollment', 'plan_type', 'snp_type', 'group_type']].drop_duplicates()
        by_contract = by_contract.merge(
            enrollment_with_dims,
            on=['contract_id', 'year'],
            how='left'
        )

        # Fill missing values
        by_contract['enrollment'] = by_contract['enrollment'].fillna(0)
        by_contract['plan_type'] = by_contract['plan_type'].fillna('Unknown')
        by_contract['snp_type'] = by_contract['snp_type'].fillna('Unknown')
        by_contract['group_type'] = by_contract['group_type'].fillna('Unknown')

        # Step 3: Calculate weighted risk
        by_contract['weighted_risk'] = by_contract['avg_risk_score'] * by_contract['enrollment']

        # Create aggregation BY parent_org only (for backward compatibility)
        by_parent_year = by_contract.groupby(['year', 'parent_org']).agg({
            'avg_risk_score': 'mean',
            'weighted_risk': 'sum',
            'enrollment': 'sum',
            'contract_id': 'nunique'
        }).reset_index()

        by_parent_year['wavg_risk_score'] = by_parent_year.apply(
            lambda row: round(row['weighted_risk'] / row['enrollment'], 4) if row['enrollment'] > 0 else row['avg_risk_score'],
            axis=1
        )
        by_parent_year = by_parent_year.drop(columns=['weighted_risk'])
        by_parent_year = by_parent_year.rename(columns={
            'avg_risk_score': 'simple_avg_risk_score',
            'contract_id': 'contract_count'
        })
        upload_parquet_to_s3(by_parent_year, 'processed/unified/risk_scores_by_parent_year.parquet')

        # Create aggregation WITH filter dimensions (for filtered queries)
        by_parent_dims = by_contract.groupby(['year', 'parent_org', 'plan_type', 'snp_type', 'group_type']).agg({
            'avg_risk_score': 'mean',
            'weighted_risk': 'sum',
            'enrollment': 'sum',
            'contract_id': 'nunique'
        }).reset_index()

        by_parent_dims['wavg_risk_score'] = by_parent_dims.apply(
            lambda row: round(row['weighted_risk'] / row['enrollment'], 4) if row['enrollment'] > 0 else row['avg_risk_score'],
            axis=1
        )
        by_parent_dims = by_parent_dims.drop(columns=['weighted_risk'])
        by_parent_dims = by_parent_dims.rename(columns={
            'avg_risk_score': 'simple_avg_risk_score',
            'contract_id': 'contract_count'
        })
        upload_parquet_to_s3(by_parent_dims, 'processed/unified/risk_scores_by_parent_dims.parquet')
        print(f"  Risk scores with dimensions: {len(by_parent_dims):,} rows")

    # Create unified fact table
    final_cols = ['year', 'contract_id', 'plan_id', 'contract_name', 'plan_type',
                  'parent_org', 'group_type', 'snp_type', 'avg_risk_score', 'enrollment']
    available_cols = [c for c in final_cols if c in risk_scores.columns]
    unified = risk_scores[available_cols].copy()

    # Normalize plan types
    plan_type_map = {
        'HMO': 'HMO',
        'HMO-POS': 'HMO',
        'HMOPOS': 'HMO',
        'HMO/HMOPOS': 'HMO',
        'Local PPO': 'PPO',
        'Regional PPO': 'RPPO',
        'PFFS': 'PFFS',
        'MSA': 'MSA',
        'National PACE': 'PACE',
        'PACE': 'PACE',
    }

    if 'plan_type' in unified.columns:
        unified['plan_type_normalized'] = unified['plan_type'].map(
            lambda x: plan_type_map.get(x, x) if pd.notna(x) else None
        )

    upload_parquet_to_s3(unified, 'processed/unified/fact_risk_scores_unified.parquet')

    # Print summary
    print("\n" + "=" * 60)
    print("UNIFIED RISK SCORES COMPLETE")
    print("=" * 60)
    print(f"\nFinal dataset: {len(unified):,} rows")
    print(f"Years: {sorted(unified['year'].unique())}")
    print(f"Contracts: {unified['contract_id'].nunique():,}")
    if 'parent_org' in unified.columns:
        print(f"Parent orgs: {unified['parent_org'].nunique():,}")

    print("\nRisk score coverage by year:")
    for year in sorted(unified['year'].unique()):
        year_data = unified[unified['year'] == year]
        with_risk = year_data['avg_risk_score'].notna().sum()
        print(f"  {year}: {with_risk:,} / {len(year_data):,} with risk scores")


if __name__ == '__main__':
    main()
