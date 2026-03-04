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
            # Note: CMS uses different column names across years with newlines and variations
            col_map = {
                # Contract ID variations
                'Contract Number': 'contract_id',
                'Contract': 'contract_id',
                # Plan ID variations (some have newlines)
                'Plan Benefit Package': 'plan_id',
                'Plan Benefit\nPackage': 'plan_id',  # 2014-2018 format with newline
                'Plan ID': 'plan_id',
                # Contract name
                'Contract Name': 'contract_name',
                'Plan Type': 'plan_type',
                # Risk score variations across years (2006-2024)
                'Average Risk Score': 'avg_risk_score',  # 2006-2013
                'Average Part\nRisk Score': 'avg_risk_score',  # 2014
                'Average Part C\nRisk Score': 'avg_risk_score',  # 2015
                'Average Part\nC Risk Score': 'avg_risk_score',  # 2016
                'Average Part C Risk Score': 'avg_risk_score',  # 2017+
                # Payment variations
                'Average AB PM/PM Payment': 'avg_ab_payment',  # 2006-2013
                'Average A/B PM/PM Payment': 'avg_ab_payment',  # 2017+
                'Average A/B PM/PM\nPayment': 'avg_ab_payment',  # 2014-2016
                'Average Rebate PM/PM Payment': 'avg_rebate',
                'Average Rebate PM/PM\nPayment': 'avg_rebate',  # 2014-2016
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


def load_snp_subtype(year: int) -> pd.DataFrame:
    """
    Load SNP subtype data (D-SNP, C-SNP, I-SNP) for a given year.
    Returns a lookup table of contract_id + plan_id -> snp_subtype.
    """
    try:
        df = pd.DataFrame()  # Initialize df before the loop

        # Try December first, then January
        for month in ['12', '01']:
            try:
                key = f'processed/snp/{year}/{month}/data.parquet'
                df = load_parquet(key)
                if not df.empty:
                    break
            except:
                continue

        if df.empty:
            return pd.DataFrame()

        # Map SNP types to standard names
        snp_map = {
            'Dual-Eligible': 'D-SNP',
            'Chronic or Disabling Condition': 'C-SNP',
            'Institutional': 'I-SNP'
        }
        df['snp_subtype'] = df['snp_type'].map(snp_map)
        df = df.dropna(subset=['plan_id'])
        df['plan_id'] = df['plan_id'].astype(int).astype(str)

        return df[['contract_id', 'plan_id', 'snp_subtype']].drop_duplicates()
    except Exception as e:
        print(f"  SNP subtype {year}: Error - {e}")
        return pd.DataFrame()


def load_enrollment_by_plan() -> pd.DataFrame:
    """
    Load enrollment data from MONTHLY ENROLLMENT BY PLAN files at PLAN level.

    Uses monthly enrollment (more complete than CPSC or Stars) for each year.
    Each risk score year uses enrollment from THE SAME year.

    IMPORTANT:
    - Keep at PLAN level for proper joining with plan-level risk scores
    - Monthly enrollment by plan is the most complete source
    - CPSC has geographic detail but missing some contracts
    - Stars enrollment only has contracts in Stars file (~60% of MA)

    Derives:
    - group_type: plan_id >= 800 = Group, < 800 = Individual
    - snp_type: D-SNP, C-SNP, I-SNP, or Non-SNP (from SNP file + is_snp field)
    """
    all_years = []

    # Load enrollment for each year (2013-2025 - full range of available data)
    # Risk scores go back to 2006, but enrollment only available from 2013
    for year in range(2013, 2026):
        # Try December first, then January, then any available month
        df = pd.DataFrame()
        month_used = None
        for month in ['12', '01', '11', '10', '06', '03']:  # Prioritize end-of-year
            key = f'processed/fact_enrollment/{year}/{month}/data.parquet'
            try:
                df = load_parquet(key)
                if not df.empty:
                    month_used = month
                    break
            except:
                continue

        if df.empty:
            print(f"  {year}: No enrollment found")
            continue

        print(f"  {year}: Using month {month_used} enrollment")

        # Filter to MA plans only (exclude Part D)
        ma_types = ['HMO/HMOPOS', 'Local PPO', 'Regional PPO', 'PFFS', 'MSA',
                   'National PACE', 'Medicare-Medicaid Plan HMO/HMOPOS', '1876 Cost']
        df = df[df['plan_type'].isin(ma_types)]

        # Derive group_type from plan_id
        if 'plan_id' in df.columns:
            df['group_type'] = df['plan_id'].apply(lambda x: 'Group' if int(x) >= 800 else 'Individual')
        else:
            df['group_type'] = 'Unknown'

        # Get SNP subtype (D-SNP, C-SNP, I-SNP) from SNP file
        snp_lookup = load_snp_subtype(year)
        df['plan_id_str'] = df['plan_id'].astype(str)

        if not snp_lookup.empty:
            df = df.merge(snp_lookup, left_on=['contract_id', 'plan_id_str'],
                         right_on=['contract_id', 'plan_id'], how='left', suffixes=('', '_snp'))
            # Use snp_subtype if available, otherwise fall back to is_snp
            df['snp_type'] = df['snp_subtype'].fillna(
                df['is_snp'].apply(lambda x: 'SNP' if x == 'Yes' else 'Non-SNP')
            )
            df = df.drop(columns=['plan_id_snp', 'snp_subtype', 'plan_id_str'], errors='ignore')
        elif 'is_snp' in df.columns:
            df['snp_type'] = df['is_snp'].apply(lambda x: 'SNP' if x == 'Yes' else 'Non-SNP')
            df = df.drop(columns=['plan_id_str'], errors='ignore')
        else:
            df['snp_type'] = 'Unknown'
            df = df.drop(columns=['plan_id_str'], errors='ignore')

        # Normalize plan_type to match our standard naming
        plan_type_map = {
            'HMO/HMOPOS': 'HMO/HMOPOS',
            'Local PPO': 'Local PPO',
            'Regional PPO': 'Regional PPO',
            'PFFS': 'PFFS',
            'MSA': 'MSA',
            'National PACE': 'National PACE',
            'Medicare-Medicaid Plan HMO/HMOPOS': 'HMO/HMOPOS',  # MMPs are HMO-based
            '1876 Cost': '1876 Cost'
        }
        df['plan_type'] = df['plan_type'].map(plan_type_map).fillna(df['plan_type'])

        # Aggregate to PLAN level (contract + plan_id) - sum across counties
        # This gives us enrollment per plan, which matches risk score granularity
        agg = df.groupby(['contract_id', 'plan_id', 'plan_type', 'group_type', 'snp_type']).agg({
            'enrollment': 'sum',
            'parent_org': 'first',
        }).reset_index()
        agg['year'] = year
        # Ensure plan_id is string (to match risk scores)
        agg['plan_id'] = agg['plan_id'].astype(str)

        all_years.append(agg)
        print(f"  {year}: {len(agg):,} plans, {agg['enrollment'].sum():,.0f} enrollment")

    if not all_years:
        print("Warning: No enrollment data loaded!")
        return pd.DataFrame()

    result = pd.concat(all_years, ignore_index=True)
    print(f"  Total: {len(result):,} plan-level rows across {len(all_years)} years")
    return result


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

    # Load enrollment data for weighting (at PLAN level for proper joining)
    print("\n=== LOADING ENROLLMENT DATA ===")
    enrollment = load_enrollment_by_plan()
    print(f"Enrollment records (plan-level): {len(enrollment):,}")

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
        print(f"Matched to parent org (from mapping): {matched:,} / {len(risk_scores):,}")

    # Fill in missing parent_org from enrollment data
    # IMPORTANT: Join on YEAR + contract_id to get the parent_org for THAT specific year
    # (contracts can change parent orgs over time due to acquisitions)
    if not enrollment.empty and 'parent_org' in enrollment.columns:
        # Get unique contract -> parent_org BY YEAR from enrollment
        enrollment_parents = enrollment[['contract_id', 'year', 'parent_org']].drop_duplicates()
        # Keep one parent_org per contract per year
        enrollment_parents = enrollment_parents.groupby(['contract_id', 'year']).first().reset_index()
        enrollment_parents = enrollment_parents.rename(columns={'parent_org': 'parent_org_enroll'})

        risk_scores = risk_scores.merge(
            enrollment_parents,
            on=['contract_id', 'year'],
            how='left'
        )

        # Use enrollment parent_org where mapping parent_org is missing
        if 'parent_org' not in risk_scores.columns:
            risk_scores['parent_org'] = risk_scores['parent_org_enroll']
        else:
            mask = risk_scores['parent_org'].isna()
            risk_scores.loc[mask, 'parent_org'] = risk_scores.loc[mask, 'parent_org_enroll']

        risk_scores = risk_scores.drop(columns=['parent_org_enroll'], errors='ignore')

        matched_after = risk_scores['parent_org'].notna().sum()
        print(f"Matched to parent org (after enrollment, year-matched): {matched_after:,} / {len(risk_scores):,}")

    # Join with enrollment to get enrollment weights and additional fields
    # Handle two cases:
    # 1. Plan-level join for years with plan_id (2019+)
    # 2. Contract-level join for years without plan_id (2014-2018)
    if not enrollment.empty:
        # Split risk scores by whether they have plan_id
        has_plan_id = risk_scores['plan_id'].notna()
        risk_with_plan = risk_scores[has_plan_id].copy()
        risk_without_plan = risk_scores[~has_plan_id].copy()

        print(f"Risk scores with plan_id: {len(risk_with_plan):,}")
        print(f"Risk scores without plan_id: {len(risk_without_plan):,}")

        # Join plan-level for rows WITH plan_id
        if len(risk_with_plan) > 0:
            risk_with_plan = risk_with_plan.merge(
                enrollment[['contract_id', 'plan_id', 'year', 'enrollment', 'plan_type', 'group_type', 'snp_type']],
                on=['contract_id', 'plan_id', 'year'],
                how='left',
                suffixes=('', '_enroll')
            )

        # Join contract-level for rows WITHOUT plan_id (aggregate BOTH risk and enrollment to contract level)
        if len(risk_without_plan) > 0:
            # First aggregate risk scores to contract level (weighted avg by existing plan counts)
            # Since we don't have plan-level enrollment, use simple average
            risk_contract = risk_without_plan.groupby(['contract_id', 'year']).agg({
                'avg_risk_score': 'mean',  # Simple average across plans
                'contract_name': 'first',
                'plan_type': 'first',
                'parent_org': 'first'
            }).reset_index()
            # Set plan_id to '0' to indicate contract-level record
            risk_contract['plan_id'] = '0'

            # Aggregate enrollment to contract level
            contract_enrollment = enrollment.groupby(['contract_id', 'year']).agg({
                'enrollment': 'sum',
                'plan_type': 'first',
                'group_type': 'first',
                'snp_type': 'first',
                'parent_org': 'first'
            }).reset_index()
            contract_enrollment = contract_enrollment.rename(columns={
                'plan_type': 'plan_type_enroll',
                'group_type': 'group_type_enroll',
                'snp_type': 'snp_type_enroll'
            })

            # Now join (1:1 at contract level)
            risk_without_plan = risk_contract.merge(
                contract_enrollment[['contract_id', 'year', 'enrollment', 'plan_type_enroll', 'group_type_enroll', 'snp_type_enroll']],
                on=['contract_id', 'year'],
                how='left'
            )
            # Copy dimension fields
            if 'group_type' not in risk_without_plan.columns:
                risk_without_plan['group_type'] = risk_without_plan.get('group_type_enroll')
            if 'snp_type' not in risk_without_plan.columns:
                risk_without_plan['snp_type'] = risk_without_plan.get('snp_type_enroll')
            risk_without_plan = risk_without_plan.drop(columns=['plan_type_enroll', 'group_type_enroll', 'snp_type_enroll'], errors='ignore')

        # Combine back together
        risk_with_enrollment = pd.concat([risk_with_plan, risk_without_plan], ignore_index=True)

        # Use enrollment plan_type if risk score plan_type is missing
        if 'plan_type_enroll' in risk_with_enrollment.columns:
            mask = risk_with_enrollment['plan_type'].isna() | (risk_with_enrollment['plan_type'] == '')
            risk_with_enrollment.loc[mask, 'plan_type'] = risk_with_enrollment.loc[mask, 'plan_type_enroll']
            risk_with_enrollment = risk_with_enrollment.drop(columns=['plan_type_enroll'])

        risk_scores = risk_with_enrollment

        with_enrollment = risk_scores['enrollment'].notna().sum()
        print(f"Matched to enrollment (total): {with_enrollment:,} / {len(risk_scores):,}")

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
    # Now that enrollment is joined at PLAN level, we can aggregate directly
    if 'parent_org' in valid_risk.columns:
        # Data is now at plan level with enrollment and dimensions already joined
        by_plan = valid_risk.copy()

        # Fill missing values
        by_plan['enrollment'] = by_plan['enrollment'].fillna(0)
        by_plan['plan_type'] = by_plan['plan_type'].fillna('Unknown')
        by_plan['snp_type'] = by_plan['snp_type'].fillna('Unknown')
        by_plan['group_type'] = by_plan['group_type'].fillna('Unknown')

        # Calculate weighted risk at plan level
        by_plan['weighted_risk'] = by_plan['avg_risk_score'] * by_plan['enrollment']

        # Create aggregation BY parent_org only (for backward compatibility)
        by_parent_year = by_plan.groupby(['year', 'parent_org']).agg({
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
        by_parent_dims = by_plan.groupby(['year', 'parent_org', 'plan_type', 'snp_type', 'group_type']).agg({
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
