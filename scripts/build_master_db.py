#!/usr/bin/env python3
"""
Build Master Queryable Database

Connects all data sources:
- Enrollment (fact_enrollment)
- Stars (standardized across years)
- SNP
- Risk Adjustment
- Creates unified views by Parent Organization
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import zipfile
import tempfile
import os

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"  Uploaded: {s3_key} ({len(df):,} rows)")


def download_parquet_from_s3(s3_key: str) -> pd.DataFrame:
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def list_s3_parquet_files(prefix: str) -> list:
    """List all parquet files under a prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                files.append(obj['Key'])
    return files


def build_unified_stars():
    """
    Build unified Stars table across all years.
    Standardize column names and combine.
    """
    print("\n=== BUILDING UNIFIED STARS ===")

    # Find all stars summary files
    stars_files = list_s3_parquet_files('processed/stars/')
    summary_files = [f for f in stars_files if 'summary' in f.lower()]

    all_stars = []

    for s3_key in summary_files:
        try:
            df = download_parquet_from_s3(s3_key)

            # Extract year from path
            year = int(s3_key.split('/')[2])
            df['rating_year'] = year

            # Standardize column names
            col_map = {
                'Contract Number': 'contract_id',
                'Organization Type': 'org_type',
                'Contract Name': 'contract_name',
                'Organization Marketing Name': 'marketing_name',
                'Parent Organization': 'parent_org',
            }

            for old, new in col_map.items():
                if old in df.columns:
                    df = df.rename(columns={old: new})

            # Find overall/summary rating columns
            rating_cols = [c for c in df.columns if 'overall' in c.lower() or 'summary' in c.lower()]

            # Keep key columns
            keep_cols = ['contract_id', 'rating_year', 'parent_org', 'marketing_name', 'org_type']
            keep_cols.extend([c for c in rating_cols if c in df.columns])

            available_cols = [c for c in keep_cols if c in df.columns]
            df = df[available_cols].copy()

            all_stars.append(df)
            print(f"  Loaded {year}: {len(df)} contracts")

        except Exception as e:
            print(f"  Error loading {s3_key}: {e}")

    if all_stars:
        unified = pd.concat(all_stars, ignore_index=True)

        # Clean contract_id
        unified['contract_id'] = unified['contract_id'].astype(str).str.strip()

        upload_parquet_to_s3(unified, 'processed/unified/stars_summary.parquet')
        return unified

    return None


def build_unified_enrollment():
    """
    Build aggregated enrollment by parent org and year.
    """
    print("\n=== BUILDING UNIFIED ENROLLMENT ===")

    enrollment_files = list_s3_parquet_files('processed/fact_enrollment/')

    # Sample: get one month per year to build annual summary
    years_data = {}

    for s3_key in enrollment_files:
        try:
            parts = s3_key.split('/')
            year = int(parts[2])
            month = int(parts[3])

            # Use January of each year
            if month == 1 and year not in years_data:
                df = download_parquet_from_s3(s3_key)
                years_data[year] = df
                print(f"  Loaded {year}-{month:02d}: {len(df):,} rows")
        except Exception as e:
            continue

    # Build annual summary by parent org
    annual_summary = []

    for year, df in sorted(years_data.items()):
        by_parent = df.groupby('parent_org').agg({
            'enrollment': 'sum',
            'contract_id': 'nunique',
            'plan_id': 'nunique',
            'fips_code': 'nunique'
        }).reset_index()

        by_parent = by_parent.rename(columns={
            'enrollment': 'total_enrollment',
            'contract_id': 'contract_count',
            'plan_id': 'plan_count',
            'fips_code': 'county_count'
        })

        by_parent['year'] = year
        annual_summary.append(by_parent)

    if annual_summary:
        unified = pd.concat(annual_summary, ignore_index=True)
        upload_parquet_to_s3(unified, 'processed/unified/enrollment_by_parent_annual.parquet')
        return unified

    return None


def build_unified_snp():
    """
    Build unified SNP table across all years.
    """
    print("\n=== BUILDING UNIFIED SNP ===")

    snp_files = list_s3_parquet_files('processed/snp/')

    all_snp = []

    for s3_key in snp_files:
        try:
            df = download_parquet_from_s3(s3_key)
            all_snp.append(df)
        except:
            continue

    if all_snp:
        # Process each df to standardize before concat
        processed = []
        for df in all_snp:
            # Remove duplicate columns FIRST (before any processing)
            df = df.loc[:, ~df.columns.duplicated()]

            # Standardize column names
            col_map = {
                'Contract Number': 'contract_id',
                'Plan Enrollment': 'enrollment',
                'Special Needs Plan Type': 'snp_type',
            }
            for old, new in col_map.items():
                if old in df.columns:
                    df = df.rename(columns={old: new})

            # Keep only essential columns
            keep_cols = ['contract_id', 'plan_id', 'plan_name', 'plan_type', 'states',
                         'enrollment', 'snp_type', 'integration_status', 'year', 'month']
            available = [c for c in keep_cols if c in df.columns]

            if available:
                subset = df[available].copy()

                # Convert enrollment to numeric (handle mixed types)
                if 'enrollment' in subset.columns:
                    subset['enrollment'] = pd.to_numeric(
                        subset['enrollment'].astype(str).str.replace(',', '').str.replace('*', ''),
                        errors='coerce'
                    )

                # Ensure string columns are strings
                for col in ['contract_id', 'plan_id', 'plan_name', 'plan_type', 'states', 'snp_type', 'integration_status']:
                    if col in subset.columns:
                        subset[col] = subset[col].astype(str).replace('nan', '')

                processed.append(subset)

        if processed:
            unified = pd.concat(processed, ignore_index=True)
            upload_parquet_to_s3(unified, 'processed/unified/snp_all.parquet')
            print(f"  Total SNP records: {len(unified):,}")
            return unified

    return None


def build_master_contract_view():
    """
    Build master contract view joining Stars + Enrollment.
    """
    print("\n=== BUILDING MASTER CONTRACT VIEW ===")

    try:
        stars = download_parquet_from_s3('processed/unified/stars_summary.parquet')
        enrollment = download_parquet_from_s3('processed/unified/enrollment_by_parent_annual.parquet')

        # For each contract-year, get stars rating
        # For each parent-year, get enrollment

        # Create contract-level view with latest enrollment
        # This joins on parent_org

        master = stars.merge(
            enrollment,
            left_on=['parent_org', 'rating_year'],
            right_on=['parent_org', 'year'],
            how='left'
        )

        upload_parquet_to_s3(master, 'processed/unified/master_contract_stars_enrollment.parquet')
        print(f"  Master view: {len(master):,} records")
        return master

    except Exception as e:
        print(f"  Error: {e}")
        return None


def build_master_with_risk_scores():
    """
    Build master contract view with risk scores.
    """
    print("\n=== BUILDING MASTER CONTRACT VIEW WITH RISK SCORES ===")

    try:
        # Load existing master
        master = download_parquet_from_s3('processed/unified/master_contract_stars_enrollment.parquet')

        # Load risk scores
        try:
            risk = download_parquet_from_s3('processed/unified/risk_scores_by_contract.parquet')
            risk = risk.rename(columns={'year': 'rating_year'})

            # Merge
            master = master.merge(
                risk[['contract_id', 'rating_year', 'contract_avg_risk_score']],
                on=['contract_id', 'rating_year'],
                how='left'
            )
            print(f"  Added risk scores: {master['contract_avg_risk_score'].notna().sum():,} contracts")
        except Exception as e:
            print(f"  No risk scores available: {e}")

        upload_parquet_to_s3(master, 'processed/unified/master_contract_complete.parquet')
        print(f"  Master complete: {len(master):,} records")
        return master

    except Exception as e:
        print(f"  Error: {e}")
        return None


def build_parent_org_summary():
    """
    Build comprehensive parent org summary across all years.
    """
    print("\n=== BUILDING PARENT ORG SUMMARY ===")

    try:
        enrollment = download_parquet_from_s3('processed/unified/enrollment_by_parent_annual.parquet')
        stars = download_parquet_from_s3('processed/unified/stars_summary.parquet')

        # Pivot enrollment to wide format (year columns)
        enrollment_pivot = enrollment.pivot_table(
            index='parent_org',
            columns='year',
            values='total_enrollment',
            aggfunc='sum'
        ).reset_index()

        # Rename columns
        enrollment_pivot.columns = ['parent_org'] + [f'enrollment_{y}' for y in enrollment_pivot.columns[1:]]

        # Get latest year data
        latest_year = enrollment['year'].max()
        latest_enrollment = enrollment[enrollment['year'] == latest_year][['parent_org', 'total_enrollment', 'contract_count', 'plan_count', 'county_count']]

        # Get average star rating by parent
        stars_by_parent = stars.groupby('parent_org').agg({
            'contract_id': 'nunique',
            'rating_year': 'max'
        }).reset_index()
        stars_by_parent = stars_by_parent.rename(columns={
            'contract_id': 'contracts_rated',
            'rating_year': 'latest_rating_year'
        })

        # Merge all
        summary = latest_enrollment.merge(enrollment_pivot, on='parent_org', how='outer')
        summary = summary.merge(stars_by_parent, on='parent_org', how='left')

        # Calculate growth
        if 'enrollment_2020' in summary.columns and f'enrollment_{latest_year}' in summary.columns:
            summary['growth_since_2020'] = summary[f'enrollment_{latest_year}'] - summary['enrollment_2020']
            summary['growth_pct'] = (summary['growth_since_2020'] / summary['enrollment_2020'] * 100).round(1)

        summary = summary.sort_values('total_enrollment', ascending=False)

        upload_parquet_to_s3(summary, 'processed/unified/parent_org_summary.parquet')
        print(f"  Parent org summary: {len(summary):,} orgs")
        return summary

    except Exception as e:
        print(f"  Error: {e}")
        return None


def main():
    print("=" * 70)
    print("BUILDING MASTER QUERYABLE DATABASE")
    print("=" * 70)

    # Build unified tables
    stars = build_unified_stars()
    enrollment = build_unified_enrollment()
    snp = build_unified_snp()

    # Build joined views
    master = build_master_contract_view()
    master_complete = build_master_with_risk_scores()
    parent_summary = build_parent_org_summary()

    print("\n" + "=" * 70)
    print("MASTER DATABASE COMPLETE")
    print("=" * 70)
    print("\nUnified tables in S3:")
    print("  s3://ma-data123/processed/unified/stars_summary.parquet")
    print("  s3://ma-data123/processed/unified/enrollment_by_parent_annual.parquet")
    print("  s3://ma-data123/processed/unified/snp_all.parquet")
    print("  s3://ma-data123/processed/unified/master_contract_stars_enrollment.parquet")
    print("  s3://ma-data123/processed/unified/master_contract_complete.parquet")
    print("  s3://ma-data123/processed/unified/parent_org_summary.parquet")
    print("  s3://ma-data123/processed/unified/risk_scores_by_plan.parquet")
    print("  s3://ma-data123/processed/unified/risk_scores_by_contract.parquet")


if __name__ == '__main__':
    main()
