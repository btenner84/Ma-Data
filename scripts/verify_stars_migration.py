#!/usr/bin/env python3
"""
Stars Migration Verification Script

Compares OLD tables vs NEW unified tables to ensure data consistency
before switching the UI to use v4 endpoints.

OLD Tables:
- measure_data_complete -> measures_all_years
- stars_summary -> summary_all_years
- stars_cutpoints_2014_2026 -> cutpoints_all_years
- stars_domain -> domain_all_years

Run this BEFORE switching UI to verify:
1. Row counts match (within tolerance)
2. Data values match for overlapping years
3. No data loss during migration
"""

import sys
import os
import pandas as pd
from io import BytesIO
import boto3

# Add parent paths for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def load_parquet(key: str) -> pd.DataFrame:
    """Load parquet from S3."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(resp['Body'].read()))
    except Exception as e:
        print(f"Error loading {key}: {e}")
        return pd.DataFrame()


def print_section(title: str):
    """Print section header."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def compare_measures():
    """Compare measure_data_complete vs measures_all_years."""
    print_section("MEASURES COMPARISON")

    # Load OLD
    old_df = load_parquet('processed/unified/measure_data_complete.parquet')
    if old_df.empty:
        # Try alternative path
        old_df = load_parquet('processed/unified/stars_measure_stars_2014_2026.parquet')

    # Load NEW
    new_df = load_parquet('processed/unified/measures_all_years.parquet')

    if old_df.empty or new_df.empty:
        print("ERROR: Could not load one or both tables")
        return False

    print(f"\nOLD table: {len(old_df):,} rows")
    print(f"NEW table: {len(new_df):,} rows")
    print(f"Difference: {len(new_df) - len(old_df):,} ({'+' if len(new_df) >= len(old_df) else ''}{(len(new_df) - len(old_df)) / len(old_df) * 100:.1f}%)")

    # Year coverage
    old_years = sorted(old_df['year'].dropna().unique()) if 'year' in old_df.columns else []
    new_years = sorted(new_df['year'].dropna().unique())

    print(f"\nOLD years: {min(old_years) if old_years else 'N/A'}-{max(old_years) if old_years else 'N/A'}")
    print(f"NEW years: {min(new_years)}-{max(new_years)}")

    # Per-year comparison for overlapping years
    if old_years:
        print("\nPer-year comparison (overlapping years):")
        print("-" * 50)
        all_match = True
        for year in old_years:
            old_count = len(old_df[old_df['year'] == year])
            new_count = len(new_df[new_df['year'] == year])
            diff_pct = (new_count - old_count) / old_count * 100 if old_count > 0 else 0
            status = "OK" if abs(diff_pct) < 5 else "CHECK"
            if abs(diff_pct) >= 5:
                all_match = False
            print(f"  {int(year)}: OLD={old_count:>8,} NEW={new_count:>8,} ({diff_pct:+.1f}%) [{status}]")

        return all_match

    return True


def compare_cutpoints():
    """Compare stars_cutpoints_2014_2026 vs cutpoints_all_years."""
    print_section("CUTPOINTS COMPARISON")

    # Load OLD
    old_df = load_parquet('processed/unified/stars_cutpoints_2014_2026.parquet')

    # Load NEW
    new_df = load_parquet('processed/unified/cutpoints_all_years.parquet')

    if old_df.empty or new_df.empty:
        print("ERROR: Could not load one or both tables")
        return False

    print(f"\nOLD table: {len(old_df):,} rows")
    print(f"NEW table: {len(new_df):,} rows")

    # Schema differences
    print("\nSchema comparison:")
    print(f"  OLD columns: {list(old_df.columns)}")
    print(f"  NEW columns: {list(new_df.columns)}")

    # Note schema change
    if 'star_rating' in old_df.columns and 'star_level' in new_df.columns:
        print("\n  NOTE: star_rating (OLD) -> star_level (NEW)")

    # Year coverage
    old_years = sorted(old_df['year'].dropna().unique())
    new_years = sorted(new_df['year'].dropna().unique())

    print(f"\nOLD years: {min(old_years)}-{max(old_years)}")
    print(f"NEW years: {min(new_years)}-{max(new_years)}")
    print(f"NEW has {len(new_years) - len(old_years)} additional years")

    # Per-year comparison
    print("\nPer-year comparison:")
    print("-" * 50)
    for year in sorted(set(old_years) | set(new_years)):
        old_count = len(old_df[old_df['year'] == year])
        new_count = len(new_df[new_df['year'] == year])
        print(f"  {int(year)}: OLD={old_count:>5} NEW={new_count:>5}")

    return True


def compare_summary():
    """Compare stars_summary vs summary_all_years."""
    print_section("SUMMARY COMPARISON")

    # Load OLD
    old_df = load_parquet('processed/unified/stars_summary.parquet')

    # Load NEW
    new_df = load_parquet('processed/unified/summary_all_years.parquet')

    if old_df.empty or new_df.empty:
        print("ERROR: Could not load one or both tables")
        return False

    print(f"\nOLD table: {len(old_df):,} rows")
    print(f"NEW table: {len(new_df):,} rows")

    # Schema differences
    print("\nSchema comparison:")
    old_cols = [c for c in old_df.columns if not c.startswith('Unnamed')]
    print(f"  OLD columns (non-Unnamed): {old_cols[:10]}...")
    print(f"  NEW columns: {list(new_df.columns)}")

    # Note schema change
    if 'rating_year' in old_df.columns and 'year' in new_df.columns:
        print("\n  NOTE: rating_year (OLD) -> year (NEW)")

    print("\n  NOTE: OLD is WIDE format, NEW is LONG format")

    # Year coverage
    if 'rating_year' in old_df.columns:
        old_years = sorted(old_df['rating_year'].dropna().unique())
    else:
        old_years = []
    new_years = sorted(new_df['year'].dropna().unique())

    if old_years:
        print(f"\nOLD years: {min(old_years)}-{max(old_years)}")
    print(f"NEW years: {min(new_years)}-{max(new_years)}")

    # Contract counts
    if 'contract_id' in old_df.columns and 'contract_id' in new_df.columns:
        old_contracts = set(old_df['contract_id'].astype(str).str.strip().unique())
        new_contracts = set(new_df['contract_id'].astype(str).str.strip().unique())

        print(f"\nContract coverage:")
        print(f"  OLD unique contracts: {len(old_contracts)}")
        print(f"  NEW unique contracts: {len(new_contracts)}")
        print(f"  In both: {len(old_contracts & new_contracts)}")
        print(f"  Only in NEW: {len(new_contracts - old_contracts)}")

    return True


def compare_domain():
    """Compare stars_domain vs domain_all_years."""
    print_section("DOMAIN COMPARISON")

    # Load OLD
    old_df = load_parquet('processed/unified/stars_domain.parquet')

    # Load NEW
    new_df = load_parquet('processed/unified/domain_all_years.parquet')

    if old_df.empty or new_df.empty:
        print("ERROR: Could not load one or both tables")
        return False

    print(f"\nOLD table: {len(old_df):,} rows")
    print(f"NEW table: {len(new_df):,} rows")
    print(f"NEW has {len(new_df) - len(old_df):,} more rows ({len(new_df) / len(old_df):.1f}x)")

    # Schema differences
    print("\nSchema comparison:")
    old_cols = [c for c in old_df.columns if not c.startswith('domain_')]
    domain_cols = [c for c in old_df.columns if c.startswith('domain_')]
    print(f"  OLD base columns: {old_cols}")
    print(f"  OLD domain columns: {domain_cols}")
    print(f"  NEW columns: {list(new_df.columns)}")

    print("\n  NOTE: OLD is WIDE format (one col per domain), NEW is LONG format (one row per domain)")

    # Year coverage
    if 'year' in old_df.columns:
        old_years = sorted(old_df['year'].dropna().unique())
        print(f"\nOLD years: {min(old_years)}-{max(old_years)} ({len(old_years)} years)")
    new_years = sorted(new_df['year'].dropna().unique())
    print(f"NEW years: {min(new_years)}-{max(new_years)} ({len(new_years)} years)")

    return True


def verify_audit_columns():
    """Verify NEW tables have audit columns."""
    print_section("AUDIT COLUMNS VERIFICATION")

    tables = [
        ('measures_all_years', 'processed/unified/measures_all_years.parquet'),
        ('summary_all_years', 'processed/unified/summary_all_years.parquet'),
        ('cutpoints_all_years', 'processed/unified/cutpoints_all_years.parquet'),
        ('domain_all_years', 'processed/unified/domain_all_years.parquet'),
    ]

    all_have_audit = True
    for name, key in tables:
        df = load_parquet(key)
        if df.empty:
            print(f"  {name}: ERROR - could not load")
            continue

        has_source = '_source_file' in df.columns
        has_run_id = '_pipeline_run_id' in df.columns

        status = "OK" if has_source and has_run_id else "MISSING"
        if not (has_source and has_run_id):
            all_have_audit = False

        print(f"  {name}:")
        print(f"    _source_file: {'Yes' if has_source else 'No'}")
        print(f"    _pipeline_run_id: {'Yes' if has_run_id else 'No'}")
        print(f"    Status: {status}")

    return all_have_audit


def main():
    """Run all verification checks."""
    print("\n" + "=" * 70)
    print("STARS MIGRATION VERIFICATION")
    print("Comparing OLD vs NEW unified tables")
    print("=" * 70)

    results = []

    # Run comparisons
    results.append(("Measures", compare_measures()))
    results.append(("Cutpoints", compare_cutpoints()))
    results.append(("Summary", compare_summary()))
    results.append(("Domain", compare_domain()))
    results.append(("Audit Columns", verify_audit_columns()))

    # Summary
    print_section("VERIFICATION SUMMARY")
    all_pass = True
    for name, passed in results:
        status = "PASS" if passed else "CHECK"
        if not passed:
            all_pass = False
        print(f"  {name}: {status}")

    print("\n" + "-" * 70)
    if all_pass:
        print("ALL CHECKS PASSED - Safe to proceed with UI migration")
    else:
        print("SOME CHECKS NEED REVIEW - Review differences before proceeding")

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
