#!/usr/bin/env python3
"""
Gold Layer Validation
=====================

Validates the Gold layer tables for:
1. MECE (Mutually Exclusive, Collectively Exhaustive) - dimensions sum correctly
2. Year coverage - all expected years present
3. National vs Geographic reconciliation - totals align within suppression tolerance
4. Entity coverage - all contracts have entity mappings
5. Lineage coverage - all facts have source tracking

Run after building Gold layer to verify data integrity.

Usage:
    python validate_gold_layer.py           # Run all validations
    python validate_gold_layer.py --quick   # Quick checks only
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
import sys
import argparse
import json

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
GOLD_PREFIX = "gold"

s3 = boto3.client('s3')


def load_parquet(key: str) -> pd.DataFrame:
    """Load parquet file from S3."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"  [WARN] Could not load {key}: {e}")
        return pd.DataFrame()


class ValidationResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.message = ""
        self.details = {}
    
    def to_dict(self):
        return {
            "name": self.name,
            "passed": self.passed,
            "message": self.message,
            "details": self.details
        }


def validate_mece_snp(df: pd.DataFrame) -> ValidationResult:
    """Validate SNP types are MECE (sum to total)."""
    result = ValidationResult("MECE: SNP Types")
    
    if 'snp_type' not in df.columns or 'enrollment' not in df.columns:
        result.message = "Required columns not found"
        return result
    
    total_enrollment = df['enrollment'].sum()
    
    snp_totals = df.groupby('snp_type')['enrollment'].sum()
    snp_sum = snp_totals.sum()
    
    diff_pct = abs(total_enrollment - snp_sum) / total_enrollment * 100 if total_enrollment > 0 else 0
    
    result.details = {
        "total_enrollment": int(total_enrollment),
        "snp_sum": int(snp_sum),
        "difference_pct": round(diff_pct, 2),
        "snp_breakdown": snp_totals.to_dict()
    }
    
    result.passed = diff_pct < 1.0
    result.message = f"SNP types sum to {100-diff_pct:.1f}% of total" if result.passed else f"SNP mismatch: {diff_pct:.1f}%"
    
    return result


def validate_mece_group(df: pd.DataFrame) -> ValidationResult:
    """Validate group types are MECE (Individual + Group = Total)."""
    result = ValidationResult("MECE: Group Types")
    
    if 'group_type' not in df.columns or 'enrollment' not in df.columns:
        result.message = "Required columns not found"
        return result
    
    total_enrollment = df['enrollment'].sum()
    group_totals = df.groupby('group_type')['enrollment'].sum()
    group_sum = group_totals.sum()
    
    diff_pct = abs(total_enrollment - group_sum) / total_enrollment * 100 if total_enrollment > 0 else 0
    
    result.details = {
        "total_enrollment": int(total_enrollment),
        "group_sum": int(group_sum),
        "difference_pct": round(diff_pct, 2),
        "group_breakdown": group_totals.to_dict()
    }
    
    result.passed = diff_pct < 1.0
    result.message = f"Group types sum correctly" if result.passed else f"Group mismatch: {diff_pct:.1f}%"
    
    return result


def validate_year_coverage(df: pd.DataFrame, expected_years: list) -> ValidationResult:
    """Validate expected years are present."""
    result = ValidationResult("Year Coverage")
    
    if 'year' not in df.columns:
        result.message = "Year column not found"
        return result
    
    actual_years = sorted(df['year'].unique())
    missing_years = [y for y in expected_years if y not in actual_years]
    extra_years = [y for y in actual_years if y not in expected_years]
    
    result.details = {
        "expected_years": expected_years,
        "actual_years": actual_years,
        "missing_years": missing_years,
        "extra_years": extra_years
    }
    
    result.passed = len(missing_years) == 0
    result.message = f"All {len(expected_years)} years present" if result.passed else f"Missing: {missing_years}"
    
    return result


def validate_national_vs_geographic(nat_df: pd.DataFrame, geo_df: pd.DataFrame) -> ValidationResult:
    """Validate national and geographic totals align (within suppression tolerance)."""
    result = ValidationResult("National vs Geographic Reconciliation")
    
    if nat_df.empty or geo_df.empty:
        result.message = "One or both datasets empty"
        return result
    
    common_years = set(nat_df['year'].unique()) & set(geo_df['year'].unique())
    
    comparisons = []
    for year in sorted(common_years):
        nat_total = nat_df[nat_df['year'] == year]['enrollment'].sum()
        geo_total = geo_df[geo_df['year'] == year]['enrollment'].sum()
        
        diff_pct = (nat_total - geo_total) / nat_total * 100 if nat_total > 0 else 0
        
        comparisons.append({
            "year": year,
            "national": int(nat_total),
            "geographic": int(geo_total),
            "diff_pct": round(diff_pct, 2)
        })
    
    max_diff = max(abs(c['diff_pct']) for c in comparisons) if comparisons else 0
    
    result.details = {
        "comparisons": comparisons,
        "max_difference_pct": round(max_diff, 2)
    }
    
    result.passed = max_diff < 5.0
    result.message = f"Max difference {max_diff:.1f}% (expected ~2-3% from suppression)" if result.passed else f"Large discrepancy: {max_diff:.1f}%"
    
    return result


def validate_entity_coverage(fact_df: pd.DataFrame, entity_df: pd.DataFrame) -> ValidationResult:
    """Validate all contracts have entity mappings."""
    result = ValidationResult("Entity Coverage")
    
    if fact_df.empty or entity_df.empty:
        result.message = "One or both datasets empty"
        return result
    
    fact_contracts = set(fact_df['contract_id'].unique())
    entity_contracts = set(entity_df['contract_id'].unique())
    
    unmapped = fact_contracts - entity_contracts
    coverage_pct = (len(fact_contracts) - len(unmapped)) / len(fact_contracts) * 100 if fact_contracts else 0
    
    result.details = {
        "fact_contracts": len(fact_contracts),
        "entity_contracts": len(entity_contracts),
        "unmapped_count": len(unmapped),
        "coverage_pct": round(coverage_pct, 2),
        "sample_unmapped": list(unmapped)[:10]
    }
    
    result.passed = coverage_pct >= 95
    result.message = f"{coverage_pct:.1f}% coverage" if result.passed else f"Low coverage: {coverage_pct:.1f}%"
    
    return result


def validate_lineage(df: pd.DataFrame) -> ValidationResult:
    """Validate all rows have lineage tracking columns."""
    result = ValidationResult("Lineage Coverage")
    
    lineage_cols = ['_source_file', '_pipeline_run_id', '_loaded_at']
    present_cols = [c for c in lineage_cols if c in df.columns]
    missing_cols = [c for c in lineage_cols if c not in df.columns]
    
    if not present_cols:
        result.message = "No lineage columns found"
        result.details = {"missing_columns": missing_cols}
        return result
    
    coverage = {}
    for col in present_cols:
        non_null = df[col].notna().sum()
        coverage[col] = {
            "non_null": int(non_null),
            "total": len(df),
            "coverage_pct": round(non_null / len(df) * 100, 2) if len(df) > 0 else 0
        }
    
    min_coverage = min(c['coverage_pct'] for c in coverage.values())
    
    result.details = {
        "present_columns": present_cols,
        "missing_columns": missing_cols,
        "coverage": coverage
    }
    
    result.passed = min_coverage >= 90 and len(missing_cols) == 0
    result.message = f"Lineage columns: {min_coverage:.0f}% populated" if result.passed else f"Missing or incomplete lineage"
    
    return result


def run_validations(quick: bool = False) -> list:
    """Run all validation checks."""
    results = []
    
    print("\n" + "=" * 70)
    print("LOADING GOLD LAYER TABLES")
    print("=" * 70)
    
    fact_national = load_parquet(f"{GOLD_PREFIX}/fact_enrollment_national.parquet")
    print(f"  fact_enrollment_national: {len(fact_national):,} rows")
    
    fact_geographic = load_parquet(f"{GOLD_PREFIX}/fact_enrollment_geographic.parquet")
    print(f"  fact_enrollment_geographic: {len(fact_geographic):,} rows")
    
    dim_entity = load_parquet(f"{GOLD_PREFIX}/dim_entity.parquet")
    print(f"  dim_entity: {len(dim_entity):,} rows")
    
    fact_stars = load_parquet(f"{GOLD_PREFIX}/fact_stars.parquet")
    print(f"  fact_stars: {len(fact_stars):,} rows")
    
    fact_risk = load_parquet(f"{GOLD_PREFIX}/fact_risk_scores.parquet")
    print(f"  fact_risk_scores: {len(fact_risk):,} rows")
    
    print("\n" + "=" * 70)
    print("RUNNING VALIDATIONS")
    print("=" * 70)
    
    if not fact_national.empty:
        r = validate_mece_snp(fact_national)
        results.append(r)
        print(f"\n{'✓' if r.passed else '✗'} {r.name}: {r.message}")
        
        r = validate_mece_group(fact_national)
        results.append(r)
        print(f"{'✓' if r.passed else '✗'} {r.name}: {r.message}")
        
        r = validate_year_coverage(fact_national, list(range(2007, 2027)))
        results.append(r)
        print(f"{'✓' if r.passed else '✗'} {r.name}: {r.message}")
        
        r = validate_lineage(fact_national)
        results.append(r)
        print(f"{'✓' if r.passed else '✗'} {r.name}: {r.message}")
    
    if not quick and not fact_national.empty and not fact_geographic.empty:
        r = validate_national_vs_geographic(fact_national, fact_geographic)
        results.append(r)
        print(f"\n{'✓' if r.passed else '✗'} {r.name}: {r.message}")
    
    if not fact_national.empty and not dim_entity.empty:
        r = validate_entity_coverage(fact_national, dim_entity)
        results.append(r)
        print(f"{'✓' if r.passed else '✗'} {r.name}: {r.message}")
    
    if not fact_stars.empty:
        r = validate_year_coverage(fact_stars, list(range(2009, 2027)))
        r.name = "Stars Year Coverage"
        results.append(r)
        print(f"\n{'✓' if r.passed else '✗'} {r.name}: {r.message}")
    
    if not fact_risk.empty:
        r = validate_year_coverage(fact_risk, list(range(2006, 2025)))
        r.name = "Risk Scores Year Coverage"
        results.append(r)
        print(f"{'✓' if r.passed else '✗'} {r.name}: {r.message}")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Validate Gold layer data")
    parser.add_argument("--quick", action="store_true", help="Run quick checks only")
    parser.add_argument("--output", type=str, help="Save results to JSON file")
    args = parser.parse_args()
    
    print("=" * 70)
    print("GOLD LAYER VALIDATION")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    
    results = run_validations(quick=args.quick)
    
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Passed: {passed}/{len(results)}")
    print(f"Failed: {failed}/{len(results)}")
    print(f"Status: {'ALL PASSED' if failed == 0 else 'FAILURES DETECTED'}")
    
    if args.output:
        output_data = {
            "timestamp": datetime.now().isoformat(),
            "passed": passed,
            "failed": failed,
            "results": [r.to_dict() for r in results]
        }
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output}")
    
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
