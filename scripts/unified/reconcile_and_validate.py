#!/usr/bin/env python3
"""
Reconciliation and Validation

Compares totals across sources, validates dimension sums, and generates
data quality reports.

Outputs:
- reconciliation_totals.parquet
- reconciliation_dimensions.parquet
- data_quality_report.json
"""

import os
import sys
import json
import tempfile
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO

# Configuration
S3_BUCKET = "ma-data123"
S3_PREFIX_UNIFIED = "processed/facts/fact_enrollment_unified"
S3_PREFIX_GEOGRAPHIC = "processed/facts/fact_enrollment_geographic"
OUTPUT_PREFIX = "processed/reconciliation"

s3 = boto3.client('s3')


def load_unified_totals(year: int, month: int) -> Optional[Dict]:
    """Load totals from unified fact table."""
    s3_key = f"{S3_PREFIX_UNIFIED}/year={year}/month={month:02d}/data.parquet"

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))

        return {
            'total_enrollment': df['enrollment'].sum(),
            'record_count': len(df),
            'contract_count': df['contract_id'].nunique(),
            'plan_count': df[['contract_id', 'plan_id']].drop_duplicates().shape[0],

            # By product_type
            'by_product_type': df.groupby('product_type')['enrollment'].sum().to_dict(),

            # By group_type
            'by_group_type': df.groupby('group_type')['enrollment'].sum().to_dict(),

            # By snp_type
            'by_snp_type': df.groupby('snp_type')['enrollment'].sum().to_dict(),

            # Confidence metrics
            'group_type_confidence_avg': df['group_type_confidence'].mean(),
            'group_type_unknown_pct': (df['group_type'] == 'Unknown').mean() * 100,
        }
    except Exception as e:
        return None


def load_geographic_totals(year: int, month: int) -> Optional[Dict]:
    """Load totals from geographic fact table (sum across states)."""
    prefix = f"{S3_PREFIX_GEOGRAPHIC}/year={year}/month={month:02d}/"

    try:
        # List all state partitions
        paginator = s3.get_paginator('list_objects_v2')
        files = []
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    files.append(obj['Key'])

        if not files:
            return None

        total_enrollment = 0
        total_suppressed = 0
        record_count = 0

        for s3_key in files:
            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))

            total_enrollment += df['enrollment'].fillna(0).sum()
            total_suppressed += df['is_suppressed'].sum()
            record_count += len(df)

        return {
            'total_enrollment': total_enrollment,
            'total_suppressed_records': total_suppressed,
            'suppression_pct': (total_suppressed / record_count * 100) if record_count > 0 else 0,
            'record_count': record_count,
            'estimated_suppressed_enrollment': total_suppressed * 5.5,  # Midpoint estimate
        }
    except Exception as e:
        return None


def reconcile_month(year: int, month: int) -> Optional[Dict]:
    """
    Reconcile totals for a single month.
    """
    unified = load_unified_totals(year, month)
    geographic = load_geographic_totals(year, month)

    if unified is None:
        return None

    result = {
        'year': year,
        'month': month,

        # Unified totals
        'unified_enrollment': unified['total_enrollment'],
        'unified_records': unified['record_count'],
        'unified_contracts': unified['contract_count'],
        'unified_plans': unified['plan_count'],

        # Geographic totals (if available)
        'geographic_enrollment': geographic['total_enrollment'] if geographic else None,
        'geographic_suppressed_records': geographic['total_suppressed_records'] if geographic else None,
        'geographic_suppression_pct': geographic['suppression_pct'] if geographic else None,

        # Discrepancy
        'discrepancy': None,
        'discrepancy_pct': None,
    }

    if geographic:
        # Calculate discrepancy (unified should be higher due to suppression)
        discrepancy = unified['total_enrollment'] - geographic['total_enrollment']
        discrepancy_pct = (discrepancy / unified['total_enrollment'] * 100) if unified['total_enrollment'] > 0 else 0

        result['discrepancy'] = discrepancy
        result['discrepancy_pct'] = discrepancy_pct

        # Estimate if discrepancy is explained by suppression
        estimated_suppressed = geographic['estimated_suppressed_enrollment']
        unexplained = discrepancy - estimated_suppressed
        result['unexplained_discrepancy'] = unexplained

    return result


def validate_dimension_sums(year: int, month: int) -> List[Dict]:
    """
    Validate that dimension breakdowns sum to totals.
    """
    s3_key = f"{S3_PREFIX_UNIFIED}/year={year}/month={month:02d}/data.parquet"

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        df = pd.read_parquet(response['Body'])
    except:
        return []

    total_enrollment = df['enrollment'].sum()
    results = []

    # Validate each dimension
    for dimension in ['product_type', 'group_type', 'snp_type', 'plan_type_simplified']:
        if dimension not in df.columns:
            continue

        by_dim = df.groupby(dimension)['enrollment'].sum()
        dim_sum = by_dim.sum()
        discrepancy = total_enrollment - dim_sum
        discrepancy_pct = (discrepancy / total_enrollment * 100) if total_enrollment > 0 else 0

        # Check for Unknown values
        unknown_pct = 0
        for val in by_dim.index:
            if pd.isna(val) or str(val).lower() in ['unknown', 'none', 'nan']:
                unknown_pct = (by_dim[val] / total_enrollment * 100) if total_enrollment > 0 else 0

        results.append({
            'year': year,
            'month': month,
            'dimension': dimension,
            'total_enrollment': total_enrollment,
            'dimension_sum': dim_sum,
            'discrepancy': discrepancy,
            'discrepancy_pct': discrepancy_pct,
            'unknown_pct': unknown_pct,
            'is_valid': abs(discrepancy_pct) < 0.1,  # Less than 0.1% discrepancy
            'breakdown': by_dim.to_dict(),
        })

    return results


def generate_quality_report(recon_df: pd.DataFrame, dims_df: pd.DataFrame) -> Dict:
    """
    Generate overall data quality report.
    """
    report = {
        'generated_at': datetime.now().isoformat(),
        'summary': {},
        'issues': [],
        'recommendations': [],
    }

    # Reconciliation summary
    if len(recon_df) > 0:
        avg_discrepancy = recon_df['discrepancy_pct'].mean()
        max_discrepancy = recon_df['discrepancy_pct'].max()
        avg_suppression = recon_df['geographic_suppression_pct'].mean()

        report['summary']['avg_source_discrepancy_pct'] = avg_discrepancy
        report['summary']['max_source_discrepancy_pct'] = max_discrepancy
        report['summary']['avg_cpsc_suppression_pct'] = avg_suppression

        if avg_discrepancy > 3:
            report['issues'].append({
                'severity': 'high',
                'issue': f'Average source discrepancy is {avg_discrepancy:.1f}%',
                'detail': 'Unified and geographic totals differ more than expected'
            })

    # Dimension validation summary
    if len(dims_df) > 0:
        invalid_dims = dims_df[~dims_df['is_valid']]
        if len(invalid_dims) > 0:
            report['issues'].append({
                'severity': 'medium',
                'issue': f'{len(invalid_dims)} dimension validations failed',
                'detail': invalid_dims['dimension'].unique().tolist()
            })

        # High unknown percentages
        high_unknown = dims_df[dims_df['unknown_pct'] > 5]
        if len(high_unknown) > 0:
            for _, row in high_unknown.iterrows():
                report['issues'].append({
                    'severity': 'medium',
                    'issue': f'High unknown rate for {row["dimension"]}',
                    'detail': f'{row["unknown_pct"]:.1f}% in {row["year"]}-{row["month"]:02d}'
                })

    # Recommendations
    if avg_suppression and avg_suppression > 2:
        report['recommendations'].append(
            'Use unified table for totals, geographic table only for state/county breakdowns'
        )

    return report


def main():
    print("=" * 70)
    print("RECONCILIATION AND VALIDATION")
    print("=" * 70)
    print(f"Started: {datetime.now()}")

    reconciliation_records = []
    dimension_validations = []

    for year in range(2007, 2027):
        print(f"\n=== Year {year} ===")

        for month in range(1, 13):
            if year == 2026 and month > 2:
                continue

            # Reconcile totals
            recon = reconcile_month(year, month)
            if recon:
                reconciliation_records.append(recon)
                if recon['discrepancy_pct']:
                    print(f"  {year}-{month:02d}: Discrepancy {recon['discrepancy_pct']:.2f}%")

            # Validate dimensions
            dim_results = validate_dimension_sums(year, month)
            dimension_validations.extend(dim_results)

    # Convert to DataFrames
    recon_df = pd.DataFrame(reconciliation_records)
    dims_df = pd.DataFrame(dimension_validations)

    # Generate quality report
    report = generate_quality_report(recon_df, dims_df)

    # Upload results
    print("\n=== Uploading Results ===")

    # Reconciliation totals
    if len(recon_df) > 0:
        s3_key = f"{OUTPUT_PREFIX}/reconciliation_totals.parquet"
        with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
            recon_df.to_parquet(f.name, compression='snappy')
            s3.upload_file(f.name, S3_BUCKET, s3_key)
        print(f"  Uploaded {s3_key}")

    # Dimension validations
    if len(dims_df) > 0:
        s3_key = f"{OUTPUT_PREFIX}/reconciliation_dimensions.parquet"
        with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
            dims_df.to_parquet(f.name, compression='snappy')
            s3.upload_file(f.name, S3_BUCKET, s3_key)
        print(f"  Uploaded {s3_key}")

    # Quality report
    s3_key = f"{OUTPUT_PREFIX}/data_quality_report.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(report, indent=2),
        ContentType='application/json'
    )
    print(f"  Uploaded {s3_key}")

    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Reconciliation records: {len(recon_df)}")
    print(f"Dimension validations: {len(dims_df)}")
    print(f"Issues found: {len(report['issues'])}")

    for issue in report['issues']:
        print(f"  [{issue['severity'].upper()}] {issue['issue']}")

    print(f"\nFinished: {datetime.now()}")


if __name__ == '__main__':
    main()
