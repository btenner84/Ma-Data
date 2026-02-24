#!/usr/bin/env python3
"""
Build Data Catalog

Creates a comprehensive data catalog documenting:
1. All source files with metadata
2. All tables with schemas
3. Field-level lineage (which source each field comes from)
4. Data quality metrics
5. Refresh schedule and history

Output: processed/catalog/
"""

import os
import sys
import json
import hashlib
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

S3_BUCKET = "ma-data123"
CATALOG_PREFIX = "processed/catalog"

s3 = boto3.client('s3')


# =============================================================================
# SOURCE FILE DEFINITIONS
# =============================================================================

SOURCE_DEFINITIONS = {
    'enrollment_by_plan': {
        'name': 'Monthly Enrollment by Plan',
        's3_prefix': 'raw/enrollment/by_plan',
        'frequency': 'monthly',
        'years': (2007, 2026),
        'publisher': 'CMS',
        'url': 'https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-plan',
        'description': 'Monthly Medicare Advantage and Part D enrollment at the plan level. This is the authoritative source for enrollment totals (no suppression).',
        'key_fields': ['Contract Number', 'Plan ID', 'Enrollment'],
        'grain': 'contract_id + plan_id + year + month',
        'update_lag_days': 15,  # Usually available by 15th of following month
    },
    'cpsc': {
        'name': 'CPSC Enrollment (County-Plan-State-Contract)',
        's3_prefix': 'raw/enrollment/cpsc',
        'frequency': 'monthly',
        'years': (2013, 2026),
        'publisher': 'CMS',
        'url': 'https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/ma-state-county-penetration',
        'description': 'County-level enrollment with contract metadata. Contains Contract_Info sheet with parent org, plan type, EGHP. Enrollment values <11 are suppressed as "*" for HIPAA.',
        'key_fields': ['Contract Number', 'Plan ID', 'State', 'County', 'FIPS', 'Enrollment'],
        'grain': 'contract_id + plan_id + state + county + year + month',
        'suppression': 'Values < 11 shown as "*"',
        'contains_sheets': ['CPSC_Contract_Info', 'CPSC_Enrollment_Info'],
    },
    'snp_report': {
        'name': 'Special Needs Plan Comprehensive Report',
        's3_prefix': 'raw/snp',
        'frequency': 'monthly',
        'years': (2007, 2026),
        'publisher': 'CMS',
        'url': 'https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/special-needs-plan-snp-data',
        'description': 'Detailed data for Special Needs Plans only. Provides D-SNP, C-SNP, I-SNP classification. Does NOT include Non-SNP plans.',
        'key_fields': ['Contract Number', 'Plan ID', 'Special Needs Plan Type', 'Enrollment', 'Integration Status'],
        'grain': 'contract_id + plan_id + year + month',
        'limitation': 'Only contains SNP plans, not Non-SNP',
    },
    'stars': {
        'name': 'Medicare Star Ratings',
        's3_prefix': 'raw/stars',
        'frequency': 'yearly',
        'years': (2007, 2026),
        'publisher': 'CMS',
        'url': 'https://www.cms.gov/medicare/quality/medicare-star-ratings-program/star-ratings-data',
        'description': 'Quality star ratings at the contract level. Also contains enrollment breakdowns by Individual/Group and SNP/Non-SNP segments. Best source for parent organization.',
        'key_fields': ['Contract ID', 'Parent Organization', 'Overall Rating', 'Enrollment'],
        'grain': 'contract_id + year',
        'note': 'Contract-level only, not plan-level. Released in October for following payment year.',
    },
    'risk_scores': {
        'name': 'Plan Payment Data (Risk Scores)',
        's3_prefix': 'raw/plan_payment',
        'frequency': 'yearly',
        'years': (2006, 2024),
        'publisher': 'CMS',
        'url': 'https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/ma-ratebook-and-plan-payment-data',
        'description': 'Contains average Part C risk scores and payment amounts at the plan level.',
        'key_fields': ['Contract Number', 'Plan ID', 'Average Part C Risk Score', 'Average A/B PM/PM Payment'],
        'grain': 'contract_id + plan_id + year',
    },
    'crosswalk': {
        'name': 'Plan Crosswalk Files',
        's3_prefix': 'raw/crosswalks',
        'frequency': 'yearly',
        'years': (2006, 2026),
        'publisher': 'CMS',
        'url': 'https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/plan-crosswalks',
        'description': 'Maps plan IDs from year N-1 to year N. Critical for longitudinal tracking when plans change IDs due to benefit changes or mergers.',
        'key_fields': ['Previous Contract', 'Previous Plan', 'Current Contract', 'Current Plan'],
        'grain': 'contract_id + plan_id per year transition',
    },
    'ratebook': {
        'name': 'County Benchmark Rates',
        's3_prefix': 'raw/rates/ratebook',
        'frequency': 'yearly',
        'years': (2016, 2026),
        'publisher': 'CMS',
        'description': 'County-level benchmark rates used for MA payment calculations.',
        'key_fields': ['State', 'County', 'FIPS', 'Aged Benchmark', 'Disabled Benchmark'],
        'grain': 'county + year',
    },
}


# =============================================================================
# OUTPUT TABLE DEFINITIONS
# =============================================================================

TABLE_DEFINITIONS = {
    'dim_entity': {
        'name': 'Entity Dimension',
        'description': 'Stable entity IDs tracking plans across years via crosswalk chains',
        's3_path': 'processed/dimensions/dim_entity.parquet',
        'grain': 'entity_id (one per unique plan chain)',
        'sources': ['crosswalk', 'enrollment_by_plan'],
        'fields': {
            'entity_id': {
                'type': 'UUID',
                'description': 'Stable unique identifier for a plan across all years',
                'source': 'Generated',
            },
            'current_contract_id': {
                'type': 'VARCHAR(10)',
                'description': 'Most recent contract ID for this entity',
                'source': 'enrollment_by_plan',
            },
            'current_plan_id': {
                'type': 'VARCHAR(5)',
                'description': 'Most recent plan ID for this entity',
                'source': 'enrollment_by_plan',
            },
            'first_year': {
                'type': 'INT',
                'description': 'First year this entity appears in data',
                'source': 'Derived from crosswalk chain',
            },
            'last_year': {
                'type': 'INT',
                'description': 'Last year this entity appears in data',
                'source': 'Derived from crosswalk chain',
            },
            'identity_chain': {
                'type': 'JSON',
                'description': 'Array of {year, contract_id, plan_id, source} tracking ID changes',
                'source': 'crosswalk',
            },
        },
    },
    'dim_parent_org': {
        'name': 'Parent Organization Dimension',
        'description': 'Canonical parent organization names with M&A history',
        's3_path': 'processed/dimensions/dim_parent_org.parquet',
        'grain': 'parent_org_id (one per canonical organization)',
        'sources': ['stars', 'cpsc'],
        'fields': {
            'parent_org_id': {
                'type': 'UUID',
                'description': 'Unique identifier for the canonical organization',
                'source': 'Generated',
            },
            'canonical_name': {
                'type': 'VARCHAR(200)',
                'description': 'Current/final name of the organization',
                'source': 'Manual mapping + stars',
            },
            'name_history': {
                'type': 'JSON',
                'description': 'Historical names used by this organization',
                'source': 'stars, cpsc',
            },
            'ma_history': {
                'type': 'JSON',
                'description': 'M&A events (acquisitions, rebrands)',
                'source': 'Manual mapping',
            },
        },
    },
    'fact_enrollment_unified': {
        'name': 'Unified Enrollment Fact',
        'description': 'Master enrollment fact supporting all filter combinations',
        's3_path': 'processed/facts/fact_enrollment_unified/',
        'partitioning': ['year', 'month'],
        'grain': 'contract_id + plan_id + year + month',
        'sources': ['enrollment_by_plan', 'cpsc', 'snp_report'],
        'fields': {
            'contract_id': {
                'type': 'VARCHAR(10)',
                'description': 'CMS contract identifier',
                'source': 'enrollment_by_plan',
            },
            'plan_id': {
                'type': 'VARCHAR(5)',
                'description': 'Plan identifier within contract',
                'source': 'enrollment_by_plan',
            },
            'year': {
                'type': 'INT',
                'description': 'Reporting year',
                'source': 'enrollment_by_plan',
            },
            'month': {
                'type': 'INT',
                'description': 'Reporting month (1-12)',
                'source': 'enrollment_by_plan',
            },
            'parent_org': {
                'type': 'VARCHAR(200)',
                'description': 'Parent organization name',
                'source': 'cpsc.Contract_Info.Parent Organization',
                'fallback': 'stars.Parent Organization',
            },
            'plan_type': {
                'type': 'VARCHAR(50)',
                'description': 'Original plan type from CMS',
                'source': 'cpsc.Contract_Info.Plan Type',
            },
            'plan_type_simplified': {
                'type': 'VARCHAR(20)',
                'description': 'Simplified plan type (HMO, PPO, PFFS, etc.)',
                'source': 'Derived from plan_type via PLAN_TYPE_MAP',
            },
            'product_type': {
                'type': 'VARCHAR(20)',
                'description': 'Product type (MA-only, MAPD, PDP)',
                'source': 'Derived from contract_id prefix + cpsc.Offers Part D',
                'derivation': 'H/R prefix + Offers Part D = MAPD, else MA-only; S prefix = PDP',
            },
            'group_type': {
                'type': 'VARCHAR(20)',
                'description': 'Market segment (Individual, Group, Unknown)',
                'source': 'cpsc.Contract_Info.EGHP OR plan_id >= 800 heuristic',
            },
            'group_type_source': {
                'type': 'VARCHAR(30)',
                'description': 'How group_type was determined',
                'source': 'Metadata',
                'values': ['eghp_yes', 'eghp_no', 'plan_id_gte_800', 'plan_id_lt_800', 'unknown'],
            },
            'group_type_confidence': {
                'type': 'DECIMAL(3,2)',
                'description': 'Confidence score for group_type (0-1)',
                'source': 'Derived based on source quality',
            },
            'snp_type': {
                'type': 'VARCHAR(20)',
                'description': 'SNP type (D-SNP, C-SNP, I-SNP, Non-SNP, SNP-Unknown)',
                'source': 'snp_report.Special Needs Plan Type',
                'fallback': 'cpsc.Contract_Info.SNP Plan flag',
            },
            'snp_type_source': {
                'type': 'VARCHAR(30)',
                'description': 'Source of SNP type determination',
                'source': 'Metadata',
                'values': ['snp_report', 'cpsc_flag', 'assumed'],
            },
            'enrollment': {
                'type': 'INT',
                'description': 'Number of enrolled beneficiaries',
                'source': 'enrollment_by_plan.Enrollment',
                'note': 'Authoritative - no suppression',
            },
            'enrollment_source': {
                'type': 'VARCHAR(30)',
                'description': 'Source of enrollment figure',
                'source': 'Metadata',
            },
        },
    },
    'fact_enrollment_geographic': {
        'name': 'Geographic Enrollment Fact',
        'description': 'County-level enrollment with suppression tracking',
        's3_path': 'processed/facts/fact_enrollment_geographic/',
        'partitioning': ['year', 'month', 'state'],
        'grain': 'contract_id + plan_id + year + month + state + county',
        'sources': ['cpsc'],
        'fields': {
            'state': {
                'type': 'VARCHAR(50)',
                'description': 'State name',
                'source': 'cpsc.Enrollment_Info.State',
            },
            'county': {
                'type': 'VARCHAR(100)',
                'description': 'County name',
                'source': 'cpsc.Enrollment_Info.County',
            },
            'fips_code': {
                'type': 'VARCHAR(5)',
                'description': '5-digit FIPS county code',
                'source': 'cpsc.Enrollment_Info.FIPS State County Code',
            },
            'enrollment': {
                'type': 'INT',
                'description': 'Enrollment count (NULL if suppressed)',
                'source': 'cpsc.Enrollment_Info.Enrollment',
            },
            'is_suppressed': {
                'type': 'BOOLEAN',
                'description': 'True if original value was "*" (< 11 enrollees)',
                'source': 'Derived from cpsc.Enrollment = "*"',
            },
            'enrollment_estimated': {
                'type': 'DECIMAL(10,1)',
                'description': 'Estimated enrollment (5.5 for suppressed, actual for others)',
                'source': 'Derived',
            },
        },
    },
}


# =============================================================================
# FIELD LINEAGE
# =============================================================================

def build_field_lineage() -> Dict:
    """
    Build complete field-level lineage documentation.

    Returns a dict mapping output_field -> source chain
    """
    lineage = {}

    for table_name, table_def in TABLE_DEFINITIONS.items():
        for field_name, field_def in table_def.get('fields', {}).items():
            key = f"{table_name}.{field_name}"
            lineage[key] = {
                'table': table_name,
                'field': field_name,
                'type': field_def.get('type'),
                'description': field_def.get('description'),
                'primary_source': field_def.get('source'),
                'fallback_source': field_def.get('fallback'),
                'derivation_logic': field_def.get('derivation'),
                'possible_values': field_def.get('values'),
                'notes': field_def.get('note'),
            }

    return lineage


# =============================================================================
# DATA QUALITY METRICS
# =============================================================================

def compute_quality_metrics() -> Dict:
    """
    Compute data quality metrics for the catalog.
    """
    metrics = {
        'generated_at': datetime.now().isoformat(),
        'tables': {},
    }

    for table_name, table_def in TABLE_DEFINITIONS.items():
        s3_path = table_def.get('s3_path', '')

        # Try to load sample data
        try:
            if s3_path.endswith('/'):
                # Partitioned table - find a partition
                paginator = s3.get_paginator('list_objects_v2')
                files = []
                for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=s3_path, MaxKeys=10):
                    for obj in page.get('Contents', []):
                        if obj['Key'].endswith('.parquet'):
                            files.append(obj['Key'])
                            break
                    if files:
                        break

                if files:
                    response = s3.get_object(Bucket=S3_BUCKET, Key=files[0])
                    df = pd.read_parquet(response['Body'])
                else:
                    df = None
            else:
                response = s3.get_object(Bucket=S3_BUCKET, Key=s3_path)
                df = pd.read_parquet(response['Body'])

            if df is not None:
                metrics['tables'][table_name] = {
                    'row_count_sample': len(df),
                    'column_count': len(df.columns),
                    'null_rates': {
                        col: float(df[col].isna().mean())
                        for col in df.columns
                    },
                    'columns': list(df.columns),
                }
        except Exception as e:
            metrics['tables'][table_name] = {
                'error': str(e),
                'status': 'not_available'
            }

    return metrics


# =============================================================================
# CATALOG GENERATION
# =============================================================================

def generate_catalog():
    """Generate the complete data catalog."""
    print("=" * 70)
    print("BUILDING DATA CATALOG")
    print("=" * 70)

    catalog = {
        'version': '1.0',
        'generated_at': datetime.now().isoformat(),
        'description': 'MA Intelligence Platform Data Catalog',

        'source_files': SOURCE_DEFINITIONS,
        'tables': TABLE_DEFINITIONS,
        'field_lineage': build_field_lineage(),

        'relationships': {
            'dim_entity -> fact_enrollment_unified': {
                'type': 'one-to-many',
                'join_keys': ['entity_id'],
                'description': 'Links stable entity ID to enrollment records',
            },
            'dim_parent_org -> fact_enrollment_unified': {
                'type': 'one-to-many',
                'join_keys': ['parent_org_id'],
                'description': 'Links canonical parent org to enrollment',
            },
            'fact_enrollment_unified -> fact_enrollment_geographic': {
                'type': 'one-to-many',
                'join_keys': ['contract_id', 'plan_id', 'year', 'month'],
                'description': 'Unified provides dimensions, geographic provides county detail',
            },
        },

        'data_flow': {
            'enrollment_by_plan': ['fact_enrollment_unified.enrollment'],
            'cpsc.Contract_Info': [
                'fact_enrollment_unified.parent_org',
                'fact_enrollment_unified.plan_type',
                'fact_enrollment_unified.group_type (via EGHP)',
            ],
            'cpsc.Enrollment_Info': [
                'fact_enrollment_geographic.*',
            ],
            'snp_report': [
                'fact_enrollment_unified.snp_type (D-SNP/C-SNP/I-SNP)',
            ],
            'crosswalk': [
                'dim_entity.identity_chain',
            ],
            'stars': [
                'dim_parent_org.canonical_name',
                'fact_star_ratings.*',
            ],
        },
    }

    # Save catalog JSON
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{CATALOG_PREFIX}/data_catalog.json",
        Body=json.dumps(catalog, indent=2),
        ContentType='application/json'
    )
    print(f"Saved: {CATALOG_PREFIX}/data_catalog.json")

    # Save field lineage as separate file for easy access
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{CATALOG_PREFIX}/field_lineage.json",
        Body=json.dumps(catalog['field_lineage'], indent=2),
        ContentType='application/json'
    )
    print(f"Saved: {CATALOG_PREFIX}/field_lineage.json")

    # Compute and save quality metrics
    print("\nComputing quality metrics...")
    metrics = compute_quality_metrics()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{CATALOG_PREFIX}/quality_metrics.json",
        Body=json.dumps(metrics, indent=2),
        ContentType='application/json'
    )
    print(f"Saved: {CATALOG_PREFIX}/quality_metrics.json")

    # Generate markdown documentation
    markdown = generate_markdown_docs(catalog)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{CATALOG_PREFIX}/DATA_CATALOG.md",
        Body=markdown,
        ContentType='text/markdown'
    )
    print(f"Saved: {CATALOG_PREFIX}/DATA_CATALOG.md")

    print("\n" + "=" * 70)
    print("CATALOG COMPLETE")
    print("=" * 70)

    return catalog


def generate_markdown_docs(catalog: Dict) -> str:
    """Generate markdown documentation from catalog."""
    lines = [
        "# MA Intelligence Platform - Data Catalog",
        "",
        f"*Generated: {catalog['generated_at']}*",
        "",
        "## Overview",
        "",
        "This catalog documents all data sources, tables, and field-level lineage",
        "in the MA Intelligence Platform.",
        "",
        "---",
        "",
        "## Source Files",
        "",
    ]

    # Source files
    for source_id, source in catalog['source_files'].items():
        lines.extend([
            f"### {source['name']}",
            "",
            f"**ID:** `{source_id}`",
            f"**S3 Prefix:** `{source['s3_prefix']}`",
            f"**Frequency:** {source['frequency']}",
            f"**Years:** {source['years'][0]} - {source['years'][1]}",
            "",
            source['description'],
            "",
            f"**Grain:** `{source.get('grain', 'N/A')}`",
            "",
            "**Key Fields:**",
            "",
        ])
        for field in source.get('key_fields', []):
            lines.append(f"- {field}")
        lines.append("")

        if 'limitation' in source:
            lines.append(f"**Limitation:** {source['limitation']}")
            lines.append("")

        if 'suppression' in source:
            lines.append(f"**Suppression:** {source['suppression']}")
            lines.append("")

        lines.append("---")
        lines.append("")

    # Tables
    lines.extend([
        "## Output Tables",
        "",
    ])

    for table_id, table in catalog['tables'].items():
        lines.extend([
            f"### {table['name']}",
            "",
            f"**ID:** `{table_id}`",
            f"**S3 Path:** `{table['s3_path']}`",
            f"**Grain:** `{table['grain']}`",
            "",
            table['description'],
            "",
            "**Sources:** " + ", ".join(f"`{s}`" for s in table.get('sources', [])),
            "",
            "#### Fields",
            "",
            "| Field | Type | Source | Description |",
            "|-------|------|--------|-------------|",
        ])

        for field_name, field_def in table.get('fields', {}).items():
            source = field_def.get('source', 'N/A')
            if len(source) > 40:
                source = source[:37] + "..."
            lines.append(
                f"| `{field_name}` | {field_def.get('type', 'N/A')} | {source} | {field_def.get('description', '')} |"
            )

        lines.append("")
        lines.append("---")
        lines.append("")

    # Data Flow
    lines.extend([
        "## Data Flow",
        "",
        "Shows which source files feed into which output fields.",
        "",
        "```",
    ])

    for source, targets in catalog['data_flow'].items():
        lines.append(f"{source}")
        for target in targets:
            lines.append(f"  └─> {target}")

    lines.extend([
        "```",
        "",
    ])

    return "\n".join(lines)


if __name__ == '__main__':
    generate_catalog()
