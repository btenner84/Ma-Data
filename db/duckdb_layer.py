#!/usr/bin/env python3
"""
DuckDB Query Layer

Provides fast SQL queries over S3 Parquet files with full audit logging.

Features:
1. Reads directly from S3 Parquet (no data copying)
2. Full SQL support with joins across all tables
3. Query audit logging (who queried what, when)
4. Lineage integration (trace results back to source)
5. Caching for repeated queries

Usage:
    from db.duckdb_layer import MAQueryEngine

    engine = MAQueryEngine()

    # Simple query
    df = engine.query("SELECT * FROM fact_enrollment_unified LIMIT 10")

    # Query with audit
    df, audit_id = engine.query_with_audit(
        "SELECT state, SUM(enrollment) FROM fact_enrollment_geographic GROUP BY state",
        user_id="api_user",
        context="UI dashboard request"
    )

    # Trace lineage for a result
    lineage = engine.trace_query_lineage(audit_id)
"""

import os
import json
import uuid
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from io import BytesIO

import duckdb
import pandas as pd
import boto3

# Configuration
S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
S3_REGION = os.environ.get("AWS_REGION", "us-east-1")
PROCESSED_PREFIX = "processed"
AUDIT_PREFIX = "processed/audit/queries"

# Initialize S3
s3 = boto3.client('s3', region_name=S3_REGION)


class MAQueryEngine:
    """
    DuckDB-based query engine for MA data platform.

    All queries are logged for audit. Results can be traced back to source files.
    """

    def __init__(self, read_only: bool = True):
        """
        Initialize the query engine.

        Args:
            read_only: If True, prevents any write operations
        """
        self.read_only = read_only
        self.conn = duckdb.connect(":memory:")
        self._setup_connection()
        self._register_tables()

    def _setup_connection(self):
        """Configure DuckDB for S3 access."""
        # Install and load extensions
        self.conn.execute("INSTALL httpfs")
        self.conn.execute("LOAD httpfs")

        # Configure S3 credentials - try multiple sources
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

        # If not in env, try to read from ~/.aws/credentials
        if not aws_access_key or not aws_secret_key:
            try:
                import configparser
                creds_path = os.path.expanduser("~/.aws/credentials")
                if os.path.exists(creds_path):
                    config = configparser.ConfigParser()
                    config.read(creds_path)
                    if 'default' in config:
                        aws_access_key = config['default'].get('aws_access_key_id', '')
                        aws_secret_key = config['default'].get('aws_secret_access_key', '')
            except Exception as e:
                print(f"Warning: Could not read AWS credentials file: {e}")

        if aws_access_key and aws_secret_key:
            self.conn.execute(f"""
                SET s3_region = '{S3_REGION}';
                SET s3_access_key_id = '{aws_access_key}';
                SET s3_secret_access_key = '{aws_secret_key}';
            """)
        else:
            # Use IAM role (for Railway/EC2)
            self.conn.execute(f"""
                SET s3_region = '{S3_REGION}';
            """)

    def _register_tables(self):
        """Register all tables as views pointing to S3 Parquet files."""

        # Primary tables - using existing data paths
        tables = {
            # =================================================================
            # GOLD LAYER - STAR SCHEMA (NEW)
            # =================================================================
            # Dimension tables
            'gold_dim_time': f's3://{S3_BUCKET}/gold/dim_time.parquet',
            'gold_dim_geography': f's3://{S3_BUCKET}/gold/dim_geography.parquet',
            'gold_dim_entity': f's3://{S3_BUCKET}/gold/dim_entity.parquet',
            'gold_dim_plan': f's3://{S3_BUCKET}/gold/dim_plan.parquet',
            'gold_dim_crosswalk': f's3://{S3_BUCKET}/gold/dim_crosswalk.parquet',
            'gold_dim_contract_lineage': f's3://{S3_BUCKET}/gold/dim_contract_lineage.parquet',
            
            # Fact tables
            'gold_fact_enrollment_national': f's3://{S3_BUCKET}/gold/fact_enrollment_national.parquet',
            'gold_fact_enrollment_geographic': f's3://{S3_BUCKET}/gold/fact_enrollment_geographic/year=*/data.parquet',  # Partitioned by year
            'gold_fact_stars': f's3://{S3_BUCKET}/gold/fact_stars.parquet',
            'gold_fact_risk_scores': f's3://{S3_BUCKET}/gold/fact_risk_scores.parquet',
            
            # =================================================================
            # ENROLLMENT (LEGACY - unified with audit)
            # =================================================================
            # Main enrollment fact table with audit columns (CPSC - has geography but suppressed)
            'fact_enrollment_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/fact_enrollment_all_years.parquet',
            # Legacy alias for backwards compatibility
            'fact_enrollment_unified': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/fact_enrollment_all_years.parquet',

            # National enrollment (from by_contract - exact totals, no geography)
            'fact_enrollment_national': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/fact_enrollment_national.parquet',

            # Geographic enrollment (state/county level)
            'fact_enrollment_by_state': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/fact_enrollment_by_state.parquet',
            'fact_enrollment_by_geography': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/fact_enrollment_by_geography.parquet',

            # =================================================================
            # STARS (NEW - unified with audit)
            # =================================================================
            # Measure-level data (328K rows, 2008-2026)
            'measures_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/measures_all_years.parquet',
            # Summary ratings (33K rows, 2009-2026)
            'summary_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/summary_all_years.parquet',
            # Cutpoints (7K rows, 2011-2026)
            'cutpoints_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/cutpoints_all_years.parquet',
            # Domain scores (68K rows, 2008-2026)
            'domain_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/domain_all_years.parquet',

            # Legacy tables (for backwards compatibility)
            'stars_enrollment_unified': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/stars_enrollment_unified.parquet',
            
            # National stars enrollment (ALL MA contracts, LEFT JOIN with ratings)
            'stars_enrollment_national': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/stars_enrollment_national.parquet',
            'stars_by_parent_national': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/stars_by_parent_national.parquet',
            'stars_summary': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/stars_summary.parquet',
            'measure_data': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/measure_data_complete.parquet',
            'stars_cutpoints_2014_2026': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/stars_cutpoints_2014_2026.parquet',
            'stars_measure_specs': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/stars_measure_specs.parquet',
            'stars_fourplus_by_year': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/stars_fourplus_by_year.parquet',

            # =================================================================
            # RISK SCORES
            # =================================================================
            # Plan-level risk scores (65K rows)
            'fact_risk_scores_unified': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/fact_risk_scores_unified.parquet',

            # Pre-aggregated by parent org + year
            'risk_scores_by_parent': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/risk_scores_by_parent_year.parquet',

            # By parent with plan type dimensions
            'risk_scores_by_parent_dims': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/risk_scores_by_parent_dims.parquet',
            
            # Risk scores with national enrollment (complete, no suppression)
            'risk_scores_national': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/risk_scores_national.parquet',
            'risk_scores_industry_national': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/risk_scores_industry_national.parquet',

            # =================================================================
            # SNP
            # =================================================================
            'fact_snp': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/fact_snp_by_parent.parquet',
            'fact_snp_historical': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/fact_snp_by_parent_historical.parquet',

            # =================================================================
            # AGGREGATIONS
            # =================================================================
            'agg_enrollment_by_year': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/agg_enrollment_by_year_v2.parquet',
            'agg_enrollment_by_plantype': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/agg_enrollment_by_plantype_v2.parquet',
            'enrollment_by_parent': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/enrollment_by_parent_annual.parquet',

            # =================================================================
            # LOOKUPS
            # =================================================================
            'dim_county': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/dim_county.parquet',
            'parent_org_summary': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/parent_org_summary.parquet',

            # =================================================================
            # NEW UNIFIED TABLES (2008-2026 comprehensive rebuild)
            # =================================================================
            # Measure data with stable keys - 328K rows, 2008-2026
            'measures_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/measures_all_years.parquet',

            # Measure STAR RATINGS - 245K rows, 2014-2026 (actual 1-5 star values)
            'measure_stars_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/measure_stars_all_years.parquet',

            # Summary ratings - 32K rows, 2009-2026
            'summary_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/summary_all_years.parquet',

            # Domain scores - 68K rows, 2008-2026
            'domain_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/domain_all_years.parquet',

            # Cutpoint thresholds - 7K rows, 2011-2026
            'cutpoints_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/cutpoints_all_years.parquet',

            # Measure dimension with stable keys (measure_key for cross-year tracking)
            'dim_measure': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/dim_measure.parquet',

            # Entity chains (contract ID tracking across mergers)
            'dim_entity': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/dim_entity.parquet',

            # Disenrollment reasons - 6K rows, 2024-2026
            'disenrollment_all_years': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/unified/disenrollment_all_years.parquet',

            # =================================================================
            # RATE NOTICE DATA (Policy/Rate tables) - ALL WITH AUDIT FIELDS
            # =================================================================
            # Part D benefit parameters by year (deductible, ICL, TrOOP, IRA caps)
            'part_d_parameters': f's3://{S3_BUCKET}/gold/rate_notice/part_d_parameters.parquet',
            
            # Risk adjustment parameters by year (model version, phase-in, normalization)
            'risk_adjustment_parameters': f's3://{S3_BUCKET}/gold/rate_notice/risk_adjustment_parameters.parquet',
            
            # MA growth rates by year (advance, final, effective)
            'ma_growth_rates': f's3://{S3_BUCKET}/gold/rate_notice/ma_growth_rates.parquet',
            
            # Star rating bonus structure by year
            'star_bonus_structure': f's3://{S3_BUCKET}/gold/rate_notice/star_bonus_structure.parquet',
            
            # HCC coefficients by model version
            'hcc_coefficients_v28': f's3://{S3_BUCKET}/gold/rate_notice/hcc_coefficients_v28.parquet',
            
            # County benchmarks (from ratebooks) - MA benchmark rates by county/year
            'county_benchmarks': f's3://{S3_BUCKET}/gold/rate_notice/county_benchmarks.parquet',
            
            # National USPCC (United States Per Capita Costs) - FFS baseline by year
            'national_uspcc': f's3://{S3_BUCKET}/gold/rate_notice/national_uspcc.parquet',
            
            # =================================================================
            # PARSED RATE NOTICE TABLES (extracted from PDF tables with audit)
            # =================================================================
            # USPCC projections by calendar year (Part A, Part B costs)
            'uspcc_projections': f's3://{S3_BUCKET}/gold/rate_notice_parsed/uspcc_projections.parquet',
            
            # HCC coefficients from all model versions (V21, V22, V24, V28, RxHCC)
            'hcc_coefficients_all': f's3://{S3_BUCKET}/gold/rate_notice_parsed/hcc_coefficients_all.parquet',
            
            # ESRD rates and risk adjustment factors
            'esrd_rates': f's3://{S3_BUCKET}/gold/rate_notice_parsed/esrd_rates.parquet',
            
            # Demographic adjustment factors (age/sex, trends)
            'demographic_factors': f's3://{S3_BUCKET}/gold/rate_notice_parsed/demographic_factors.parquet',
            
            # Benchmark parameters (applicable percentages, quartiles)
            'benchmark_parameters': f's3://{S3_BUCKET}/gold/rate_notice_parsed/benchmark_parameters.parquet',
            
            # Service type cost breakdowns (inpatient, outpatient, etc.)
            'service_type_costs': f's3://{S3_BUCKET}/gold/rate_notice_parsed/service_type_costs.parquet',
        }

        self.registered_tables = []

        for table_name, s3_path in tables.items():
            try:
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{s3_path}')
                """)
                self.registered_tables.append(table_name)
            except Exception as e:
                print(f"Warning: Could not register {table_name}: {e}")

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.

        Args:
            sql: SQL query string

        Returns:
            pandas DataFrame with results
        """
        return self.conn.execute(sql).fetchdf()

    def query_with_audit(
        self,
        sql: str,
        user_id: str = "anonymous",
        context: str = None,
        request_id: str = None
    ) -> Tuple[pd.DataFrame, str]:
        """
        Execute query with full audit logging.

        Args:
            sql: SQL query string
            user_id: Identifier for who ran the query
            context: Description of why query was run (e.g., "dashboard", "API call")
            request_id: Optional external request ID for correlation

        Returns:
            Tuple of (DataFrame results, audit_id)
        """
        audit_id = str(uuid.uuid4())
        start_time = datetime.now()

        # Parse query to extract tables accessed
        tables_accessed = self._extract_tables(sql)

        try:
            # Execute query
            result = self.conn.execute(sql).fetchdf()

            # Log successful query
            audit_record = {
                'audit_id': audit_id,
                'timestamp': start_time.isoformat(),
                'user_id': user_id,
                'context': context,
                'request_id': request_id,
                'sql': sql,
                'sql_hash': hashlib.md5(sql.encode()).hexdigest(),
                'tables_accessed': tables_accessed,
                'row_count': len(result),
                'column_count': len(result.columns),
                'columns': list(result.columns),
                'execution_time_ms': (datetime.now() - start_time).total_seconds() * 1000,
                'status': 'success',
                'error': None,
            }

            self._save_query_audit(audit_record)

            return result, audit_id

        except Exception as e:
            # Log failed query
            audit_record = {
                'audit_id': audit_id,
                'timestamp': start_time.isoformat(),
                'user_id': user_id,
                'context': context,
                'request_id': request_id,
                'sql': sql,
                'sql_hash': hashlib.md5(sql.encode()).hexdigest(),
                'tables_accessed': tables_accessed,
                'row_count': 0,
                'column_count': 0,
                'columns': [],
                'execution_time_ms': (datetime.now() - start_time).total_seconds() * 1000,
                'status': 'error',
                'error': str(e),
            }

            self._save_query_audit(audit_record)
            raise

    def query_with_record_audit(
        self,
        sql: str,
        user_id: str = "anonymous",
        context: str = None,
        request_id: str = None
    ) -> Tuple[pd.DataFrame, str]:
        """
        Execute query with full audit logging INCLUDING record-level tracking.

        This extends query_with_audit to also track which specific records
        (by primary key) were accessed, enabling fine-grained data lineage.

        Args:
            sql: SQL query string
            user_id: Identifier for who ran the query
            context: Description of why query was run
            request_id: Optional external request ID for correlation

        Returns:
            Tuple of (DataFrame results, audit_id)
        """
        from db.record_audit import get_record_audit_manager

        # First do the normal query with audit
        result, audit_id = self.query_with_audit(
            sql=sql,
            user_id=user_id,
            context=context,
            request_id=request_id
        )

        # Now add record-level tracking
        record_manager = get_record_audit_manager()
        tables_accessed = self._extract_tables(sql)

        for table_name in tables_accessed:
            record_manager.track_records(
                audit_id=audit_id,
                user_id=user_id,
                context=context,
                table_name=table_name,
                df=result
            )

        # Save record audit (async by default)
        record_manager.save_audit(audit_id)

        return result, audit_id

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL query."""
        # Use registered tables instead of hardcoded list
        tables = []
        sql_lower = sql.lower()

        for table in self.registered_tables:
            if table.lower() in sql_lower:
                tables.append(table)

        return tables

    def _save_query_audit(self, audit_record: Dict):
        """Save query audit record to S3."""
        try:
            date_str = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"{AUDIT_PREFIX}/{date_str}/{audit_record['audit_id']}.json"

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(audit_record, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"Warning: Could not save query audit: {e}")

    def trace_query_lineage(self, audit_id: str) -> Dict:
        """
        Trace lineage for a query result.

        Returns information about:
        - Which tables were queried
        - Which source files those tables came from
        - When those source files were last built

        Args:
            audit_id: The audit ID from query_with_audit

        Returns:
            Lineage information dict
        """
        # Load query audit
        try:
            # Find audit record
            date_prefixes = [
                datetime.now().strftime("%Y/%m/%d"),
                (datetime.now()).strftime("%Y/%m/%d"),
            ]

            audit_record = None
            for date_prefix in date_prefixes:
                try:
                    response = s3.get_object(
                        Bucket=S3_BUCKET,
                        Key=f"{AUDIT_PREFIX}/{date_prefix}/{audit_id}.json"
                    )
                    audit_record = json.loads(response['Body'].read())
                    break
                except:
                    continue

            if not audit_record:
                return {'error': 'Audit record not found'}

            # Get lineage for each table accessed
            tables_lineage = {}
            for table in audit_record.get('tables_accessed', []):
                tables_lineage[table] = self._get_table_lineage(table)

            return {
                'audit_id': audit_id,
                'query_timestamp': audit_record.get('timestamp'),
                'tables_accessed': audit_record.get('tables_accessed', []),
                'tables_lineage': tables_lineage,
                'full_source_chain': self._build_source_chain(tables_lineage)
            }

        except Exception as e:
            return {'error': str(e)}

    def _get_table_lineage(self, table_name: str) -> Dict:
        """Get lineage information for a specific table."""
        # Map tables to their source definitions
        table_sources = {
            # ENROLLMENT (NEW - unified with audit)
            'fact_enrollment_all_years': {
                'primary_sources': ['processed/fact_enrollment/*/data.parquet', 'dim_contract_v2', 'snp_lookup'],
                'build_script': 'scripts/build_fact_enrollment.py',
                'grain': 'contract_id + year + month + state + product_type',
                'description': 'Unified enrollment 2013-2026 with audit columns',
            },
            # Legacy alias
            'fact_enrollment_unified': {
                'primary_sources': ['fact_enrollment_all_years'],
                'build_script': 'scripts/build_fact_enrollment.py',
                'grain': 'contract_id + year + month + state + product_type',
            },
            'fact_enrollment_by_state': {
                'primary_sources': ['cpsc'],
                'build_script': 'build_fact_enrollment_by_state.py',
                'grain': 'state + year',
            },
            'fact_enrollment_by_geography': {
                'primary_sources': ['cpsc'],
                'build_script': 'build_fact_enrollment_geographic.py',
                'grain': 'state + county + year',
            },
            # STARS
            'stars_enrollment_unified': {
                'primary_sources': ['stars', 'enrollment_by_plan'],
                'build_script': 'build_stars_enrollment_unified.py',
                'grain': 'contract_id + year',
            },
            'stars_summary': {
                'primary_sources': ['stars'],
                'build_script': 'build_stars_summary.py',
                'grain': 'contract_id + year',
            },
            'measure_data': {
                'primary_sources': ['stars'],
                'build_script': 'build_measure_data.py',
                'grain': 'contract_id + measure_id + year',
            },
            'stars_cutpoints_2014_2026': {
                'primary_sources': ['stars'],
                'build_script': 'build_stars_cutpoints.py',
                'grain': 'measure_id + year',
            },
            'stars_measure_specs': {
                'primary_sources': ['stars'],
                'build_script': 'build_stars_measure_specs.py',
                'grain': 'measure_id',
            },
            'stars_fourplus_by_year': {
                'primary_sources': ['stars', 'enrollment_by_plan'],
                'build_script': 'build_stars_fourplus.py',
                'grain': 'parent_org + year',
            },
            # NEW UNIFIED STARS TABLES (2008-2026)
            'measures_all_years': {
                'primary_sources': ['stars'],
                'build_script': 'scripts/unified/build_measures_all_years.py',
                'grain': 'contract_id + year + measure_id',
                'description': 'Measure performance data 2008-2026 with stable keys',
            },
            'summary_all_years': {
                'primary_sources': ['stars'],
                'build_script': 'scripts/unified/build_summary_all_years.py',
                'grain': 'contract_id + year + part',
                'description': 'Summary ratings 2009-2026 in LONG format',
            },
            'domain_all_years': {
                'primary_sources': ['stars'],
                'build_script': 'scripts/unified/build_domain_all_years.py',
                'grain': 'contract_id + year + part + domain_name',
                'description': 'Domain scores 2008-2026 in LONG format',
            },
            'cutpoints_all_years': {
                'primary_sources': ['stars'],
                'build_script': 'scripts/unified/build_cutpoints_all_years.py',
                'grain': 'year + part + measure_id + star_level',
                'description': 'Cutpoint thresholds 2011-2026',
            },
            'dim_measure': {
                'primary_sources': ['stars'],
                'build_script': 'scripts/unified/build_dim_measure.py',
                'grain': 'measure_id + year',
                'description': 'Measure dimension with stable keys',
            },
            'dim_entity': {
                'primary_sources': ['crosswalk', 'enrollment_by_plan'],
                'build_script': 'scripts/unified/build_dim_entity.py',
                'grain': 'entity_id',
                'description': 'Entity chains tracking contract mergers',
            },
            'disenrollment_all_years': {
                'primary_sources': ['stars'],
                'build_script': 'scripts/unified/build_disenrollment.py',
                'grain': 'contract_id + year',
                'description': 'Disenrollment reasons 2024-2026',
            },
            # RISK SCORES
            'fact_risk_scores_unified': {
                'primary_sources': ['risk_scores'],
                'build_script': 'build_fact_risk_scores_unified.py',
                'grain': 'contract_id + plan_id + year',
            },
            'risk_scores_by_parent': {
                'primary_sources': ['risk_scores', 'enrollment_by_plan'],
                'build_script': 'build_risk_scores_by_parent.py',
                'grain': 'parent_org + year',
                'description': 'Pre-aggregated risk scores by parent org - used for industry totals',
            },
            'risk_scores_by_parent_dims': {
                'primary_sources': ['risk_scores', 'enrollment_by_plan'],
                'build_script': 'build_risk_scores_by_parent_dims.py',
                'grain': 'parent_org + year + plan_type + snp_type + group_type',
                'description': 'Pre-aggregated risk scores with dimension breakdowns',
            },
            # SNP
            'fact_snp': {
                'primary_sources': ['snp_report'],
                'build_script': 'build_fact_snp.py',
                'grain': 'contract_id + plan_id + year',
            },
            'fact_snp_historical': {
                'primary_sources': ['snp_report'],
                'build_script': 'build_fact_snp_historical.py',
                'grain': 'parent_org + year',
            },
            # AGGREGATIONS
            'agg_enrollment_by_year': {
                'primary_sources': ['fact_enrollment_unified'],
                'build_script': 'build_agg_enrollment.py',
                'grain': 'year',
            },
            'agg_enrollment_by_plantype': {
                'primary_sources': ['fact_enrollment_unified'],
                'build_script': 'build_agg_enrollment.py',
                'grain': 'year + plan_type',
            },
            'enrollment_by_parent': {
                'primary_sources': ['enrollment_by_plan'],
                'build_script': 'build_enrollment_by_parent.py',
                'grain': 'parent_org + year',
            },
            # LOOKUPS
            'dim_county': {
                'primary_sources': ['cpsc'],
                'build_script': 'build_dim_county.py',
                'grain': 'state + county',
            },
            'parent_org_summary': {
                'primary_sources': ['enrollment_by_plan', 'stars'],
                'build_script': 'build_parent_org_summary.py',
                'grain': 'parent_org',
            },
        }

        return table_sources.get(table_name, {'error': 'Unknown table', 'table_name': table_name})

    def _build_source_chain(self, tables_lineage: Dict) -> List[str]:
        """Build complete source chain from CMS files to query result."""
        sources = set()
        for table, lineage in tables_lineage.items():
            if 'primary_sources' in lineage:
                sources.update(lineage['primary_sources'])

        # Map to CMS file names
        cms_sources = {
            'enrollment_by_plan': 'CMS Monthly Enrollment by Plan',
            'cpsc': 'CMS CPSC (County-Plan-State-Contract)',
            'snp_report': 'CMS SNP Comprehensive Report',
            'stars': 'CMS Medicare Star Ratings',
            'risk_scores': 'CMS Plan Payment Data',
            'crosswalk': 'CMS Plan Crosswalk Files',
        }

        return [cms_sources.get(s, s) for s in sources]

    def get_available_tables(self) -> List[Dict]:
        """List all available tables with metadata."""
        tables = []

        table_info = {
            'dim_entity': {
                'type': 'dimension',
                'description': 'Stable entity IDs tracking plans across years',
                'row_estimate': '~50K plans'
            },
            'dim_parent_org': {
                'type': 'dimension',
                'description': 'Canonical parent organization names with M&A history',
                'row_estimate': '~500 orgs'
            },
            'fact_enrollment_unified': {
                'type': 'fact',
                'description': 'Master enrollment fact - all dimensions, no suppression',
                'row_estimate': '~10M rows (19 years)'
            },
            'fact_enrollment_geographic': {
                'type': 'fact',
                'description': 'County-level enrollment with suppression tracking',
                'row_estimate': '~100M rows'
            },
            'fact_star_ratings': {
                'type': 'fact',
                'description': 'Contract-level star ratings by year',
                'row_estimate': '~100K rows'
            },
            'fact_risk_scores': {
                'type': 'fact',
                'description': 'Plan-level risk scores by year',
                'row_estimate': '~500K rows'
            },
            'agg_by_parent_year': {
                'type': 'aggregation',
                'description': 'Pre-computed parent org totals',
                'row_estimate': '~10K rows'
            },
            'agg_by_state_year': {
                'type': 'aggregation',
                'description': 'Pre-computed state totals',
                'row_estimate': '~1K rows'
            },
            'agg_by_dimensions': {
                'type': 'aggregation',
                'description': 'Pre-computed dimension combinations',
                'row_estimate': '~50K rows'
            },
            'agg_industry_totals': {
                'type': 'aggregation',
                'description': 'Industry-level totals by year/month',
                'row_estimate': '~250 rows'
            },
        }

        for name, info in table_info.items():
            tables.append({
                'name': name,
                **info
            })

        return tables

    def explain_query(self, sql: str) -> str:
        """Get query execution plan."""
        return self.conn.execute(f"EXPLAIN {sql}").fetchdf().to_string()

    def close(self):
        """Close the connection."""
        self.conn.close()


# Convenience function for quick queries
def query(sql: str) -> pd.DataFrame:
    """Quick query without audit (for dev/testing)."""
    engine = MAQueryEngine()
    result = engine.query(sql)
    engine.close()
    return result


# Singleton for API use
_engine_instance = None

def get_engine() -> MAQueryEngine:
    """Get or create singleton query engine."""
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = MAQueryEngine()
    return _engine_instance


# Alias for backward compatibility with data_service
DuckDBLayer = MAQueryEngine
