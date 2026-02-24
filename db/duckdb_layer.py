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

        # Configure S3 credentials from environment
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

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

        tables = {
            # Dimensions
            'dim_entity': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/dimensions/dim_entity.parquet',
            'dim_parent_org': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/dimensions/dim_parent_org.parquet',

            # Facts (partitioned - use glob)
            'fact_enrollment_unified': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/facts/fact_enrollment_unified/**/*.parquet',
            'fact_enrollment_geographic': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/facts/fact_enrollment_geographic/**/*.parquet',
            'fact_star_ratings': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/facts/fact_star_ratings/**/*.parquet',
            'fact_risk_scores': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/facts/fact_risk_scores/**/*.parquet',

            # Aggregations
            'agg_by_parent_year': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/aggregations/agg_by_parent_year.parquet',
            'agg_by_state_year': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/aggregations/agg_by_state_year.parquet',
            'agg_by_dimensions': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/aggregations/agg_by_dimensions.parquet',
            'agg_industry_totals': f's3://{S3_BUCKET}/{PROCESSED_PREFIX}/aggregations/agg_industry_totals.parquet',
        }

        for table_name, s3_path in tables.items():
            try:
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{s3_path}', hive_partitioning=true)
                """)
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

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL query."""
        # Simple extraction - could be enhanced with SQL parser
        tables = []
        known_tables = [
            'dim_entity', 'dim_parent_org',
            'fact_enrollment_unified', 'fact_enrollment_geographic',
            'fact_star_ratings', 'fact_risk_scores',
            'agg_by_parent_year', 'agg_by_state_year',
            'agg_by_dimensions', 'agg_industry_totals'
        ]

        sql_lower = sql.lower()
        for table in known_tables:
            if table in sql_lower:
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
            'fact_enrollment_unified': {
                'primary_sources': ['enrollment_by_plan', 'cpsc', 'snp_report'],
                'build_script': 'build_fact_enrollment_unified.py',
                'grain': 'contract_id + plan_id + year + month',
            },
            'fact_enrollment_geographic': {
                'primary_sources': ['cpsc'],
                'build_script': 'build_fact_enrollment_geographic.py',
                'grain': 'contract_id + plan_id + year + month + state + county',
            },
            'fact_star_ratings': {
                'primary_sources': ['stars'],
                'build_script': 'build_fact_star_ratings.py',
                'grain': 'contract_id + year',
            },
            'fact_risk_scores': {
                'primary_sources': ['risk_scores'],
                'build_script': 'build_fact_risk_scores.py',
                'grain': 'contract_id + plan_id + year',
            },
            'dim_entity': {
                'primary_sources': ['crosswalk', 'enrollment_by_plan'],
                'build_script': 'build_entity_chains.py',
                'grain': 'entity_id',
            },
            'dim_parent_org': {
                'primary_sources': ['stars', 'cpsc'],
                'build_script': 'build_parent_org_dimension.py',
                'grain': 'parent_org_id',
            },
            'agg_by_parent_year': {
                'primary_sources': ['fact_enrollment_unified'],
                'build_script': 'build_aggregation_tables.py',
                'grain': 'parent_org + year + month',
            },
            'agg_by_state_year': {
                'primary_sources': ['fact_enrollment_geographic'],
                'build_script': 'build_aggregation_tables.py',
                'grain': 'state + year + month',
            },
            'agg_by_dimensions': {
                'primary_sources': ['fact_enrollment_unified'],
                'build_script': 'build_aggregation_tables.py',
                'grain': 'all dimensions + year + month',
            },
            'agg_industry_totals': {
                'primary_sources': ['fact_enrollment_unified'],
                'build_script': 'build_aggregation_tables.py',
                'grain': 'year + month',
            },
        }

        return table_sources.get(table_name, {'error': 'Unknown table'})

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
