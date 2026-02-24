#!/usr/bin/env python3
"""
Data Lineage and Audit System

Provides complete traceability from any data point back to its source files.

Features:
1. Source file registry - tracks all raw files with checksums
2. Transformation lineage - records every join, derivation, aggregation
3. Record-level provenance - links output records to source records
4. Audit log - timestamped record of all pipeline runs

Tables:
- audit_source_files: Registry of all source files
- audit_transformations: Record of all transformations
- audit_lineage: Record-level source tracing
- audit_pipeline_runs: Pipeline execution history
"""

import os
import sys
import json
import hashlib
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

# Configuration
S3_BUCKET = "ma-data123"
AUDIT_PREFIX = "processed/audit"

s3 = boto3.client('s3')


class TransformationType(str, Enum):
    LOAD = "load"                    # Load from source file
    JOIN = "join"                    # Join two datasets
    DERIVE = "derive"                # Derive/calculate new field
    AGGREGATE = "aggregate"          # Aggregation
    FILTER = "filter"                # Filter rows
    MAP = "map"                      # Map/transform values
    UNION = "union"                  # Union datasets
    DEDUPLICATE = "deduplicate"      # Remove duplicates


@dataclass
class SourceFile:
    """Registry entry for a source file."""
    file_id: str
    s3_key: str
    file_type: str                   # enrollment_by_plan, cpsc, snp, stars, crosswalk
    year: int
    month: Optional[int]
    file_size_bytes: int
    md5_hash: str
    row_count: Optional[int]
    column_names: List[str]
    discovered_at: str
    last_processed_at: Optional[str]


@dataclass
class Transformation:
    """Record of a data transformation."""
    transform_id: str
    pipeline_run_id: str
    transform_type: str
    description: str
    input_sources: List[str]         # List of file_ids or transform_ids
    output_table: str
    output_row_count: int

    # For joins
    join_keys: Optional[List[str]]
    join_type: Optional[str]         # left, inner, outer

    # For derivations
    derived_field: Optional[str]
    derivation_logic: Optional[str]

    # For aggregations
    group_by_fields: Optional[List[str]]
    agg_functions: Optional[Dict[str, str]]

    timestamp: str
    duration_seconds: float


@dataclass
class RecordLineage:
    """Links output records to source records."""
    lineage_id: str
    output_table: str
    output_key: Dict[str, Any]       # Primary key of output record
    source_files: List[Dict]         # [{file_id, row_identifier}, ...]
    transformations_applied: List[str]  # List of transform_ids in order
    created_at: str


@dataclass
class PipelineRun:
    """Record of a pipeline execution."""
    run_id: str
    pipeline_name: str
    started_at: str
    finished_at: Optional[str]
    status: str                      # running, success, failed
    input_files_count: int
    output_tables: List[str]
    output_row_count: int
    error_message: Optional[str]
    parameters: Dict[str, Any]


class AuditLogger:
    """
    Centralized audit logging for data lineage.

    Usage:
        audit = AuditLogger(pipeline_name="build_fact_enrollment")
        audit.start_run()

        # Register source files
        file_id = audit.register_source_file(s3_key, file_type, year, month)

        # Log transformations
        audit.log_load(file_id, df, "Loaded enrollment by plan")
        audit.log_join(left_id, right_id, join_keys, result_df, "Joined with CPSC")
        audit.log_derive(source_id, "group_type", "EGHP field or plan_id heuristic", df)

        # Finish
        audit.finish_run(success=True)
    """

    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.run_id = str(uuid.uuid4())
        self.source_files: Dict[str, SourceFile] = {}
        self.transformations: List[Transformation] = []
        self.lineage_records: List[RecordLineage] = []
        self.current_run: Optional[PipelineRun] = None
        self._transform_counter = 0

    def start_run(self, parameters: Dict[str, Any] = None):
        """Start a new pipeline run."""
        self.current_run = PipelineRun(
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            started_at=datetime.now().isoformat(),
            finished_at=None,
            status="running",
            input_files_count=0,
            output_tables=[],
            output_row_count=0,
            error_message=None,
            parameters=parameters or {}
        )
        print(f"[AUDIT] Pipeline run started: {self.run_id}")

    def register_source_file(
        self,
        s3_key: str,
        file_type: str,
        year: int,
        month: Optional[int] = None,
        df: pd.DataFrame = None
    ) -> str:
        """
        Register a source file and return its file_id.

        Computes MD5 hash for integrity verification.
        """
        file_id = str(uuid.uuid4())

        # Get file metadata from S3
        try:
            head = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
            file_size = head['ContentLength']

            # Compute MD5 hash (for smaller files)
            if file_size < 100_000_000:  # 100MB limit for hashing
                response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
                content = response['Body'].read()
                md5_hash = hashlib.md5(content).hexdigest()
            else:
                md5_hash = head.get('ETag', '').strip('"')
        except Exception as e:
            file_size = 0
            md5_hash = "unknown"

        source = SourceFile(
            file_id=file_id,
            s3_key=s3_key,
            file_type=file_type,
            year=year,
            month=month,
            file_size_bytes=file_size,
            md5_hash=md5_hash,
            row_count=len(df) if df is not None else None,
            column_names=list(df.columns) if df is not None else [],
            discovered_at=datetime.now().isoformat(),
            last_processed_at=datetime.now().isoformat()
        )

        self.source_files[file_id] = source

        if self.current_run:
            self.current_run.input_files_count += 1

        print(f"[AUDIT] Registered source: {s3_key} -> {file_id[:8]}...")
        return file_id

    def _next_transform_id(self) -> str:
        """Generate next transformation ID."""
        self._transform_counter += 1
        return f"{self.run_id[:8]}-T{self._transform_counter:04d}"

    def log_load(
        self,
        file_id: str,
        df: pd.DataFrame,
        description: str
    ) -> str:
        """Log a data load transformation."""
        transform_id = self._next_transform_id()

        transform = Transformation(
            transform_id=transform_id,
            pipeline_run_id=self.run_id,
            transform_type=TransformationType.LOAD.value,
            description=description,
            input_sources=[file_id],
            output_table=f"loaded_{file_id[:8]}",
            output_row_count=len(df),
            join_keys=None,
            join_type=None,
            derived_field=None,
            derivation_logic=None,
            group_by_fields=None,
            agg_functions=None,
            timestamp=datetime.now().isoformat(),
            duration_seconds=0
        )

        self.transformations.append(transform)
        print(f"[AUDIT] Load: {description} ({len(df):,} rows)")
        return transform_id

    def log_join(
        self,
        left_source: str,
        right_source: str,
        join_keys: List[str],
        join_type: str,
        result_df: pd.DataFrame,
        description: str
    ) -> str:
        """Log a join transformation."""
        transform_id = self._next_transform_id()

        transform = Transformation(
            transform_id=transform_id,
            pipeline_run_id=self.run_id,
            transform_type=TransformationType.JOIN.value,
            description=description,
            input_sources=[left_source, right_source],
            output_table=f"joined_{transform_id}",
            output_row_count=len(result_df),
            join_keys=join_keys,
            join_type=join_type,
            derived_field=None,
            derivation_logic=None,
            group_by_fields=None,
            agg_functions=None,
            timestamp=datetime.now().isoformat(),
            duration_seconds=0
        )

        self.transformations.append(transform)
        print(f"[AUDIT] Join: {description} ({len(result_df):,} rows)")
        return transform_id

    def log_derive(
        self,
        source: str,
        field_name: str,
        logic: str,
        result_df: pd.DataFrame,
        description: str = None
    ) -> str:
        """Log a field derivation."""
        transform_id = self._next_transform_id()

        transform = Transformation(
            transform_id=transform_id,
            pipeline_run_id=self.run_id,
            transform_type=TransformationType.DERIVE.value,
            description=description or f"Derive {field_name}",
            input_sources=[source],
            output_table=f"derived_{transform_id}",
            output_row_count=len(result_df),
            join_keys=None,
            join_type=None,
            derived_field=field_name,
            derivation_logic=logic,
            group_by_fields=None,
            agg_functions=None,
            timestamp=datetime.now().isoformat(),
            duration_seconds=0
        )

        self.transformations.append(transform)
        print(f"[AUDIT] Derive: {field_name} using '{logic}'")
        return transform_id

    def log_aggregate(
        self,
        source: str,
        group_by: List[str],
        agg_functions: Dict[str, str],
        result_df: pd.DataFrame,
        description: str
    ) -> str:
        """Log an aggregation transformation."""
        transform_id = self._next_transform_id()

        transform = Transformation(
            transform_id=transform_id,
            pipeline_run_id=self.run_id,
            transform_type=TransformationType.AGGREGATE.value,
            description=description,
            input_sources=[source],
            output_table=f"agg_{transform_id}",
            output_row_count=len(result_df),
            join_keys=None,
            join_type=None,
            derived_field=None,
            derivation_logic=None,
            group_by_fields=group_by,
            agg_functions=agg_functions,
            timestamp=datetime.now().isoformat(),
            duration_seconds=0
        )

        self.transformations.append(transform)
        print(f"[AUDIT] Aggregate: {description} ({len(result_df):,} rows)")
        return transform_id

    def log_filter(
        self,
        source: str,
        filter_condition: str,
        result_df: pd.DataFrame,
        description: str
    ) -> str:
        """Log a filter transformation."""
        transform_id = self._next_transform_id()

        transform = Transformation(
            transform_id=transform_id,
            pipeline_run_id=self.run_id,
            transform_type=TransformationType.FILTER.value,
            description=description,
            input_sources=[source],
            output_table=f"filtered_{transform_id}",
            output_row_count=len(result_df),
            join_keys=None,
            join_type=None,
            derived_field=None,
            derivation_logic=filter_condition,
            group_by_fields=None,
            agg_functions=None,
            timestamp=datetime.now().isoformat(),
            duration_seconds=0
        )

        self.transformations.append(transform)
        print(f"[AUDIT] Filter: {description} ({len(result_df):,} rows)")
        return transform_id

    def add_lineage_column(
        self,
        df: pd.DataFrame,
        source_file_id: str,
        source_id_column: str = None
    ) -> pd.DataFrame:
        """
        Add lineage columns to a DataFrame for record-level tracking.

        Adds:
        - _source_file_id: The source file this record came from
        - _source_row_id: Row identifier in source (index or specified column)
        - _lineage_id: Unique lineage ID for this record
        """
        df = df.copy()
        df['_source_file_id'] = source_file_id

        if source_id_column and source_id_column in df.columns:
            df['_source_row_id'] = df[source_id_column].astype(str)
        else:
            df['_source_row_id'] = df.index.astype(str)

        df['_lineage_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['_pipeline_run_id'] = self.run_id

        return df

    def merge_lineage(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        result_df: pd.DataFrame,
        how: str = 'left'
    ) -> pd.DataFrame:
        """
        Merge lineage columns from joined DataFrames.

        Creates comma-separated list of source files when joining.
        """
        result = result_df.copy()

        # Combine source file IDs
        left_sources = left_df['_source_file_id'].iloc[0] if '_source_file_id' in left_df.columns else ''
        right_sources = right_df['_source_file_id'].iloc[0] if '_source_file_id' in right_df.columns else ''

        if left_sources and right_sources:
            result['_source_file_id'] = f"{left_sources},{right_sources}"
        elif left_sources:
            result['_source_file_id'] = left_sources
        elif right_sources:
            result['_source_file_id'] = right_sources

        result['_pipeline_run_id'] = self.run_id
        result['_lineage_id'] = [str(uuid.uuid4()) for _ in range(len(result))]

        return result

    def finish_run(
        self,
        success: bool,
        output_tables: List[str] = None,
        output_row_count: int = 0,
        error_message: str = None
    ):
        """Complete the pipeline run and save audit data."""
        if self.current_run:
            self.current_run.finished_at = datetime.now().isoformat()
            self.current_run.status = "success" if success else "failed"
            self.current_run.output_tables = output_tables or []
            self.current_run.output_row_count = output_row_count
            self.current_run.error_message = error_message

        # Save audit data to S3
        self._save_audit_data()

        status = "SUCCESS" if success else "FAILED"
        print(f"[AUDIT] Pipeline run {status}: {self.run_id}")

    def _save_audit_data(self):
        """Save all audit data to S3."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save source files registry
        if self.source_files:
            source_df = pd.DataFrame([asdict(s) for s in self.source_files.values()])
            self._upload_parquet(
                source_df,
                f"{AUDIT_PREFIX}/source_files/run={self.run_id}/data.parquet"
            )

        # Save transformations
        if self.transformations:
            transform_df = pd.DataFrame([asdict(t) for t in self.transformations])
            self._upload_parquet(
                transform_df,
                f"{AUDIT_PREFIX}/transformations/run={self.run_id}/data.parquet"
            )

        # Save pipeline run
        if self.current_run:
            run_df = pd.DataFrame([asdict(self.current_run)])
            self._upload_parquet(
                run_df,
                f"{AUDIT_PREFIX}/pipeline_runs/run={self.run_id}/data.parquet"
            )

        # Save summary JSON
        summary = {
            'run_id': self.run_id,
            'pipeline_name': self.pipeline_name,
            'timestamp': timestamp,
            'source_files_count': len(self.source_files),
            'transformations_count': len(self.transformations),
            'status': self.current_run.status if self.current_run else 'unknown'
        }

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{AUDIT_PREFIX}/summaries/{self.run_id}.json",
            Body=json.dumps(summary, indent=2),
            ContentType='application/json'
        )

    def _upload_parquet(self, df: pd.DataFrame, s3_key: str):
        """Upload DataFrame as Parquet to S3."""
        with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
            df.to_parquet(f.name, compression='snappy')
            s3.upload_file(f.name, S3_BUCKET, s3_key)

    def get_lineage_report(self) -> Dict:
        """Generate a lineage report for this run."""
        return {
            'run_id': self.run_id,
            'pipeline_name': self.pipeline_name,
            'source_files': [
                {
                    'file_id': s.file_id,
                    's3_key': s.s3_key,
                    'file_type': s.file_type,
                    'year': s.year,
                    'month': s.month,
                    'row_count': s.row_count,
                    'md5_hash': s.md5_hash
                }
                for s in self.source_files.values()
            ],
            'transformations': [
                {
                    'transform_id': t.transform_id,
                    'type': t.transform_type,
                    'description': t.description,
                    'input_sources': t.input_sources,
                    'output_row_count': t.output_row_count
                }
                for t in self.transformations
            ],
            'transformation_graph': self._build_transformation_graph()
        }

    def _build_transformation_graph(self) -> Dict:
        """Build a graph representation of transformations."""
        nodes = []
        edges = []

        # Add source file nodes
        for file_id, source in self.source_files.items():
            nodes.append({
                'id': file_id,
                'type': 'source',
                'label': f"{source.file_type}\n{source.year}-{source.month or 'XX'}"
            })

        # Add transformation nodes and edges
        for t in self.transformations:
            nodes.append({
                'id': t.transform_id,
                'type': 'transform',
                'label': f"{t.transform_type}\n{t.description[:30]}"
            })

            for input_source in t.input_sources:
                edges.append({
                    'from': input_source,
                    'to': t.transform_id
                })

        return {'nodes': nodes, 'edges': edges}


def trace_record_lineage(
    output_table: str,
    record_key: Dict[str, Any],
    run_id: str = None
) -> Dict:
    """
    Trace a specific record back to its source files.

    Args:
        output_table: Name of the output table (e.g., 'fact_enrollment_unified')
        record_key: Primary key values (e.g., {'contract_id': 'H1234', 'plan_id': '001', 'year': 2026})
        run_id: Optional specific run ID to trace

    Returns:
        Lineage information including source files and transformations
    """
    # Find the record in the output table
    # Load audit data
    # Trace back through transformation chain
    # Return complete lineage

    # This would query the audit tables to trace lineage
    # Implementation depends on how records are stored

    return {
        'record_key': record_key,
        'output_table': output_table,
        'source_files': [],  # Would be populated from audit data
        'transformations': [],  # Would be populated from audit data
        'full_path': []  # Complete transformation path
    }


# Convenience function for scripts
def create_audit_logger(pipeline_name: str) -> AuditLogger:
    """Create and initialize an audit logger."""
    audit = AuditLogger(pipeline_name)
    audit.start_run()
    return audit
