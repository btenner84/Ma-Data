#!/usr/bin/env python3
"""
Record-Level Audit Manager

Tracks exactly which records (data points) were accessed in each query.
This extends the query-level audit to provide fine-grained data lineage.

Storage: s3://ma-data123/processed/audit/records/{date}/{audit_id}.json

Example audit record:
{
    "audit_id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-03-03T10:15:30.123456",
    "tables": {
        "measures_all_years": {
            "primary_keys": ["contract_id", "year", "measure_id"],
            "record_count": 150,
            "columns_accessed": ["contract_id", "year", "measure_id", "star_rating"],
            "sample_keys": [
                {"contract_id": "H0028", "year": 2024, "measure_id": "C01"},
                {"contract_id": "H0028", "year": 2024, "measure_id": "C02"},
                ...
            ]
        }
    }
}
"""

import os
import json
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict

import pandas as pd
import boto3

from db.table_schemas import get_primary_keys, has_primary_keys

# Configuration
S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
S3_REGION = os.environ.get("AWS_REGION", "us-east-1")
RECORD_AUDIT_PREFIX = "processed/audit/records"

# Feature flag
ENABLE_RECORD_AUDIT = os.environ.get("ENABLE_RECORD_AUDIT", "true").lower() == "true"

# Max records to store in sample (to avoid huge audit files)
MAX_SAMPLE_KEYS = 1000

# Thread pool for async S3 writes
_executor = ThreadPoolExecutor(max_workers=4)

# S3 client (thread-safe)
_s3_client = None
_s3_lock = threading.Lock()


def _get_s3_client():
    """Get thread-safe S3 client."""
    global _s3_client
    if _s3_client is None:
        with _s3_lock:
            if _s3_client is None:
                _s3_client = boto3.client('s3', region_name=S3_REGION)
    return _s3_client


@dataclass
class TableRecordAudit:
    """Audit record for a single table access."""
    table_name: str
    primary_keys: List[str]
    record_count: int
    columns_accessed: List[str]
    sample_keys: List[Dict[str, Any]]  # First N records' primary keys


@dataclass
class RecordAuditEntry:
    """Complete record-level audit entry for a query."""
    audit_id: str
    timestamp: str
    user_id: str
    context: Optional[str]
    tables: Dict[str, TableRecordAudit]

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'audit_id': self.audit_id,
            'timestamp': self.timestamp,
            'user_id': self.user_id,
            'context': self.context,
            'tables': {
                name: asdict(audit) for name, audit in self.tables.items()
            }
        }


class RecordAuditManager:
    """
    Manages record-level audit tracking.

    Usage:
        manager = RecordAuditManager()

        # Track records from a query result
        manager.track_records(
            audit_id="abc-123",
            user_id="api_user",
            context="dashboard",
            table_name="measures_all_years",
            df=result_df
        )

        # Finalize and save (async)
        manager.save_audit(audit_id)
    """

    def __init__(self):
        self._pending_audits: Dict[str, RecordAuditEntry] = {}
        self._lock = threading.Lock()

    def track_records(
        self,
        audit_id: str,
        user_id: str,
        context: Optional[str],
        table_name: str,
        df: pd.DataFrame
    ) -> None:
        """
        Track which records were accessed from a table.

        Args:
            audit_id: The query audit ID
            user_id: Who ran the query
            context: Why the query was run
            table_name: Name of the table queried
            df: The query result DataFrame
        """
        if not ENABLE_RECORD_AUDIT:
            return

        if df.empty:
            return

        # Get primary keys for this table
        pk_columns = get_primary_keys(table_name)

        # Extract primary key values from result
        sample_keys = []
        if pk_columns:
            # Check which pk columns are in the result
            available_pks = [pk for pk in pk_columns if pk in df.columns]
            if available_pks:
                # Get sample of primary key values
                sample_df = df[available_pks].head(MAX_SAMPLE_KEYS)
                sample_keys = sample_df.to_dict('records')

        # Create table audit record
        table_audit = TableRecordAudit(
            table_name=table_name,
            primary_keys=pk_columns,
            record_count=len(df),
            columns_accessed=list(df.columns),
            sample_keys=sample_keys
        )

        # Add to pending audit
        with self._lock:
            if audit_id not in self._pending_audits:
                self._pending_audits[audit_id] = RecordAuditEntry(
                    audit_id=audit_id,
                    timestamp=datetime.now().isoformat(),
                    user_id=user_id,
                    context=context,
                    tables={}
                )
            self._pending_audits[audit_id].tables[table_name] = table_audit

    def save_audit(self, audit_id: str, sync: bool = False) -> None:
        """
        Save the record audit to S3.

        Args:
            audit_id: The audit ID to save
            sync: If True, wait for save to complete. If False (default), save async.
        """
        if not ENABLE_RECORD_AUDIT:
            return

        with self._lock:
            if audit_id not in self._pending_audits:
                return
            audit_entry = self._pending_audits.pop(audit_id)

        if sync:
            self._save_to_s3(audit_entry)
        else:
            _executor.submit(self._save_to_s3, audit_entry)

    def _save_to_s3(self, audit_entry: RecordAuditEntry) -> None:
        """Save audit entry to S3."""
        try:
            s3 = _get_s3_client()
            date_str = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"{RECORD_AUDIT_PREFIX}/{date_str}/{audit_entry.audit_id}.json"

            # Convert to JSON-serializable format
            body = json.dumps(audit_entry.to_dict(), indent=2, default=str)

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=body,
                ContentType='application/json'
            )
        except Exception as e:
            # Don't fail the query if audit save fails
            print(f"Warning: Could not save record audit {audit_entry.audit_id}: {e}")

    def get_audit(self, audit_id: str, date_hint: Optional[str] = None) -> Optional[Dict]:
        """
        Retrieve a record audit from S3.

        Args:
            audit_id: The audit ID to retrieve
            date_hint: Optional date string (YYYY/MM/DD) to narrow search

        Returns:
            Audit record dict or None if not found
        """
        s3 = _get_s3_client()

        # Try specific date if provided, otherwise try recent dates
        dates_to_try = []
        if date_hint:
            dates_to_try.append(date_hint)
        else:
            # Try today and yesterday
            from datetime import timedelta
            today = datetime.now()
            dates_to_try.append(today.strftime("%Y/%m/%d"))
            dates_to_try.append((today - timedelta(days=1)).strftime("%Y/%m/%d"))

        for date_str in dates_to_try:
            try:
                s3_key = f"{RECORD_AUDIT_PREFIX}/{date_str}/{audit_id}.json"
                response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
                return json.loads(response['Body'].read())
            except s3.exceptions.NoSuchKey:
                continue
            except Exception:
                continue

        return None

    def get_records_for_audit(self, audit_id: str) -> Dict[str, List[Dict]]:
        """
        Get all records (primary keys) accessed in an audit.

        Args:
            audit_id: The audit ID

        Returns:
            Dict mapping table names to lists of primary key dicts
        """
        audit = self.get_audit(audit_id)
        if not audit:
            return {}

        result = {}
        for table_name, table_audit in audit.get('tables', {}).items():
            result[table_name] = table_audit.get('sample_keys', [])

        return result


# Singleton instance
_record_audit_manager = None
_manager_lock = threading.Lock()


def get_record_audit_manager() -> RecordAuditManager:
    """Get or create singleton RecordAuditManager."""
    global _record_audit_manager
    if _record_audit_manager is None:
        with _manager_lock:
            if _record_audit_manager is None:
                _record_audit_manager = RecordAuditManager()
    return _record_audit_manager
