"""
Persistent Audit Log Storage
============================

Stores audit metadata for all queries, enabling:
1. Full traceability of any data point
2. Query replay and debugging
3. Usage analytics
4. Compliance reporting

Storage options:
- SQLite (default for development)
- S3 (for production persistence)
- DuckDB (for analytics on audit data)
"""

import os
import json
import sqlite3
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from contextlib import contextmanager
import uuid

AUDIT_DB_PATH = os.environ.get(
    'AUDIT_DB_PATH',
    os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'audit.db')
)


@dataclass
class AuditRecord:
    """A single audit log entry."""
    query_id: str
    sql: str
    tables_queried: List[str]
    filters_applied: Dict[str, Any]
    row_count: int
    source_files: List[str]
    pipeline_run_id: Optional[str]
    executed_at: datetime
    execution_ms: float
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    endpoint: Optional[str] = None
    client_ip: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'query_id': self.query_id,
            'sql': self.sql,
            'tables_queried': self.tables_queried,
            'filters_applied': self.filters_applied,
            'row_count': self.row_count,
            'source_files': self.source_files,
            'pipeline_run_id': self.pipeline_run_id,
            'executed_at': self.executed_at.isoformat() if self.executed_at else None,
            'execution_ms': self.execution_ms,
            'user_id': self.user_id,
            'session_id': self.session_id,
            'endpoint': self.endpoint,
            'client_ip': self.client_ip,
        }


class AuditStore:
    """
    Persistent storage for audit records.
    
    Uses SQLite for local development, can be extended
    to use S3 or other storage for production.
    """
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or AUDIT_DB_PATH
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize the SQLite database schema."""
        with self._get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_id TEXT UNIQUE NOT NULL,
                    sql TEXT NOT NULL,
                    tables_queried TEXT NOT NULL,
                    filters_applied TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    source_files TEXT,
                    pipeline_run_id TEXT,
                    executed_at TIMESTAMP NOT NULL,
                    execution_ms REAL,
                    user_id TEXT,
                    session_id TEXT,
                    endpoint TEXT,
                    client_ip TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_audit_executed_at 
                ON audit_log(executed_at)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_audit_tables 
                ON audit_log(tables_queried)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_audit_user 
                ON audit_log(user_id)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_audit_session 
                ON audit_log(session_id)
            ''')
            
            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def log(self, record: AuditRecord) -> str:
        """
        Store an audit record.
        
        Returns the query_id.
        """
        with self._get_connection() as conn:
            conn.execute('''
                INSERT INTO audit_log (
                    query_id, sql, tables_queried, filters_applied,
                    row_count, source_files, pipeline_run_id,
                    executed_at, execution_ms, user_id, session_id,
                    endpoint, client_ip
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record.query_id,
                record.sql,
                json.dumps(record.tables_queried),
                json.dumps(record.filters_applied),
                record.row_count,
                json.dumps(record.source_files) if record.source_files else None,
                record.pipeline_run_id,
                record.executed_at.isoformat() if record.executed_at else None,
                record.execution_ms,
                record.user_id,
                record.session_id,
                record.endpoint,
                record.client_ip,
            ))
            conn.commit()
        
        return record.query_id
    
    def get(self, query_id: str) -> Optional[AuditRecord]:
        """Retrieve a specific audit record by query ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                'SELECT * FROM audit_log WHERE query_id = ?',
                (query_id,)
            ).fetchone()
        
        if not row:
            return None
        
        return self._row_to_record(row)
    
    def search(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        tables: Optional[List[str]] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        min_rows: Optional[int] = None,
        max_rows: Optional[int] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[AuditRecord]:
        """
        Search audit records with filters.
        
        Returns matching records in reverse chronological order.
        """
        conditions = []
        params = []
        
        if start_date:
            conditions.append('executed_at >= ?')
            params.append(start_date.isoformat())
        
        if end_date:
            conditions.append('executed_at <= ?')
            params.append(end_date.isoformat())
        
        if tables:
            for table in tables:
                conditions.append('tables_queried LIKE ?')
                params.append(f'%{table}%')
        
        if user_id:
            conditions.append('user_id = ?')
            params.append(user_id)
        
        if session_id:
            conditions.append('session_id = ?')
            params.append(session_id)
        
        if endpoint:
            conditions.append('endpoint = ?')
            params.append(endpoint)
        
        if min_rows is not None:
            conditions.append('row_count >= ?')
            params.append(min_rows)
        
        if max_rows is not None:
            conditions.append('row_count <= ?')
            params.append(max_rows)
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        
        query = f'''
            SELECT * FROM audit_log 
            WHERE {where_clause}
            ORDER BY executed_at DESC
            LIMIT ? OFFSET ?
        '''
        params.extend([limit, offset])
        
        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
        
        return [self._row_to_record(row) for row in rows]
    
    def get_recent(self, hours: int = 24, limit: int = 100) -> List[AuditRecord]:
        """Get audit records from the last N hours."""
        start_date = datetime.now() - timedelta(hours=hours)
        return self.search(start_date=start_date, limit=limit)
    
    def get_by_session(self, session_id: str) -> List[AuditRecord]:
        """Get all audit records for a session."""
        return self.search(session_id=session_id, limit=1000)
    
    def get_stats(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get usage statistics for audit records."""
        conditions = []
        params = []
        
        if start_date:
            conditions.append('executed_at >= ?')
            params.append(start_date.isoformat())
        
        if end_date:
            conditions.append('executed_at <= ?')
            params.append(end_date.isoformat())
        
        where_clause = ' AND '.join(conditions) if conditions else '1=1'
        
        with self._get_connection() as conn:
            total = conn.execute(
                f'SELECT COUNT(*) FROM audit_log WHERE {where_clause}',
                params
            ).fetchone()[0]
            
            avg_exec = conn.execute(
                f'SELECT AVG(execution_ms) FROM audit_log WHERE {where_clause}',
                params
            ).fetchone()[0]
            
            avg_rows = conn.execute(
                f'SELECT AVG(row_count) FROM audit_log WHERE {where_clause}',
                params
            ).fetchone()[0]
            
            tables = conn.execute(f'''
                SELECT tables_queried, COUNT(*) as count 
                FROM audit_log 
                WHERE {where_clause}
                GROUP BY tables_queried 
                ORDER BY count DESC 
                LIMIT 10
            ''', params).fetchall()
        
        return {
            'total_queries': total,
            'avg_execution_ms': round(avg_exec or 0, 2),
            'avg_row_count': round(avg_rows or 0, 2),
            'top_tables': [
                {'tables': json.loads(t['tables_queried']), 'count': t['count']}
                for t in tables
            ]
        }
    
    def cleanup(self, days_to_keep: int = 90):
        """Remove audit records older than specified days."""
        cutoff = datetime.now() - timedelta(days=days_to_keep)
        
        with self._get_connection() as conn:
            result = conn.execute(
                'DELETE FROM audit_log WHERE executed_at < ?',
                (cutoff.isoformat(),)
            )
            deleted = result.rowcount
            conn.commit()
        
        return deleted
    
    def _row_to_record(self, row: sqlite3.Row) -> AuditRecord:
        """Convert a database row to an AuditRecord."""
        return AuditRecord(
            query_id=row['query_id'],
            sql=row['sql'],
            tables_queried=json.loads(row['tables_queried']),
            filters_applied=json.loads(row['filters_applied']),
            row_count=row['row_count'],
            source_files=json.loads(row['source_files']) if row['source_files'] else [],
            pipeline_run_id=row['pipeline_run_id'],
            executed_at=datetime.fromisoformat(row['executed_at']) if row['executed_at'] else None,
            execution_ms=row['execution_ms'] or 0,
            user_id=row['user_id'],
            session_id=row['session_id'],
            endpoint=row['endpoint'],
            client_ip=row['client_ip'],
        )


_audit_store: Optional[AuditStore] = None


def get_audit_store() -> AuditStore:
    """Get the global audit store instance."""
    global _audit_store
    if _audit_store is None:
        _audit_store = AuditStore()
    return _audit_store


def log_audit(
    query_id: str,
    sql: str,
    tables_queried: List[str],
    filters_applied: Dict[str, Any],
    row_count: int,
    source_files: List[str] = None,
    pipeline_run_id: str = None,
    executed_at: datetime = None,
    execution_ms: float = 0,
    user_id: str = None,
    session_id: str = None,
    endpoint: str = None,
    client_ip: str = None,
) -> str:
    """Convenience function to log an audit record."""
    record = AuditRecord(
        query_id=query_id or str(uuid.uuid4())[:8],
        sql=sql,
        tables_queried=tables_queried or [],
        filters_applied=filters_applied or {},
        row_count=row_count,
        source_files=source_files or [],
        pipeline_run_id=pipeline_run_id,
        executed_at=executed_at or datetime.now(),
        execution_ms=execution_ms,
        user_id=user_id,
        session_id=session_id,
        endpoint=endpoint,
        client_ip=client_ip,
    )
    
    return get_audit_store().log(record)
