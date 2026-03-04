"""
Unit Tests for AuditStore
=========================

Tests for persistent audit log storage.
"""

import pytest
import os
import sys
import tempfile
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from api.services.audit_store import AuditStore, AuditRecord, log_audit


class TestAuditRecord:
    """Tests for AuditRecord dataclass."""
    
    def test_create_record(self):
        """Test creating an audit record."""
        record = AuditRecord(
            query_id='test-123',
            sql='SELECT * FROM test',
            tables_queried=['test_table'],
            filters_applied={'year': 2024},
            row_count=100,
            source_files=['file1.parquet'],
            pipeline_run_id='run-001',
            executed_at=datetime.now(),
            execution_ms=45.5
        )
        
        assert record.query_id == 'test-123'
        assert record.row_count == 100
    
    def test_to_dict(self):
        """Test converting record to dictionary."""
        record = AuditRecord(
            query_id='test-123',
            sql='SELECT 1',
            tables_queried=['table1'],
            filters_applied={'key': 'value'},
            row_count=10,
            source_files=[],
            pipeline_run_id=None,
            executed_at=datetime(2024, 1, 15, 10, 0, 0),
            execution_ms=100
        )
        
        d = record.to_dict()
        
        assert d['query_id'] == 'test-123'
        assert d['executed_at'] == '2024-01-15T10:00:00'


class TestAuditStore:
    """Tests for AuditStore class."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database file."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        yield db_path
        os.unlink(db_path)
    
    @pytest.fixture
    def store(self, temp_db):
        """Create an audit store with temp DB."""
        return AuditStore(db_path=temp_db)
    
    def test_store_initialization(self, store):
        """Test that store initializes correctly."""
        assert os.path.exists(store.db_path)
    
    def test_log_record(self, store):
        """Test logging an audit record."""
        record = AuditRecord(
            query_id='log-test-001',
            sql='SELECT * FROM users',
            tables_queried=['users'],
            filters_applied={'active': True},
            row_count=50,
            source_files=['s3://bucket/file.parquet'],
            pipeline_run_id='run-123',
            executed_at=datetime.now(),
            execution_ms=25.5
        )
        
        query_id = store.log(record)
        
        assert query_id == 'log-test-001'
    
    def test_get_record(self, store):
        """Test retrieving a logged record."""
        record = AuditRecord(
            query_id='get-test-001',
            sql='SELECT * FROM orders',
            tables_queried=['orders'],
            filters_applied={'year': 2024},
            row_count=1000,
            source_files=[],
            pipeline_run_id='run-456',
            executed_at=datetime.now(),
            execution_ms=150
        )
        
        store.log(record)
        retrieved = store.get('get-test-001')
        
        assert retrieved is not None
        assert retrieved.query_id == 'get-test-001'
        assert retrieved.sql == 'SELECT * FROM orders'
        assert retrieved.row_count == 1000
    
    def test_get_nonexistent_record(self, store):
        """Test retrieving a non-existent record."""
        retrieved = store.get('does-not-exist')
        
        assert retrieved is None
    
    def test_search_by_date(self, store):
        """Test searching records by date range."""
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        
        record = AuditRecord(
            query_id='search-date-001',
            sql='SELECT 1',
            tables_queried=['test'],
            filters_applied={},
            row_count=1,
            source_files=[],
            pipeline_run_id=None,
            executed_at=now,
            execution_ms=10
        )
        store.log(record)
        
        results = store.search(start_date=yesterday)
        
        assert len(results) >= 1
        assert any(r.query_id == 'search-date-001' for r in results)
    
    def test_search_by_tables(self, store):
        """Test searching records by table name."""
        record = AuditRecord(
            query_id='search-table-001',
            sql='SELECT * FROM gold_fact_enrollment',
            tables_queried=['gold_fact_enrollment'],
            filters_applied={},
            row_count=100,
            source_files=[],
            pipeline_run_id=None,
            executed_at=datetime.now(),
            execution_ms=50
        )
        store.log(record)
        
        results = store.search(tables=['gold_fact_enrollment'])
        
        assert len(results) >= 1
        assert any(r.query_id == 'search-table-001' for r in results)
    
    def test_search_by_session(self, store):
        """Test searching records by session ID."""
        record = AuditRecord(
            query_id='search-session-001',
            sql='SELECT 1',
            tables_queried=['test'],
            filters_applied={},
            row_count=1,
            source_files=[],
            pipeline_run_id=None,
            executed_at=datetime.now(),
            execution_ms=5,
            session_id='session-abc'
        )
        store.log(record)
        
        results = store.get_by_session('session-abc')
        
        assert len(results) >= 1
        assert all(r.session_id == 'session-abc' for r in results)
    
    def test_get_recent(self, store):
        """Test getting recent records."""
        for i in range(5):
            record = AuditRecord(
                query_id=f'recent-{i}',
                sql='SELECT 1',
                tables_queried=['test'],
                filters_applied={},
                row_count=1,
                source_files=[],
                pipeline_run_id=None,
                executed_at=datetime.now(),
                execution_ms=10
            )
            store.log(record)
        
        results = store.get_recent(hours=1, limit=3)
        
        assert len(results) == 3
    
    def test_get_stats(self, store):
        """Test getting usage statistics."""
        for i in range(10):
            record = AuditRecord(
                query_id=f'stats-{i}',
                sql='SELECT * FROM test',
                tables_queried=['test_table'],
                filters_applied={},
                row_count=100 + i * 10,
                source_files=[],
                pipeline_run_id=None,
                executed_at=datetime.now(),
                execution_ms=50 + i * 5
            )
            store.log(record)
        
        stats = store.get_stats()
        
        assert stats['total_queries'] >= 10
        assert stats['avg_execution_ms'] > 0
        assert stats['avg_row_count'] > 0
        assert len(stats['top_tables']) > 0
    
    def test_cleanup(self, store):
        """Test cleanup of old records."""
        old_date = datetime.now() - timedelta(days=100)
        
        old_record = AuditRecord(
            query_id='cleanup-old',
            sql='SELECT 1',
            tables_queried=['test'],
            filters_applied={},
            row_count=1,
            source_files=[],
            pipeline_run_id=None,
            executed_at=old_date,
            execution_ms=10
        )
        store.log(old_record)
        
        new_record = AuditRecord(
            query_id='cleanup-new',
            sql='SELECT 1',
            tables_queried=['test'],
            filters_applied={},
            row_count=1,
            source_files=[],
            pipeline_run_id=None,
            executed_at=datetime.now(),
            execution_ms=10
        )
        store.log(new_record)
        
        deleted = store.cleanup(days_to_keep=30)
        
        assert deleted >= 1
        assert store.get('cleanup-old') is None
        assert store.get('cleanup-new') is not None


class TestLogAuditConvenience:
    """Tests for log_audit convenience function."""
    
    @pytest.fixture(autouse=True)
    def setup_temp_store(self, monkeypatch):
        """Set up temporary store for tests."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        monkeypatch.setenv('AUDIT_DB_PATH', db_path)
        
        import api.services.audit_store as audit_module
        audit_module._audit_store = None
        
        yield db_path
        
        os.unlink(db_path)
    
    def test_log_audit_simple(self):
        """Test simple audit logging."""
        import uuid
        unique_id = f'simple-{uuid.uuid4().hex[:8]}'
        query_id = log_audit(
            query_id=unique_id,
            sql='SELECT * FROM users',
            tables_queried=['users'],
            filters_applied={'active': True},
            row_count=10
        )
        
        assert query_id == unique_id
    
    def test_log_audit_generates_id(self):
        """Test that log_audit generates ID if not provided."""
        query_id = log_audit(
            query_id=None,
            sql='SELECT 1',
            tables_queried=[],
            filters_applied={},
            row_count=1
        )
        
        assert query_id is not None
        assert len(query_id) > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
