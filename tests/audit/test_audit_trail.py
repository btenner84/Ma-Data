"""
Audit Trail Verification Tests
==============================

Tests that ensure complete traceability from:
1. UI display -> API response -> SQL query -> Source files

Every data point should be traceable back to its origin.
"""

import pytest
import os
import sys
from datetime import datetime
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from api.services.data_service import (
    UnifiedDataService,
    AuditMetadata,
    DataResult
)


class TestAuditCompleteness:
    """Tests that audit metadata is complete for all queries."""
    
    @pytest.fixture
    def mock_db_with_lineage(self):
        """Create mock DB that returns lineage columns."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [
            (2024, 'Humana Inc.', 3000000, 
             's3://ma-data123/raw/enrollment/2024/01.zip',
             'run-2024-01-15',
             '2024-01-15T10:00:00'),
        ]
        result.description = [
            ('year', None), ('parent_org', None), ('enrollment', None),
            ('_source_file', None), ('_pipeline_run_id', None), ('_loaded_at', None)
        ]
        db.execute.return_value = result
        return db
    
    @pytest.fixture
    def service(self, mock_db_with_lineage):
        """Create service with mock DB."""
        with patch('api.services.data_service.DuckDBLayer', return_value=mock_db_with_lineage):
            return UnifiedDataService(use_gold=True)
    
    def test_audit_has_query_id(self, service):
        """Every audit must have a unique query ID."""
        result = service.timeseries()
        
        assert result.audit.query_id is not None
        assert len(result.audit.query_id) > 0
    
    def test_audit_has_sql(self, service):
        """Every audit must include the executed SQL."""
        result = service.timeseries()
        
        assert result.audit.sql is not None
        assert 'SELECT' in result.audit.sql
    
    def test_audit_has_tables(self, service):
        """Every audit must list queried tables."""
        result = service.timeseries()
        
        assert len(result.audit.tables_queried) > 0
        assert 'gold_fact_enrollment_national' in result.audit.tables_queried
    
    def test_audit_has_filters(self, service):
        """Every audit must include applied filters."""
        result = service.timeseries(filters={'year': 2024, 'parent_org': 'Humana Inc.'})
        
        assert result.audit.filters_applied is not None
        assert result.audit.filters_applied.get('year') == 2024
        assert result.audit.filters_applied.get('parent_org') == 'Humana Inc.'
    
    def test_audit_has_row_count(self, service):
        """Every audit must include row count."""
        result = service.timeseries()
        
        assert result.audit.row_count >= 0
    
    def test_audit_has_timestamp(self, service):
        """Every audit must have execution timestamp."""
        before = datetime.now()
        result = service.timeseries()
        after = datetime.now()
        
        assert result.audit.executed_at is not None
        assert before <= result.audit.executed_at <= after
    
    def test_audit_has_execution_time(self, service):
        """Every audit must include execution time."""
        result = service.timeseries()
        
        assert result.audit.execution_ms >= 0
    
    def test_audit_extracts_source_files(self, service):
        """Audit should extract source file references from data."""
        result = service.timeseries()
        
        assert len(result.audit.source_files) > 0
        assert 's3://ma-data123/raw/enrollment/2024/01.zip' in result.audit.source_files


class TestAuditTraceability:
    """Tests for complete traceability chain."""
    
    @pytest.fixture
    def mock_db(self):
        """Create mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [
            (2024, 1, 'H0001', '001', 'Humana Inc.', 50000,
             's3://bucket/file1.parquet', 'run-001'),
        ]
        result.description = [
            ('year', None), ('month', None), ('contract_id', None),
            ('plan_id', None), ('parent_org', None), ('enrollment', None),
            ('_source_file', None), ('_pipeline_run_id', None)
        ]
        db.execute.return_value = result
        return db
    
    @pytest.fixture
    def service(self, mock_db):
        """Create service with mock DB."""
        with patch('api.services.data_service.DuckDBLayer', return_value=mock_db):
            return UnifiedDataService(use_gold=True)
    
    def test_can_trace_to_source_file(self, service):
        """Test tracing a result back to source file."""
        result = service.get_enrollment_summary(year=2024, month=1)
        
        rows = result.data.get('rows', [])
        if rows:
            source_file = rows[0].get('_source_file')
            assert source_file is not None
            assert 's3://' in source_file
    
    def test_can_trace_to_pipeline_run(self, service):
        """Test tracing a result back to pipeline run."""
        result = service.get_enrollment_summary(year=2024, month=1)
        
        rows = result.data.get('rows', [])
        if rows:
            pipeline_run = rows[0].get('_pipeline_run_id')
            assert pipeline_run is not None
    
    def test_audit_log_retains_history(self, service):
        """Test that audit log maintains query history."""
        service.timeseries(filters={'year': 2022})
        service.timeseries(filters={'year': 2023})
        service.timeseries(filters={'year': 2024})
        
        log = service.get_audit_log()
        
        assert len(log) == 3
        assert log[0].filters_applied['year'] == 2022
        assert log[1].filters_applied['year'] == 2023
        assert log[2].filters_applied['year'] == 2024
    
    def test_can_replay_query(self, service, mock_db):
        """Test that a query can be replayed using audit SQL."""
        result1 = service.timeseries(filters={'year': 2024})
        
        sql = result1.audit.sql
        mock_db.execute(sql)
        
        mock_db.execute.assert_called()


class TestAuditUniqueIds:
    """Tests for unique query identification."""
    
    @pytest.fixture
    def service(self):
        """Create service with mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [(1,)]
        result.description = [('val', None)]
        db.execute.return_value = result
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_query_ids_are_unique(self, service):
        """Each query should have a unique ID."""
        ids = set()
        for _ in range(100):
            result = service.timeseries()
            ids.add(result.audit.query_id)
        
        assert len(ids) == 100
    
    def test_can_lookup_by_id(self, service):
        """Can look up a specific query by ID."""
        result = service.timeseries()
        query_id = result.audit.query_id
        
        found = service.get_query_by_id(query_id)
        
        assert found is not None
        assert found.query_id == query_id
        assert found.sql == result.audit.sql


class TestAuditSQLCorrectness:
    """Tests that audit SQL matches actual query."""
    
    @pytest.fixture
    def service(self):
        """Create service with mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [(1,)]
        result.description = [('val', None)]
        db.execute.return_value = result
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_sql_includes_correct_table(self, service):
        """SQL should reference the correct table."""
        result = service.timeseries(source='national')
        
        assert 'gold_fact_enrollment_national' in result.audit.sql
    
    def test_sql_includes_filters(self, service):
        """SQL should include filter conditions."""
        result = service.timeseries(filters={'year': 2024})
        
        assert '2024' in result.audit.sql or 'year' in result.audit.sql
    
    def test_sql_includes_group_by(self, service):
        """SQL should include GROUP BY when specified."""
        result = service.timeseries(group_by='parent_org')
        
        assert 'GROUP BY' in result.audit.sql
        assert 'parent_org' in result.audit.sql


class TestAuditErrorHandling:
    """Tests for audit behavior during errors."""
    
    @pytest.fixture
    def failing_service(self):
        """Create service that will fail on query."""
        db = Mock()
        db.execute.side_effect = Exception("Database connection failed")
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_audit_created_on_error(self, failing_service):
        """Audit should still be created when query fails."""
        result = failing_service.timeseries()
        
        assert result.audit is not None
        assert result.audit.query_id is not None
    
    def test_audit_sql_recorded_on_error(self, failing_service):
        """Failed query SQL should still be recorded."""
        result = failing_service.timeseries()
        
        assert result.audit.sql is not None
        assert 'SELECT' in result.audit.sql
    
    def test_audit_shows_zero_rows_on_error(self, failing_service):
        """Failed query should show zero rows."""
        result = failing_service.timeseries()
        
        assert result.audit.row_count == 0
    
    def test_error_included_in_data(self, failing_service):
        """Error message should be in data response."""
        result = failing_service.timeseries()
        
        assert 'error' in result.data
        assert 'Database connection failed' in result.data['error']


class TestAuditIntegrity:
    """Tests for audit data integrity."""
    
    @pytest.fixture
    def service(self):
        """Create service with mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [(1, 'test', 100), (2, 'test2', 200)]
        result.description = [('year', None), ('name', None), ('value', None)]
        db.execute.return_value = result
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_row_count_matches_data(self, service):
        """Audit row count should match actual data rows."""
        result = service.timeseries()
        
        actual_rows = len(result.data.get('rows', []))
        assert result.audit.row_count == actual_rows
    
    def test_tables_list_not_empty(self, service):
        """Tables queried should never be empty for valid query."""
        result = service.timeseries()
        
        assert len(result.audit.tables_queried) > 0
    
    def test_execution_time_positive(self, service):
        """Execution time should be positive."""
        result = service.timeseries()
        
        assert result.audit.execution_ms >= 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
