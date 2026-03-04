"""
Unit Tests for UnifiedDataService
==================================

Tests all data service methods for correct SQL generation,
filter handling, and audit metadata creation.
"""

import pytest
import os
import sys
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from api.services.data_service import (
    UnifiedDataService,
    AuditMetadata,
    DataResult,
    get_data_service
)


class TestAuditMetadata:
    """Tests for AuditMetadata dataclass."""
    
    def test_audit_metadata_creation(self):
        """Test creating audit metadata with all fields."""
        audit = AuditMetadata(
            query_id="test123",
            sql="SELECT * FROM test",
            tables_queried=["test_table"],
            filters_applied={"year": 2024},
            row_count=100,
            source_files=["file1.parquet"],
            pipeline_run_id="run-001",
            execution_ms=45.5
        )
        
        assert audit.query_id == "test123"
        assert audit.sql == "SELECT * FROM test"
        assert audit.tables_queried == ["test_table"]
        assert audit.filters_applied == {"year": 2024}
        assert audit.row_count == 100
        assert audit.source_files == ["file1.parquet"]
        assert audit.pipeline_run_id == "run-001"
        assert audit.execution_ms == 45.5
        assert isinstance(audit.executed_at, datetime)
    
    def test_audit_metadata_defaults(self):
        """Test audit metadata with minimal fields."""
        audit = AuditMetadata(
            query_id="test",
            sql="SELECT 1",
            tables_queried=[],
            filters_applied={},
            row_count=0
        )
        
        assert audit.source_files == []
        assert audit.pipeline_run_id is None
        assert audit.execution_ms == 0


class TestDataResult:
    """Tests for DataResult dataclass."""
    
    def test_data_result_creation(self):
        """Test creating data result with audit."""
        audit = AuditMetadata(
            query_id="test",
            sql="SELECT 1",
            tables_queried=[],
            filters_applied={},
            row_count=1
        )
        
        result = DataResult(
            data={"rows": [{"a": 1}], "columns": ["a"]},
            audit=audit
        )
        
        assert result.data["rows"] == [{"a": 1}]
        assert result.audit.query_id == "test"


class TestWhereClauseBuilder:
    """Tests for SQL WHERE clause construction."""
    
    @pytest.fixture
    def service(self):
        """Create mock service without DuckDB connection."""
        with patch('api.services.data_service.DuckDBLayer'):
            return UnifiedDataService(use_gold=True)
    
    def test_exact_match_filter(self, service):
        """Test exact value matching."""
        where, params = service._build_where_clause({'year': 2024})
        assert 'year = $year' in where
        assert params['year'] == 2024
    
    def test_list_filter_in_clause(self, service):
        """Test IN clause for list values."""
        where, params = service._build_where_clause({'state': ['CA', 'TX']})
        assert "state IN ('CA', 'TX')" in where
    
    def test_gte_filter(self, service):
        """Test greater-than-or-equal filter."""
        where, params = service._build_where_clause({'year_gte': 2020})
        assert 'year >= $year_gte' in where
        assert params['year_gte'] == 2020
    
    def test_lte_filter(self, service):
        """Test less-than-or-equal filter."""
        where, params = service._build_where_clause({'year_lte': 2025})
        assert 'year <= $year_lte' in where
        assert params['year_lte'] == 2025
    
    def test_like_filter(self, service):
        """Test LIKE pattern matching."""
        where, params = service._build_where_clause({'parent_org_like': 'Humana%'})
        assert 'parent_org LIKE $parent_org_like' in where
        assert params['parent_org_like'] == 'Humana%'
    
    def test_combined_filters(self, service):
        """Test multiple filters combined with AND."""
        where, params = service._build_where_clause({
            'year': 2024,
            'state': ['CA', 'TX'],
            'enrollment_gte': 1000
        })
        
        assert 'year = $year' in where
        assert "state IN ('CA', 'TX')" in where
        assert 'enrollment >= $enrollment_gte' in where
        assert ' AND ' in where
    
    def test_null_filter_ignored(self, service):
        """Test that None values are ignored."""
        where, params = service._build_where_clause({'year': 2024, 'state': None})
        assert 'state' not in where
        assert 'year = $year' in where
    
    def test_empty_filters(self, service):
        """Test empty filter returns 1=1."""
        where, params = service._build_where_clause({})
        assert where == '1=1'
        assert params == {}


class TestTableSelection:
    """Tests for table name selection based on mode."""
    
    @pytest.fixture
    def gold_service(self):
        """Create service in Gold mode."""
        with patch('api.services.data_service.DuckDBLayer'):
            return UnifiedDataService(use_gold=True)
    
    @pytest.fixture
    def legacy_service(self):
        """Create service in legacy mode."""
        with patch('api.services.data_service.DuckDBLayer'):
            return UnifiedDataService(use_gold=False)
    
    def test_gold_enrollment_national(self, gold_service):
        """Gold mode uses gold_fact_enrollment_national."""
        table = gold_service._get_table('enrollment_national')
        assert table == 'gold_fact_enrollment_national'
    
    def test_gold_enrollment_geographic(self, gold_service):
        """Gold mode uses gold_fact_enrollment_geographic."""
        table = gold_service._get_table('enrollment_geographic')
        assert table == 'gold_fact_enrollment_geographic'
    
    def test_gold_stars(self, gold_service):
        """Gold mode uses gold_fact_stars."""
        table = gold_service._get_table('stars')
        assert table == 'gold_fact_stars'
    
    def test_gold_risk_scores(self, gold_service):
        """Gold mode uses gold_fact_risk_scores."""
        table = gold_service._get_table('risk_scores')
        assert table == 'gold_fact_risk_scores'
    
    def test_legacy_enrollment(self, legacy_service):
        """Legacy mode uses fact_enrollment_unified."""
        table = legacy_service._get_table('enrollment_national')
        assert table == 'fact_enrollment_unified'
    
    def test_legacy_stars(self, legacy_service):
        """Legacy mode uses summary_all_years."""
        table = legacy_service._get_table('stars')
        assert table == 'summary_all_years'


class TestQueryExecution:
    """Tests for query execution and result formatting."""
    
    @pytest.fixture
    def mock_db(self):
        """Create mock DuckDB layer."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [
            (2024, 'Humana Inc.', 1000000),
            (2024, 'UnitedHealth Group, Inc.', 2000000),
        ]
        result.description = [
            ('year', None), ('parent_org', None), ('enrollment', None)
        ]
        db.execute.return_value = result
        return db
    
    @pytest.fixture
    def service(self, mock_db):
        """Create service with mock DB."""
        with patch('api.services.data_service.DuckDBLayer', return_value=mock_db):
            return UnifiedDataService(use_gold=True)
    
    def test_execute_returns_data_result(self, service):
        """Test that execute returns DataResult."""
        result = service._execute_query(
            "SELECT * FROM test",
            ["test_table"],
            {"year": 2024}
        )
        
        assert isinstance(result, DataResult)
        assert isinstance(result.audit, AuditMetadata)
    
    def test_execute_populates_audit(self, service):
        """Test that audit metadata is populated."""
        result = service._execute_query(
            "SELECT year, enrollment FROM test WHERE year = 2024",
            ["gold_fact_enrollment_national"],
            {"year": 2024}
        )
        
        assert 'SELECT year, enrollment' in result.audit.sql
        assert 'gold_fact_enrollment_national' in result.audit.tables_queried
        assert result.audit.filters_applied == {"year": 2024}
        assert result.audit.row_count == 2
        assert len(result.audit.query_id) > 0
    
    def test_execute_converts_to_dicts(self, service):
        """Test that rows are converted to dictionaries."""
        result = service._execute_query("SELECT * FROM test", [], {})
        
        rows = result.data['rows']
        assert len(rows) == 2
        assert rows[0] == {'year': 2024, 'parent_org': 'Humana Inc.', 'enrollment': 1000000}
    
    def test_execute_logs_query(self, service):
        """Test that query is added to audit log."""
        service._execute_query("SELECT 1", [], {})
        service._execute_query("SELECT 2", [], {})
        
        log = service.get_audit_log()
        assert len(log) == 2
    
    def test_execute_handles_errors(self, service, mock_db):
        """Test error handling in query execution."""
        mock_db.execute.side_effect = Exception("DB Error")
        
        result = service._execute_query("BAD SQL", [], {})
        
        assert 'error' in result.data
        assert 'DB Error' in result.data['error']
        assert result.audit.row_count == 0


class TestTimeseriesMethod:
    """Tests for timeseries() method."""
    
    @pytest.fixture
    def service(self):
        """Create service with mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [
            (2022, 1000000), (2023, 1100000), (2024, 1200000)
        ]
        result.description = [('year', None), ('enrollment', None)]
        db.execute.return_value = result
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_timeseries_basic(self, service):
        """Test basic timeseries query."""
        result = service.timeseries(metric='enrollment')
        
        assert result.audit.row_count == 3
        assert 'gold_fact_enrollment_national' in result.audit.tables_queried
    
    def test_timeseries_with_group_by(self, service):
        """Test timeseries with grouping."""
        result = service.timeseries(
            metric='enrollment',
            group_by='parent_org'
        )
        
        assert 'GROUP BY year, parent_org' in result.audit.sql
    
    def test_timeseries_geographic_source(self, service):
        """Test timeseries with geographic data source."""
        result = service.timeseries(
            metric='enrollment',
            source='geographic'
        )
        
        assert 'gold_fact_enrollment_geographic' in result.audit.tables_queried


class TestGetDimensions:
    """Tests for get_dimensions() method."""
    
    @pytest.fixture
    def service(self):
        """Create service with mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [
            ('Humana Inc.',), ('UnitedHealth Group, Inc.',), ('CVS Health',)
        ]
        result.description = [('parent_org', None)]
        db.execute.return_value = result
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_get_parent_org_dimensions(self, service):
        """Test getting parent org dimension values."""
        result = service.get_dimensions('parent_org')
        
        assert result.audit.row_count == 3
        assert 'SELECT DISTINCT parent_org' in result.audit.sql
    
    def test_get_state_dimensions(self, service):
        """Test getting state dimension values."""
        result = service.get_dimensions('state')
        
        assert 'SELECT DISTINCT state' in result.audit.sql
    
    def test_dimensions_ordered(self, service):
        """Test that dimensions are ordered."""
        result = service.get_dimensions('parent_org')
        
        assert 'ORDER BY parent_org' in result.audit.sql


class TestEnrollmentSummary:
    """Tests for get_enrollment_summary() method."""
    
    @pytest.fixture
    def service(self):
        """Create service with mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [
            (33000000, 500, 480, 25)
        ]
        result.description = [
            ('total_enrollment', None),
            ('contract_count', None),
            ('entity_count', None),
            ('parent_org_count', None)
        ]
        db.execute.return_value = result
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_enrollment_summary_basic(self, service):
        """Test basic enrollment summary."""
        result = service.get_enrollment_summary(year=2026, month=1)
        
        assert 'SUM(enrollment) as total_enrollment' in result.audit.sql
        assert result.audit.filters_applied['year'] == 2026
        assert result.audit.filters_applied['month'] == 1
    
    def test_enrollment_summary_with_filters(self, service):
        """Test enrollment summary with additional filters."""
        result = service.get_enrollment_summary(
            year=2026,
            month=1,
            filters={'parent_org': 'Humana Inc.'}
        )
        
        assert result.audit.filters_applied['parent_org'] == 'Humana Inc.'


class TestStarsDistribution:
    """Tests for get_stars_distribution() method."""
    
    @pytest.fixture
    def service(self):
        """Create service with mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [
            (3, 50), (4, 200), (5, 100)
        ]
        result.description = [('stars', None), ('contract_count', None)]
        db.execute.return_value = result
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_stars_distribution(self, service):
        """Test stars distribution query."""
        result = service.get_stars_distribution(year=2026)
        
        assert 'CAST(overall_rating AS INT) as stars' in result.audit.sql
        assert 'GROUP BY' in result.audit.sql
        assert 'gold_fact_stars' in result.audit.tables_queried


class TestRiskScores:
    """Tests for get_risk_scores_by_parent() method."""
    
    @pytest.fixture
    def service(self):
        """Create service with mock DB."""
        db = Mock()
        result = Mock()
        result.fetchall.return_value = [
            ('Humana Inc.', 1.05, 3000000),
            ('UnitedHealth Group, Inc.', 0.98, 5000000)
        ]
        result.description = [
            ('parent_org', None),
            ('avg_risk_score', None),
            ('total_enrollment', None)
        ]
        db.execute.return_value = result
        
        with patch('api.services.data_service.DuckDBLayer', return_value=db):
            return UnifiedDataService(use_gold=True)
    
    def test_risk_scores_by_parent(self, service):
        """Test risk scores by parent query."""
        result = service.get_risk_scores_by_parent(year=2024)
        
        assert 'AVG(risk_score)' in result.audit.sql
        assert 'GROUP BY parent_org' in result.audit.sql
        assert 'gold_fact_risk_scores' in result.audit.tables_queried


class TestAuditLog:
    """Tests for audit logging functionality."""
    
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
    
    def test_audit_log_accumulates(self, service):
        """Test that audit log accumulates queries."""
        service.timeseries()
        service.get_enrollment_summary()
        service.get_stars_distribution()
        
        log = service.get_audit_log()
        assert len(log) == 3
    
    def test_audit_log_limit(self, service):
        """Test audit log respects limit parameter."""
        for _ in range(5):
            service.timeseries()
        
        log = service.get_audit_log(limit=3)
        assert len(log) == 3
    
    def test_get_query_by_id(self, service):
        """Test retrieving query by ID."""
        service.timeseries()
        
        log = service.get_audit_log()
        query_id = log[0].query_id
        
        audit = service.get_query_by_id(query_id)
        assert audit is not None
        assert audit.query_id == query_id
    
    def test_get_query_by_id_not_found(self, service):
        """Test retrieving non-existent query ID."""
        audit = service.get_query_by_id("nonexistent")
        assert audit is None


class TestFactoryFunction:
    """Tests for get_data_service() factory function."""
    
    def test_default_uses_gold(self):
        """Test default mode uses Gold layer."""
        with patch('api.services.data_service.DuckDBLayer'):
            with patch.dict(os.environ, {}, clear=True):
                service = get_data_service()
                assert service.use_gold is True
    
    def test_env_var_enables_gold(self):
        """Test USE_GOLD_LAYER=true enables Gold."""
        with patch('api.services.data_service.DuckDBLayer'):
            with patch.dict(os.environ, {'USE_GOLD_LAYER': 'true'}):
                service = get_data_service()
                assert service.use_gold is True
    
    def test_env_var_disables_gold(self):
        """Test USE_GOLD_LAYER=false uses legacy."""
        with patch('api.services.data_service.DuckDBLayer'):
            with patch.dict(os.environ, {'USE_GOLD_LAYER': 'false'}):
                service = get_data_service()
                assert service.use_gold is False


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
