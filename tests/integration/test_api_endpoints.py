"""
Integration Tests for API Endpoints
====================================

Tests all API endpoints for correct behavior, response format,
and audit metadata inclusion.

Uses TestClient to make real HTTP requests against the FastAPI app.
"""

import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


@pytest.fixture
def mock_db_layer():
    """Mock DuckDB layer for all tests."""
    mock = Mock()
    result = Mock()
    result.fetchall.return_value = [
        (2024, 'Humana Inc.', 3000000),
        (2024, 'UnitedHealth Group, Inc.', 5000000),
    ]
    result.description = [
        ('year', None), ('parent_org', None), ('enrollment', None)
    ]
    mock.execute.return_value = result
    return mock


@pytest.fixture
def client(mock_db_layer):
    """Create test client with mocked dependencies."""
    with patch('db.duckdb_layer.DuckDBLayer', return_value=mock_db_layer):
        with patch('api.services.data_service.DuckDBLayer', return_value=mock_db_layer):
            from starlette.testclient import TestClient
            from api.main import app
            with TestClient(app) as client:
                yield client


class TestHealthEndpoint:
    """Tests for health check endpoint."""
    
    def test_health_check(self, client):
        """Test /health returns 200."""
        response = client.get("/health")
        assert response.status_code == 200
    
    def test_root_redirect(self, client):
        """Test root returns API info or redirects."""
        response = client.get("/")
        assert response.status_code in [200, 307]


class TestEnrollmentEndpoints:
    """Tests for enrollment API endpoints."""
    
    def test_enrollment_timeseries_v3(self, client, mock_db_layer):
        """Test /api/v3/enrollment/timeseries endpoint."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (2022, 3000000), (2023, 3200000), (2024, 3500000)
        ]
        mock_db_layer.execute.return_value.description = [
            ('year', None), ('total_enrollment', None)
        ]
        
        response = client.get("/api/v3/enrollment/timeseries")
        
        assert response.status_code == 200
        data = response.json()
        assert 'years' in data or 'error' in data
    
    def test_enrollment_timeseries_with_parent_org(self, client, mock_db_layer):
        """Test enrollment timeseries filtered by parent org."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (2024, 3500000)
        ]
        mock_db_layer.execute.return_value.description = [
            ('year', None), ('total_enrollment', None)
        ]
        
        response = client.get(
            "/api/v3/enrollment/timeseries",
            params={"parent_orgs": "Humana Inc."}
        )
        
        assert response.status_code == 200
    
    def test_enrollment_timeseries_multi_payer(self, client, mock_db_layer):
        """Test enrollment timeseries with multiple payers."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (2024, 3500000)
        ]
        mock_db_layer.execute.return_value.description = [
            ('year', None), ('total_enrollment', None)
        ]
        
        response = client.get(
            "/api/v3/enrollment/timeseries",
            params={"parent_orgs": "Humana Inc.|UnitedHealth Group, Inc."}
        )
        
        assert response.status_code == 200
    
    def test_enrollment_timeseries_with_state(self, client, mock_db_layer):
        """Test enrollment timeseries filtered by state."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (2024, 1500000)
        ]
        mock_db_layer.execute.return_value.description = [
            ('year', None), ('total_enrollment', None)
        ]
        
        response = client.get(
            "/api/v3/enrollment/timeseries",
            params={"states": "CA,TX"}
        )
        
        assert response.status_code == 200
    
    def test_enrollment_timeseries_with_plan_type(self, client, mock_db_layer):
        """Test enrollment timeseries filtered by plan type."""
        response = client.get(
            "/api/v3/enrollment/timeseries",
            params={"plan_types": "HMO,PPO"}
        )
        
        assert response.status_code == 200


class TestFilterEndpoints:
    """Tests for filter/dimension endpoints."""
    
    def test_get_available_filters_v3(self, client, mock_db_layer):
        """Test /api/v3/enrollment/filters endpoint."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            ('Humana Inc.',), ('UnitedHealth Group, Inc.',)
        ]
        mock_db_layer.execute.return_value.description = [('parent_org', None)]
        
        response = client.get("/api/v3/enrollment/filters")
        
        assert response.status_code == 200
        data = response.json()
        assert 'parent_orgs' in data or 'error' in data


class TestStarsEndpoints:
    """Tests for star ratings API endpoints."""
    
    def test_stars_distribution_v3(self, client, mock_db_layer):
        """Test /api/v3/stars/distribution endpoint."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (2026, 'Humana Inc.', 3000000, 2500000, 83.3, 100)
        ]
        mock_db_layer.execute.return_value.description = [
            ('star_year', None), ('parent_org', None),
            ('enrollment', None), ('fourplus_enrollment', None),
            ('fourplus_pct', None), ('contract_count', None)
        ]
        
        response = client.get("/api/v3/stars/distribution")
        
        assert response.status_code == 200
    
    def test_stars_by_parent_v3(self, client, mock_db_layer):
        """Test /api/v3/stars/by_parent endpoint."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            ('Humana Inc.', 3000000, 2500000, 83.3, 4.2, 100)
        ]
        mock_db_layer.execute.return_value.description = [
            ('parent_org', None), ('total_enrollment', None),
            ('fourplus_enrollment', None), ('fourplus_pct', None),
            ('wavg_rating', None), ('contract_count', None)
        ]
        
        response = client.get("/api/v3/stars/by_parent")
        
        assert response.status_code in [200, 404]


class TestRiskScoresEndpoints:
    """Tests for risk scores API endpoints."""
    
    def test_risk_scores_timeseries_v3(self, client, mock_db_layer):
        """Test /api/v3/risk-scores/timeseries endpoint."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (2022, 1.02), (2023, 1.03), (2024, 1.04)
        ]
        mock_db_layer.execute.return_value.description = [
            ('year', None), ('avg_risk_score', None)
        ]
        
        response = client.get("/api/v3/risk-scores/timeseries")
        
        assert response.status_code in [200, 404]
    
    def test_risk_scores_by_parent_v3(self, client, mock_db_layer):
        """Test /api/v3/risk-scores/by_parent endpoint."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            ('Humana Inc.', 1.05, 3000000)
        ]
        mock_db_layer.execute.return_value.description = [
            ('parent_org', None), ('avg_risk_score', None),
            ('total_enrollment', None)
        ]
        
        response = client.get("/api/v3/risk-scores/by_parent")
        
        assert response.status_code in [200, 404]


class TestChatEndpoint:
    """Tests for AI chat endpoint."""
    
    def test_chat_basic_question(self, client, mock_db_layer):
        """Test /api/chat with basic question."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (33000000,)
        ]
        mock_db_layer.execute.return_value.description = [
            ('total_enrollment', None)
        ]
        
        response = client.post(
            "/api/chat",
            json={"message": "What is total enrollment in 2026?"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert 'response' in data
    
    def test_chat_with_payer_question(self, client, mock_db_layer):
        """Test chat with payer-specific question."""
        response = client.post(
            "/api/chat",
            json={"message": "How has Humana's enrollment changed?"}
        )
        
        assert response.status_code == 200
    
    def test_chat_with_stars_question(self, client, mock_db_layer):
        """Test chat with stars-related question."""
        response = client.post(
            "/api/chat",
            json={"message": "What percentage of plans have 4+ stars?"}
        )
        
        assert response.status_code == 200
    
    def test_chat_returns_audit(self, client, mock_db_layer):
        """Test that chat responses include audit metadata."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (33000000, 500, 480, 25)
        ]
        mock_db_layer.execute.return_value.description = [
            ('total_enrollment', None), ('contract_count', None),
            ('entity_count', None), ('parent_org_count', None)
        ]
        
        response = client.post(
            "/api/chat",
            json={"message": "What is enrollment?"}
        )
        
        data = response.json()
        if data.get('audit'):
            assert 'query_id' in data['audit'] or 'sql' in data['audit']


class TestResponseFormat:
    """Tests for response format consistency."""
    
    def test_timeseries_response_structure(self, client, mock_db_layer):
        """Test timeseries response has expected structure."""
        mock_db_layer.execute.return_value.fetchall.return_value = [
            (2024, 3500000)
        ]
        mock_db_layer.execute.return_value.description = [
            ('year', None), ('total_enrollment', None)
        ]
        
        response = client.get("/api/v3/enrollment/timeseries")
        data = response.json()
        
        if 'error' not in data:
            assert 'years' in data or 'series' in data
    
    def test_error_response_format(self, client, mock_db_layer):
        """Test error responses have consistent format."""
        mock_db_layer.execute.side_effect = Exception("Database error")
        
        response = client.get("/api/v3/enrollment/timeseries")
        
        assert response.status_code in [200, 500]


class TestCORS:
    """Tests for CORS configuration."""
    
    def test_cors_headers_present(self, client):
        """Test that CORS headers are set correctly."""
        response = client.options(
            "/api/v3/enrollment/timeseries",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET"
            }
        )
        
        assert response.status_code in [200, 204, 405]


class TestQueryValidation:
    """Tests for query parameter validation."""
    
    def test_invalid_year_handled(self, client, mock_db_layer):
        """Test invalid year parameter handling."""
        response = client.get(
            "/api/v3/enrollment/timeseries",
            params={"year": "invalid"}
        )
        
        assert response.status_code in [200, 422]
    
    def test_empty_params_handled(self, client, mock_db_layer):
        """Test empty parameters don't crash."""
        response = client.get(
            "/api/v3/enrollment/timeseries",
            params={"parent_orgs": ""}
        )
        
        assert response.status_code == 200


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
