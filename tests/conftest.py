"""
Pytest Configuration
====================

Shared fixtures and configuration for all test suites.
"""

import pytest
import os
import sys
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))


@pytest.fixture
def mock_duckdb():
    """Mock DuckDB connection for tests that don't need real DB."""
    db = Mock()
    result = Mock()
    result.fetchall.return_value = []
    result.description = []
    db.execute.return_value = result
    return db


@pytest.fixture
def sample_enrollment_data():
    """Sample enrollment data for testing."""
    return [
        {
            'year': 2024,
            'month': 1,
            'contract_id': 'H0001',
            'plan_id': '001',
            'parent_org': 'Humana Inc.',
            'enrollment': 50000,
            'plan_type': 'HMO',
            'snp_type': 'Non-SNP',
            'group_type': 'Individual',
            '_source_file': 's3://bucket/raw/2024/01.zip',
            '_pipeline_run_id': 'run-001',
        },
        {
            'year': 2024,
            'month': 1,
            'contract_id': 'H0002',
            'plan_id': '001',
            'parent_org': 'UnitedHealth Group, Inc.',
            'enrollment': 100000,
            'plan_type': 'PPO',
            'snp_type': 'D-SNP',
            'group_type': 'Individual',
            '_source_file': 's3://bucket/raw/2024/01.zip',
            '_pipeline_run_id': 'run-001',
        },
    ]


@pytest.fixture
def sample_stars_data():
    """Sample stars data for testing."""
    return [
        {
            'year': 2026,
            'contract_id': 'H0001',
            'parent_org': 'Humana Inc.',
            'overall_rating': 4.5,
            'part_c_rating': 4.0,
            'part_d_rating': 4.5,
        },
        {
            'year': 2026,
            'contract_id': 'H0002',
            'parent_org': 'UnitedHealth Group, Inc.',
            'overall_rating': 3.5,
            'part_c_rating': 3.5,
            'part_d_rating': 3.5,
        },
    ]


@pytest.fixture
def sample_audit_metadata():
    """Sample audit metadata for testing."""
    from datetime import datetime
    return {
        'query_id': 'test-123',
        'sql': 'SELECT * FROM enrollment WHERE year = 2024',
        'tables_queried': ['gold_fact_enrollment_national'],
        'filters_applied': {'year': 2024},
        'row_count': 100,
        'source_files': ['s3://bucket/file.parquet'],
        'pipeline_run_id': 'run-001',
        'executed_at': datetime.now().isoformat(),
        'execution_ms': 45.5,
    }


def pytest_configure(config):
    """Configure pytest."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "audit: marks tests as audit trail tests"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on markers."""
    pass
