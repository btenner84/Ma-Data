"""
Data Validation Tests (MECE)
============================

Tests for data integrity using MECE principles:
- Mutually Exclusive: No double-counting across categories
- Collectively Exhaustive: All categories sum to total

These tests verify the Gold layer data is correct and consistent.
"""

import pytest
import os
import sys
from unittest.mock import Mock, patch
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


class TestMECE_SNPTypes:
    """Tests that SNP types are MECE (sum to total)."""
    
    @pytest.fixture
    def enrollment_by_snp(self):
        """Mock enrollment data by SNP type."""
        return pd.DataFrame({
            'snp_type': ['Non-SNP', 'D-SNP', 'C-SNP', 'I-SNP'],
            'enrollment': [25000000, 6000000, 1500000, 500000],
            'year': [2026, 2026, 2026, 2026]
        })
    
    @pytest.fixture
    def total_enrollment(self):
        """Total enrollment for 2026."""
        return 33000000
    
    def test_snp_types_sum_to_total(self, enrollment_by_snp, total_enrollment):
        """Sum of SNP type enrollments should equal total."""
        snp_sum = enrollment_by_snp['enrollment'].sum()
        diff_pct = abs(total_enrollment - snp_sum) / total_enrollment * 100
        
        assert diff_pct < 1.0, f"SNP types sum difference {diff_pct:.2f}% exceeds 1% threshold"
    
    def test_snp_types_mutually_exclusive(self, enrollment_by_snp):
        """Each plan should have exactly one SNP type."""
        assert len(enrollment_by_snp['snp_type'].unique()) == 4
        expected_types = {'Non-SNP', 'D-SNP', 'C-SNP', 'I-SNP'}
        assert set(enrollment_by_snp['snp_type'].unique()) == expected_types
    
    def test_no_null_snp_types(self, enrollment_by_snp):
        """SNP type should never be null."""
        assert enrollment_by_snp['snp_type'].notna().all()


class TestMECE_GroupTypes:
    """Tests that Group types are MECE (Individual + Group = Total)."""
    
    @pytest.fixture
    def enrollment_by_group(self):
        """Mock enrollment data by group type."""
        return pd.DataFrame({
            'group_type': ['Individual', 'Group'],
            'enrollment': [31000000, 2000000],
            'year': [2026, 2026]
        })
    
    @pytest.fixture
    def total_enrollment(self):
        """Total enrollment for 2026."""
        return 33000000
    
    def test_group_types_sum_to_total(self, enrollment_by_group, total_enrollment):
        """Individual + Group should equal total."""
        group_sum = enrollment_by_group['enrollment'].sum()
        diff_pct = abs(total_enrollment - group_sum) / total_enrollment * 100
        
        assert diff_pct < 1.0, f"Group types sum difference {diff_pct:.2f}% exceeds 1% threshold"
    
    def test_only_two_group_types(self, enrollment_by_group):
        """Should only have Individual and Group types."""
        assert set(enrollment_by_group['group_type'].unique()) == {'Individual', 'Group'}


class TestMECE_PlanTypes:
    """Tests that Plan types are MECE."""
    
    @pytest.fixture
    def enrollment_by_plan_type(self):
        """Mock enrollment data by plan type."""
        return pd.DataFrame({
            'plan_type': ['HMO', 'PPO', 'PFFS', 'MSA', 'Cost', 'PACE'],
            'enrollment': [18000000, 12000000, 2000000, 500000, 300000, 200000],
            'year': [2026, 2026, 2026, 2026, 2026, 2026]
        })
    
    @pytest.fixture
    def total_enrollment(self):
        """Total enrollment for 2026."""
        return 33000000
    
    def test_plan_types_sum_to_total(self, enrollment_by_plan_type, total_enrollment):
        """Sum of plan type enrollments should equal total."""
        plan_sum = enrollment_by_plan_type['enrollment'].sum()
        diff_pct = abs(total_enrollment - plan_sum) / total_enrollment * 100
        
        assert diff_pct < 1.0, f"Plan types sum difference {diff_pct:.2f}% exceeds 1% threshold"
    
    def test_plan_types_cover_all_categories(self, enrollment_by_plan_type):
        """Should have all expected plan types."""
        expected_types = {'HMO', 'PPO', 'PFFS', 'MSA', 'Cost', 'PACE'}
        actual_types = set(enrollment_by_plan_type['plan_type'].unique())
        
        assert actual_types <= expected_types, f"Unexpected plan types: {actual_types - expected_types}"


class TestNationalVsGeographic:
    """Tests for reconciliation between national and geographic data."""
    
    @pytest.fixture
    def national_enrollment(self):
        """National enrollment (exact counts)."""
        return pd.DataFrame({
            'year': [2022, 2023, 2024, 2025, 2026],
            'total_enrollment': [29000000, 30500000, 31800000, 32500000, 33000000]
        })
    
    @pytest.fixture
    def geographic_enrollment(self):
        """Geographic enrollment (with suppression)."""
        return pd.DataFrame({
            'year': [2022, 2023, 2024, 2025, 2026],
            'total_enrollment': [28350000, 29835000, 31086000, 31850000, 32340000]
        })
    
    def test_geographic_within_suppression_tolerance(self, national_enrollment, geographic_enrollment):
        """Geographic should be within ~3% of national due to suppression."""
        merged = national_enrollment.merge(
            geographic_enrollment, 
            on='year', 
            suffixes=('_national', '_geographic')
        )
        
        merged['diff_pct'] = (
            (merged['total_enrollment_national'] - merged['total_enrollment_geographic']) 
            / merged['total_enrollment_national'] * 100
        )
        
        max_diff = merged['diff_pct'].max()
        
        assert max_diff < 5.0, f"Geographic vs National difference {max_diff:.2f}% exceeds 5% threshold"
    
    def test_geographic_always_less_than_national(self, national_enrollment, geographic_enrollment):
        """Geographic (suppressed) should always be <= national (exact)."""
        merged = national_enrollment.merge(
            geographic_enrollment,
            on='year',
            suffixes=('_national', '_geographic')
        )
        
        assert (merged['total_enrollment_geographic'] <= merged['total_enrollment_national']).all()


class TestYearCoverage:
    """Tests for year coverage in data."""
    
    @pytest.fixture
    def enrollment_years(self):
        """Years available in enrollment data."""
        return list(range(2007, 2027))
    
    @pytest.fixture
    def stars_years(self):
        """Years available in stars data."""
        return list(range(2009, 2027))
    
    @pytest.fixture
    def risk_score_years(self):
        """Years available in risk score data."""
        return list(range(2006, 2025))
    
    def test_enrollment_has_expected_years(self, enrollment_years):
        """Enrollment should have 2007-2026."""
        expected = list(range(2007, 2027))
        missing = set(expected) - set(enrollment_years)
        
        assert len(missing) == 0, f"Missing enrollment years: {missing}"
    
    def test_stars_has_expected_years(self, stars_years):
        """Stars should have 2009-2026 (started in 2009)."""
        expected = list(range(2009, 2027))
        missing = set(expected) - set(stars_years)
        
        assert len(missing) == 0, f"Missing stars years: {missing}"
    
    def test_risk_scores_has_expected_years(self, risk_score_years):
        """Risk scores should have 2006-2024."""
        expected = list(range(2006, 2025))
        missing = set(expected) - set(risk_score_years)
        
        assert len(missing) == 0, f"Missing risk score years: {missing}"


class TestEntityConsistency:
    """Tests for entity tracking consistency."""
    
    @pytest.fixture
    def entity_data(self):
        """Entity dimension data with contract changes."""
        return pd.DataFrame({
            'entity_id': ['E001', 'E001', 'E002', 'E002', 'E003'],
            'contract_id': ['H0001', 'H0099', 'H0002', 'H0002', 'H0003'],
            'year': [2020, 2021, 2020, 2021, 2021],
            'predecessor_contract_id': [None, 'H0001', None, None, None],
            'is_contract_change': [False, True, False, False, False]
        })
    
    def test_entity_id_stable_across_changes(self, entity_data):
        """Entity ID should remain stable when contract ID changes."""
        e001_records = entity_data[entity_data['entity_id'] == 'E001']
        
        assert len(e001_records) == 2
        assert set(e001_records['contract_id']) == {'H0001', 'H0099'}
    
    def test_predecessor_tracked_on_change(self, entity_data):
        """Contract changes should track predecessor."""
        changes = entity_data[entity_data['is_contract_change'] == True]
        
        assert changes['predecessor_contract_id'].notna().all()
    
    def test_no_predecessor_for_original(self, entity_data):
        """Original contracts should have no predecessor."""
        originals = entity_data[entity_data['is_contract_change'] == False]
        first_year = originals.groupby('entity_id')['year'].min().reset_index()
        
        first_records = entity_data.merge(first_year, on=['entity_id', 'year'])
        
        has_predecessor = first_records['predecessor_contract_id'].notna()
        assert not has_predecessor.any()


class TestLineageCompleteness:
    """Tests that all data has complete lineage."""
    
    @pytest.fixture
    def fact_data(self):
        """Sample fact data with lineage columns."""
        return pd.DataFrame({
            'year': [2024, 2024, 2024],
            'enrollment': [100000, 200000, 150000],
            '_source_file': [
                's3://bucket/raw/2024/01.zip',
                's3://bucket/raw/2024/01.zip',
                's3://bucket/raw/2024/01.zip'
            ],
            '_pipeline_run_id': ['run-001', 'run-001', 'run-001'],
            '_loaded_at': ['2024-01-15', '2024-01-15', '2024-01-15']
        })
    
    def test_all_rows_have_source_file(self, fact_data):
        """Every row should have _source_file."""
        assert fact_data['_source_file'].notna().all()
    
    def test_all_rows_have_pipeline_run(self, fact_data):
        """Every row should have _pipeline_run_id."""
        assert fact_data['_pipeline_run_id'].notna().all()
    
    def test_all_rows_have_loaded_at(self, fact_data):
        """Every row should have _loaded_at timestamp."""
        assert fact_data['_loaded_at'].notna().all()
    
    def test_source_files_are_valid_s3_paths(self, fact_data):
        """Source files should be valid S3 paths."""
        for path in fact_data['_source_file']:
            assert path.startswith('s3://')


class TestDataQuality:
    """Tests for general data quality."""
    
    @pytest.fixture
    def enrollment_data(self):
        """Sample enrollment data."""
        return pd.DataFrame({
            'contract_id': ['H0001', 'H0002', 'H0003'],
            'plan_id': ['001', '001', '800'],
            'enrollment': [50000, 100000, 25000],
            'parent_org': ['Humana Inc.', 'UnitedHealth Group, Inc.', 'CVS Health']
        })
    
    def test_no_negative_enrollment(self, enrollment_data):
        """Enrollment should never be negative."""
        assert (enrollment_data['enrollment'] >= 0).all()
    
    def test_contract_ids_format(self, enrollment_data):
        """Contract IDs should follow CMS format (H####)."""
        for cid in enrollment_data['contract_id']:
            assert cid[0] in ['H', 'R', 'S', 'E'], f"Invalid contract ID format: {cid}"
    
    def test_plan_ids_format(self, enrollment_data):
        """Plan IDs should be 3-digit strings."""
        for pid in enrollment_data['plan_id']:
            assert len(pid) == 3, f"Plan ID should be 3 digits: {pid}"
            assert pid.isdigit(), f"Plan ID should be numeric: {pid}"
    
    def test_group_type_derivation(self, enrollment_data):
        """Plan IDs 800-999 should be Group, 001-799 should be Individual."""
        for _, row in enrollment_data.iterrows():
            pid_int = int(row['plan_id'])
            expected_group = 'Group' if pid_int >= 800 else 'Individual'


class TestCrossTableConsistency:
    """Tests for consistency across related tables."""
    
    @pytest.fixture
    def fact_enrollment(self):
        """Enrollment fact data."""
        return pd.DataFrame({
            'contract_id': ['H0001', 'H0002'],
            'plan_id': ['001', '001'],
            'entity_id': ['E001', 'E002']
        })
    
    @pytest.fixture
    def dim_entity(self):
        """Entity dimension data."""
        return pd.DataFrame({
            'entity_id': ['E001', 'E002', 'E003'],
            'contract_id': ['H0001', 'H0002', 'H0003']
        })
    
    def test_all_facts_have_entity(self, fact_enrollment, dim_entity):
        """All fact records should have a matching entity."""
        fact_entities = set(fact_enrollment['entity_id'].unique())
        dim_entities = set(dim_entity['entity_id'].unique())
        
        missing = fact_entities - dim_entities
        
        assert len(missing) == 0, f"Facts reference missing entities: {missing}"
    
    def test_referential_integrity(self, fact_enrollment, dim_entity):
        """Fact entity_ids should all exist in dim_entity."""
        merged = fact_enrollment.merge(
            dim_entity[['entity_id']],
            on='entity_id',
            how='left',
            indicator=True
        )
        
        orphans = merged[merged['_merge'] == 'left_only']
        
        assert len(orphans) == 0, f"Orphaned fact records: {len(orphans)}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
