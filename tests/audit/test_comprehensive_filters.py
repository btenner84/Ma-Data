#!/usr/bin/env python3
"""
Comprehensive Filter & Data Integrity Test Suite

Tests all filter combinations across:
- Enrollment (National & Geographic)
- Stars Ratings
- Risk Scores

Verifies:
1. Filter sums equal totals (MECE - Mutually Exclusive, Collectively Exhaustive)
2. Combined filters work correctly
3. Data ranges are reasonable
4. All dimension linkages exist
"""

import requests
from typing import Dict, Any, List, Optional

# Configuration
API_BASE = "https://ma-data-production.up.railway.app"
TEST_YEAR = 2024

class TestDataIntegrity:
    """Test data integrity and filter mathematics."""
    
    def get_enrollment(self, filters: Optional[Dict] = None, source: str = "national") -> float:
        """Helper to get enrollment with filters."""
        params = {'start_year': TEST_YEAR, 'end_year': TEST_YEAR, 'source': source}
        if filters:
            params.update(filters)
        r = requests.get(f"{API_BASE}/api/v5/enrollment/timeseries", params=params, timeout=30)
        data = r.json()
        return data.get('enrollment', [0])[-1] if data.get('enrollment') else 0
    
    def test_total_enrollment_reasonable(self):
        """Total enrollment should be between 50M and 70M for 2024."""
        total = self.get_enrollment()
        assert 50_000_000 < total < 70_000_000, f"Total enrollment {total:,.0f} out of expected range"
        print(f"✅ Total enrollment: {total:,.0f}")
    
    def test_snp_types_sum_to_total(self):
        """SNP types (D-SNP + C-SNP + I-SNP + Non-SNP) should sum to total."""
        total = self.get_enrollment()
        snp_sum = 0
        for snp in ['D-SNP', 'C-SNP', 'I-SNP', 'Non-SNP']:
            val = self.get_enrollment({'snp_types': snp})
            snp_sum += val
            print(f"  {snp}: {val:,.0f}")
        
        diff_pct = abs(snp_sum - total) / total * 100
        assert diff_pct < 1, f"SNP sum {snp_sum:,.0f} differs from total {total:,.0f} by {diff_pct:.1f}%"
        print(f"✅ SNP types sum: {snp_sum:,.0f} (total: {total:,.0f}, diff: {diff_pct:.2f}%)")
    
    def test_group_types_sum_to_total(self):
        """Group types (Individual + Group) should sum close to total."""
        total = self.get_enrollment()
        grp_sum = 0
        for grp in ['Individual', 'Group']:
            val = self.get_enrollment({'group_types': grp})
            grp_sum += val
            print(f"  {grp}: {val:,.0f}")
        
        diff_pct = abs(grp_sum - total) / total * 100
        assert diff_pct < 2, f"Group sum {grp_sum:,.0f} differs from total {total:,.0f} by {diff_pct:.1f}%"
        print(f"✅ Group types sum: {grp_sum:,.0f} (total: {total:,.0f}, diff: {diff_pct:.2f}%)")
    
    def test_product_types_sum_to_total(self):
        """Product types (MAPD + PDP) should sum to total."""
        total = self.get_enrollment()
        prod_sum = 0
        for prod in ['MAPD', 'PDP']:
            val = self.get_enrollment({'product_types': prod})
            prod_sum += val
            print(f"  {prod}: {val:,.0f}")
        
        diff_pct = abs(prod_sum - total) / total * 100
        assert diff_pct < 1, f"Product sum {prod_sum:,.0f} differs from total {total:,.0f} by {diff_pct:.1f}%"
        print(f"✅ Product types sum: {prod_sum:,.0f} (total: {total:,.0f}, diff: {diff_pct:.2f}%)")
    
    def test_dsnp_enrollment_reasonable(self):
        """D-SNP enrollment should be between 5M and 8M for 2024."""
        dsnp = self.get_enrollment({'snp_types': 'D-SNP'})
        assert 5_000_000 < dsnp < 8_000_000, f"D-SNP enrollment {dsnp:,.0f} out of expected range"
        print(f"✅ D-SNP enrollment: {dsnp:,.0f}")
    
    def test_dsnp_is_all_individual(self):
        """D-SNP should be 100% Individual (no Group D-SNP)."""
        dsnp_total = self.get_enrollment({'snp_types': 'D-SNP'})
        dsnp_individual = self.get_enrollment({'snp_types': 'D-SNP', 'group_types': 'Individual'})
        
        diff_pct = abs(dsnp_total - dsnp_individual) / dsnp_total * 100 if dsnp_total > 0 else 0
        assert diff_pct < 1, f"D-SNP Individual {dsnp_individual:,.0f} differs from total {dsnp_total:,.0f}"
        print(f"✅ D-SNP is {100 - diff_pct:.1f}% Individual")
    
    def test_dsnp_is_all_mapd(self):
        """D-SNP should be 100% MAPD (no PDP D-SNP)."""
        dsnp_total = self.get_enrollment({'snp_types': 'D-SNP'})
        dsnp_mapd = self.get_enrollment({'snp_types': 'D-SNP', 'product_types': 'MAPD'})
        
        diff_pct = abs(dsnp_total - dsnp_mapd) / dsnp_total * 100 if dsnp_total > 0 else 0
        assert diff_pct < 1, f"D-SNP MAPD {dsnp_mapd:,.0f} differs from total {dsnp_total:,.0f}"
        print(f"✅ D-SNP is {100 - diff_pct:.1f}% MAPD")


class TestCombinedFilters:
    """Test combined filter scenarios."""
    
    def get_enrollment(self, filters: Optional[Dict] = None) -> float:
        params = {'start_year': TEST_YEAR, 'end_year': TEST_YEAR}
        if filters:
            params.update(filters)
        r = requests.get(f"{API_BASE}/api/v5/enrollment/timeseries", params=params, timeout=30)
        data = r.json()
        return data.get('enrollment', [0])[-1] if data.get('enrollment') else 0
    
    def test_mapd_individual(self):
        """MAPD + Individual filter works."""
        val = self.get_enrollment({'product_types': 'MAPD', 'group_types': 'Individual'})
        assert val > 20_000_000, f"MAPD Individual {val:,.0f} seems too low"
        print(f"✅ MAPD + Individual: {val:,.0f}")
    
    def test_nonsnp_mapd_individual(self):
        """Non-SNP + MAPD + Individual filter works."""
        val = self.get_enrollment({
            'snp_types': 'Non-SNP',
            'product_types': 'MAPD',
            'group_types': 'Individual'
        })
        assert val > 15_000_000, f"Non-SNP MAPD Individual {val:,.0f} seems too low"
        print(f"✅ Non-SNP + MAPD + Individual: {val:,.0f}")
    
    def test_filter_reduces_total(self):
        """Any single filter should reduce total (not increase)."""
        total = self.get_enrollment()
        
        filters = [
            ('D-SNP', {'snp_types': 'D-SNP'}),
            ('MAPD', {'product_types': 'MAPD'}),
            ('Individual', {'group_types': 'Individual'}),
        ]
        
        for name, f in filters:
            val = self.get_enrollment(f)
            assert val < total, f"{name} filter {val:,.0f} should be less than total {total:,.0f}"
            print(f"✅ {name}: {val:,.0f} < {total:,.0f}")


class TestParentOrgFilters:
    """Test parent organization (payer) filtering."""
    
    def get_enrollment(self, filters: Optional[Dict] = None) -> float:
        params = {'start_year': TEST_YEAR, 'end_year': TEST_YEAR}
        if filters:
            params.update(filters)
        r = requests.get(f"{API_BASE}/api/v5/enrollment/timeseries", params=params, timeout=30)
        data = r.json()
        return data.get('enrollment', [0])[-1] if data.get('enrollment') else 0
    
    def test_top_payers_exist(self):
        """Top payers should have significant enrollment."""
        payers = [
            ('UnitedHealth Group, Inc.', 10_000_000, 15_000_000),
            ('Humana Inc.', 7_000_000, 12_000_000),
            ('CVS Health Corporation', 8_000_000, 12_000_000),
        ]
        
        for payer, min_expected, max_expected in payers:
            val = self.get_enrollment({'parent_org': payer})
            assert min_expected < val < max_expected, f"{payer} enrollment {val:,.0f} out of expected range"
            print(f"✅ {payer}: {val:,.0f}")
    
    def test_payer_with_snp_filter(self):
        """Payer + SNP filter combination works."""
        val = self.get_enrollment({
            'parent_org': 'UnitedHealth Group, Inc.',
            'snp_types': 'D-SNP'
        })
        assert val > 500_000, f"UHC D-SNP {val:,.0f} seems too low"
        print(f"✅ UHC + D-SNP: {val:,.0f}")


class TestYearRange:
    """Test year range coverage."""
    
    def test_full_year_range(self):
        """API should return data for 2013-2026."""
        r = requests.get(f"{API_BASE}/api/v5/enrollment/timeseries", 
                        params={'start_year': 2013, 'end_year': 2026}, timeout=30)
        data = r.json()
        years = data.get('years', [])
        
        assert len(years) >= 12, f"Only {len(years)} years returned"
        assert min(years) <= 2014, f"Missing early years (min: {min(years)})"
        assert max(years) >= 2025, f"Missing recent years (max: {max(years)})"
        print(f"✅ Year range: {min(years)}-{max(years)} ({len(years)} years)")
    
    def test_enrollment_growth_over_time(self):
        """Enrollment should generally increase over time."""
        r = requests.get(f"{API_BASE}/api/v5/enrollment/timeseries",
                        params={'start_year': 2015, 'end_year': 2024}, timeout=30)
        data = r.json()
        enrollments = data.get('enrollment', [])
        
        assert len(enrollments) >= 8, "Not enough years for growth check"
        assert enrollments[-1] > enrollments[0], "2024 should have higher enrollment than 2015"
        
        growth = (enrollments[-1] - enrollments[0]) / enrollments[0] * 100
        assert growth > 20, f"Only {growth:.0f}% growth from 2015-2024 seems too low"
        print(f"✅ Enrollment growth 2015-2024: {growth:.0f}%")


class TestStarsAPI:
    """Test Stars ratings API."""
    
    def test_stars_endpoint_works(self):
        """Stars timeseries endpoint returns data."""
        r = requests.get(f"{API_BASE}/api/v5/stars/timeseries",
                        params={'start_year': 2020, 'end_year': 2024}, timeout=30)
        assert r.status_code == 200, f"Stars API returned {r.status_code}"
        data = r.json()
        assert 'years' in data or 'error' not in data, f"Stars API error: {data}"
        print(f"✅ Stars API working")
    
    def test_stars_with_snp_filter(self):
        """Stars API accepts SNP filter."""
        r = requests.get(f"{API_BASE}/api/v5/stars/timeseries",
                        params={'start_year': 2020, 'end_year': 2024, 'snp_types': 'D-SNP'}, timeout=30)
        assert r.status_code == 200, f"Stars SNP filter returned {r.status_code}"
        print(f"✅ Stars SNP filter working")


class TestRiskScoresAPI:
    """Test Risk Scores API."""
    
    def test_risk_endpoint_works(self):
        """Risk timeseries endpoint returns data."""
        r = requests.get(f"{API_BASE}/api/v5/risk/timeseries",
                        params={'start_year': 2020, 'end_year': 2024}, timeout=30)
        assert r.status_code == 200, f"Risk API returned {r.status_code}"
        data = r.json()
        assert 'years' in data or 'error' not in data, f"Risk API error: {data}"
        print(f"✅ Risk API working")
    
    def test_risk_with_snp_filter(self):
        """Risk API accepts SNP filter."""
        r = requests.get(f"{API_BASE}/api/v5/risk/timeseries",
                        params={'start_year': 2020, 'end_year': 2024, 'snp_types': 'D-SNP'}, timeout=30)
        assert r.status_code == 200, f"Risk SNP filter returned {r.status_code}"
        print(f"✅ Risk SNP filter working")


class TestFiltersEndpoint:
    """Test the filters metadata endpoint."""
    
    def test_filters_endpoint_returns_options(self):
        """Filters endpoint returns all filter options."""
        r = requests.get(f"{API_BASE}/api/v5/filters", timeout=30)
        assert r.status_code == 200, f"Filters API returned {r.status_code}"
        
        data = r.json()
        
        # Check required fields exist
        required = ['years', 'parent_orgs']
        for field in required:
            assert field in data, f"Missing {field} in filters response"
        
        # Check years range
        years = data.get('years', [])
        assert len(years) > 10, f"Only {len(years)} years in filters"
        
        # Check parent orgs
        parent_orgs = data.get('parent_orgs', [])
        assert len(parent_orgs) > 50, f"Only {len(parent_orgs)} parent orgs"
        
        print(f"✅ Filters endpoint: {len(years)} years, {len(parent_orgs)} payers")


def run_all_tests():
    """Run all tests and summarize results."""
    import sys
    
    test_classes = [
        TestDataIntegrity,
        TestCombinedFilters,
        TestParentOrgFilters,
        TestYearRange,
        TestStarsAPI,
        TestRiskScoresAPI,
        TestFiltersEndpoint,
    ]
    
    print("=" * 70)
    print("COMPREHENSIVE FILTER & DATA INTEGRITY AUDIT")
    print("=" * 70)
    print(f"API: {API_BASE}")
    print(f"Test Year: {TEST_YEAR}")
    print("=" * 70)
    
    total_tests = 0
    passed_tests = 0
    failed_tests = []
    
    for test_class in test_classes:
        print(f"\n{test_class.__name__}")
        print("-" * 50)
        
        instance = test_class()
        methods = [m for m in dir(instance) if m.startswith('test_')]
        
        for method_name in methods:
            total_tests += 1
            try:
                method = getattr(instance, method_name)
                method()
                passed_tests += 1
            except AssertionError as e:
                failed_tests.append((test_class.__name__, method_name, str(e)))
                print(f"❌ {method_name}: {e}")
            except Exception as e:
                failed_tests.append((test_class.__name__, method_name, str(e)))
                print(f"❌ {method_name}: {type(e).__name__}: {e}")
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {len(failed_tests)}")
    
    if failed_tests:
        print("\nFailed tests:")
        for class_name, method_name, error in failed_tests:
            print(f"  - {class_name}.{method_name}: {error[:60]}")
    
    print("\n" + "=" * 70)
    if len(failed_tests) == 0:
        print("ALL TESTS PASSED ✅")
    else:
        print(f"TESTS FAILED: {len(failed_tests)} ❌")
    print("=" * 70)
    
    return len(failed_tests) == 0


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
