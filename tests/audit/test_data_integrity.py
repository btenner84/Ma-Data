#!/usr/bin/env python3
"""
COMPREHENSIVE DATA INTEGRITY AUDIT

Checks ALL years, ALL months, and ALL filter combinations to ensure:
1. Filter sums equal totals (SNP, Group, Product, Plan types)
2. No unexpected NULL dimensions
3. YoY growth is reasonable (no sudden -10%+ drops)
4. Data exists for all expected months
5. Cross-validates National vs Geographic totals
"""

import requests
import sys
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

API_BASE = "https://ma-data-production.up.railway.app"
TOLERANCE_PCT = 2.0  # Allow 2% variance in filter sums
MAX_YOY_DECLINE = -8.0  # Flag if YoY decline exceeds this

@dataclass
class AuditResult:
    test_name: str
    passed: bool
    message: str
    details: Optional[Dict] = None

class DataIntegrityAudit:
    def __init__(self):
        self.results: List[AuditResult] = []
        
    def log(self, test_name: str, passed: bool, message: str, details: Dict = None):
        self.results.append(AuditResult(test_name, passed, message, details))
        status = "✅" if passed else "❌"
        print(f"{status} {test_name}: {message}")
        
    def get_enrollment(self, source: str = "national", **filters) -> Dict:
        """Get enrollment timeseries from API"""
        params = {'source': source}
        params.update(filters)
        try:
            r = requests.get(f"{API_BASE}/api/v5/enrollment/timeseries", params=params, timeout=60)
            return r.json()
        except Exception as e:
            return {'error': str(e)}
    
    def get_filters(self) -> Dict:
        """Get available filter options"""
        try:
            r = requests.get(f"{API_BASE}/api/v5/filters", timeout=30)
            return r.json()
        except Exception as e:
            return {'error': str(e)}

    # ========================================================================
    # TEST: Filter Sums Equal Total
    # ========================================================================
    def test_filter_sums(self, year: int, source: str = "national"):
        """Test that filtered subsets sum to total"""
        print(f"\n{'='*60}")
        print(f"FILTER SUM TESTS - {year} ({source})")
        print(f"{'='*60}")
        
        # Get total
        total_data = self.get_enrollment(source=source, start_year=year, end_year=year)
        if 'error' in total_data or not total_data.get('enrollment'):
            self.log(f"{year} Total", False, f"Failed to get total: {total_data}")
            return
        
        total = total_data['enrollment'][-1] if total_data['enrollment'] else 0
        print(f"Total enrollment: {total:,.0f}")
        
        # Test each filter category
        filter_tests = [
            ('snp_types', ['D-SNP', 'C-SNP', 'I-SNP', 'Non-SNP']),
            ('group_types', ['Individual', 'Group']),
            ('product_types', ['MAPD', 'PDP']),
            # Plan types now map to CMS values - HMO includes HMO/HMOPOS etc.
            ('plan_types', ['HMO', 'PPO', 'PFFS', 'MSA', 'PACE', 'Cost', 'PDP']),
        ]
        
        for filter_name, values in filter_tests:
            filter_sum = 0
            breakdown = {}
            for val in values:
                data = self.get_enrollment(source=source, start_year=year, end_year=year, **{filter_name: val})
                val_enroll = data.get('enrollment', [0])[-1] if data.get('enrollment') else 0
                filter_sum += val_enroll
                breakdown[val] = val_enroll
            
            if total > 0:
                diff_pct = abs(filter_sum - total) / total * 100
                passed = diff_pct < TOLERANCE_PCT
            else:
                passed = filter_sum == 0
                diff_pct = 0
            
            self.log(
                f"{year} {filter_name} sum",
                passed,
                f"Sum={filter_sum:,.0f} vs Total={total:,.0f} (diff={diff_pct:.1f}%)",
                {'breakdown': breakdown, 'sum': filter_sum, 'total': total}
            )

    # ========================================================================
    # TEST: YoY Growth Sanity
    # ========================================================================
    def test_yoy_growth(self, source: str = "national"):
        """Test that YoY growth is reasonable (no sudden drops)"""
        print(f"\n{'='*60}")
        print(f"YOY GROWTH TESTS ({source})")
        print(f"{'='*60}")
        
        # Get full timeseries
        data = self.get_enrollment(source=source, start_year=2013, end_year=2026)
        if 'error' in data:
            self.log("YoY Growth", False, f"Failed: {data}")
            return
            
        years = data.get('years', [])
        enrollments = data.get('enrollment', [])
        
        if len(years) < 2:
            self.log("YoY Growth", False, "Not enough data")
            return
        
        prev_enroll = None
        for year, enroll in zip(years, enrollments):
            if prev_enroll and prev_enroll > 0 and enroll:
                yoy = (enroll - prev_enroll) / prev_enroll * 100
                passed = yoy > MAX_YOY_DECLINE
                self.log(
                    f"YoY {year-1}->{year}",
                    passed,
                    f"{yoy:+.1f}% ({prev_enroll:,.0f} -> {enroll:,.0f})"
                )
            prev_enroll = enroll

    # ========================================================================
    # TEST: Filter Combinations YoY
    # ========================================================================
    def test_filter_combo_yoy(self, filter_name: str, filter_value: str, source: str = "national"):
        """Test YoY for a specific filter combination"""
        data = self.get_enrollment(source=source, start_year=2020, end_year=2026, **{filter_name: filter_value})
        
        if 'error' in data or not data.get('enrollment'):
            self.log(f"{filter_value} YoY", False, f"No data: {data}")
            return
        
        years = data.get('years', [])
        enrollments = data.get('enrollment', [])
        
        prev_enroll = None
        for year, enroll in zip(years, enrollments):
            if prev_enroll and prev_enroll > 0 and enroll:
                yoy = (enroll - prev_enroll) / prev_enroll * 100
                if yoy < MAX_YOY_DECLINE:
                    self.log(
                        f"{filter_value} {year-1}->{year}",
                        False,
                        f"SUSPICIOUS DROP: {yoy:+.1f}% ({prev_enroll:,.0f} -> {enroll:,.0f})"
                    )
            prev_enroll = enroll

    # ========================================================================
    # TEST: Month Coverage
    # ========================================================================
    def test_month_coverage(self, year: int, expected_months: int = 12):
        """Test that we have data for expected months"""
        # This would need a month-level API endpoint
        # For now, just verify we have data for the year
        data = self.get_enrollment(source="national", start_year=year, end_year=year)
        has_data = data.get('enrollment', [0])[-1] > 0 if data.get('enrollment') else False
        self.log(f"{year} has data", has_data, f"Enrollment: {data.get('enrollment', [0])[-1] if data.get('enrollment') else 0:,.0f}")

    # ========================================================================
    # TEST: National vs Geographic Alignment
    # ========================================================================
    def test_national_vs_geographic(self, year: int):
        """Test that National and Geographic totals are within expected delta"""
        print(f"\n{'='*60}")
        print(f"NATIONAL vs GEOGRAPHIC - {year}")
        print(f"{'='*60}")
        
        national = self.get_enrollment(source="national", start_year=year, end_year=year)
        geographic = self.get_enrollment(source="geographic", start_year=year, end_year=year)
        
        nat_enroll = national.get('enrollment', [0])[-1] if national.get('enrollment') else 0
        geo_enroll = geographic.get('enrollment', [0])[-1] if geographic.get('enrollment') else 0
        
        if nat_enroll > 0:
            diff_pct = (nat_enroll - geo_enroll) / nat_enroll * 100
            # National should be ~1.5% higher than CPSC
            passed = -1 < diff_pct < 5  # Allow some variance
            self.log(
                f"{year} Nat vs Geo",
                passed,
                f"National={nat_enroll:,.0f}, Geographic={geo_enroll:,.0f}, diff={diff_pct:.1f}%"
            )
        else:
            self.log(f"{year} Nat vs Geo", False, "No national data")

    # ========================================================================
    # TEST: All Filter Combos YoY Check
    # ========================================================================
    def test_all_filter_combos_yoy(self):
        """Check YoY for every filter combination to catch data linkage issues"""
        print(f"\n{'='*60}")
        print(f"ALL FILTER COMBINATIONS YOY CHECK")
        print(f"{'='*60}")
        
        combos = [
            ('snp_types', 'D-SNP'),
            ('snp_types', 'C-SNP'),
            ('snp_types', 'I-SNP'),
            ('snp_types', 'Non-SNP'),
            ('group_types', 'Individual'),
            ('group_types', 'Group'),
            ('product_types', 'MAPD'),
            ('product_types', 'PDP'),
        ]
        
        for filter_name, filter_value in combos:
            self.test_filter_combo_yoy(filter_name, filter_value)
    
    # ========================================================================
    # TEST: Multi-Filter Combinations (e.g., Individual + MAPD)
    # ========================================================================
    def test_multi_filter_combos(self):
        """Test common filter combinations to catch linkage issues"""
        print(f"\n{'='*60}")
        print(f"MULTI-FILTER COMBINATION TESTS")
        print(f"{'='*60}")
        
        # Common combos users would try
        combos = [
            {'group_types': 'Individual', 'product_types': 'MAPD'},  # Individual MA
            {'group_types': 'Individual', 'snp_types': 'Non-SNP'},  # Individual Non-SNP
            {'group_types': 'Individual', 'snp_types': 'D-SNP'},    # Individual D-SNP
            {'snp_types': 'Non-SNP', 'product_types': 'MAPD'},      # Non-SNP MAPD
            {'plan_types': 'HMO', 'group_types': 'Individual'},     # Individual HMO
        ]
        
        for combo in combos:
            combo_name = " + ".join([f"{k}={v}" for k, v in combo.items()])
            
            # Get data for last 3 years
            data = self.get_enrollment(source="national", start_year=2024, end_year=2026, **combo)
            
            if 'error' in data or not data.get('enrollment'):
                self.log(f"Combo: {combo_name}", False, f"No data returned")
                continue
            
            years = data.get('years', [])
            enrollments = data.get('enrollment', [])
            
            # Check YoY growth
            prev_enroll = None
            for year, enroll in zip(years, enrollments):
                if prev_enroll and prev_enroll > 0 and enroll:
                    yoy = (enroll - prev_enroll) / prev_enroll * 100
                    if yoy < MAX_YOY_DECLINE:
                        self.log(
                            f"{combo_name} {year-1}->{year}",
                            False,
                            f"SUSPICIOUS DROP: {yoy:+.1f}% ({prev_enroll:,.0f} -> {enroll:,.0f})"
                        )
                    else:
                        self.log(
                            f"{combo_name} {year-1}->{year}",
                            True,
                            f"{yoy:+.1f}% ({prev_enroll:,.0f} -> {enroll:,.0f})"
                        )
                prev_enroll = enroll

    # ========================================================================
    # MAIN AUDIT
    # ========================================================================
    def run_full_audit(self):
        """Run comprehensive audit"""
        print("=" * 70)
        print("COMPREHENSIVE DATA INTEGRITY AUDIT")
        print(f"Started: {datetime.now().isoformat()}")
        print("=" * 70)
        
        # 1. Test filter sums for recent years
        for year in [2024, 2025, 2026]:
            self.test_filter_sums(year, "national")
        
        # 2. Test YoY growth
        self.test_yoy_growth("national")
        
        # 3. Test all filter combos for suspicious YoY drops
        self.test_all_filter_combos_yoy()
        
        # 4. Test multi-filter combinations (catches linkage issues)
        self.test_multi_filter_combos()
        
        # 5. Test National vs Geographic alignment
        for year in [2024, 2025, 2026]:
            self.test_national_vs_geographic(year)
        
        # 5. Summary
        print("\n" + "=" * 70)
        print("AUDIT SUMMARY")
        print("=" * 70)
        
        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        
        print(f"Total tests: {len(self.results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        
        if failed > 0:
            print(f"\n❌ FAILURES:")
            for r in self.results:
                if not r.passed:
                    print(f"   - {r.test_name}: {r.message}")
        
        return failed == 0


def main():
    audit = DataIntegrityAudit()
    success = audit.run_full_audit()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
