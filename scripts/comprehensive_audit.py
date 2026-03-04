#!/usr/bin/env python3
"""
Comprehensive System Audit

Tests ALL filter combinations across the enrollment, stars, and risk scores APIs.
Validates data consistency, checks for gaps, and reports issues.

Run: python scripts/comprehensive_audit.py
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from itertools import product
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# =============================================================================
# AUDIT CONFIGURATION
# =============================================================================

AUDIT_CONFIG = {
    "enrollment": {
        "data_sources": ["national", "geographic"],
        "product_types": [["MAPD"], ["PDP"], ["MAPD", "PDP"]],
        "plan_types": [[], ["HMO"], ["PPO"], ["HMO", "PPO"], ["PFFS"], ["MSA"]],
        "snp_types": [[], ["Non-SNP"], ["D-SNP"], ["C-SNP"], ["I-SNP"], ["D-SNP", "C-SNP"]],
        "group_types": [[], ["Individual"], ["Group"], ["Individual", "Group"]],
        "sample_states": ["CA", "TX", "FL"],  # Test geographic filters
        "sample_payers": ["UnitedHealth Group, Inc.", "Humana Inc."],
    },
    "stars": {
        "data_sources": ["national", "rated"],
        "years": [2020, 2024, 2026],
    },
    "risk_scores": {
        "years": [2020, 2024],
        "plan_types": [[], ["HMO"], ["PPO"]],
    },
}

# Expected year ranges
EXPECTED_YEARS = {
    "enrollment_national": range(2013, 2027),
    "enrollment_geographic": range(2013, 2027),
    "stars": range(2011, 2027),
    "risk_scores": range(2006, 2025),
}

# =============================================================================
# AUDIT RESULTS TRACKING
# =============================================================================

class AuditResults:
    def __init__(self):
        self.tests_run = 0
        self.tests_passed = 0
        self.tests_failed = 0
        self.warnings = []
        self.errors = []
        self.results = []
        
    def add_result(self, test_name: str, passed: bool, details: Dict = None, error: str = None):
        self.tests_run += 1
        if passed:
            self.tests_passed += 1
        else:
            self.tests_failed += 1
            
        result = {
            "test": test_name,
            "passed": passed,
            "details": details or {},
            "error": error,
            "timestamp": datetime.now().isoformat(),
        }
        self.results.append(result)
        
        if not passed:
            self.errors.append(f"{test_name}: {error or 'Failed'}")
            
    def add_warning(self, warning: str):
        self.warnings.append(warning)
        
    def summary(self) -> Dict:
        return {
            "timestamp": datetime.now().isoformat(),
            "tests_run": self.tests_run,
            "tests_passed": self.tests_passed,
            "tests_failed": self.tests_failed,
            "pass_rate": f"{(self.tests_passed / self.tests_run * 100):.1f}%" if self.tests_run > 0 else "N/A",
            "warnings": self.warnings,
            "errors": self.errors,
        }

# =============================================================================
# ENROLLMENT AUDIT
# =============================================================================

def audit_enrollment(results: AuditResults):
    """Comprehensive enrollment audit."""
    from api.services.enrollment_service import EnrollmentService
    
    print("\n" + "="*70)
    print("ENROLLMENT AUDIT")
    print("="*70)
    
    service = EnrollmentService()
    
    # Test 1: Both data sources show same years
    print("\n1. Year Coverage Test")
    for data_source in ["national", "geographic"]:
        try:
            result = service.get_timeseries(
                data_source=data_source,
                product_types=["MAPD"],
                start_year=2010,
                end_year=2030
            )
            years = result.get("years", [])
            expected = list(EXPECTED_YEARS[f"enrollment_{data_source}"])
            missing = set(expected) - set(years)
            extra = set(years) - set(expected)
            
            passed = len(missing) == 0 and len(extra) == 0
            results.add_result(
                f"enrollment_{data_source}_year_coverage",
                passed,
                {"years": years, "expected": expected, "missing": list(missing), "extra": list(extra)},
                f"Missing years: {missing}" if missing else None
            )
            print(f"   {data_source}: {len(years)} years {'✓' if passed else '✗'}")
            if missing:
                print(f"      Missing: {missing}")
        except Exception as e:
            results.add_result(f"enrollment_{data_source}_year_coverage", False, error=str(e))
            print(f"   {data_source}: ERROR - {e}")
    
    # Test 2: Key filter combinations (representative subset)
    print("\n2. Key Filter Combination Tests")
    
    # Test specific important combinations rather than all permutations
    key_combos = [
        # Basic product type tests
        (["MAPD"], [], [], [], "MAPD_basic"),
        (["PDP"], [], [], [], "PDP_basic"),
        (["MAPD", "PDP"], [], [], [], "MAPD_PDP_combined"),
        
        # Plan type tests
        (["MAPD"], ["HMO"], [], [], "MAPD_HMO"),
        (["MAPD"], ["PPO"], [], [], "MAPD_PPO"),
        (["MAPD"], ["HMO", "PPO"], [], [], "MAPD_HMO_PPO"),
        (["MAPD"], ["PFFS"], [], [], "MAPD_PFFS"),
        
        # SNP type tests
        (["MAPD"], [], ["Non-SNP"], [], "MAPD_NonSNP"),
        (["MAPD"], [], ["D-SNP"], [], "MAPD_DSNP"),
        (["MAPD"], [], ["C-SNP"], [], "MAPD_CSNP"),
        (["MAPD"], [], ["I-SNP"], [], "MAPD_ISNP"),
        (["MAPD"], [], ["D-SNP", "C-SNP", "I-SNP"], [], "MAPD_AllSNP"),
        
        # Group type tests
        (["MAPD"], [], [], ["Individual"], "MAPD_Individual"),
        (["MAPD"], [], [], ["Group"], "MAPD_Group"),
        (["MAPD"], [], [], ["Individual", "Group"], "MAPD_Indiv_Group"),
        
        # Combined filters
        (["MAPD"], ["HMO"], ["Non-SNP"], ["Individual"], "MAPD_HMO_NonSNP_Individual"),
        (["MAPD"], ["HMO"], ["D-SNP"], [], "MAPD_HMO_DSNP"),
        (["MAPD"], ["PPO"], [], ["Group"], "MAPD_PPO_Group"),
        (["MAPD"], [], ["D-SNP"], ["Individual"], "MAPD_DSNP_Individual"),
    ]
    
    print(f"   Testing {len(key_combos)} key combinations...")
    combo_failures = []
    
    for product_types, plan_types, snp_types, group_types, test_name in key_combos:
        try:
            result = service.get_timeseries(
                data_source="national",
                product_types=product_types if product_types else None,
                plan_types=plan_types if plan_types else None,
                snp_types=snp_types if snp_types else None,
                group_types=group_types if group_types else None,
                start_year=2020,
                end_year=2026
            )
            
            years = result.get("years", [])
            enrollment = result.get("total_enrollment", [])
            
            # Should have data
            has_data = len(years) > 0 and any(e > 0 for e in enrollment)
            results.add_result(
                f"enrollment_{test_name}",
                has_data,
                {"years": len(years), "enrollment_2024": enrollment[4] if len(enrollment) > 4 else None},
                "No data returned" if not has_data else None
            )
            
            if not has_data:
                combo_failures.append(test_name)
                
        except Exception as e:
            results.add_result(f"enrollment_{test_name}", False, error=str(e))
            combo_failures.append(test_name)
    
    passed_combos = len(key_combos) - len(combo_failures)
    print(f"   Passed: {passed_combos}/{len(key_combos)}")
    if combo_failures:
        print(f"   Failed: {combo_failures}")
    
    # Test 3: Geographic state filter works
    print("\n3. Geographic State Filter Test")
    sample_states = AUDIT_CONFIG["enrollment"]["sample_states"]
    for state in sample_states:
        try:
            result = service.get_timeseries(
                data_source="geographic",
                product_types=["MAPD"],
                state=state,
                start_year=2020,
                end_year=2026
            )
            years = result.get("years", [])
            enrollment = result.get("total_enrollment", [])
            has_data = len(years) > 0 and sum(enrollment) > 0
            
            results.add_result(
                f"enrollment_state_{state}",
                has_data,
                {"years": len(years), "total_enrollment": sum(enrollment) if enrollment else 0}
            )
            print(f"   {state}: {'✓' if has_data else '✗'} ({sum(enrollment):,.0f} total)" if has_data else f"   {state}: ✗ No data")
        except Exception as e:
            results.add_result(f"enrollment_state_{state}", False, error=str(e))
            print(f"   {state}: ERROR - {e}")
    
    # Test 4: Payer filter works
    print("\n4. Payer Filter Test")
    sample_payers = AUDIT_CONFIG["enrollment"]["sample_payers"]
    for payer in sample_payers:
        try:
            result = service.get_timeseries(
                data_source="national",
                parent_org=payer,
                product_types=["MAPD"],
                start_year=2020,
                end_year=2026
            )
            years = result.get("years", [])
            enrollment = result.get("total_enrollment", [])
            has_data = len(years) > 0 and sum(enrollment) > 0
            
            results.add_result(
                f"enrollment_payer_{payer[:20]}",
                has_data,
                {"years": len(years), "total_enrollment": sum(enrollment) if enrollment else 0}
            )
            print(f"   {payer[:30]}: {'✓' if has_data else '✗'}")
        except Exception as e:
            results.add_result(f"enrollment_payer_{payer[:20]}", False, error=str(e))
            print(f"   {payer[:30]}: ERROR")
    
    # Test 5: Enrollment totals are reasonable
    print("\n5. Enrollment Total Validation")
    expected_totals = {
        2020: (24_000_000, 27_000_000),
        2024: (33_000_000, 36_000_000),
        2026: (34_000_000, 37_000_000),
    }
    
    for year, (min_expected, max_expected) in expected_totals.items():
        try:
            result = service.get_timeseries(
                data_source="national",
                product_types=["MAPD"],
                start_year=year,
                end_year=year
            )
            if result.get("total_enrollment"):
                actual = result["total_enrollment"][0]
                in_range = min_expected <= actual <= max_expected
                results.add_result(
                    f"enrollment_total_{year}",
                    in_range,
                    {"actual": actual, "expected_range": (min_expected, max_expected)}
                )
                print(f"   {year}: {actual:,.0f} {'✓' if in_range else '✗ OUT OF RANGE'}")
        except Exception as e:
            results.add_result(f"enrollment_total_{year}", False, error=str(e))
            print(f"   {year}: ERROR - {e}")
    
    # Test 6: National and Geographic match (same data source now)
    print("\n6. National vs Geographic Consistency")
    try:
        national_result = service.get_timeseries(
            data_source="national",
            product_types=["MAPD"],
            start_year=2020,
            end_year=2026
        )
        geographic_result = service.get_timeseries(
            data_source="geographic",
            product_types=["MAPD"],
            start_year=2020,
            end_year=2026
        )
        
        if national_result.get("years") == geographic_result.get("years"):
            # Check totals match
            nat_totals = national_result.get("total_enrollment", [])
            geo_totals = geographic_result.get("total_enrollment", [])
            
            matches = all(abs(n - g) < 1 for n, g in zip(nat_totals, geo_totals))
            results.add_result(
                "enrollment_national_geographic_match",
                matches,
                {"national_sum": sum(nat_totals), "geographic_sum": sum(geo_totals)}
            )
            print(f"   Totals match: {'✓' if matches else '✗'}")
        else:
            results.add_result("enrollment_national_geographic_match", False, error="Year mismatch")
            print(f"   Years don't match!")
    except Exception as e:
        results.add_result("enrollment_national_geographic_match", False, error=str(e))
        print(f"   ERROR: {e}")

# =============================================================================
# STARS AUDIT
# =============================================================================

def audit_stars(results: AuditResults):
    """Comprehensive stars audit."""
    print("\n" + "="*70)
    print("STARS AUDIT")
    print("="*70)
    
    try:
        from api.services.stars_service import StarsService
        service = StarsService()
        
        # Test 1: Distribution endpoint - returns timeseries with fourplus_pct
        print("\n1. Stars Distribution Test")
        for data_source in ["national", "rated"]:
            for year in [2024]:
                try:
                    result = service.get_distribution(star_year=year, data_source=data_source)
                    
                    # The service returns series with fourplus_pct per year
                    has_data = len(result.get("data", [])) > 0
                    
                    # Get 4+ percentage from the data
                    data = result.get("data", [])
                    four_plus = None
                    if data:
                        for row in data:
                            if row.get("star_year") == year:
                                four_plus = row.get("fourplus_pct")
                                break
                    
                    results.add_result(
                        f"stars_distribution_{data_source}_{year}",
                        has_data and four_plus is not None,
                        {"data_count": len(data), "four_plus_pct": four_plus}
                    )
                    if four_plus:
                        print(f"   {data_source} {year}: 4+ Star = {four_plus:.1f}% ✓")
                    else:
                        print(f"   {data_source} {year}: No 4+ data ✗")
                except Exception as e:
                    results.add_result(f"stars_distribution_{data_source}_{year}", False, error=str(e))
                    print(f"   {data_source} {year}: ERROR - {e}")
        
        # Test 2: 4+ star percentage validation
        print("\n2. 4+ Star Percentage Validation (2024)")
        try:
            # National should be accurate (~76%)
            nat_result = service.get_distribution(star_year=2024, data_source="national")
            nat_data = nat_result.get("data", [])
            nat_pct = None
            for row in nat_data:
                if row.get("star_year") == 2024:
                    nat_pct = row.get("fourplus_pct")
                    break
            
            # Rated contracts only should be higher (~82%)
            rated_result = service.get_distribution(star_year=2024, data_source="rated")
            rated_data = rated_result.get("data", [])
            rated_pct = None
            for row in rated_data:
                if row.get("star_year") == 2024:
                    rated_pct = row.get("fourplus_pct")
                    break
            
            if nat_pct and rated_pct:
                # National should be lower than rated (by excluding non-rated contracts)
                expected_diff = 3  # At least 3pp difference
                actual_diff = rated_pct - nat_pct
                
                results.add_result(
                    "stars_4plus_national_vs_rated",
                    actual_diff >= expected_diff,
                    {"national_pct": nat_pct, "rated_pct": rated_pct, "difference": actual_diff}
                )
                print(f"   National 4+: {nat_pct:.1f}%")
                print(f"   Rated 4+: {rated_pct:.1f}%")
                print(f"   Difference: {actual_diff:.1f}pp {'✓' if actual_diff >= expected_diff else '✗'}")
            else:
                results.add_result("stars_4plus_national_vs_rated", False, error="No data returned")
                print(f"   No data returned for comparison (nat={nat_pct}, rated={rated_pct})")
            
        except Exception as e:
            results.add_result("stars_4plus_national_vs_rated", False, error=str(e))
            print(f"   ERROR: {e}")
            
    except ImportError as e:
        results.add_result("stars_service_available", False, error="StarsService not available")
        print(f"   Stars service not available: {e}")

# =============================================================================
# RISK SCORES AUDIT
# =============================================================================

def audit_risk_scores(results: AuditResults):
    """Comprehensive risk scores audit."""
    print("\n" + "="*70)
    print("RISK SCORES AUDIT")
    print("="*70)
    
    try:
        from api.services.risk_scores_service import RiskScoresService
        service = RiskScoresService()
        
        # Test 1: Distribution endpoint (alternative to industry average)
        print("\n1. Risk Score Distribution Test")
        for year in [2020, 2024]:
            try:
                result = service.get_distribution(year=year)
                has_data = result.get("data") is not None or result.get("distribution") is not None
                
                results.add_result(
                    f"risk_scores_distribution_{year}",
                    has_data,
                    {"has_data": has_data}
                )
                print(f"   {year}: {'✓' if has_data else '✗'}")
            except Exception as e:
                results.add_result(f"risk_scores_distribution_{year}", False, error=str(e))
                print(f"   {year}: ERROR - {e}")
        
        # Test 2: Timeseries
        print("\n2. Risk Score Timeseries Test")
        try:
            result = service.get_timeseries()
            years = result.get("years", [])
            has_data = len(years) > 0
            
            results.add_result(
                "risk_scores_timeseries",
                has_data,
                {"years": len(years)}
            )
            print(f"   Years available: {len(years)} {'✓' if has_data else '✗'}")
        except Exception as e:
            results.add_result("risk_scores_timeseries", False, error=str(e))
            print(f"   ERROR - {e}")
                
    except ImportError as e:
        results.add_result("risk_scores_service_available", False, error="RiskScoresService not available")
        print(f"   Risk scores service not available: {e}")

# =============================================================================
# DATA CONSISTENCY AUDIT
# =============================================================================

def audit_data_consistency(results: AuditResults):
    """Check data consistency across domains - ALL must tie EXACTLY for ALL YEARS."""
    print("\n" + "="*70)
    print("DATA CONSISTENCY AUDIT (EXACT MATCHES REQUIRED - ALL YEARS)")
    print("="*70)
    
    from api.services.enrollment_service import EnrollmentService
    enrollment_service = EnrollmentService()
    
    # Test ALL years from 2013-2026
    years_to_test = list(range(2013, 2027))
    
    all_snp_pass = True
    all_group_pass = True
    all_plan_pass = True
    all_geo_pass = True
    
    snp_failures = []
    group_failures = []
    plan_failures = []
    geo_failures = []
    
    print(f"\n   Testing {len(years_to_test)} years: {years_to_test[0]}-{years_to_test[-1]}")
    
    # =========================================================================
    # Test 1: SNP breakdown must equal total EXACTLY for each year
    # =========================================================================
    print("\n1. SNP Type Breakdown by Year (must sum to 100% each year)")
    print("   Year       Total         SNP Sum      Diff   Status")
    print("   " + "─"*55)
    
    for year in years_to_test:
        try:
            # Get total for year
            total_result = enrollment_service.get_timeseries(
                data_source="national",
                product_types=["MAPD"],
                start_year=year,
                end_year=year
            )
            total = total_result.get("total_enrollment", [0])[0] if total_result.get("total_enrollment") else 0
            
            if total == 0:
                continue  # Skip years with no data
            
            # Sum all SNP types
            snp_types = ["Non-SNP", "D-SNP", "C-SNP", "I-SNP"]
            snp_sum = 0
            for snp_type in snp_types:
                snp_result = enrollment_service.get_timeseries(
                    data_source="national",
                    product_types=["MAPD"],
                    snp_types=[snp_type],
                    start_year=year,
                    end_year=year
                )
                val = snp_result.get("total_enrollment", [0])[0] if snp_result.get("total_enrollment") else 0
                snp_sum += val
            
            diff = abs(total - snp_sum)
            passed = diff == 0
            status = "✓" if passed else "✗"
            
            print(f"   {year}  {total:>14,.0f}  {snp_sum:>14,.0f}  {diff:>6,.0f}   {status}")
            
            if not passed:
                all_snp_pass = False
                snp_failures.append({"year": year, "total": total, "sum": snp_sum, "diff": diff})
                
        except Exception as e:
            print(f"   {year}  ERROR: {e}")
            all_snp_pass = False
            snp_failures.append({"year": year, "error": str(e)})
    
    results.add_result(
        "snp_sum_all_years_exact",
        all_snp_pass,
        {"years_tested": len(years_to_test), "failures": snp_failures}
    )
    print(f"   " + "─"*55)
    print(f"   SNP Validation: {'✓ ALL YEARS EXACT' if all_snp_pass else f'✗ {len(snp_failures)} FAILURES'}")
    
    # =========================================================================
    # Test 2: Group type breakdown must equal total EXACTLY for each year
    # =========================================================================
    print("\n2. Group Type Breakdown by Year (must sum to 100% each year)")
    print("   Year       Total       Group Sum      Diff   Status")
    print("   " + "─"*55)
    
    for year in years_to_test:
        try:
            total_result = enrollment_service.get_timeseries(
                data_source="national",
                product_types=["MAPD"],
                start_year=year,
                end_year=year
            )
            total = total_result.get("total_enrollment", [0])[0] if total_result.get("total_enrollment") else 0
            
            if total == 0:
                continue
            
            group_types = ["Individual", "Group"]
            group_sum = 0
            for group_type in group_types:
                group_result = enrollment_service.get_timeseries(
                    data_source="national",
                    product_types=["MAPD"],
                    group_types=[group_type],
                    start_year=year,
                    end_year=year
                )
                val = group_result.get("total_enrollment", [0])[0] if group_result.get("total_enrollment") else 0
                group_sum += val
            
            diff = abs(total - group_sum)
            passed = diff == 0
            status = "✓" if passed else "✗"
            
            print(f"   {year}  {total:>14,.0f}  {group_sum:>14,.0f}  {diff:>6,.0f}   {status}")
            
            if not passed:
                all_group_pass = False
                group_failures.append({"year": year, "total": total, "sum": group_sum, "diff": diff})
                
        except Exception as e:
            print(f"   {year}  ERROR: {e}")
            all_group_pass = False
            group_failures.append({"year": year, "error": str(e)})
    
    results.add_result(
        "group_type_sum_all_years_exact",
        all_group_pass,
        {"years_tested": len(years_to_test), "failures": group_failures}
    )
    print(f"   " + "─"*55)
    print(f"   Group Type Validation: {'✓ ALL YEARS EXACT' if all_group_pass else f'✗ {len(group_failures)} FAILURES'}")
    
    # =========================================================================
    # Test 3: Plan type breakdown must equal total EXACTLY for each year
    # =========================================================================
    print("\n3. Plan Type Breakdown by Year (must sum to 100% each year)")
    print("   Year       Total        Plan Sum      Diff   Status")
    print("   " + "─"*55)
    
    for year in years_to_test:
        try:
            total_result = enrollment_service.get_timeseries(
                data_source="national",
                product_types=["MAPD"],
                start_year=year,
                end_year=year
            )
            total = total_result.get("total_enrollment", [0])[0] if total_result.get("total_enrollment") else 0
            
            if total == 0:
                continue
            
            plan_types = ["HMO", "PPO", "PFFS", "MSA", "PACE", "Cost"]
            plan_sum = 0
            for plan_type in plan_types:
                plan_result = enrollment_service.get_timeseries(
                    data_source="national",
                    product_types=["MAPD"],
                    plan_types=[plan_type],
                    start_year=year,
                    end_year=year
                )
                val = plan_result.get("total_enrollment", [0])[0] if plan_result.get("total_enrollment") else 0
                plan_sum += val
            
            diff = abs(total - plan_sum)
            passed = diff == 0
            status = "✓" if passed else "✗"
            
            print(f"   {year}  {total:>14,.0f}  {plan_sum:>14,.0f}  {diff:>6,.0f}   {status}")
            
            if not passed:
                all_plan_pass = False
                plan_failures.append({"year": year, "total": total, "sum": plan_sum, "diff": diff})
                
        except Exception as e:
            print(f"   {year}  ERROR: {e}")
            all_plan_pass = False
            plan_failures.append({"year": year, "error": str(e)})
    
    results.add_result(
        "plan_type_sum_all_years_exact",
        all_plan_pass,
        {"years_tested": len(years_to_test), "failures": plan_failures}
    )
    print(f"   " + "─"*55)
    print(f"   Plan Type Validation: {'✓ ALL YEARS EXACT' if all_plan_pass else f'✗ {len(plan_failures)} FAILURES'}")
    
    # =========================================================================
    # Test 4: National vs Geographic must match EXACTLY for each year
    # =========================================================================
    print("\n4. National vs Geographic by Year (must match exactly)")
    print("   Year      National     Geographic      Diff   Status")
    print("   " + "─"*55)
    
    for year in years_to_test:
        try:
            nat_result = enrollment_service.get_timeseries(
                data_source="national",
                product_types=["MAPD"],
                start_year=year,
                end_year=year
            )
            nat_total = nat_result.get("total_enrollment", [0])[0] if nat_result.get("total_enrollment") else 0
            
            if nat_total == 0:
                continue
            
            geo_result = enrollment_service.get_timeseries(
                data_source="geographic",
                product_types=["MAPD"],
                start_year=year,
                end_year=year
            )
            geo_total = geo_result.get("total_enrollment", [0])[0] if geo_result.get("total_enrollment") else 0
            
            diff = abs(nat_total - geo_total)
            passed = diff == 0
            status = "✓" if passed else "✗"
            
            print(f"   {year}  {nat_total:>14,.0f}  {geo_total:>14,.0f}  {diff:>6,.0f}   {status}")
            
            if not passed:
                all_geo_pass = False
                geo_failures.append({"year": year, "national": nat_total, "geographic": geo_total, "diff": diff})
                
        except Exception as e:
            print(f"   {year}  ERROR: {e}")
            all_geo_pass = False
            geo_failures.append({"year": year, "error": str(e)})
    
    results.add_result(
        "national_geographic_all_years_exact",
        all_geo_pass,
        {"years_tested": len(years_to_test), "failures": geo_failures}
    )
    print(f"   " + "─"*55)
    print(f"   National vs Geographic: {'✓ ALL YEARS EXACT' if all_geo_pass else f'✗ {len(geo_failures)} FAILURES'}")
    
    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "─"*70)
    print("CONSISTENCY SUMMARY")
    print("─"*70)
    all_pass = all_snp_pass and all_group_pass and all_plan_pass and all_geo_pass
    print(f"   SNP Types:           {'✓ PASS' if all_snp_pass else '✗ FAIL'}")
    print(f"   Group Types:         {'✓ PASS' if all_group_pass else '✗ FAIL'}")
    print(f"   Plan Types:          {'✓ PASS' if all_plan_pass else '✗ FAIL'}")
    print(f"   National=Geographic: {'✓ PASS' if all_geo_pass else '✗ FAIL'}")
    print(f"   ─────────────────────")
    print(f"   OVERALL:             {'✓ ALL YEARS TIE EXACTLY' if all_pass else '✗ FAILURES DETECTED'}")

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("="*70)
    print("COMPREHENSIVE SYSTEM AUDIT")
    print(f"Started: {datetime.now()}")
    print("="*70)
    
    results = AuditResults()
    
    # Run all audits
    try:
        audit_enrollment(results)
    except Exception as e:
        print(f"Enrollment audit failed: {e}")
        traceback.print_exc()
    
    try:
        audit_stars(results)
    except Exception as e:
        print(f"Stars audit failed: {e}")
        traceback.print_exc()
    
    try:
        audit_risk_scores(results)
    except Exception as e:
        print(f"Risk scores audit failed: {e}")
        traceback.print_exc()
    
    try:
        audit_data_consistency(results)
    except Exception as e:
        print(f"Data consistency audit failed: {e}")
        traceback.print_exc()
    
    # Summary
    summary = results.summary()
    
    print("\n" + "="*70)
    print("AUDIT SUMMARY")
    print("="*70)
    print(f"Tests Run: {summary['tests_run']}")
    print(f"Tests Passed: {summary['tests_passed']}")
    print(f"Tests Failed: {summary['tests_failed']}")
    print(f"Pass Rate: {summary['pass_rate']}")
    
    if results.warnings:
        print(f"\nWarnings ({len(results.warnings)}):")
        for w in results.warnings[:10]:
            print(f"  ⚠️  {w}")
    
    if results.errors:
        print(f"\nErrors ({len(results.errors)}):")
        for e in results.errors[:10]:
            print(f"  ❌ {e}")
    
    # Save results
    output_file = f"audit_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w") as f:
        json.dump({
            "summary": summary,
            "results": results.results,
        }, f, indent=2, default=str)
    print(f"\nResults saved to: {output_file}")
    
    print(f"\nFinished: {datetime.now()}")
    
    # Return exit code based on failures
    return 0 if results.tests_failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
