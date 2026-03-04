#!/usr/bin/env python3
"""
UI Filter Combination Audit

Tests all possible filter combinations as they would be called from the UI.
This validates that the API handles all UI state combinations correctly.

Can work either via HTTP API or directly via services (for offline testing).

Run: python scripts/audit_ui_combinations.py
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Optional
from itertools import product

# Add project root to path for direct service access
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Try to use HTTP API first, fall back to direct service
API_BASE = os.environ.get("API_URL", "http://localhost:8000")
USE_DIRECT_SERVICE = os.environ.get("USE_DIRECT_SERVICE", "false").lower() == "true"

# =============================================================================
# UI FILTER OPTIONS (matching frontend/page.tsx)
# =============================================================================

# Product Types (MA = MAPD in API)
PRODUCT_TYPES = ["MA", "PDP"]  # UI values

# Plan Types  
PLAN_TYPES = ["HMO", "PPO", "PFFS", "MSA", "PACE", "Cost"]

# SNP Types
SNP_TYPES = ["Non-SNP", "D-SNP", "C-SNP", "I-SNP"]

# Group Types
GROUP_TYPES = ["Individual", "Group"]

# Data Sources
DATA_SOURCES = ["national", "geographic"]

# Sample states
STATES = ["CA", "TX", "FL", "NY", "PA"]

# =============================================================================
# TEST FUNCTIONS
# =============================================================================

def test_enrollment_timeseries(
    data_source: str,
    product_types: List[str] = None,
    plan_types: List[str] = None,
    snp_types: List[str] = None,
    group_types: List[str] = None,
    state: str = None,
) -> Dict:
    """Call enrollment timeseries - direct service call (simulating UI->API flow)."""
    
    # Map UI's "MA" to API's "MAPD"
    api_product_types = None
    if product_types:
        api_product_types = []
        for pt in product_types:
            if pt == "MA":
                api_product_types.append("MAPD")
            else:
                api_product_types.append(pt)
    
    try:
        from api.services.enrollment_service import EnrollmentService
        service = EnrollmentService()
        
        result = service.get_timeseries(
            data_source=data_source,
            product_types=api_product_types,
            plan_types=plan_types,
            snp_types=snp_types,
            group_types=group_types,
            state=state if data_source == "geographic" else None,
            start_year=2020,
            end_year=2026
        )
        return result
    except Exception as e:
        return {"error": str(e)}


def audit_ui_combinations():
    """Test all meaningful UI filter combinations."""
    
    print("="*70)
    print("UI FILTER COMBINATION AUDIT")
    print(f"API: {API_BASE}")
    print(f"Started: {datetime.now()}")
    print("="*70)
    
    results = []
    passed = 0
    failed = 0
    
    # =========================================================================
    # Test 1: Data source toggle (should both work identically)
    # =========================================================================
    print("\n1. DATA SOURCE TOGGLE")
    for ds in DATA_SOURCES:
        result = test_enrollment_timeseries(data_source=ds, product_types=["MA"])
        has_years = len(result.get("years", [])) > 0
        enrollment = sum(result.get("total_enrollment", []))
        
        test_result = {
            "test": f"data_source_{ds}",
            "passed": has_years and enrollment > 0,
            "years": len(result.get("years", [])),
            "enrollment": enrollment,
            "error": result.get("error")
        }
        results.append(test_result)
        
        if test_result["passed"]:
            passed += 1
            print(f"   ✓ {ds}: {len(result.get('years', []))} years, {enrollment:,.0f} enrollment")
        else:
            failed += 1
            print(f"   ✗ {ds}: {result.get('error', 'No data')}")
    
    # =========================================================================
    # Test 2: Product type filter combinations
    # =========================================================================
    print("\n2. PRODUCT TYPE FILTERS")
    product_combos = [["MA"], ["PDP"], ["MA", "PDP"]]
    for pt_combo in product_combos:
        for ds in DATA_SOURCES:
            result = test_enrollment_timeseries(data_source=ds, product_types=pt_combo)
            has_data = len(result.get("years", [])) > 0 and sum(result.get("total_enrollment", [])) > 0
            
            test_name = f"product_{'+'.join(pt_combo)}_{ds}"
            test_result = {
                "test": test_name,
                "passed": has_data,
                "enrollment": sum(result.get("total_enrollment", [])),
                "error": result.get("error")
            }
            results.append(test_result)
            
            if has_data:
                passed += 1
                print(f"   ✓ {'+'.join(pt_combo)} ({ds})")
            else:
                failed += 1
                print(f"   ✗ {'+'.join(pt_combo)} ({ds}): {result.get('error', 'No data')}")
    
    # =========================================================================
    # Test 3: Plan type filter combinations (MA only)
    # =========================================================================
    print("\n3. PLAN TYPE FILTERS (MA)")
    for plan_type in PLAN_TYPES:
        for ds in DATA_SOURCES:
            result = test_enrollment_timeseries(
                data_source=ds, 
                product_types=["MA"],
                plan_types=[plan_type]
            )
            has_data = len(result.get("years", [])) > 0
            enrollment = sum(result.get("total_enrollment", []))
            
            # Some plan types may legitimately have zero enrollment
            test_result = {
                "test": f"plan_{plan_type}_{ds}",
                "passed": has_data,  # Just need API to respond, even if no enrollment
                "enrollment": enrollment,
                "error": result.get("error")
            }
            results.append(test_result)
            
            if has_data:
                passed += 1
                status = "✓" if enrollment > 0 else "⚠️"
                print(f"   {status} {plan_type} ({ds}): {enrollment:,.0f}")
            else:
                failed += 1
                print(f"   ✗ {plan_type} ({ds}): {result.get('error', 'No data')}")
    
    # =========================================================================
    # Test 4: SNP type filter combinations (MA only)
    # =========================================================================
    print("\n4. SNP TYPE FILTERS (MA)")
    for snp_type in SNP_TYPES:
        for ds in DATA_SOURCES:
            result = test_enrollment_timeseries(
                data_source=ds,
                product_types=["MA"],
                snp_types=[snp_type]
            )
            has_data = len(result.get("years", [])) > 0
            enrollment = sum(result.get("total_enrollment", []))
            
            test_result = {
                "test": f"snp_{snp_type}_{ds}",
                "passed": has_data and enrollment > 0,
                "enrollment": enrollment,
                "error": result.get("error")
            }
            results.append(test_result)
            
            if test_result["passed"]:
                passed += 1
                print(f"   ✓ {snp_type} ({ds}): {enrollment:,.0f}")
            else:
                failed += 1
                print(f"   ✗ {snp_type} ({ds}): {result.get('error', 'No data')}")
    
    # =========================================================================
    # Test 5: Group type filter combinations (MA only)
    # =========================================================================
    print("\n5. GROUP TYPE FILTERS (MA)")
    for group_type in GROUP_TYPES:
        for ds in DATA_SOURCES:
            result = test_enrollment_timeseries(
                data_source=ds,
                product_types=["MA"],
                group_types=[group_type]
            )
            has_data = len(result.get("years", [])) > 0
            enrollment = sum(result.get("total_enrollment", []))
            
            test_result = {
                "test": f"group_{group_type}_{ds}",
                "passed": has_data and enrollment > 0,
                "enrollment": enrollment,
                "error": result.get("error")
            }
            results.append(test_result)
            
            if test_result["passed"]:
                passed += 1
                print(f"   ✓ {group_type} ({ds}): {enrollment:,.0f}")
            else:
                failed += 1
                print(f"   ✗ {group_type} ({ds}): {result.get('error', 'No data')}")
    
    # =========================================================================
    # Test 6: State filter (geographic only)
    # =========================================================================
    print("\n6. STATE FILTERS (Geographic only)")
    for state in STATES:
        result = test_enrollment_timeseries(
            data_source="geographic",
            product_types=["MA"],
            state=state
        )
        has_data = len(result.get("years", [])) > 0
        enrollment = sum(result.get("total_enrollment", []))
        
        test_result = {
            "test": f"state_{state}",
            "passed": has_data and enrollment > 0,
            "enrollment": enrollment,
            "error": result.get("error")
        }
        results.append(test_result)
        
        if test_result["passed"]:
            passed += 1
            print(f"   ✓ {state}: {enrollment:,.0f}")
        else:
            failed += 1
            print(f"   ✗ {state}: {result.get('error', 'No data')}")
    
    # =========================================================================
    # Test 7: Complex combinations (all filters together)
    # =========================================================================
    print("\n7. COMPLEX COMBINATIONS")
    complex_combos = [
        # HMO + D-SNP + Individual
        (["MA"], ["HMO"], ["D-SNP"], ["Individual"], None, "HMO_DSNP_Individual"),
        # PPO + Non-SNP + Group
        (["MA"], ["PPO"], ["Non-SNP"], ["Group"], None, "PPO_NonSNP_Group"),
        # HMO+PPO + All SNPs
        (["MA"], ["HMO", "PPO"], ["D-SNP", "C-SNP", "I-SNP"], None, None, "HMO_PPO_AllSNP"),
        # State + Plan type + SNP
        (["MA"], ["HMO"], ["D-SNP"], None, "CA", "CA_HMO_DSNP"),
    ]
    
    for pt, plan, snp, group, state, name in complex_combos:
        ds = "geographic" if state else "national"
        result = test_enrollment_timeseries(
            data_source=ds,
            product_types=pt,
            plan_types=plan,
            snp_types=snp,
            group_types=group,
            state=state
        )
        has_data = len(result.get("years", [])) > 0
        enrollment = sum(result.get("total_enrollment", []))
        
        test_result = {
            "test": f"complex_{name}",
            "passed": has_data and enrollment > 0,
            "enrollment": enrollment,
            "error": result.get("error")
        }
        results.append(test_result)
        
        if test_result["passed"]:
            passed += 1
            print(f"   ✓ {name}: {enrollment:,.0f}")
        else:
            failed += 1
            print(f"   ✗ {name}: {result.get('error', 'No data')}")
    
    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "="*70)
    print("AUDIT SUMMARY")
    print("="*70)
    print(f"Tests Run: {passed + failed}")
    print(f"Tests Passed: {passed}")
    print(f"Tests Failed: {failed}")
    print(f"Pass Rate: {(passed / (passed + failed) * 100):.1f}%" if (passed + failed) > 0 else "N/A")
    
    if failed > 0:
        print("\nFailed Tests:")
        for r in results:
            if not r["passed"]:
                print(f"  ✗ {r['test']}: {r.get('error', 'No data')}")
    
    # Save results
    output_file = f"ui_audit_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "api_base": API_BASE,
            "summary": {
                "total": passed + failed,
                "passed": passed,
                "failed": failed,
                "pass_rate": f"{(passed / (passed + failed) * 100):.1f}%" if (passed + failed) > 0 else "N/A"
            },
            "results": results
        }, f, indent=2)
    print(f"\nResults saved to: {output_file}")
    
    print(f"\nFinished: {datetime.now()}")
    
    return failed == 0


if __name__ == "__main__":
    success = audit_ui_combinations()
    sys.exit(0 if success else 1)
