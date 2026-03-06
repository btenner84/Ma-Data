"""
MA Agent V2 Comprehensive Test Suite
=====================================

Tests the multi-step AI agent for:
1. Data retrieval accuracy (SQL queries work)
2. Visual output generation (charts, tables)
3. All data capabilities (enrollment, stars, SNP, geographic, etc.)
4. Response quality (conversational, insightful)
5. Audit trail completeness

Run with: 
    python tests/test_agent_v2.py                    # Run all tests
    python tests/test_agent_v2.py --category stars   # Run specific category
    python tests/test_agent_v2.py --quick            # Quick smoke test
"""

import requests
import json
import time
import sys
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

API_BASE = "http://localhost:8000"


@dataclass
class AgentTest:
    """Test case for the V2 agent."""
    id: str
    category: str
    question: str
    
    # Expected in response
    expected_keywords: List[str] = field(default_factory=list)
    
    # Expected outputs
    should_have_chart: bool = False
    chart_type: Optional[str] = None  # line, bar, area
    should_have_table: bool = False
    min_table_rows: int = 0
    
    # Quality checks
    min_confidence: float = 0.6
    max_latency_ms: int = 60000
    
    # Audit checks
    min_tool_calls: int = 1
    expected_tables_queried: List[str] = field(default_factory=list)


# ===========================================================================
# TEST QUESTIONS BY CAPABILITY
# ===========================================================================

ENROLLMENT_TESTS = [
    AgentTest(
        id="enroll_1",
        category="enrollment",
        question="What is the total MA enrollment in 2026?",
        expected_keywords=["million", "enrollment", "2026"],
        should_have_table=True,
        min_tool_calls=1,
    ),
    AgentTest(
        id="enroll_2",
        category="enrollment",
        question="Show me Humana's enrollment from 2020 to 2026 with a chart",
        expected_keywords=["Humana", "enrollment"],
        should_have_chart=True,
        chart_type="line",
        min_tool_calls=1,
    ),
    AgentTest(
        id="enroll_3",
        category="enrollment",
        question="Top 5 parent organizations by enrollment in 2026",
        expected_keywords=["United", "Humana"],
        should_have_table=True,
        should_have_chart=True,
        chart_type="bar",
    ),
]

STAR_RATING_TESTS = [
    AgentTest(
        id="stars_1",
        category="stars",
        question="Show me companies with major 4+ star drops and their recovery patterns",
        expected_keywords=["drop", "recovery", "star"],
        should_have_chart=True,
        should_have_table=True,
        min_confidence=0.7,
    ),
    AgentTest(
        id="stars_2",
        category="stars",
        question="What is Humana's 4+ star enrollment percentage history with a chart?",
        expected_keywords=["Humana", "percent", "star"],
        should_have_chart=True,
        chart_type="line",
    ),
    AgentTest(
        id="stars_3",
        category="stars",
        question="Which payers have the highest % in 4+ star contracts in 2026?",
        expected_keywords=["percent", "star"],
        should_have_table=True,
        should_have_chart=True,
        chart_type="bar",
    ),
]

CUTPOINT_TESTS = [
    AgentTest(
        id="cutpoint_1",
        category="cutpoints",
        question="What score do I need for 4 stars on measure C01 Breast Cancer Screening?",
        expected_keywords=["cutpoint", "threshold", "percent"],
        should_have_table=True,
        expected_tables_queried=["cutpoints_all_years"],
    ),
    AgentTest(
        id="cutpoint_2",
        category="cutpoints",
        question="How have the 4-star cutpoints changed over time for C01?",
        expected_keywords=["cutpoint", "change"],
        should_have_chart=True,
        chart_type="line",
    ),
]

MEASURE_TESTS = [
    AgentTest(
        id="measure_1",
        category="measures",
        question="Which star rating measures have the highest weights?",
        expected_keywords=["weight", "measure"],
        should_have_table=True,
        expected_tables_queried=["stars_measure_specs"],
    ),
    AgentTest(
        id="measure_2",
        category="measures",
        question="Show me measure performance for Humana on the top 5 weighted measures",
        expected_keywords=["Humana", "measure", "performance"],
        should_have_table=True,
    ),
]

SNP_TESTS = [
    AgentTest(
        id="snp_1",
        category="snp",
        question="Show D-SNP enrollment growth by parent organization from 2018-2026",
        expected_keywords=["D-SNP", "enrollment", "growth"],
        should_have_chart=True,
        chart_type="line",
        expected_tables_queried=["fact_snp"],
    ),
    AgentTest(
        id="snp_2",
        category="snp",
        question="Which payers have the most D-SNP enrollment in 2026?",
        expected_keywords=["D-SNP"],
        should_have_table=True,
        should_have_chart=True,
        chart_type="bar",
    ),
]

GEOGRAPHIC_TESTS = [
    AgentTest(
        id="geo_1",
        category="geographic",
        question="What is UnitedHealth's market share by state in 2026?",
        expected_keywords=["market share", "state"],
        should_have_table=True,
        expected_tables_queried=["fact_enrollment_by_state"],
    ),
    AgentTest(
        id="geo_2",
        category="geographic",
        question="Top 10 counties in Florida by MA enrollment",
        expected_keywords=["Florida", "county", "enrollment"],
        should_have_table=True,
        expected_tables_queried=["fact_enrollment_by_geography"],
    ),
]

DISENROLLMENT_TESTS = [
    AgentTest(
        id="disenroll_1",
        category="disenrollment",
        question="Which parent organizations have the highest disenrollment rates?",
        expected_keywords=["disenrollment", "rate"],
        should_have_table=True,
        expected_tables_queried=["disenrollment_all_years"],
    ),
]

RISK_TESTS = [
    AgentTest(
        id="risk_1",
        category="risk",
        question="What is the V28 phase-in schedule and how does it affect payments?",
        expected_keywords=["V28", "V24", "phase"],
        min_tool_calls=1,
    ),
    AgentTest(
        id="risk_2",
        category="risk",
        question="Compare average risk scores by parent organization",
        expected_keywords=["risk score"],
        should_have_table=True,
        should_have_chart=True,
    ),
]

POLICY_TESTS = [
    AgentTest(
        id="policy_1",
        category="policy",
        question="What are the key changes in the 2027 advance notice?",
        expected_keywords=["2027", "advance notice"],
        min_tool_calls=1,
    ),
    AgentTest(
        id="policy_2",
        category="policy",
        question="What is the Health Equity Index and how does it affect star ratings?",
        expected_keywords=["Health Equity", "HEI", "star"],
    ),
]

COMPLEX_TESTS = [
    AgentTest(
        id="complex_1",
        category="complex",
        question="Compare UnitedHealth vs CVS: enrollment trends, star ratings, and D-SNP growth from 2020-2026 with charts",
        expected_keywords=["United", "CVS"],
        should_have_chart=True,
        should_have_table=True,
        min_confidence=0.6,
        max_latency_ms=120000,
    ),
    AgentTest(
        id="complex_2",
        category="complex",
        question="Analyze Humana's star rating decline: what drove it and how does it compare to historical patterns of other payers who recovered?",
        expected_keywords=["Humana", "decline"],
        should_have_chart=True,
        should_have_table=True,
        min_confidence=0.6,
        max_latency_ms=120000,
    ),
]

ALL_TESTS = (
    ENROLLMENT_TESTS +
    STAR_RATING_TESTS +
    CUTPOINT_TESTS +
    MEASURE_TESTS +
    SNP_TESTS +
    GEOGRAPHIC_TESTS +
    DISENROLLMENT_TESTS +
    RISK_TESTS +
    POLICY_TESTS +
    COMPLEX_TESTS
)

QUICK_TESTS = [
    ENROLLMENT_TESTS[0],
    STAR_RATING_TESTS[0],
    SNP_TESTS[0],
    GEOGRAPHIC_TESTS[0],
]


# ===========================================================================
# TEST RUNNER
# ===========================================================================

def run_test(test: AgentTest, verbose: bool = True) -> Dict:
    """Run a single test and return detailed results."""
    start_time = time.time()
    
    try:
        response = requests.post(
            f"{API_BASE}/api/v2/agent/ask",
            json={
                "question": test.question,
                "include_full_audit": True,
            },
            timeout=test.max_latency_ms / 1000
        )
        response.raise_for_status()
        data = response.json()
        elapsed_ms = int((time.time() - start_time) * 1000)
        
        # Extract response data
        answer = data.get("answer", "").lower()
        charts = data.get("charts", [])
        tables = data.get("data_tables", [])
        confidence = data.get("confidence", 0)
        tool_calls = data.get("tool_calls", 0)
        llm_calls = data.get("llm_calls", 0)
        cost = data.get("cost_usd", 0)
        audit = data.get("audit", {})
        
        # Validate keywords
        keywords_found = sum(1 for kw in test.expected_keywords if kw.lower() in answer)
        keywords_pass = len(test.expected_keywords) == 0 or keywords_found >= len(test.expected_keywords) * 0.5
        
        # Validate chart
        chart_pass = True
        if test.should_have_chart:
            chart_pass = len(charts) > 0
            if test.chart_type and charts:
                chart_pass = any(c.get("chart_type") == test.chart_type for c in charts)
        
        # Validate table
        table_pass = True
        if test.should_have_table:
            table_pass = len(tables) > 0
            if test.min_table_rows and tables:
                table_pass = any(len(t.get("rows", [])) >= test.min_table_rows for t in tables)
        
        # Validate confidence
        confidence_pass = confidence >= test.min_confidence
        
        # Validate tool calls
        tool_pass = tool_calls >= test.min_tool_calls
        
        # Validate latency
        latency_pass = elapsed_ms <= test.max_latency_ms
        
        # Overall pass
        passed = all([keywords_pass, chart_pass, table_pass, confidence_pass, tool_pass, latency_pass])
        
        result = {
            "id": test.id,
            "category": test.category,
            "question": test.question[:80],
            "passed": passed,
            "elapsed_ms": elapsed_ms,
            "cost_usd": cost,
            "confidence": confidence,
            "llm_calls": llm_calls,
            "tool_calls": tool_calls,
            "charts_count": len(charts),
            "tables_count": len(tables),
            "checks": {
                "keywords": f"{'✓' if keywords_pass else '✗'} {keywords_found}/{len(test.expected_keywords)}",
                "chart": f"{'✓' if chart_pass else '✗'} {len(charts)} charts" + (f" ({test.chart_type})" if test.chart_type else ""),
                "table": f"{'✓' if table_pass else '✗'} {len(tables)} tables",
                "confidence": f"{'✓' if confidence_pass else '✗'} {confidence:.2f} >= {test.min_confidence}",
                "tools": f"{'✓' if tool_pass else '✗'} {tool_calls} >= {test.min_tool_calls}",
                "latency": f"{'✓' if latency_pass else '✗'} {elapsed_ms}ms <= {test.max_latency_ms}ms",
            },
            "answer_preview": data.get("answer", "")[:300],
        }
        
        if verbose:
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"\n{status} [{test.id}] {test.category}")
            print(f"  Q: {test.question[:70]}...")
            print(f"  Checks: {' | '.join(result['checks'].values())}")
            print(f"  Stats: {llm_calls} LLM calls, {tool_calls} tools, ${cost:.4f}, {elapsed_ms}ms")
            if charts:
                print(f"  Charts: {[c.get('chart_type', 'unknown') for c in charts]}")
            if tables:
                print(f"  Tables: {[t.get('title', 'untitled')[:30] for t in tables]}")
        
        return result
        
    except requests.exceptions.Timeout:
        if verbose:
            print(f"\n⏱️ TIMEOUT [{test.id}]: Exceeded {test.max_latency_ms}ms")
        return {
            "id": test.id,
            "category": test.category,
            "passed": False,
            "error": "Timeout",
        }
    except Exception as e:
        if verbose:
            print(f"\n❌ ERROR [{test.id}]: {str(e)}")
        return {
            "id": test.id,
            "category": test.category,
            "passed": False,
            "error": str(e),
        }


def run_test_suite(
    tests: List[AgentTest],
    verbose: bool = True,
    delay: float = 2.0
) -> Dict:
    """Run a suite of tests."""
    
    print(f"\n{'='*70}")
    print("MA AGENT V2 - COMPREHENSIVE TEST SUITE")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"{'='*70}")
    print(f"Running {len(tests)} tests...")
    
    results = []
    total_cost = 0
    
    for i, test in enumerate(tests, 1):
        print(f"\n[{i}/{len(tests)}] Testing: {test.id}")
        result = run_test(test, verbose=verbose)
        results.append(result)
        total_cost += result.get("cost_usd", 0)
        
        if delay and i < len(tests):
            time.sleep(delay)
    
    # Summary
    passed = sum(1 for r in results if r.get("passed"))
    failed = len(results) - passed
    
    # By category
    categories = {}
    for r in results:
        cat = r["category"]
        if cat not in categories:
            categories[cat] = {"passed": 0, "total": 0}
        categories[cat]["total"] += 1
        if r.get("passed"):
            categories[cat]["passed"] += 1
    
    # Visual output stats
    charts_generated = sum(r.get("charts_count", 0) for r in results)
    tables_generated = sum(r.get("tables_count", 0) for r in results)
    
    print(f"\n{'='*70}")
    print("TEST SUMMARY")
    print(f"{'='*70}")
    print(f"Total: {len(results)} | Passed: {passed} | Failed: {failed}")
    print(f"Pass Rate: {passed/len(results)*100:.1f}%")
    print(f"Total Cost: ${total_cost:.4f}")
    print(f"\nVisual Outputs: {charts_generated} charts, {tables_generated} tables")
    
    print(f"\nBy Category:")
    for cat, stats in sorted(categories.items()):
        pct = stats["passed"] / stats["total"] * 100
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        print(f"  {cat:15} [{bar}] {stats['passed']}/{stats['total']} ({pct:.0f}%)")
    
    # Failed tests
    failed_tests = [r for r in results if not r.get("passed")]
    if failed_tests:
        print(f"\n❌ Failed Tests:")
        for r in failed_tests:
            print(f"  - {r['id']}: {r.get('error', 'Check failed')}")
    
    return {
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "pass_rate": passed / len(results) * 100,
        "total_cost": total_cost,
        "charts_generated": charts_generated,
        "tables_generated": tables_generated,
        "by_category": categories,
        "results": results,
    }


# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test MA Agent V2")
    parser.add_argument("--category", "-c", help="Test specific category")
    parser.add_argument("--id", help="Test specific question ID")
    parser.add_argument("--quick", "-q", action="store_true", help="Quick smoke test (4 tests)")
    parser.add_argument("--quiet", action="store_true", help="Less verbose output")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between tests (seconds)")
    parser.add_argument("--api", default="http://localhost:8000", help="API base URL")
    
    args = parser.parse_args()
    
    API_BASE = args.api
    
    if args.id:
        # Single test
        test = next((t for t in ALL_TESTS if t.id == args.id), None)
        if test:
            run_test(test, verbose=not args.quiet)
        else:
            print(f"Test ID not found: {args.id}")
            print(f"Available: {[t.id for t in ALL_TESTS]}")
            sys.exit(1)
    elif args.quick:
        # Quick smoke test
        run_test_suite(QUICK_TESTS, verbose=not args.quiet, delay=args.delay)
    elif args.category:
        # Category filter
        tests = [t for t in ALL_TESTS if t.category == args.category]
        if tests:
            run_test_suite(tests, verbose=not args.quiet, delay=args.delay)
        else:
            print(f"Category not found: {args.category}")
            print(f"Available: {set(t.category for t in ALL_TESTS)}")
            sys.exit(1)
    else:
        # All tests
        run_test_suite(ALL_TESTS, verbose=not args.quiet, delay=args.delay)
