"""
MA Chatbot Test Question Set
=============================

A comprehensive test suite for evaluating the MA Intelligence Assistant
across different question categories.

Categories:
1. Data Queries - Enrollment, market share, trends
2. Star Ratings Analysis - Overall ratings, % in 4+ stars, declines
3. Measure-Level Analysis - Performance, cutpoints, changes
4. Risk Scores - RAF, HCC models, V28 transition
5. Policy/Technical Notes - Rates, methodology, regulations
6. Complex Analytical - Multi-step reasoning, comparisons
7. Definitions/Methodology - Terms, calculations

Run with: python -m pytest tests/test_questions.py -v
"""

import requests
import json
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

API_BASE = "http://localhost:8000"


@dataclass
class TestQuestion:
    """A test question with expected validation."""
    id: str
    category: str
    question: str
    expected_keywords: List[str]  # Response should contain these
    expected_tools: List[str]  # Should use these tools
    expected_sources: List[str]  # Should cite these sources
    min_confidence: float = 0.7
    timeout: int = 120


# ===========================================================================
# TEST QUESTIONS BY CATEGORY
# ===========================================================================

ENROLLMENT_QUESTIONS = [
    TestQuestion(
        id="enroll_1",
        category="enrollment",
        question="What is the total MA enrollment in 2026?",
        expected_keywords=["million", "enrollment", "2026"],
        expected_tools=["calculate_metric", "query_database"],
        expected_sources=["fact_enrollment_unified"],
    ),
    TestQuestion(
        id="enroll_2",
        category="enrollment",
        question="What is UnitedHealth's market share in 2026?",
        expected_keywords=["percent", "market share", "United"],
        expected_tools=["calculate_metric"],
        expected_sources=["fact_enrollment_unified"],
    ),
    TestQuestion(
        id="enroll_3",
        category="enrollment",
        question="How has Humana's enrollment changed from 2020 to 2026?",
        expected_keywords=["Humana", "growth", "increased", "enrollment"],
        expected_tools=["query_database"],
        expected_sources=["fact_enrollment_unified"],
    ),
]

STAR_RATINGS_QUESTIONS = [
    TestQuestion(
        id="stars_1",
        category="star_ratings",
        question="What was Humana's % in 4+ star contracts in 2024 vs 2025?",
        expected_keywords=["96", "40", "decline", "Humana"],
        expected_tools=["calculate_metric"],
        expected_sources=["stars_enrollment_unified"],
    ),
    TestQuestion(
        id="stars_2",
        category="star_ratings",
        question="Why did Humana's star ratings decline? Was it their performance or cutpoints getting tougher?",
        expected_keywords=["Humana", "decline", "industry", "2024", "2025"],
        expected_tools=["query_database"],
        expected_sources=["stars_enrollment_unified"],
    ),
    TestQuestion(
        id="stars_3",
        category="star_ratings",
        question="Which payers have the highest % of enrollment in 4+ star contracts in 2026?",
        expected_keywords=["percent", "4+", "star", "enrollment"],
        expected_tools=["query_database"],
        expected_sources=["stars_enrollment_unified"],
    ),
]

MEASURE_ANALYSIS_QUESTIONS = [
    TestQuestion(
        id="measure_1",
        category="measures",
        question="What is the industry average performance on Breast Cancer Screening (C01) in 2025?",
        expected_keywords=["percent", "C01", "Breast Cancer", "performance"],
        expected_tools=["query_database"],
        expected_sources=["measures_all_years"],
    ),
    TestQuestion(
        id="measure_2",
        category="measures",
        question="What are the 4-star and 5-star cutpoints for C01 Breast Cancer Screening in 2026?",
        expected_keywords=["cutpoint", "4-star", "5-star", "percent"],
        expected_tools=["query_database", "lookup_knowledge"],
        expected_sources=["cutpoints_all_years"],
    ),
]

RISK_SCORE_QUESTIONS = [
    TestQuestion(
        id="risk_1",
        category="risk_scores",
        question="What is the V28 phase-in schedule for risk adjustment?",
        expected_keywords=["V28", "V24", "phase-in", "2024", "2025", "2026"],
        expected_tools=["lookup_knowledge"],
        expected_sources=["MA Knowledge Base"],
    ),
    TestQuestion(
        id="risk_2",
        category="risk_scores",
        question="What is Humana's average risk score in 2024?",
        expected_keywords=["risk score", "Humana", "2024"],
        expected_tools=["query_database"],
        expected_sources=["fact_risk_scores_unified"],
    ),
]

POLICY_QUESTIONS = [
    TestQuestion(
        id="policy_1",
        category="policy",
        question="What is the MA benchmark and how does it relate to star ratings?",
        expected_keywords=["benchmark", "bonus", "star", "payment"],
        expected_tools=["lookup_knowledge"],
        expected_sources=["MA Knowledge Base"],
    ),
    TestQuestion(
        id="policy_2",
        category="policy",
        question="What is a D-SNP?",
        expected_keywords=["Dual", "SNP", "Medicaid", "Medicare"],
        expected_tools=["lookup_knowledge"],
        expected_sources=["MA Knowledge Base"],
    ),
]

COMPLEX_QUESTIONS = [
    TestQuestion(
        id="complex_1",
        category="complex",
        question="Compare UnitedHealth vs CVS market share and star ratings trends from 2020 to 2026",
        expected_keywords=["United", "CVS", "market share", "star"],
        expected_tools=["query_database", "calculate_metric"],
        expected_sources=["fact_enrollment_unified", "stars_enrollment_unified"],
        min_confidence=0.6,
        timeout=180,
    ),
    TestQuestion(
        id="complex_2",
        category="complex",
        question="Which payer had the biggest improvement in 4+ star enrollment from 2020 to 2024?",
        expected_keywords=["improvement", "4+", "star", "2020", "2024"],
        expected_tools=["query_database"],
        expected_sources=["stars_enrollment_unified"],
        timeout=180,
    ),
]

ALL_QUESTIONS = (
    ENROLLMENT_QUESTIONS +
    STAR_RATINGS_QUESTIONS +
    MEASURE_ANALYSIS_QUESTIONS +
    RISK_SCORE_QUESTIONS +
    POLICY_QUESTIONS +
    COMPLEX_QUESTIONS
)


# ===========================================================================
# TEST RUNNER
# ===========================================================================

def run_test(question: TestQuestion, verbose: bool = True) -> Dict:
    """Run a single test question and return results."""
    start_time = time.time()
    
    try:
        response = requests.post(
            f"{API_BASE}/api/chat",
            json={
                "message": question.question,
                "conversation_id": f"test-{question.id}"
            },
            timeout=question.timeout
        )
        response.raise_for_status()
        data = response.json()
        elapsed_ms = int((time.time() - start_time) * 1000)
        
        # Validate response
        answer = data.get("response", "").lower()
        tools_used = [t.get("tool") for t in data.get("tools_used", [])]
        sources = data.get("sources", [])
        confidence = data.get("confidence", 0)
        
        # Check keywords
        keywords_found = sum(1 for kw in question.expected_keywords if kw.lower() in answer)
        keywords_pass = keywords_found >= len(question.expected_keywords) * 0.6
        
        # Check tools
        tools_found = sum(1 for t in question.expected_tools if t in tools_used)
        tools_pass = tools_found >= len(question.expected_tools) * 0.5
        
        # Check confidence
        confidence_pass = confidence >= question.min_confidence
        
        passed = keywords_pass and tools_pass and confidence_pass
        
        result = {
            "id": question.id,
            "category": question.category,
            "question": question.question[:100],
            "passed": passed,
            "elapsed_ms": elapsed_ms,
            "confidence": confidence,
            "keywords_found": f"{keywords_found}/{len(question.expected_keywords)}",
            "tools_used": tools_used,
            "expected_tools": question.expected_tools,
            "sources": sources,
            "response_preview": data.get("response", "")[:200],
            "reasoning": data.get("reasoning", []),
        }
        
        if verbose:
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"\n{status} [{question.id}] {question.category}")
            print(f"  Q: {question.question[:80]}...")
            print(f"  Keywords: {keywords_found}/{len(question.expected_keywords)}, Tools: {tools_found}/{len(question.expected_tools)}")
            print(f"  Confidence: {confidence:.2f}, Time: {elapsed_ms}ms")
            if result.get("reasoning"):
                print(f"  Reasoning steps: {len(result['reasoning'])}")
                for step in result["reasoning"][:3]:
                    print(f"    {step['step']}. {step['action']}")
        
        return result
        
    except Exception as e:
        if verbose:
            print(f"\n❌ ERROR [{question.id}]: {str(e)}")
        return {
            "id": question.id,
            "category": question.category,
            "passed": False,
            "error": str(e),
        }


def run_all_tests(categories: Optional[List[str]] = None, verbose: bool = True) -> Dict:
    """Run all tests or specific categories."""
    questions = ALL_QUESTIONS
    if categories:
        questions = [q for q in questions if q.category in categories]
    
    print(f"\n{'='*60}")
    print(f"MA CHATBOT TEST SUITE")
    print(f"{'='*60}")
    print(f"Running {len(questions)} tests...")
    
    results = []
    for q in questions:
        result = run_test(q, verbose=verbose)
        results.append(result)
        time.sleep(1)  # Rate limiting
    
    # Summary
    passed = sum(1 for r in results if r.get("passed"))
    failed = len(results) - passed
    
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total: {len(results)} | Passed: {passed} | Failed: {failed}")
    print(f"Pass Rate: {passed/len(results)*100:.1f}%")
    
    # By category
    categories_seen = set(r["category"] for r in results)
    for cat in sorted(categories_seen):
        cat_results = [r for r in results if r["category"] == cat]
        cat_passed = sum(1 for r in cat_results if r.get("passed"))
        print(f"  {cat}: {cat_passed}/{len(cat_results)}")
    
    return {
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "pass_rate": passed / len(results) * 100,
        "results": results,
    }


# ===========================================================================
# PYTEST INTEGRATION
# ===========================================================================

def test_enrollment_queries():
    """Test enrollment-related queries."""
    for q in ENROLLMENT_QUESTIONS:
        result = run_test(q, verbose=False)
        assert result.get("passed"), f"Failed: {q.question}"


def test_star_ratings_queries():
    """Test star ratings queries."""
    for q in STAR_RATINGS_QUESTIONS:
        result = run_test(q, verbose=False)
        assert result.get("passed"), f"Failed: {q.question}"


def test_risk_score_queries():
    """Test risk score queries."""
    for q in RISK_SCORE_QUESTIONS:
        result = run_test(q, verbose=False)
        assert result.get("passed"), f"Failed: {q.question}"


def test_policy_questions():
    """Test policy/knowledge questions."""
    for q in POLICY_QUESTIONS:
        result = run_test(q, verbose=False)
        assert result.get("passed"), f"Failed: {q.question}"


# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run MA Chatbot tests")
    parser.add_argument("--category", "-c", help="Test specific category")
    parser.add_argument("--id", help="Test specific question ID")
    parser.add_argument("--quiet", "-q", action="store_true", help="Quiet mode")
    args = parser.parse_args()
    
    if args.id:
        # Single question
        question = next((q for q in ALL_QUESTIONS if q.id == args.id), None)
        if question:
            run_test(question, verbose=not args.quiet)
        else:
            print(f"Question ID not found: {args.id}")
    elif args.category:
        # Category
        run_all_tests(categories=[args.category], verbose=not args.quiet)
    else:
        # All
        run_all_tests(verbose=not args.quiet)
