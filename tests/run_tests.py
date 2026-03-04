#!/usr/bin/env python3
"""
Master Test Runner
==================

Runs all test suites for the MA Data Platform:
1. Unit tests (data service, validation)
2. Integration tests (API endpoints)
3. Audit trail tests
4. Data validation (MECE) tests

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py --unit             # Run only unit tests
    python run_tests.py --integration      # Run only integration tests
    python run_tests.py --audit            # Run only audit tests
    python run_tests.py --validation       # Run only validation tests
    python run_tests.py --coverage         # Run with coverage report
    python run_tests.py --verbose          # Verbose output
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime
from pathlib import Path

TESTS_DIR = Path(__file__).parent
PROJECT_ROOT = TESTS_DIR.parent


def run_command(cmd: list, description: str) -> tuple:
    """Run a command and capture output."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"{'='*60}")
    
    start = datetime.now()
    result = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=False,
        text=True
    )
    elapsed = (datetime.now() - start).total_seconds()
    
    return result.returncode, elapsed


def run_pytest(test_path: str, description: str, extra_args: list = None) -> tuple:
    """Run pytest on a specific path."""
    cmd = [sys.executable, '-m', 'pytest', test_path, '-v']
    if extra_args:
        cmd.extend(extra_args)
    
    return run_command(cmd, description)


def run_unit_tests(verbose: bool = False) -> tuple:
    """Run unit tests."""
    args = ['-vv'] if verbose else []
    return run_pytest(
        str(TESTS_DIR / 'unit'),
        'Unit Tests (Data Service)',
        args
    )


def run_integration_tests(verbose: bool = False) -> tuple:
    """Run integration tests."""
    args = ['-vv'] if verbose else []
    return run_pytest(
        str(TESTS_DIR / 'integration'),
        'Integration Tests (API Endpoints)',
        args
    )


def run_audit_tests(verbose: bool = False) -> tuple:
    """Run audit trail tests."""
    args = ['-vv'] if verbose else []
    return run_pytest(
        str(TESTS_DIR / 'audit'),
        'Audit Trail Tests',
        args
    )


def run_validation_tests(verbose: bool = False) -> tuple:
    """Run gold layer validation."""
    args = ['--output', str(PROJECT_ROOT / 'data' / 'validation_results.json')]
    return run_command(
        [sys.executable, str(PROJECT_ROOT / 'scripts' / 'gold' / 'validate_gold_layer.py')] + args,
        'Data Validation (MECE Checks)'
    )


def run_ui_tests() -> tuple:
    """Run UI component tests."""
    return run_command(
        ['npm', 'test', '--', '--watchAll=false'],
        'UI Component Tests'
    )


def run_all_tests(coverage: bool = False, verbose: bool = False) -> dict:
    """Run all test suites."""
    results = {
        'passed': [],
        'failed': [],
        'skipped': [],
        'total_time': 0
    }
    
    print(f"\n{'#'*60}")
    print("MA DATA PLATFORM - COMPREHENSIVE TEST SUITE")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"{'#'*60}")
    
    coverage_args = ['--cov=api', '--cov-report=html'] if coverage else []
    
    tests = [
        ('unit', lambda: run_pytest(
            str(TESTS_DIR / 'unit'),
            'Unit Tests',
            ['-vv' if verbose else '-v'] + coverage_args
        )),
        ('integration', lambda: run_pytest(
            str(TESTS_DIR / 'integration'),
            'Integration Tests',
            ['-vv' if verbose else '-v']
        )),
        ('audit', lambda: run_pytest(
            str(TESTS_DIR / 'audit'),
            'Audit Trail Tests',
            ['-vv' if verbose else '-v']
        )),
    ]
    
    for name, test_fn in tests:
        try:
            code, elapsed = test_fn()
            results['total_time'] += elapsed
            
            if code == 0:
                results['passed'].append(name)
            elif code == 5:
                results['skipped'].append(name)
            else:
                results['failed'].append(name)
        except Exception as e:
            print(f"Error running {name}: {e}")
            results['failed'].append(name)
    
    return results


def print_summary(results: dict):
    """Print test summary."""
    print(f"\n{'#'*60}")
    print("TEST SUMMARY")
    print(f"{'#'*60}")
    
    print(f"\nTotal Time: {results['total_time']:.2f}s")
    print(f"\nPassed ({len(results['passed'])}): {', '.join(results['passed']) or 'None'}")
    print(f"Failed ({len(results['failed'])}): {', '.join(results['failed']) or 'None'}")
    print(f"Skipped ({len(results['skipped'])}): {', '.join(results['skipped']) or 'None'}")
    
    total = len(results['passed']) + len(results['failed'])
    success_rate = len(results['passed']) / total * 100 if total > 0 else 0
    
    print(f"\nSuccess Rate: {success_rate:.1f}%")
    
    if results['failed']:
        print("\n⚠️  SOME TESTS FAILED")
        return 1
    else:
        print("\n✓ ALL TESTS PASSED")
        return 0


def main():
    parser = argparse.ArgumentParser(description='MA Data Platform Test Runner')
    parser.add_argument('--unit', action='store_true', help='Run unit tests only')
    parser.add_argument('--integration', action='store_true', help='Run integration tests only')
    parser.add_argument('--audit', action='store_true', help='Run audit tests only')
    parser.add_argument('--validation', action='store_true', help='Run validation tests only')
    parser.add_argument('--ui', action='store_true', help='Run UI tests only')
    parser.add_argument('--coverage', action='store_true', help='Generate coverage report')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.unit:
        code, _ = run_unit_tests(args.verbose)
        sys.exit(code)
    
    if args.integration:
        code, _ = run_integration_tests(args.verbose)
        sys.exit(code)
    
    if args.audit:
        code, _ = run_audit_tests(args.verbose)
        sys.exit(code)
    
    if args.validation:
        code, _ = run_validation_tests(args.verbose)
        sys.exit(code)
    
    if args.ui:
        code, _ = run_ui_tests()
        sys.exit(code)
    
    results = run_all_tests(coverage=args.coverage, verbose=args.verbose)
    exit_code = print_summary(results)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
