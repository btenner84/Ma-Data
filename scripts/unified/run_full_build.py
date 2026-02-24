#!/usr/bin/env python3
"""
Master Orchestrator - Full Unified Data Build

Runs all build scripts in the correct order:
1. Dimension tables (entity chains, parent orgs, geography)
2. Fact tables (unified enrollment, geographic, stars, risk scores)
3. Reconciliation and validation
4. Aggregation tables
5. Semantic layer generation

Usage:
    python run_full_build.py --all           # Run everything
    python run_full_build.py --dimensions    # Only dimension tables
    python run_full_build.py --facts         # Only fact tables
    python run_full_build.py --validate      # Only validation
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime
from pathlib import Path

# Script directory
SCRIPT_DIR = Path(__file__).parent

# Build order
BUILD_PHASES = {
    'dimensions': [
        ('build_entity_chains.py', 'Entity Chains (Crosswalk Integration)'),
        ('build_parent_org_dimension.py', 'Parent Organization Dimension'),
    ],
    'facts': [
        ('build_fact_enrollment_unified.py', 'Unified Enrollment Fact'),
        ('build_fact_enrollment_geographic.py', 'Geographic Enrollment Fact'),
    ],
    'validate': [
        ('reconcile_and_validate.py', 'Reconciliation & Validation'),
    ],
    'aggregations': [
        ('build_aggregation_tables.py', 'Aggregation Tables'),
    ],
    'catalog': [
        ('build_data_catalog.py', 'Data Catalog'),
        ('audit_lineage.py', 'Audit Lineage Setup'),
    ],
}


def run_script(script_name: str, description: str) -> bool:
    """Run a build script and return success status."""
    script_path = SCRIPT_DIR / script_name

    if not script_path.exists():
        print(f"  [ERROR] Script not found: {script_path}")
        return False

    print(f"\n{'='*70}")
    print(f"RUNNING: {description}")
    print(f"Script: {script_name}")
    print(f"{'='*70}")

    start = datetime.now()

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=False,
            text=True
        )

        elapsed = (datetime.now() - start).total_seconds()

        if result.returncode == 0:
            print(f"\n[SUCCESS] {description} completed in {elapsed:.1f}s")
            return True
        else:
            print(f"\n[FAILED] {description} failed after {elapsed:.1f}s")
            return False

    except Exception as e:
        print(f"\n[ERROR] {description}: {e}")
        return False


def run_phase(phase_name: str) -> bool:
    """Run all scripts in a phase."""
    if phase_name not in BUILD_PHASES:
        print(f"Unknown phase: {phase_name}")
        return False

    scripts = BUILD_PHASES[phase_name]
    print(f"\n{'#'*70}")
    print(f"# PHASE: {phase_name.upper()}")
    print(f"# Scripts: {len(scripts)}")
    print(f"{'#'*70}")

    success = True
    for script_name, description in scripts:
        if not run_script(script_name, description):
            success = False
            print(f"\n[ABORT] Phase {phase_name} failed at {script_name}")
            break

    return success


def main():
    parser = argparse.ArgumentParser(description='Run unified data build')
    parser.add_argument('--all', action='store_true', help='Run all phases')
    parser.add_argument('--dimensions', action='store_true', help='Run dimension builds')
    parser.add_argument('--facts', action='store_true', help='Run fact table builds')
    parser.add_argument('--validate', action='store_true', help='Run validation')
    parser.add_argument('--aggregations', action='store_true', help='Run aggregation builds')
    parser.add_argument('--catalog', action='store_true', help='Build data catalog')
    parser.add_argument('--continue-on-error', action='store_true', help='Continue even if a phase fails')
    args = parser.parse_args()

    # Default to all if nothing specified
    if not any([args.all, args.dimensions, args.facts, args.validate, args.aggregations, args.catalog]):
        args.all = True

    print("=" * 70)
    print("UNIFIED DATA BUILD ORCHESTRATOR")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print(f"Script directory: {SCRIPT_DIR}")

    phases_to_run = []
    if args.all:
        phases_to_run = ['dimensions', 'facts', 'validate', 'aggregations', 'catalog']
    else:
        if args.dimensions:
            phases_to_run.append('dimensions')
        if args.facts:
            phases_to_run.append('facts')
        if args.validate:
            phases_to_run.append('validate')
        if args.aggregations:
            phases_to_run.append('aggregations')
        if args.catalog:
            phases_to_run.append('catalog')

    print(f"Phases to run: {phases_to_run}")

    results = {}
    overall_success = True

    for phase in phases_to_run:
        success = run_phase(phase)
        results[phase] = success

        if not success:
            overall_success = False
            if not args.continue_on_error:
                print(f"\n[ABORT] Stopping due to failure in {phase}")
                break

    # Summary
    print("\n" + "=" * 70)
    print("BUILD SUMMARY")
    print("=" * 70)

    for phase, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        print(f"  {phase}: {status}")

    print(f"\nOverall: {'SUCCESS' if overall_success else 'FAILED'}")
    print(f"Finished: {datetime.now()}")

    sys.exit(0 if overall_success else 1)


if __name__ == '__main__':
    main()
