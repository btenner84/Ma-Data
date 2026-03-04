#!/usr/bin/env python3
"""
Pipeline Orchestrator
=====================

Runs the full data pipeline from raw files to Gold layer tables.

Stages:
1. Silver Layer - Clean raw files
2. Gold Layer - Build star schema

Usage:
    python build_pipeline.py              # Full rebuild
    python build_pipeline.py --silver     # Silver only
    python build_pipeline.py --gold       # Gold only (requires silver)
    python build_pipeline.py --dry-run    # Show what would run
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent


SILVER_SCRIPTS = [
    "silver/build_silver_cpsc.py",
    "silver/build_silver_by_plan.py",
    "silver/build_silver_snp.py",
    "silver/build_silver_stars.py",
    "silver/build_silver_risk.py",
    "silver/build_silver_crosswalks.py",
]


GOLD_SCRIPTS_DIMENSIONS = [
    "gold/build_dim_time.py",
    "gold/build_dim_geography.py",
    "gold/build_dim_entity.py",
    "gold/build_dim_plan.py",
]


GOLD_SCRIPTS_FACTS = [
    "gold/build_fact_enrollment_national.py",
    "gold/build_fact_enrollment_geographic.py",
    "gold/build_fact_stars.py",
    "gold/build_fact_risk_scores.py",
]


def run_script(script_path: str, dry_run: bool = False, extra_args: list = None) -> bool:
    """Run a Python script and return success status."""
    full_path = SCRIPTS_DIR / script_path
    
    if not full_path.exists():
        print(f"  [SKIP] Script not found: {script_path}")
        return True
    
    cmd = [sys.executable, str(full_path)]
    if extra_args:
        cmd.extend(extra_args)
    
    if dry_run:
        print(f"  [DRY RUN] Would run: {' '.join(cmd)}")
        return True
    
    print(f"  Running: {script_path}")
    result = subprocess.run(cmd, cwd=str(SCRIPTS_DIR.parent))
    
    if result.returncode != 0:
        print(f"  [FAILED] Exit code: {result.returncode}")
        return False
    
    return True


def run_stage(name: str, scripts: list, dry_run: bool = False, extra_args: list = None) -> bool:
    """Run a stage of scripts."""
    print(f"\n{'=' * 70}")
    print(f"STAGE: {name}")
    print(f"{'=' * 70}")
    
    success_count = 0
    fail_count = 0
    
    for script in scripts:
        success = run_script(script, dry_run=dry_run, extra_args=extra_args)
        if success:
            success_count += 1
        else:
            fail_count += 1
    
    print(f"\n{name} Complete: {success_count} succeeded, {fail_count} failed")
    return fail_count == 0


def main():
    parser = argparse.ArgumentParser(description="Run the data pipeline")
    parser.add_argument("--silver", action="store_true", help="Run Silver layer only")
    parser.add_argument("--gold", action="store_true", help="Run Gold layer only")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run")
    parser.add_argument("--limit", type=int, help="Limit files per script")
    parser.add_argument("--year", type=int, help="Process single year only")
    args = parser.parse_args()
    
    print("=" * 70)
    print("MA DATA PLATFORM - PIPELINE ORCHESTRATOR")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'EXECUTE'}")
    
    extra_args = []
    if args.limit:
        extra_args.extend(["--limit", str(args.limit)])
    if args.year:
        extra_args.extend(["--year", str(args.year)])
    
    run_silver = args.silver or (not args.silver and not args.gold)
    run_gold = args.gold or (not args.silver and not args.gold)
    
    all_success = True
    
    if run_silver:
        success = run_stage("SILVER LAYER", SILVER_SCRIPTS, 
                           dry_run=args.dry_run, extra_args=extra_args)
        if not success:
            all_success = False
            if not args.dry_run:
                print("\nSilver layer failed. Fix errors before running Gold layer.")
    
    if run_gold:
        success = run_stage("GOLD LAYER - DIMENSIONS", GOLD_SCRIPTS_DIMENSIONS,
                           dry_run=args.dry_run)
        if not success:
            all_success = False
        
        success = run_stage("GOLD LAYER - FACTS", GOLD_SCRIPTS_FACTS,
                           dry_run=args.dry_run)
        if not success:
            all_success = False
    
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"Status: {'SUCCESS' if all_success else 'FAILED'}")
    print(f"Finished: {datetime.now()}")
    
    if not all_success:
        sys.exit(1)


if __name__ == "__main__":
    main()
