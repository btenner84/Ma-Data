#!/usr/bin/env python3
"""
Fix All Enrollment Downloads

Re-downloads broken files using correct URL patterns:
- CPSC: 2018+ (2013-2017 no longer available from CMS)
- By Contract: 2020+ 
- By Plan: 2020+

Validates files are actual ZIPs (not HTML error pages)
"""

import boto3
import subprocess
import json
import zipfile
from datetime import datetime
from pathlib import Path
from io import BytesIO

S3_BUCKET = "ma-data123"
LOCAL_TEMP = "/tmp/cms_downloads"
LOG_DIR = Path(__file__).parent.parent / "logs"

MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]

s3 = boto3.client('s3')


def get_cpsc_url(year: int, month: int) -> str:
    """Get CPSC URL with correct pattern per year."""
    month_num = f"{month:02d}"
    month_name = MONTH_NAMES[month - 1]
    
    if year >= 2020:
        return f"https://www.cms.gov/files/zip/monthly-enrollment-cpsc-{month_name}-{year}.zip"
    elif year in [2018, 2019]:
        return f"https://downloads.cms.gov/files/cpsc_enrollment_{year}_{month_num}.zip"
    else:
        return None  # Historical data no longer available


def get_contract_url(year: int, month: int) -> str:
    """Get contract enrollment URL (2020+ only)."""
    if year < 2020:
        return None
    month_name = MONTH_NAMES[month - 1]
    return f"https://www.cms.gov/files/zip/monthly-enrollment-contract-{month_name}-{year}.zip"


def get_plan_url(year: int, month: int) -> str:
    """Get plan enrollment URL (2020+ only)."""
    if year < 2020:
        return None
    month_name = MONTH_NAMES[month - 1]
    return f"https://www.cms.gov/files/zip/monthly-enrollment-plan-{month_name}-{year}.zip"


def check_existing_file(key: str) -> str:
    """Check if S3 file exists and is valid ZIP."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key, Range='bytes=0-10')
        data = resp['Body'].read()
        if data[:2] == b'PK':
            return "valid"
        else:
            return "broken"
    except s3.exceptions.NoSuchKey:
        return "missing"
    except Exception:
        return "error"


def validate_zip(data: bytes) -> bool:
    """Check if data is valid ZIP."""
    if len(data) < 100 or data[:2] != b'PK':
        return False
    try:
        with zipfile.ZipFile(BytesIO(data)) as zf:
            return len(zf.namelist()) > 0
    except:
        return False


def download_file(url: str) -> tuple:
    """Download file, return (data, status)."""
    try:
        result = subprocess.run(
            ["curl", "-sL", url],
            capture_output=True,
            timeout=120
        )
        data = result.stdout
        
        if validate_zip(data):
            return data, "success"
        elif b'<!DOCTYPE' in data or b'<html' in data.lower():
            return None, "html_error"
        else:
            return None, "invalid"
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except Exception as e:
        return None, f"error: {e}"


def upload_to_s3(data: bytes, key: str):
    """Upload data to S3."""
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data)


def audit_and_fix(file_type: str, start_year: int, end_year: int, dry_run: bool = False):
    """Audit existing files and fix broken ones."""
    
    url_func = {
        'cpsc': get_cpsc_url,
        'contract': get_contract_url,
        'plan': get_plan_url
    }[file_type]
    
    prefix = {
        'cpsc': 'raw/enrollment/cpsc',
        'contract': 'raw/enrollment/by_contract',
        'plan': 'raw/enrollment/by_plan'
    }[file_type]
    
    results = {
        'valid': [],
        'fixed': [],
        'failed': [],
        'unavailable': []
    }
    
    for year in range(start_year, end_year + 1):
        max_month = 2 if year == 2026 else 12
        
        for month in range(1, max_month + 1):
            ym = f"{year}-{month:02d}"
            
            # Get S3 key
            if file_type == 'cpsc':
                s3_key = f"{prefix}/{ym}/cpsc_enrollment_{year}_{month:02d}.zip"
            else:
                s3_key = f"{prefix}/{ym}/enrollment_{file_type}_{year}_{month:02d}.zip"
            
            # Check existing file
            status = check_existing_file(s3_key)
            
            if status == "valid":
                results['valid'].append(ym)
                continue
            
            # Need to download
            url = url_func(year, month)
            
            if url is None:
                results['unavailable'].append(ym)
                print(f"  {ym}: unavailable (CMS removed historical data)")
                continue
            
            print(f"  {ym}: {status} -> downloading...")
            
            if dry_run:
                results['fixed'].append(ym)
                continue
            
            # Download and upload
            data, dl_status = download_file(url)
            
            if dl_status == "success":
                upload_to_s3(data, s3_key)
                results['fixed'].append(ym)
                print(f"       ✓ fixed ({len(data)/1024:.1f} KB)")
            else:
                results['failed'].append((ym, dl_status))
                print(f"       ✗ {dl_status}")
    
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--type", choices=["cpsc", "contract", "plan", "all"], default="all")
    args = parser.parse_args()
    
    print("=" * 70)
    print("FIX ALL ENROLLMENT DOWNLOADS")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'FIX'}")
    print()
    
    all_results = {}
    
    types_to_process = ["cpsc", "contract", "plan"] if args.type == "all" else [args.type]
    
    for file_type in types_to_process:
        print(f"\n{'='*70}")
        print(f"Processing: {file_type.upper()}")
        print(f"{'='*70}")
        
        # CPSC: try from 2018 (2013-2017 unavailable)
        # Contract/Plan: try from 2020
        start_year = 2018 if file_type == "cpsc" else 2020
        
        results = audit_and_fix(file_type, start_year, 2026, args.dry_run)
        all_results[file_type] = results
        
        print()
        print(f"  Valid (unchanged): {len(results['valid'])}")
        print(f"  Fixed: {len(results['fixed'])}")
        print(f"  Failed: {len(results['failed'])}")
        print(f"  Unavailable (CMS removed): {len(results['unavailable'])}")
    
    # Save log
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"fix_enrollment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Convert tuples to strings for JSON
    json_results = {}
    for ft, res in all_results.items():
        json_results[ft] = {
            'valid': res['valid'],
            'fixed': res['fixed'],
            'failed': [f"{ym}: {status}" for ym, status in res['failed']],
            'unavailable': res['unavailable']
        }
    
    with open(log_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "mode": "dry_run" if args.dry_run else "fix",
            "results": json_results
        }, f, indent=2)
    
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    for ft, res in all_results.items():
        total = len(res['valid']) + len(res['fixed']) + len(res['failed']) + len(res['unavailable'])
        success = len(res['valid']) + len(res['fixed'])
        print(f"{ft.upper()}: {success}/{total} available ({len(res['unavailable'])} removed by CMS)")
    
    print()
    print(f"Log: {log_file}")


if __name__ == "__main__":
    main()
