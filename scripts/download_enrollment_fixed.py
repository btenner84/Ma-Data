#!/usr/bin/env python3
"""
Download MA Enrollment Files with CORRECT URLs

CMS changed their URL structure - this script uses the current format:
- Contract: https://www.cms.gov/files/zip/monthly-enrollment-contract-{month}-{year}.zip
- Plan: https://www.cms.gov/files/zip/monthly-enrollment-plan-{month}-{year}.zip
"""

import os
import subprocess
import json
import zipfile
from datetime import datetime
from pathlib import Path
import boto3
from io import BytesIO

S3_BUCKET = "ma-data123"
LOCAL_TEMP = "/tmp/cms_downloads"
LOG_DIR = Path(__file__).parent.parent / "logs"

MONTH_NAMES = {
    1: "january", 2: "february", 3: "march", 4: "april",
    5: "may", 6: "june", 7: "july", 8: "august",
    9: "september", 10: "october", 11: "november", 12: "december"
}

s3 = boto3.client('s3')


def get_urls(year: int, month: int) -> dict:
    """Get URLs for contract and plan files using NEW CMS format."""
    month_name = MONTH_NAMES[month]
    
    return {
        'contract': f"https://www.cms.gov/files/zip/monthly-enrollment-contract-{month_name}-{year}.zip",
        'plan': f"https://www.cms.gov/files/zip/monthly-enrollment-plan-{month_name}-{year}.zip"
    }


def check_url_exists(url: str) -> tuple:
    """Check if URL exists and returns a ZIP (not HTML error)."""
    result = subprocess.run(
        ["curl", "-sI", url], 
        capture_output=True, 
        text=True,
        timeout=30
    )
    headers = result.stdout.lower()
    
    is_200 = "http/2 200" in headers or "http/1.1 200" in headers
    is_zip = "application/zip" in headers
    
    return is_200, is_zip


def validate_zip(data: bytes) -> bool:
    """Validate that data is a proper ZIP file."""
    if len(data) < 100:
        return False
    if data[:2] != b'PK':
        return False
    try:
        with zipfile.ZipFile(BytesIO(data)) as zf:
            return len(zf.namelist()) > 0
    except:
        return False


def download_and_upload(year: int, month: int, file_type: str, dry_run: bool = False) -> dict:
    """Download file and upload to S3 with validation."""
    
    urls = get_urls(year, month)
    url = urls[file_type]
    month_str = f"{month:02d}"
    
    s3_key = f"raw/enrollment/by_{file_type}/{year}-{month_str}/enrollment_{file_type}_{year}_{month_str}.zip"
    
    result = {
        "year": year, 
        "month": month, 
        "type": file_type,
        "url": url, 
        "s3_key": s3_key, 
        "status": None,
        "timestamp": datetime.now().isoformat()
    }
    
    # Dry run - just check if URL exists
    if dry_run:
        is_200, is_zip = check_url_exists(url)
        if is_200 and is_zip:
            result["status"] = "available"
        elif is_200:
            result["status"] = "wrong_content_type"
        else:
            result["status"] = "not_found"
        return result
    
    try:
        # Download
        print(f"    Downloading...")
        proc = subprocess.run(
            ["curl", "-sL", url],
            capture_output=True,
            timeout=120
        )
        data = proc.stdout
        
        # Validate it's actually a ZIP
        if not validate_zip(data):
            result["status"] = "invalid_zip"
            result["size_bytes"] = len(data)
            return result
        
        # Upload to S3
        print(f"    Uploading to S3...")
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=data)
        
        result["status"] = "success"
        result["size_bytes"] = len(data)
        
    except subprocess.TimeoutExpired:
        result["status"] = "timeout"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
    
    return result


def generate_periods(start_year: int, end_year: int):
    """Generate all year-month pairs."""
    periods = []
    # CMS started this format around 2020
    for year in range(start_year, end_year + 1):
        end_month = 2 if year == 2026 else 12
        for month in range(1, end_month + 1):
            periods.append((year, month))
    return periods


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Just check URLs, don't download")
    parser.add_argument("--type", choices=["contract", "plan", "both"], default="both")
    parser.add_argument("--start-year", type=int, default=2020, help="Start year (default 2020 - older uses different URLs)")
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--year", type=int, help="Process single year only")
    parser.add_argument("--month", type=int, help="Process single month only (requires --year)")
    args = parser.parse_args()
    
    print("=" * 70)
    print("MA ENROLLMENT DOWNLOAD (FIXED URLs)")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'DOWNLOAD'}")
    print()
    
    # Generate periods
    if args.year and args.month:
        periods = [(args.year, args.month)]
    elif args.year:
        end_month = 2 if args.year == 2026 else 12
        periods = [(args.year, m) for m in range(1, end_month + 1)]
    else:
        periods = generate_periods(args.start_year, args.end_year)
    
    types = ["contract", "plan"] if args.type == "both" else [args.type]
    
    total = len(periods) * len(types)
    print(f"Files to process: {total}")
    print("-" * 70)
    
    results = []
    success = 0
    
    for i, (year, month) in enumerate(periods, 1):
        for file_type in types:
            print(f"[{len(results)+1}/{total}] {year}-{month:02d} {file_type}")
            
            result = download_and_upload(year, month, file_type, args.dry_run)
            results.append(result)
            
            status = result["status"]
            if status in ["success", "available"]:
                success += 1
                size = result.get("size_bytes", 0)
                print(f"    ✓ {status} ({size/1024:.1f} KB)")
            else:
                print(f"    ✗ {status}")
    
    # Save log
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"enrollment_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump({
            "total": total,
            "success": success,
            "mode": "dry_run" if args.dry_run else "download",
            "results": results
        }, f, indent=2)
    
    print()
    print("=" * 70)
    print(f"COMPLETE: {success}/{total} successful")
    print(f"Log saved: {log_file}")


if __name__ == "__main__":
    main()
