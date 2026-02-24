#!/usr/bin/env python3
"""
Download Monthly MA Enrollment by Contract from CMS
Total files: ~220 (Jan 2007 - Feb 2026)
"""

import os
import subprocess
import json
from datetime import datetime
from pathlib import Path

S3_BUCKET = "ma-data123"
S3_PREFIX = "raw/enrollment/by_contract"
LOCAL_TEMP = "/tmp/cms_downloads"
LOG_DIR = Path(__file__).parent.parent / "logs"

MONTH_NAMES = {
    1: "january", 2: "february", 3: "march", 4: "april",
    5: "may", 6: "june", 7: "july", 8: "august",
    9: "september", 10: "october", 11: "november", 12: "december"
}

def get_url(year: int, month: int) -> str:
    """Generate URL based on year."""
    month_name = MONTH_NAMES[month]
    month_num = f"{month:02d}"

    # 2020+: New CMS pattern
    if year >= 2020:
        return f"https://www.cms.gov/files/zip/monthly-contract-enrollment-{month_name}-{year}.zip"
    # 2018-2019: downloads.cms.gov
    elif year in [2018, 2019]:
        return f"https://downloads.cms.gov/files/ma-contract-enrollment-{year}-{month_num}.zip"
    # Older years
    else:
        return f"https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/Downloads/MA-Contract-Enrollment-{year}-{month_num}.zip"

def generate_periods():
    periods = []
    year, month = 2007, 1
    end_year, end_month = 2026, 2
    while (year < end_year) or (year == end_year and month <= end_month):
        periods.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return periods

def check_url_exists(url: str) -> bool:
    result = subprocess.run(["curl", "-sI", url], capture_output=True, text=True)
    return "HTTP/2 200" in result.stdout or "HTTP/1.1 200" in result.stdout

def download_and_upload(year: int, month: int, dry_run: bool = False) -> dict:
    url = get_url(year, month)
    month_num = f"{month:02d}"
    s3_key = f"{S3_PREFIX}/{year}-{month_num}/enrollment_contract_{year}_{month_num}.zip"

    result = {"year": year, "month": month, "url": url, "s3_key": s3_key, "status": None, "timestamp": datetime.now().isoformat()}

    if dry_run:
        result["status"] = "available" if check_url_exists(url) else "not_found"
        return result

    os.makedirs(LOCAL_TEMP, exist_ok=True)
    local_file = f"{LOCAL_TEMP}/enrollment_contract_{year}_{month_num}.zip"

    try:
        print(f"  Downloading {year}-{month_num}...")
        subprocess.run(["curl", "-sL", "-o", local_file, url], capture_output=True)

        if not os.path.exists(local_file) or os.path.getsize(local_file) < 1000:
            result["status"] = "download_failed"
            return result

        print(f"  Uploading to s3://{S3_BUCKET}/{s3_key}...")
        s3_result = subprocess.run(["aws", "s3", "cp", local_file, f"s3://{S3_BUCKET}/{s3_key}"], capture_output=True, text=True)

        result["status"] = "success" if s3_result.returncode == 0 else "upload_failed"
        if result["status"] == "success":
            result["size_bytes"] = os.path.getsize(local_file)
        os.remove(local_file)
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--start-year", type=int, default=2007)
    parser.add_argument("--end-year", type=int, default=2026)
    args = parser.parse_args()

    print("=" * 60)
    print("MA Enrollment by Contract Download")
    print("=" * 60)

    periods = [(y, m) for y, m in generate_periods() if args.start_year <= y <= args.end_year]
    print(f"Total periods: {len(periods)}, Mode: {'DRY RUN' if args.dry_run else 'DOWNLOAD'}")
    print("-" * 60)

    results = []
    success = 0

    for i, (year, month) in enumerate(periods, 1):
        print(f"[{i}/{len(periods)}] Processing {year}-{month:02d}")
        result = download_and_upload(year, month, args.dry_run)
        results.append(result)
        if result["status"] in ["success", "available"]:
            success += 1
            print(f"  ✓ {result['status']}")
        else:
            print(f"  ✗ {result['status']}")

    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"enrollment_contract_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump({"total": len(periods), "success": success, "results": results}, f, indent=2)

    print(f"\nComplete: {success}/{len(periods)} successful")

if __name__ == "__main__":
    main()
