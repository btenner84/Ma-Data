#!/usr/bin/env python3
"""
Download CPSC (Contract/Plan/State/County) Enrollment Files from CMS
This is the BASE enrollment data - everything else joins to this.

Total files: 231 (Dec 2006 - Feb 2026)
"""

import os
import subprocess
import json
from datetime import datetime, date
from pathlib import Path

# Configuration
S3_BUCKET = "ma-data123"
S3_PREFIX = "raw/enrollment/cpsc"
LOCAL_TEMP = "/tmp/cms_downloads"
LOG_DIR = Path(__file__).parent.parent / "logs"
REGISTRY_DIR = Path(__file__).parent.parent / "registry"

# Month mappings
MONTH_NAMES = {
    1: "january", 2: "february", 3: "march", 4: "april",
    5: "may", 6: "june", 7: "july", 8: "august",
    9: "september", 10: "october", 11: "november", 12: "december"
}

MONTH_ABBR = {
    1: "jan", 2: "feb", 3: "mar", 4: "apr",
    5: "may", 6: "jun", 7: "jul", 8: "aug",
    9: "sep", 10: "oct", 11: "nov", 12: "dec"
}


def get_cpsc_url(year: int, month: int) -> str:
    """
    Generate the correct CMS URL based on year.
    Different URL patterns for different year ranges.
    """
    month_num = f"{month:02d}"
    month_name = MONTH_NAMES[month]
    month_abbr = MONTH_ABBR[month]

    # 2020-2026: New CMS URL pattern
    if year >= 2020:
        return f"https://www.cms.gov/files/zip/monthly-enrollment-cpsc-{month_name}-{year}.zip"

    # 2018-2019: downloads.cms.gov pattern
    elif year in [2018, 2019]:
        return f"https://downloads.cms.gov/files/cpsc_enrollment_{year}_{month_num}.zip"

    # 2017: Specific pattern with underscore
    elif year == 2017:
        return f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2017/{month_abbr}/cpsc_enrollment_2017_{month_num}.zip"

    # 2014-2016: Pattern with dashes
    elif year in [2014, 2015, 2016]:
        return f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{year}/{month_abbr}/cpsc-enrollment-{year}-{month_num}.zip"

    # 2013: Special pattern with month-year folder
    elif year == 2013:
        return f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2013/{month_abbr}-2013/cpsc-enrollment-2013-{month_num}.zip"

    # 2007 May-Dec and 2008-2012: Simple year folder pattern
    elif (year == 2007 and month >= 5) or year in [2008, 2009, 2010, 2011, 2012]:
        return f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{year}/cpsc-enrollment-{year}-{month_num}.zip"

    # 2006 Dec - 2007 Apr: Archive pattern
    elif year == 2006 or (year == 2007 and month <= 4):
        return f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/cpsc-enrollment-files-jul2006-apr2008/cpsc-enrollment-{year}-{month_num}.zip"

    else:
        raise ValueError(f"Unknown year pattern: {year}")


def generate_all_periods() -> list:
    """
    Generate all year-month periods from Dec 2006 to Feb 2026.
    """
    periods = []

    # Start from Dec 2006
    start_year, start_month = 2006, 12
    end_year, end_month = 2026, 2

    year, month = start_year, start_month
    while (year < end_year) or (year == end_year and month <= end_month):
        periods.append((year, month))

        month += 1
        if month > 12:
            month = 1
            year += 1

    return periods


def check_url_exists(url: str) -> bool:
    """Check if URL returns 200 status."""
    result = subprocess.run(
        ["curl", "-sI", url],
        capture_output=True,
        text=True
    )
    return "HTTP/2 200" in result.stdout or "HTTP/1.1 200" in result.stdout


def download_and_upload(year: int, month: int, dry_run: bool = False) -> dict:
    """
    Download file from CMS and upload to S3.
    Returns status dict.
    """
    url = get_cpsc_url(year, month)
    month_num = f"{month:02d}"
    s3_key = f"{S3_PREFIX}/{year}-{month_num}/cpsc_enrollment_{year}_{month_num}.zip"

    result = {
        "year": year,
        "month": month,
        "url": url,
        "s3_key": s3_key,
        "status": None,
        "timestamp": datetime.now().isoformat()
    }

    if dry_run:
        # Just check if URL exists
        exists = check_url_exists(url)
        result["status"] = "available" if exists else "not_found"
        return result

    # Create temp directory
    os.makedirs(LOCAL_TEMP, exist_ok=True)
    local_file = f"{LOCAL_TEMP}/cpsc_{year}_{month_num}.zip"

    try:
        # Download from CMS
        print(f"  Downloading {year}-{month_num}...")
        dl_result = subprocess.run(
            ["curl", "-sL", "-o", local_file, url],
            capture_output=True,
            text=True
        )

        if not os.path.exists(local_file) or os.path.getsize(local_file) < 1000:
            result["status"] = "download_failed"
            return result

        # Upload to S3
        print(f"  Uploading to s3://{S3_BUCKET}/{s3_key}...")
        s3_result = subprocess.run(
            ["aws", "s3", "cp", local_file, f"s3://{S3_BUCKET}/{s3_key}"],
            capture_output=True,
            text=True
        )

        if s3_result.returncode == 0:
            result["status"] = "success"
            result["size_bytes"] = os.path.getsize(local_file)
        else:
            result["status"] = "upload_failed"
            result["error"] = s3_result.stderr

        # Clean up
        os.remove(local_file)

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Download CPSC enrollment data from CMS")
    parser.add_argument("--dry-run", action="store_true", help="Check URLs without downloading")
    parser.add_argument("--year", type=int, help="Download specific year only")
    parser.add_argument("--start-year", type=int, default=2006, help="Start year")
    parser.add_argument("--end-year", type=int, default=2026, help="End year")
    parser.add_argument("--limit", type=int, help="Limit number of downloads")
    args = parser.parse_args()

    print("=" * 60)
    print("CPSC Enrollment Data Download")
    print("=" * 60)

    periods = generate_all_periods()

    # Filter by year if specified
    if args.year:
        periods = [(y, m) for y, m in periods if y == args.year]
    else:
        periods = [(y, m) for y, m in periods if args.start_year <= y <= args.end_year]

    if args.limit:
        periods = periods[:args.limit]

    print(f"Total periods to process: {len(periods)}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'DOWNLOAD'}")
    print("-" * 60)

    results = []
    success_count = 0

    for i, (year, month) in enumerate(periods, 1):
        print(f"[{i}/{len(periods)}] Processing {year}-{month:02d}")
        result = download_and_upload(year, month, dry_run=args.dry_run)
        results.append(result)

        if result["status"] in ["success", "available"]:
            success_count += 1
            print(f"  ✓ {result['status']}")
        else:
            print(f"  ✗ {result['status']}: {result.get('error', 'unknown')}")

    # Save results log
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"cpsc_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump({
            "run_type": "dry_run" if args.dry_run else "download",
            "total_processed": len(periods),
            "success_count": success_count,
            "results": results
        }, f, indent=2)

    print("-" * 60)
    print(f"Complete: {success_count}/{len(periods)} successful")
    print(f"Log saved to: {log_file}")


if __name__ == "__main__":
    main()
