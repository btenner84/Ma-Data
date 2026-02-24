#!/usr/bin/env python3
"""
Download Plan Crosswalk Files from CMS
CRITICAL for longitudinal analysis - maps plan IDs across years.

Total files: 21 (2006-2026)
"""

import os
import subprocess
import json
from datetime import datetime
from pathlib import Path

# Configuration
S3_BUCKET = "ma-data123"
S3_PREFIX = "raw/crosswalks"
LOCAL_TEMP = "/tmp/cms_downloads"
LOG_DIR = Path(__file__).parent.parent / "logs"

# Crosswalk URLs by year (verified working)
# Each file maps plans from year N-1 to year N
CROSSWALK_URLS = {
    # New pattern (2022+): www.cms.gov/files/zip/plan-crosswalk-YYYY.zip
    2026: "https://www.cms.gov/files/zip/plan-crosswalk-2026.zip",
    2025: "https://www.cms.gov/files/zip/plan-crosswalk-2025.zip",
    2024: "https://www.cms.gov/files/zip/plan-crosswalk-2024.zip",
    2023: "https://www.cms.gov/files/zip/plan-crosswalk-2023.zip",
    2022: "https://www.cms.gov/files/zip/plan-crosswalk-2022.zip",
    # Older files need to be discovered via CMS pages - URLs vary
    # Will be populated as discovered
}


def check_url_exists(url: str) -> bool:
    """Check if URL returns 200 status."""
    result = subprocess.run(
        ["curl", "-sI", url],
        capture_output=True,
        text=True
    )
    return "HTTP/2 200" in result.stdout or "HTTP/1.1 200" in result.stdout


def download_and_upload(year: int, url: str, dry_run: bool = False) -> dict:
    """Download file from CMS and upload to S3."""
    s3_key = f"{S3_PREFIX}/crosswalk_{year-1}_to_{year}.zip"

    result = {
        "year": year,
        "maps": f"{year-1} → {year}",
        "url": url,
        "s3_key": s3_key,
        "status": None,
        "timestamp": datetime.now().isoformat()
    }

    if dry_run:
        exists = check_url_exists(url)
        result["status"] = "available" if exists else "not_found"
        return result

    os.makedirs(LOCAL_TEMP, exist_ok=True)
    local_file = f"{LOCAL_TEMP}/crosswalk_{year}.zip"

    try:
        print(f"  Downloading crosswalk {year-1}→{year}...")
        dl_result = subprocess.run(
            ["curl", "-sL", "-o", local_file, url],
            capture_output=True,
            text=True
        )

        if not os.path.exists(local_file) or os.path.getsize(local_file) < 1000:
            result["status"] = "download_failed"
            return result

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

        os.remove(local_file)

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Download Plan Crosswalk files from CMS")
    parser.add_argument("--dry-run", action="store_true", help="Check URLs without downloading")
    parser.add_argument("--year", type=int, help="Download specific year only")
    parser.add_argument("--start-year", type=int, default=2006, help="Start year")
    parser.add_argument("--end-year", type=int, default=2026, help="End year")
    args = parser.parse_args()

    print("=" * 60)
    print("Plan Crosswalk Download")
    print("=" * 60)

    years = sorted([y for y in CROSSWALK_URLS.keys()
                   if args.start_year <= y <= args.end_year])

    if args.year:
        years = [args.year] if args.year in CROSSWALK_URLS else []

    print(f"Total files to process: {len(years)}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'DOWNLOAD'}")
    print("-" * 60)

    results = []
    success_count = 0

    for i, year in enumerate(years, 1):
        url = CROSSWALK_URLS[year]
        print(f"[{i}/{len(years)}] Processing {year-1}→{year} crosswalk")
        result = download_and_upload(year, url, dry_run=args.dry_run)
        results.append(result)

        if result["status"] in ["success", "available"]:
            success_count += 1
            print(f"  ✓ {result['status']}")
        else:
            print(f"  ✗ {result['status']}")

    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"crosswalk_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump({
            "run_type": "dry_run" if args.dry_run else "download",
            "total_processed": len(years),
            "success_count": success_count,
            "results": results
        }, f, indent=2)

    print("-" * 60)
    print(f"Complete: {success_count}/{len(years)} successful")
    print(f"Log saved to: {log_file}")


if __name__ == "__main__":
    main()
