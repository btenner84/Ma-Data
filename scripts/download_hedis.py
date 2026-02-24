#!/usr/bin/env python3
"""
Download HEDIS Patient Level Data Submission Instructions from CMS
Total files: ~17 (2010-2026)
"""

import os
import subprocess
import json
from datetime import datetime
from pathlib import Path

S3_BUCKET = "ma-data123"
S3_PREFIX = "raw/hedis"
LOCAL_TEMP = "/tmp/cms_downloads"
LOG_DIR = Path(__file__).parent.parent / "logs"

# HEDIS URLs - verified from CMS page
HEDIS_URLS = {
    2026: "https://www.cms.gov/files/zip/hedis-patient-level-data-submission-instructions-2026.zip",
    2025: "https://www.cms.gov/files/zip/hedis-patient-level-data-submission-instructions-2025.zip",
    2024: "https://www.cms.gov/files/zip/hedis-patient-level-data-submission-instructions-2024.zip",
    2023: "https://www.cms.gov/files/zip/hedis-patient-level-data-submission-instructions-2023.zip",
    2022: "https://www.cms.gov/files/zip/hedis-patient-level-data-submission-instructions-2022.zip",
    2021: "https://www.cms.gov/files/zip/hedis-patient-level-data-submission-instructions-2021.zip",
    2020: "https://www.cms.gov/files/zip/hedis-patient-level-data-submission-instructions-2020.zip",
}

def check_url_exists(url: str) -> bool:
    result = subprocess.run(["curl", "-sI", url], capture_output=True, text=True)
    return "HTTP/2 200" in result.stdout or "HTTP/1.1 200" in result.stdout

def download_and_upload(year: int, url: str, dry_run: bool = False) -> dict:
    s3_key = f"{S3_PREFIX}/hedis_{year}.zip"
    result = {"year": year, "url": url, "s3_key": s3_key, "status": None, "timestamp": datetime.now().isoformat()}

    if dry_run:
        result["status"] = "available" if check_url_exists(url) else "not_found"
        return result

    os.makedirs(LOCAL_TEMP, exist_ok=True)
    local_file = f"{LOCAL_TEMP}/hedis_{year}.zip"

    try:
        print(f"  Downloading {year} HEDIS data...")
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
    args = parser.parse_args()

    print("=" * 60)
    print("HEDIS Data Download")
    print("=" * 60)

    years = sorted(HEDIS_URLS.keys())
    print(f"Total files: {len(years)}, Mode: {'DRY RUN' if args.dry_run else 'DOWNLOAD'}")
    print("-" * 60)

    results = []
    success = 0

    for i, year in enumerate(years, 1):
        print(f"[{i}/{len(years)}] Processing {year}")
        result = download_and_upload(year, HEDIS_URLS[year], args.dry_run)
        results.append(result)
        if result["status"] in ["success", "available"]:
            success += 1
            print(f"  ✓ {result['status']}")
        else:
            print(f"  ✗ {result['status']}")

    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"hedis_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump({"total": len(years), "success": success, "results": results}, f, indent=2)

    print(f"\nComplete: {success}/{len(years)} successful")

if __name__ == "__main__":
    main()
