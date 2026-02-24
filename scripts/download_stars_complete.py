#!/usr/bin/env python3
"""
Download ALL Star Ratings Data from CMS - COMPLETE
Includes: ratings, display measures, cut points, technical notes
Total files: ~25+ (2007-2026)
"""

import os
import subprocess
import json
from datetime import datetime
from pathlib import Path

S3_BUCKET = "ma-data123"
S3_PREFIX = "raw/stars"
LOCAL_TEMP = "/tmp/cms_downloads"
LOG_DIR = Path(__file__).parent.parent / "logs"

# ALL Stars URLs discovered from CMS
STARS_FILES = {
    # 2024-2026: Separate ratings and display measures
    "2026_ratings": "https://www.cms.gov/files/zip/2026-star-ratings-data-tables.zip",
    "2026_display": "https://www.cms.gov/files/zip/2026-display-measures.zip",
    "2025_ratings": "https://www.cms.gov/files/zip/2025-star-ratings-data-tables.zip",
    "2025_display": "https://www.cms.gov/files/zip/2025-display-measures.zip",
    "2024_ratings": "https://www.cms.gov/files/zip/2024-star-ratings-data-tables-jul-2-2024.zip",
    "2024_display": "https://www.cms.gov/files/zip/2024-display-measures.zip",

    # 2019-2023: Combined files
    "2023_combined": "https://www.cms.gov/files/zip/2023-star-ratings-and-display-measures.zip",
    "2022_combined": "https://www.cms.gov/files/zip/2022-star-ratings-and-display-measures.zip",
    "2021_combined": "https://www.cms.gov/files/zip/2021-star-ratings-and-display-measures.zip",
    "2020_combined": "https://www.cms.gov/files/zip/2020-star-ratings-and-display-measures.zip",
    "2019_combined": "https://www.cms.gov/files/zip/2019-star-ratings-and-display-measures.zip",

    # 2014-2018: Old path
    "2018_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2018-star-ratings-and-display-measures.zip",
    "2017_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2017_star_ratings_and_display_measures.zip",
    "2016_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2016_star_ratings_and_display_measures.zip",
    "2015_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2015_star_ratings_and_display_measures.zip",
    "2014_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2014_star_ratings_and_display_measures.zip",

    # 2010-2013: Plan ratings and display measures
    "2013_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2013_plan_ratings_and_display_measures.zip",
    "2012_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2012_plan_ratings_and_display_measures.zip",
    "2011_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2011_plan_ratings_and_display_measures.zip",
    "2010_combined": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2010_plan_ratings_and_display_measures.zip",

    # 2007-2009: Just plan ratings
    "2009_ratings": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2009_plan_ratings.zip",
    "2008_ratings": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2008_plan_ratings.zip",
    "2007_ratings": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/2007_plan_ratings.zip",

    # Special files
    "cut_point_trends": "https://www.cms.gov/files/zip/cut-point-trends.zip",
    "tukey_simulations": "https://www.cms.gov/files/zip/tukey-outlier-deletion-simulations.zip",
    "historical_cai": "https://www.cms.gov/medicare/prescription-drug-coverage/prescriptiondrugcovgenin/downloads/historical-categorical-adjustment-index-documents-.zip",
}

def check_url_exists(url: str) -> bool:
    result = subprocess.run(["curl", "-sI", url], capture_output=True, text=True)
    return "HTTP/2 200" in result.stdout or "HTTP/1.1 200" in result.stdout

def download_and_upload(name: str, url: str, dry_run: bool = False) -> dict:
    s3_key = f"{S3_PREFIX}/{name}.zip"
    result = {"name": name, "url": url, "s3_key": s3_key, "status": None, "timestamp": datetime.now().isoformat()}

    if dry_run:
        result["status"] = "available" if check_url_exists(url) else "not_found"
        return result

    os.makedirs(LOCAL_TEMP, exist_ok=True)
    local_file = f"{LOCAL_TEMP}/stars_{name}.zip"

    try:
        print(f"  Downloading {name}...")
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
    print("COMPLETE Star Ratings Data Download")
    print("=" * 60)

    files = list(STARS_FILES.items())
    print(f"Total files: {len(files)}, Mode: {'DRY RUN' if args.dry_run else 'DOWNLOAD'}")
    print("-" * 60)

    results = []
    success = 0

    for i, (name, url) in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Processing {name}")
        result = download_and_upload(name, url, args.dry_run)
        results.append(result)
        if result["status"] in ["success", "available"]:
            success += 1
            print(f"  ✓ {result['status']}")
        else:
            print(f"  ✗ {result['status']}")

    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"stars_complete_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump({"total": len(files), "success": success, "results": results}, f, indent=2)

    print(f"\nComplete: {success}/{len(files)} successful")

if __name__ == "__main__":
    main()
