#!/usr/bin/env python3
"""
Download HEDIS Historical Data from CMS (2010-2019)
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

# Historical HEDIS URLs
HEDIS_HISTORICAL = {
    2019: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2019/2019-hedis-patient-level-data-submission-instructions.zip",
    2018: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2018/2018-hedis-patient-level-data-submission-instructions.zip",
    2017: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2017/2017-hedis-patient-level-data-submission-instructions.zip",
    2016: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2016/2016-hedis-patient-level-data-submission-instructions.zip",
    2015: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2015/2015-hedis-patient-level-data-submission-instructions.zip",
    2014: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2014/2014-hedis-patient-level-data-submission-instructions.zip",
    2013: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2013/2013-hedis-patient-level-data-submission-instructions.zip",
    2012: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2012/2012-hedis-patient-level-data-submission-instructions.zip",
    2011: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2011/2011-hedis-patient-level-data-submission-instructions.zip",
    2010: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2010/2010-hedis-patient-level-data-submission-instructions.zip",
}

def download_and_upload(year: int, url: str) -> dict:
    s3_key = f"{S3_PREFIX}/hedis_{year}.zip"
    result = {"year": year, "url": url, "s3_key": s3_key, "status": None, "timestamp": datetime.now().isoformat()}

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
    print("=" * 60)
    print("HEDIS Historical Data Download (2010-2019)")
    print("=" * 60)

    years = sorted(HEDIS_HISTORICAL.keys())
    print(f"Total files: {len(years)}")
    print("-" * 60)

    results = []
    success = 0

    for i, year in enumerate(years, 1):
        print(f"[{i}/{len(years)}] Processing {year}")
        result = download_and_upload(year, HEDIS_HISTORICAL[year])
        results.append(result)
        if result["status"] == "success":
            success += 1
            print(f"  ✓ {result['status']}")
        else:
            print(f"  ✗ {result['status']}")

    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"hedis_historical_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump({"total": len(years), "success": success, "results": results}, f, indent=2)

    print(f"\nComplete: {success}/{len(years)} successful")

if __name__ == "__main__":
    main()
