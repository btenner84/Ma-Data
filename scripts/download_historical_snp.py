#!/usr/bin/env python3
"""
Download historical SNP files from CMS that are currently corrupted/missing.
"""

import boto3
import requests
import time
from datetime import datetime

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

# Month name mapping for CMS URLs
MONTH_NAMES = {
    1: 'jan', 2: 'feb', 3: 'mar', 4: 'apr',
    5: 'may', 6: 'jun', 7: 'jul', 8: 'aug',
    9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec'
}


def download_snp_file(year: int, month: int) -> bool:
    """Download SNP file from CMS and upload to S3."""
    month_name = MONTH_NAMES[month]

    # CMS URL pattern
    url = f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{year}/{month_name}/snp-{year}-{month:02d}.zip"

    # Alternative URL patterns to try
    alt_urls = [
        f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{year}/{month_name}/snp_{year}_{month:02d}.zip",
        f"https://www.cms.gov/files/zip/snp-{year}-{month:02d}.zip",
        f"https://www.cms.gov/files/zip/snp_{year}_{month:02d}.zip",
    ]

    all_urls = [url] + alt_urls

    for try_url in all_urls:
        try:
            response = requests.get(try_url, timeout=60, allow_redirects=True)

            if response.status_code == 200 and response.content[:2] == b'PK':
                # Valid zip file - upload to S3
                s3_key = f"raw/snp/{year}-{month:02d}/snp_{year}_{month:02d}.zip"
                s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
                print(f"  {year}-{month:02d}: Downloaded {len(response.content)/1024:.1f} KB -> {s3_key}")
                return True
        except Exception as e:
            continue

    return False


def check_existing_file(year: int, month: int) -> bool:
    """Check if file already exists and is valid."""
    s3_key = f"raw/snp/{year}-{month:02d}/snp_{year}_{month:02d}.zip"
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        content = response['Body'].read(4)
        return content[:2] == b'PK'  # Valid zip signature
    except:
        return False


def main():
    print("=" * 60)
    print("DOWNLOADING HISTORICAL SNP FILES FROM CMS")
    print("=" * 60)

    downloaded = 0
    failed = []
    skipped = 0

    # Download from Dec 2007 through Dec 2019
    for year in range(2007, 2020):
        start_month = 12 if year == 2007 else 1
        end_month = 12

        for month in range(start_month, end_month + 1):
            # Check if already valid
            if check_existing_file(year, month):
                skipped += 1
                continue

            # Try to download
            if download_snp_file(year, month):
                downloaded += 1
            else:
                failed.append(f"{year}-{month:02d}")

            # Be nice to CMS servers
            time.sleep(0.5)

    print(f"\n{'=' * 60}")
    print(f"COMPLETE")
    print(f"  Downloaded: {downloaded}")
    print(f"  Skipped (already valid): {skipped}")
    print(f"  Failed: {len(failed)}")
    if failed:
        print(f"  Failed months: {failed[:20]}...")


if __name__ == '__main__':
    main()
