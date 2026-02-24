#!/usr/bin/env python3
"""
Download Supplemental MA Data to S3:
- MA Penetration (State/County)
- Service Area (Contract level)
- Benefits data
"""

import requests
import boto3
from datetime import datetime
import re

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

MONTHS = ['january', 'february', 'march', 'april', 'may', 'june',
          'july', 'august', 'september', 'october', 'november', 'december']


def download_and_upload(url, s3_key, min_size=5000):
    """Download file and upload to S3."""
    try:
        response = requests.get(url, timeout=120)
        if response.status_code == 200 and len(response.content) > min_size:
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
            print(f"  [SUCCESS] {len(response.content):,} bytes -> {s3_key}")
            return True
        return False
    except Exception as e:
        return False


def download_penetration_data():
    """Download MA State/County Penetration data."""
    print("\n=== DOWNLOADING MA PENETRATION DATA ===")

    success = 0
    # Download from 2008 to 2026
    for year in range(2008, 2027):
        for month_num, month_name in enumerate(MONTHS, 1):
            # Skip future months
            if year == 2026 and month_num > 2:
                continue

            # URL pattern discovered: /files/zip/ma-state-county-penetration-{month}-{year}.zip
            url = f"https://www.cms.gov/files/zip/ma-state-county-penetration-{month_name}-{year}.zip"
            s3_key = f"raw/penetration/{year}/{month_num:02d}/ma_penetration.zip"

            if download_and_upload(url, s3_key):
                success += 1

    print(f"  Downloaded: {success} penetration files")
    return success


def download_service_area_data():
    """Download MA Contract Service Area data."""
    print("\n=== DOWNLOADING SERVICE AREA DATA ===")

    success = 0
    # Try various URL patterns
    for year in range(2008, 2027):
        for month_num, month_name in enumerate(MONTHS, 1):
            if year == 2026 and month_num > 2:
                continue

            # Try different URL patterns
            patterns = [
                f"https://www.cms.gov/files/zip/ma-contract-service-area-state-county-{month_name}-{year}.zip",
                f"https://www.cms.gov/files/zip/ma-service-area-{month_name}-{year}.zip",
                f"https://www.cms.gov/files/zip/ma-contract-service-area-{year}-{month_num:02d}.zip",
            ]

            for url in patterns:
                s3_key = f"raw/service_area/{year}/{month_num:02d}/ma_service_area.zip"
                if download_and_upload(url, s3_key):
                    success += 1
                    break

    print(f"  Downloaded: {success} service area files")
    return success


def download_benefits_data():
    """Download MA Benefits data (annual)."""
    print("\n=== DOWNLOADING BENEFITS DATA ===")

    success = 0
    # Benefits data is typically annual
    for year in range(2010, 2027):
        patterns = [
            f"https://www.cms.gov/files/zip/cy{year}-ma-benefits-data.zip",
            f"https://www.cms.gov/files/zip/{year}-ma-benefits.zip",
            f"https://www.cms.gov/files/zip/ma-benefits-{year}.zip",
            f"https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Benefits{year}.zip",
        ]

        for url in patterns:
            s3_key = f"raw/benefits/{year}/ma_benefits_{year}.zip"
            if download_and_upload(url, s3_key):
                success += 1
                break

    print(f"  Downloaded: {success} benefits files")
    return success


def discover_service_area_urls():
    """Discover actual service area download URLs by fetching index pages."""
    print("\n=== DISCOVERING SERVICE AREA URLS ===")

    # Fetch the index page to find actual download links
    try:
        response = requests.get(
            "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/ma-contract-service-area-state/county",
            timeout=30
        )
        # Find all ZIP links
        zips = re.findall(r'href="(/files/zip/[^"]*service[^"]*\.zip)"', response.text, re.I)

        print(f"  Found {len(zips)} service area ZIP links")

        success = 0
        for zip_path in zips:
            url = f"https://www.cms.gov{zip_path}"

            # Extract year-month from filename
            match = re.search(r'(\d{4})', zip_path)
            if match:
                year = match.group(1)
                s3_key = f"raw/service_area/{year}/service_area.zip"
                if download_and_upload(url, s3_key):
                    success += 1

        return success
    except Exception as e:
        print(f"  Error: {e}")
        return 0


def main():
    print("=" * 60)
    print("DOWNLOADING SUPPLEMENTAL MA DATA")
    print("=" * 60)

    # Download penetration data
    pen_count = download_penetration_data()

    # Download service area data
    sa_count = download_service_area_data()
    if sa_count == 0:
        sa_count = discover_service_area_urls()

    # Download benefits data
    ben_count = download_benefits_data()

    print("\n" + "=" * 60)
    print(f"COMPLETE:")
    print(f"  Penetration files: {pen_count}")
    print(f"  Service area files: {sa_count}")
    print(f"  Benefits files: {ben_count}")
    print("=" * 60)


if __name__ == '__main__':
    main()
