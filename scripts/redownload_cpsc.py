#!/usr/bin/env python3
"""
Re-download failed CPSC files.
"""

import requests
import boto3
from io import BytesIO

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

# Failed months to re-download
FAILED_MONTHS = [
    (2013, 2), (2013, 3), (2013, 4), (2013, 5), (2013, 6),
    (2013, 7), (2013, 8), (2013, 9), (2013, 10), (2013, 11), (2013, 12),
    (2014, 7), (2014, 9),
    (2015, 9),
    (2016, 3), (2016, 6), (2016, 7), (2016, 8), (2016, 9), (2016, 12),
    (2017, 2), (2017, 3), (2017, 4), (2017, 5), (2017, 6),
    (2017, 7), (2017, 8), (2017, 9), (2017, 10), (2017, 11), (2017, 12),
    (2019, 12),
    (2023, 7),
    (2025, 3),
]

# URL patterns to try
def get_urls(year, month):
    month_name = ['january', 'february', 'march', 'april', 'may', 'june',
                  'july', 'august', 'september', 'october', 'november', 'december'][month-1]

    return [
        # Current format (2020+)
        f"https://www.cms.gov/files/zip/monthly-enrollment-cpsc-{month_name}-{year}.zip",
        # Alt format
        f"https://www.cms.gov/files/zip/cpsc-enrollment-{month_name}-{year}.zip",
        # Old format variations
        f"https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/Downloads/CPSC-Enrollment-{month_name.capitalize()}-{year}.zip",
        f"https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/CPSC_Enrollment_{month:02d}_{year}.zip",
    ]

def download_and_upload(year, month):
    urls = get_urls(year, month)

    for url in urls:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200 and len(response.content) > 100000:  # > 100KB
                # Upload to S3
                s3_key = f"raw/enrollment/cpsc/{year}-{month:02d}/cpsc_enrollment_{year}_{month:02d}.zip"
                s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
                print(f"[SUCCESS] {year}-{month:02d}: {len(response.content):,} bytes from {url}")
                return True
        except Exception as e:
            continue

    print(f"[FAILED] {year}-{month:02d}: No valid URL found")
    return False

def main():
    print(f"Re-downloading {len(FAILED_MONTHS)} failed CPSC files...")
    print("-" * 60)

    success = 0
    for year, month in FAILED_MONTHS:
        if download_and_upload(year, month):
            success += 1

    print("-" * 60)
    print(f"Complete: {success}/{len(FAILED_MONTHS)} recovered")

if __name__ == '__main__':
    main()
