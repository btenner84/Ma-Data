#!/usr/bin/env python3
"""
Download ALL crosswalks (2006-2026) to S3.
"""

import requests
import boto3

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

# Crosswalk URLs discovered from CMS
CROSSWALKS = {
    # New format (2022-2026)
    2026: "https://www.cms.gov/files/zip/plan-crosswalk-2026.zip",
    2025: "https://www.cms.gov/files/zip/plan-crosswalk-2025.zip",
    2024: "https://www.cms.gov/files/zip/plan-crosswalk-2024.zip",
    2023: "https://www.cms.gov/files/zip/plan-crosswalk-2023.zip",
    2022: "https://www.cms.gov/files/zip/plan-crosswalk-2022.zip",

    # Old format (2013-2020)
    2020: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/plan-crosswalk/plan-crosswalk-2020.zip",
    2019: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/plan-crosswalk/plan-crosswalk-2019.zip",
    2018: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/plan-crosswalk/plan-crosswalk-2018.zip",
    2017: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/plan-crosswalk/plan-crosswalk-2017.zip",
    2016: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/plan-crosswalk/plan-crosswalk-2016.zip",
    2015: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/plan-crosswalk-2015.zip",
    2014: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/plan-crosswalk-2014.zip",
    2013: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/plan-crosswalks-2013.zip",
}

# Very old format (2006-2012) - need to extract from CMS pages
OLD_CROSSWALK_PAGES = {
    2006: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/cms1236744",
    2007: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/cms1237099",
    2008: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/cms1237100",
    2009: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/cms1237103",
    2010: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/cms1237104",
    2011: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/cms1239779",
    2012: "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/plan-crosswalks-items/cms1252440",
}

def extract_zip_link(page_url):
    """Extract ZIP link from CMS page."""
    import re
    response = requests.get(page_url, timeout=30)
    match = re.search(r'href="([^"]*\.zip)"', response.text, re.IGNORECASE)
    if match:
        link = match.group(1)
        if not link.startswith('http'):
            link = f"https://www.cms.gov{link}"
        return link
    return None

def download_and_upload(year, url):
    """Download crosswalk and upload to S3."""
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 200 and len(response.content) > 10000:
            s3_key = f"raw/crosswalks/crosswalk_{year}.zip"
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
            print(f"[SUCCESS] {year}: {len(response.content):,} bytes -> {s3_key}")
            return True
        else:
            print(f"[FAILED] {year}: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"[ERROR] {year}: {e}")
        return False

def main():
    print("=" * 60)
    print("DOWNLOADING ALL CROSSWALKS (2006-2026)")
    print("=" * 60)

    success = 0

    # Download 2013-2026 (direct URLs)
    print("\n=== 2013-2026 ===")
    for year, url in sorted(CROSSWALKS.items()):
        if download_and_upload(year, url):
            success += 1

    # Download 2006-2012 (extract from pages)
    print("\n=== 2006-2012 ===")
    for year, page_url in sorted(OLD_CROSSWALK_PAGES.items()):
        zip_url = extract_zip_link(page_url)
        if zip_url:
            if download_and_upload(year, zip_url):
                success += 1
        else:
            print(f"[FAILED] {year}: Could not extract ZIP link from {page_url}")

    print("\n" + "=" * 60)
    print(f"COMPLETE: {success} crosswalks downloaded")
    print("=" * 60)

if __name__ == '__main__':
    main()
