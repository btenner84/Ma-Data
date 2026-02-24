#!/usr/bin/env python3
"""
Download ALL Plan Payment Data (2006-2024) to S3.
Contains risk scores, rebates, benchmarks, payments by contract/plan.
"""

import requests
import boto3

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

# Plan Payment Data URLs (discovered from CMS - each year has different naming)
PLAN_PAYMENT_URLS = {
    2024: "https://www.cms.gov/files/zip/2024-plan-payment-zip-file.zip",
    2023: "https://www.cms.gov/files/zip/2023-plan-payment-data-file.zip",
    2022: "https://www.cms.gov/files/zip/2022planpayment.zip",
    2021: "https://www.cms.gov/files/zip/2021paymentdata.zip",
    2020: "https://www.cms.gov/files/zip/2020paymentdata.zip",
    2019: "https://www.cms.gov/files/zip/2019paymentdata.zip",
    2018: "https://www.cms.gov/files/zip/2018paymentdata.zip",
    2017: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2017-payment-data.zip",
    2016: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2016-payment-data.zip",
    2015: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2015-payment-data.zip",
    2014: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2014-payment-data.zip",
    2013: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2013-payment-data.zip",
    2012: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2012-payment-data.zip",
    2011: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2011data.zip",
    2010: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2010data.zip",
    2009: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2009data.zip",
    2008: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2008data.zip",
    2007: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2007data.zip",
    2006: "https://www.cms.gov/medicare/medicare-advantage/plan-payment/downloads/2006data.zip",
}

# Alternative URL patterns for older years
ALT_PATTERNS = [
    "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/PlanPayment{year}.zip",
    "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/Downloads/plan-payment-{year}.zip",
]


def download_and_upload(year, url):
    """Download plan payment data and upload to S3."""
    try:
        response = requests.get(url, timeout=120)
        if response.status_code == 200 and len(response.content) > 10000:
            s3_key = f"raw/plan_payment/{year}/plan_payment_{year}.zip"
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
            print(f"[SUCCESS] {year}: {len(response.content):,} bytes -> {s3_key}")
            return True
        else:
            return False
    except Exception as e:
        return False


def main():
    print("=" * 60)
    print("DOWNLOADING ALL PLAN PAYMENT DATA (2006-2024)")
    print("Contains: Risk Scores, Rebates, Benchmarks, Payments")
    print("=" * 60)

    success = 0
    failed = []

    for year in sorted(PLAN_PAYMENT_URLS.keys(), reverse=True):
        url = PLAN_PAYMENT_URLS[year]

        if download_and_upload(year, url):
            success += 1
        else:
            # Try alternative patterns
            found = False
            for pattern in ALT_PATTERNS:
                alt_url = pattern.format(year=year)
                if download_and_upload(year, alt_url):
                    success += 1
                    found = True
                    break

            if not found:
                failed.append(year)
                print(f"[FAILED] {year}: Could not download")

    print("\n" + "=" * 60)
    print(f"COMPLETE: {success} years downloaded")
    if failed:
        print(f"FAILED: {failed}")
    print("=" * 60)


if __name__ == '__main__':
    main()
