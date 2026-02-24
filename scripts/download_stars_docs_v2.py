#!/usr/bin/env python3
"""
Download ALL Stars Documentation from CMS Performance Data page.
"""

import requests
import boto3
import re

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def download_and_upload(url, s3_key, min_size=1000):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=120, headers=headers)
        if response.status_code == 200 and len(response.content) > min_size:
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
            print(f"[OK] {s3_key} ({len(response.content):,} bytes)")
            return True
        return False
    except Exception as e:
        return False


def main():
    print("=" * 70)
    print("DOWNLOADING ALL STARS DOCUMENTATION FROM CMS")
    print("=" * 70)

    # Based on the CMS page content
    downloads = {
        # Technical Notes & Methodology
        'docs/stars/technical_notes/2026_technical_notes.pdf': '/files/document/2026-star-ratings-technical-notes.pdf',
        'docs/stars/technical_notes/2025_technical_notes.pdf': '/files/document/2025-star-ratings-technical-notes.pdf',
        'docs/stars/technical_notes/2024_technical_notes.pdf': '/files/document/2024-star-ratings-technical-notes.pdf',
        'docs/stars/technical_notes/2023_technical_notes.pdf': '/files/document/2023-star-ratings-technical-notes.pdf',

        # Fact Sheets
        'docs/stars/fact_sheets/2026_fact_sheet.pdf': '/files/document/2026-star-ratings-fact-sheet.pdf',
        'docs/stars/fact_sheets/2025_fact_sheet.pdf': '/files/document/2025-star-ratings-fact-sheet.pdf',

        # CAI Documents
        'docs/stars/cai/2027_cai_supplement.pdf': '/files/document/2027-categorical-adjustment-index-measure-supplement.pdf',
        'docs/stars/cai/2026_cai_supplement.pdf': '/files/document/2026-categorical-adjustment-index-measure-supplement.pdf',
        'docs/stars/cai/2025_cai_supplement.pdf': '/files/document/2025-categorical-adjustment-index-measure-supplement.pdf',
        'docs/stars/cai/2024_cai_supplement.pdf': '/files/document/2024-categorical-adjustment-index-measure-supplement.pdf',
        'docs/stars/cai/historical_cai.zip': '/files/zip/historical-categorical-adjustment-index-documents.zip',

        # Measures
        'docs/stars/measures/2027_measures.pdf': '/files/document/2027-star-ratings-measures.pdf',

        # Cut Points
        'docs/stars/cut_points/cut_point_trends.zip': '/files/zip/cut-point-trends.zip',
        'docs/stars/cut_points/2024_cutpoints.xlsx': '/files/document/updated-2024-star-ratings-cutpoints-and-star-averages.xlsx',

        # Display Measures
        'docs/stars/display_measures/2026_display_measures.zip': '/files/zip/2026-display-measures.zip',
        'docs/stars/display_measures/2025_display_measures.zip': '/files/zip/2025-display-measures.zip',
        'docs/stars/display_measures/2024_display_measures.zip': '/files/zip/2024-display-measures.zip',

        # Star Ratings Data Tables
        # Star Year = year ratings announced (Oct of prior year), Payment Year = Star Year + 1
        # e.g., 2025 Star Ratings announced Oct 2024, affects 2026 payments
        'docs/stars/data_tables/2026_star_ratings.zip': '/files/zip/2026-star-ratings-data-tables.zip',
        'docs/stars/data_tables/2025_star_ratings.zip': '/files/zip/2025-star-ratings-data-tables.zip',
        'docs/stars/data_tables/2024_star_ratings_data.zip': '/files/zip/2024-star-ratings-data-tables-jul-2-2024.zip',
        'docs/stars/data_tables/2023_star_ratings.zip': '/files/zip/2023-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2022_star_ratings.zip': '/files/zip/2022-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2021_star_ratings.zip': '/files/zip/2021-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2020_star_ratings.zip': '/files/zip/2020-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2019_star_ratings.zip': '/files/zip/2019-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2018_star_ratings.zip': '/files/zip/2018-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2017_star_ratings.zip': '/files/zip/2017-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2016_star_ratings.zip': '/files/zip/2016-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2015_star_ratings.zip': '/files/zip/2015-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2014_star_ratings.zip': '/files/zip/2014-star-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2013_plan_ratings.zip': '/files/zip/2013-plan-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2012_plan_ratings.zip': '/files/zip/2012-plan-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2011_plan_ratings.zip': '/files/zip/2011-plan-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2010_plan_ratings.zip': '/files/zip/2010-plan-ratings-and-display-measures.zip',
        'docs/stars/data_tables/2009_plan_ratings.zip': '/files/zip/2009-plan-ratings.zip',
        'docs/stars/data_tables/2008_plan_ratings.zip': '/files/zip/2008-plan-ratings.zip',
        'docs/stars/data_tables/2007_plan_ratings.zip': '/files/zip/2007-plan-ratings.zip',

        # Stratified Reporting
        'docs/stars/methodology/stratified_reporting_2022_2023.pdf': '/files/document/stratified-reporting-documentation-2022-2023-star-ratings.pdf',

        # HEI Webinar
        'docs/stars/webinars/hei_webinar_2024.pdf': '/files/document/hei-webinar-november-2024.pdf',
    }

    success = 0
    failed = []

    for s3_key, path in downloads.items():
        url = f"https://www.cms.gov{path}"
        if download_and_upload(url, s3_key):
            success += 1
        else:
            failed.append(s3_key)

    print(f"\n{'='*70}")
    print(f"COMPLETE: {success}/{len(downloads)} files downloaded")
    if failed:
        print(f"\nFailed ({len(failed)}):")
        for f in failed[:10]:
            print(f"  - {f}")
    print("=" * 70)


if __name__ == '__main__':
    main()
