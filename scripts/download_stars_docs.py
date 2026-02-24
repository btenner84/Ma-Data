#!/usr/bin/env python3
"""
Download ALL Stars Technical Documentation (2015-2026)
- Technical Notes
- Fact Sheets
- Measure Technical Specifications
- Display Measure Notes
- CAI Methodology
- Cut Point Methodology
"""

import requests
import boto3

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def download_and_upload(url, s3_key, min_size=5000):
    """Download file and upload to S3."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=60, headers=headers)
        if response.status_code == 200 and len(response.content) > min_size:
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=response.content)
            print(f"  [OK] {s3_key} ({len(response.content):,} bytes)")
            return True
        return False
    except Exception as e:
        return False


def download_technical_notes():
    """Download Technical Notes PDFs."""
    print("\n=== TECHNICAL NOTES ===")

    success = 0
    for year in range(2015, 2027):
        patterns = [
            f"https://www.cms.gov/files/document/{year}-star-ratings-technical-notes.pdf",
            f"https://www.cms.gov/files/document/star-ratings-technical-notes-{year}.pdf",
            f"https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/{year}TechnicalNotes.pdf",
            f"https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/StarRatingsTechNotes{year}.pdf",
        ]

        for url in patterns:
            s3_key = f"docs/stars/technical_notes/{year}_technical_notes.pdf"
            if download_and_upload(url, s3_key):
                success += 1
                break

    return success


def download_fact_sheets():
    """Download Fact Sheets."""
    print("\n=== FACT SHEETS ===")

    success = 0
    for year in range(2015, 2027):
        patterns = [
            f"https://www.cms.gov/files/document/fact-sheet-{year}-medicare-advantage-and-part-d-star-ratings.pdf",
            f"https://www.cms.gov/files/document/{year}-star-ratings-fact-sheet.pdf",
            f"https://www.cms.gov/newsroom/fact-sheets/{year}-star-ratings",
            f"https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/{year}StarRatingsFactSheet.pdf",
        ]

        for url in patterns:
            s3_key = f"docs/stars/fact_sheets/{year}_fact_sheet.pdf"
            if download_and_upload(url, s3_key):
                success += 1
                break

    return success


def download_display_measure_notes():
    """Download Display Measure Technical Notes."""
    print("\n=== DISPLAY MEASURE TECHNICAL NOTES ===")

    success = 0
    for year in range(2015, 2027):
        patterns = [
            f"https://www.cms.gov/files/document/medicare-{year}-part-c-d-display-measure-technical-notes.pdf",
            f"https://www.cms.gov/files/document/{year}-display-measure-technical-notes.pdf",
            f"https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/{year}DisplayMeasureTechNotes.pdf",
        ]

        for url in patterns:
            s3_key = f"docs/stars/display_measures/{year}_display_measure_notes.pdf"
            if download_and_upload(url, s3_key):
                success += 1
                break

    return success


def download_measure_specifications():
    """Download Measure Specifications."""
    print("\n=== MEASURE SPECIFICATIONS ===")

    success = 0
    for year in range(2015, 2027):
        patterns = [
            f"https://www.cms.gov/files/zip/{year}-star-rating-measure-specifications.zip",
            f"https://www.cms.gov/files/document/{year}-measure-specifications.pdf",
            f"https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/{year}MeasureSpecs.zip",
        ]

        for url in patterns:
            ext = 'zip' if url.endswith('.zip') else 'pdf'
            s3_key = f"docs/stars/measure_specs/{year}_measure_specs.{ext}"
            if download_and_upload(url, s3_key):
                success += 1
                break

    return success


def download_rate_announcements():
    """Download Rate Announcements (contain Stars policy info)."""
    print("\n=== RATE ANNOUNCEMENTS ===")

    success = 0
    for year in range(2015, 2027):
        patterns = [
            f"https://www.cms.gov/files/document/{year}-announcement.pdf",
            f"https://www.cms.gov/files/document/{year}-rate-announcement.pdf",
            f"https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Announcement{year}.pdf",
        ]

        for url in patterns:
            s3_key = f"docs/stars/rate_announcements/{year}_rate_announcement.pdf"
            if download_and_upload(url, s3_key):
                success += 1
                break

    return success


def download_cai_methodology():
    """Download CAI (Categorical Adjustment Index) methodology docs."""
    print("\n=== CAI METHODOLOGY ===")

    patterns = [
        ("https://www.cms.gov/files/document/categorical-adjustment-index-methodology.pdf", "cai_methodology.pdf"),
        ("https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/CAI_Methodology.pdf", "cai_methodology_alt.pdf"),
    ]

    success = 0
    for url, filename in patterns:
        s3_key = f"docs/stars/methodology/{filename}"
        if download_and_upload(url, s3_key):
            success += 1

    return success


def download_cut_point_methodology():
    """Download Cut Point methodology docs."""
    print("\n=== CUT POINT METHODOLOGY ===")

    patterns = [
        ("https://www.cms.gov/files/document/star-ratings-cut-point-methodology.pdf", "cut_point_methodology.pdf"),
        ("https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/CutPointMethodology.pdf", "cut_point_methodology_alt.pdf"),
    ]

    success = 0
    for url, filename in patterns:
        s3_key = f"docs/stars/methodology/{filename}"
        if download_and_upload(url, s3_key):
            success += 1

    return success


def download_call_letters():
    """Download Call Letters (annual policy guidance)."""
    print("\n=== CALL LETTERS ===")

    success = 0
    for year in range(2015, 2027):
        patterns = [
            f"https://www.cms.gov/files/document/{year}-call-letter.pdf",
            f"https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/{year}CallLetter.pdf",
        ]

        for url in patterns:
            s3_key = f"docs/stars/call_letters/{year}_call_letter.pdf"
            if download_and_upload(url, s3_key):
                success += 1
                break

    return success


def main():
    print("=" * 70)
    print("DOWNLOADING ALL STARS DOCUMENTATION (2015-2026)")
    print("=" * 70)

    results = {
        'technical_notes': download_technical_notes(),
        'fact_sheets': download_fact_sheets(),
        'display_measure_notes': download_display_measure_notes(),
        'measure_specs': download_measure_specifications(),
        'rate_announcements': download_rate_announcements(),
        'cai_methodology': download_cai_methodology(),
        'cut_point_methodology': download_cut_point_methodology(),
        'call_letters': download_call_letters(),
    }

    print("\n" + "=" * 70)
    print("DOWNLOAD SUMMARY")
    print("=" * 70)
    for doc_type, count in results.items():
        print(f"  {doc_type}: {count} files")
    print(f"\n  TOTAL: {sum(results.values())} documents")
    print("=" * 70)


if __name__ == '__main__':
    main()
