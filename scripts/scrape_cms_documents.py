"""
CMS Document Scraper
====================

Scrapes and processes CMS documents for the MA Intelligence knowledge base.

Documents:
1. MA Rate Notices (Advance & Final) - 2016-2026
2. Star Ratings Technical Notes - 2016-2026
3. Call Letters - 2016-2026

Outputs:
- Structured metadata (JSON)
- Full text (for search/RAG)
- Key extracts (summaries)

Usage:
    python scripts/scrape_cms_documents.py --years 2020-2026
    python scripts/scrape_cms_documents.py --type rate_notice --year 2025
"""

import os
import sys
import json
import re
import hashlib
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import boto3
from botocore.exceptions import ClientError

# For PDF processing
try:
    import PyPDF2
    from io import BytesIO
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Warning: PyPDF2 not installed. PDF extraction disabled.")

# For web scraping
try:
    import requests
    from bs4 import BeautifulSoup
    SCRAPING_AVAILABLE = True
except ImportError:
    SCRAPING_AVAILABLE = False
    print("Warning: requests/beautifulsoup4 not installed. Web scraping disabled.")


@dataclass
class CMSDocument:
    """Represents a CMS document."""
    doc_id: str
    doc_type: str  # rate_notice_advance, rate_notice_final, tech_notes, call_letter
    year: int
    title: str
    url: str
    release_date: Optional[str] = None
    full_text: Optional[str] = None
    summary: Optional[str] = None
    key_changes: List[str] = None
    metadata: Dict = None
    file_path: Optional[str] = None
    scraped_at: str = None
    
    def __post_init__(self):
        if self.key_changes is None:
            self.key_changes = []
        if self.metadata is None:
            self.metadata = {}
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow().isoformat()


# Known CMS document URLs (2016-2027)
CMS_DOCUMENT_SOURCES = {
    "rate_notice_advance": {
        # Advance Notices (released ~January for following payment year)
        2027: "https://www.cms.gov/files/document/2027-advance-notice.pdf",
        2026: "https://www.cms.gov/files/document/2026-advance-notice.pdf",
        2025: "https://www.cms.gov/files/document/2025-advance-notice.pdf",
        2024: "https://www.cms.gov/files/document/2024-advance-notice.pdf",
        2023: "https://www.cms.gov/files/document/2023-advance-notice.pdf",
        2022: "https://www.cms.gov/files/document/2022-advance-notice.pdf",
        2021: "https://www.cms.gov/files/document/2021-advance-notice.pdf",
        2020: "https://www.cms.gov/files/document/2020-advance-notice.pdf",
        2019: "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Advance2019.pdf",
        2018: "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Advance2018.pdf",
        2017: "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Advance2017.pdf",
        2016: "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Advance2016.pdf",
    },
    "rate_notice_final": {
        # Final Rate Announcements (released ~April for following payment year)
        2027: "https://www.cms.gov/files/document/2027-announcement.pdf",  # Expected April 2026
        2026: "https://www.cms.gov/files/document/2026-announcement.pdf",
        2025: "https://www.cms.gov/files/document/2025-announcement.pdf",
        2024: "https://www.cms.gov/files/document/2024-announcement-pdf.pdf",
        2023: "https://www.cms.gov/files/document/2023-announcement.pdf",
        2022: "https://www.cms.gov/files/document/2022-announcement.pdf",
        2021: "https://www.cms.gov/files/document/2021-announcement.pdf",
        2020: "https://www.cms.gov/files/document/2020-announcement.pdf",
        2019: "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Announcement2019.pdf",
        2018: "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Announcement2018.pdf",
        2017: "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Announcement2017.pdf",
        2016: "https://www.cms.gov/Medicare/Health-Plans/MedicareAdvtgSpecRateStats/Downloads/Announcement2016.pdf",
    },
    "tech_notes": {
        # Star Ratings Technical Notes (released with October star ratings)
        2027: "https://www.cms.gov/files/document/2027-star-ratings-technical-notes.pdf",  # Expected Oct 2026
        2026: "https://www.cms.gov/files/document/2026-star-ratings-technical-notes.pdf",
        2025: "https://www.cms.gov/files/document/2025-star-ratings-technical-notes.pdf",
        2024: "https://www.cms.gov/files/document/2024-star-ratings-technical-notes.pdf",
        2023: "https://www.cms.gov/files/document/2023-star-ratings-technical-notes.pdf",
        2022: "https://www.cms.gov/files/document/2022-star-ratings-technical-notes.pdf",
        2021: "https://www.cms.gov/files/document/2021-star-ratings-technical-notes.pdf",
        2020: "https://www.cms.gov/files/document/2020-star-ratings-technical-notes.pdf",
        2019: "https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/2019-Technical-Notes.pdf",
        2018: "https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/2018-Technical-Notes.pdf",
        2017: "https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/2017-Technical-Notes.pdf",
        2016: "https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/Downloads/2016-Technical-Notes.pdf",
    },
    "call_letter": {
        # Call Letters (released with final rate announcements)
        2027: "https://www.cms.gov/files/document/2027-call-letter.pdf",  # Expected April 2026
        2026: "https://www.cms.gov/files/document/2026-call-letter.pdf",
        2025: "https://www.cms.gov/files/document/2025-call-letter.pdf",
        2024: "https://www.cms.gov/files/document/2024-call-letter.pdf",
        2023: "https://www.cms.gov/files/document/2023-call-letter.pdf",
        2022: "https://www.cms.gov/files/document/2022-call-letter.pdf",
        2021: "https://www.cms.gov/files/document/2021-call-letter.pdf",
        2020: "https://www.cms.gov/files/document/2020-call-letter.pdf",
    },
    "star_fact_sheet": {
        # Star Ratings Fact Sheets (simplified overview)
        2027: "https://www.cms.gov/files/document/2027-star-ratings-fact-sheet.pdf",  # Expected Oct 2026
        2026: "https://www.cms.gov/files/document/2026-star-ratings-fact-sheet.pdf",
        2025: "https://www.cms.gov/files/document/2025-star-ratings-fact-sheet.pdf",
        2024: "https://www.cms.gov/files/document/2024-star-ratings-fact-sheet.pdf",
        2023: "https://www.cms.gov/files/document/2023-star-ratings-fact-sheet.pdf",
    },
    "risk_adjustment_factsheet": {
        # Risk Adjustment Fact Sheets
        2027: "https://www.cms.gov/files/document/cy2027-risk-adjustment-fact-sheet.pdf",
        2026: "https://www.cms.gov/files/document/cy2026-risk-adjustment-fact-sheet.pdf",
        2025: "https://www.cms.gov/files/document/cy2025-risk-adjustment-fact-sheet.pdf",
        2024: "https://www.cms.gov/files/document/cy2024-risk-adjustment-fact-sheet.pdf",
        2023: "https://www.cms.gov/files/document/cy2023-risk-adjustment-fact-sheet.pdf",
        2022: "https://www.cms.gov/files/document/cy2022-risk-adjustment-fact-sheet.pdf",
    },
    "payment_methodology": {
        # MA Payment Methodology documentation
        2027: "https://www.cms.gov/files/document/2027-payment-methodology.pdf",
        2026: "https://www.cms.gov/files/document/2026-payment-methodology.pdf",
        2025: "https://www.cms.gov/files/document/2025-payment-methodology.pdf",
        2024: "https://www.cms.gov/files/document/2024-payment-methodology.pdf",
    },
}

# Document titles
DOCUMENT_TITLES = {
    "rate_notice_advance": "Medicare Advantage and Part D Advance Notice",
    "rate_notice_final": "Medicare Advantage and Part D Final Rate Announcement",
    "tech_notes": "Medicare Star Ratings Technical Notes",
    "call_letter": "Medicare Advantage and Part D Call Letter",
    "star_fact_sheet": "Medicare Star Ratings Fact Sheet",
    "risk_adjustment_factsheet": "Risk Adjustment Fact Sheet",
    "payment_methodology": "MA Payment Methodology",
}


class CMSDocumentScraper:
    """Scrapes and processes CMS documents."""
    
    def __init__(self, bucket: str = None, prefix: str = "documents"):
        self.bucket = bucket or os.environ.get("S3_BUCKET", "ma-data123")
        self.prefix = prefix
        self.s3 = boto3.client('s3')
        
        # Local cache directory
        self.cache_dir = os.path.join(os.path.dirname(__file__), "..", "data", "documents")
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def download_document(self, url: str, doc_type: str, year: int) -> Optional[bytes]:
        """Download a document from URL."""
        if not SCRAPING_AVAILABLE:
            print(f"  Scraping not available, skipping download")
            return None
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"  Error downloading {url}: {e}")
            return None
    
    def extract_pdf_text(self, pdf_content: bytes) -> Optional[str]:
        """Extract text from PDF content."""
        if not PDF_AVAILABLE:
            return None
        
        try:
            pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
            text_parts = []
            for page in pdf_reader.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text)
            return "\n\n".join(text_parts)
        except Exception as e:
            print(f"  Error extracting PDF text: {e}")
            return None
    
    def extract_key_changes(self, text: str, doc_type: str) -> List[str]:
        """Extract key changes/highlights from document text."""
        if not text:
            return []
        
        changes = []
        
        # Look for common section headers that indicate key changes
        change_patterns = [
            r"(?:Key|Major|Significant)\s+(?:Changes?|Updates?|Modifications?)[:\s]+([^\n]+(?:\n[^\n]+)*)",
            r"(?:What's|What is)\s+(?:New|Changed)[:\s]+([^\n]+(?:\n[^\n]+)*)",
            r"Summary of (?:Changes?|Updates?)[:\s]+([^\n]+(?:\n[^\n]+)*)",
        ]
        
        for pattern in change_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Clean and split into bullet points
                lines = match.strip().split('\n')
                for line in lines[:10]:  # Limit to first 10
                    line = line.strip()
                    if len(line) > 20:  # Skip short lines
                        changes.append(line[:500])  # Limit length
        
        # If no structured changes found, extract first paragraph after "Introduction" or "Overview"
        if not changes:
            intro_pattern = r"(?:Introduction|Overview|Executive Summary)[:\s]+([^\n]+(?:\n[^\n]+){0,3})"
            match = re.search(intro_pattern, text, re.IGNORECASE)
            if match:
                changes.append(match.group(1).strip()[:500])
        
        return changes[:10]  # Return max 10 changes
    
    def generate_summary(self, text: str, doc_type: str, year: int) -> str:
        """Generate a brief summary of the document."""
        if not text:
            return f"{DOCUMENT_TITLES.get(doc_type, 'CMS Document')} for {year}"
        
        # Extract first meaningful paragraph
        paragraphs = text.split('\n\n')
        for para in paragraphs:
            para = para.strip()
            if len(para) > 100 and not para.isupper():  # Skip headers
                return para[:500] + "..." if len(para) > 500 else para
        
        return f"{DOCUMENT_TITLES.get(doc_type, 'CMS Document')} for {year}"
    
    def scrape_document(self, doc_type: str, year: int) -> Optional[CMSDocument]:
        """Scrape a single CMS document."""
        sources = CMS_DOCUMENT_SOURCES.get(doc_type, {})
        url = sources.get(year)
        
        if not url:
            print(f"  No URL found for {doc_type} {year}")
            return None
        
        print(f"  Downloading {doc_type} {year} from {url}")
        
        # Generate document ID
        doc_id = hashlib.md5(f"{doc_type}:{year}".encode()).hexdigest()[:16]
        
        # Download document
        content = self.download_document(url, doc_type, year)
        
        if not content:
            # Create placeholder document
            return CMSDocument(
                doc_id=doc_id,
                doc_type=doc_type,
                year=year,
                title=f"{DOCUMENT_TITLES.get(doc_type, 'Document')} {year}",
                url=url,
                metadata={"status": "download_failed"},
            )
        
        # Extract text
        full_text = self.extract_pdf_text(content)
        
        # Extract key changes
        key_changes = self.extract_key_changes(full_text, doc_type)
        
        # Generate summary
        summary = self.generate_summary(full_text, doc_type, year)
        
        # Save PDF to cache
        local_path = os.path.join(self.cache_dir, f"{doc_type}_{year}.pdf")
        with open(local_path, 'wb') as f:
            f.write(content)
        
        doc = CMSDocument(
            doc_id=doc_id,
            doc_type=doc_type,
            year=year,
            title=f"{DOCUMENT_TITLES.get(doc_type, 'Document')} {year}",
            url=url,
            full_text=full_text,
            summary=summary,
            key_changes=key_changes,
            file_path=local_path,
            metadata={
                "file_size": len(content),
                "text_length": len(full_text) if full_text else 0,
                "page_count": len(PyPDF2.PdfReader(BytesIO(content)).pages) if PDF_AVAILABLE else None,
            }
        )
        
        return doc
    
    def save_document(self, doc: CMSDocument):
        """Save document metadata to S3."""
        # Save metadata JSON
        metadata_key = f"{self.prefix}/metadata/{doc.doc_type}/{doc.year}.json"
        
        # Don't include full_text in metadata (too large)
        metadata = asdict(doc)
        metadata['full_text'] = None  # Stored separately
        
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType='application/json'
            )
            print(f"  Saved metadata to s3://{self.bucket}/{metadata_key}")
        except Exception as e:
            print(f"  Error saving metadata: {e}")
        
        # Save full text separately (for search)
        if doc.full_text:
            text_key = f"{self.prefix}/text/{doc.doc_type}/{doc.year}.txt"
            try:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=text_key,
                    Body=doc.full_text.encode('utf-8'),
                    ContentType='text/plain'
                )
                print(f"  Saved text to s3://{self.bucket}/{text_key}")
            except Exception as e:
                print(f"  Error saving text: {e}")
        
        # Upload PDF
        if doc.file_path and os.path.exists(doc.file_path):
            pdf_key = f"{self.prefix}/pdf/{doc.doc_type}/{doc.year}.pdf"
            try:
                self.s3.upload_file(doc.file_path, self.bucket, pdf_key)
                print(f"  Uploaded PDF to s3://{self.bucket}/{pdf_key}")
            except Exception as e:
                print(f"  Error uploading PDF: {e}")
    
    def scrape_all(self, doc_types: List[str] = None, years: List[int] = None):
        """Scrape all documents for specified types and years."""
        if doc_types is None:
            doc_types = list(CMS_DOCUMENT_SOURCES.keys())
        
        if years is None:
            years = list(range(2016, 2028))  # Include 2027
        
        results = []
        
        for doc_type in doc_types:
            print(f"\nScraping {doc_type} documents...")
            
            for year in years:
                print(f"\n  Year {year}:")
                doc = self.scrape_document(doc_type, year)
                
                if doc:
                    self.save_document(doc)
                    results.append(doc)
                    print(f"  ✓ Processed {doc_type} {year}")
                else:
                    print(f"  ✗ Failed {doc_type} {year}")
        
        # Save index
        index = {
            "scraped_at": datetime.utcnow().isoformat(),
            "document_count": len(results),
            "documents": [
                {
                    "doc_id": d.doc_id,
                    "doc_type": d.doc_type,
                    "year": d.year,
                    "title": d.title,
                    "url": d.url,
                    "has_text": d.full_text is not None,
                    "key_changes_count": len(d.key_changes),
                }
                for d in results
            ]
        }
        
        index_key = f"{self.prefix}/index.json"
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=index_key,
                Body=json.dumps(index, indent=2),
                ContentType='application/json'
            )
            print(f"\nSaved index to s3://{self.bucket}/{index_key}")
        except Exception as e:
            print(f"\nError saving index: {e}")
        
        return results


def main():
    parser = argparse.ArgumentParser(description="Scrape CMS documents for MA Intelligence")
    parser.add_argument("--type", type=str, help="Document type (rate_notice_advance, rate_notice_final, tech_notes, call_letter)")
    parser.add_argument("--year", type=int, help="Specific year to scrape")
    parser.add_argument("--years", type=str, help="Year range (e.g., 2020-2026)")
    parser.add_argument("--bucket", type=str, default="ma-data123", help="S3 bucket")
    parser.add_argument("--list", action="store_true", help="List available documents")
    
    args = parser.parse_args()
    
    if args.list:
        print("\nAvailable CMS Documents:\n")
        for doc_type, sources in CMS_DOCUMENT_SOURCES.items():
            print(f"\n{doc_type}:")
            for year, url in sorted(sources.items()):
                print(f"  {year}: {url}")
        return
    
    scraper = CMSDocumentScraper(bucket=args.bucket)
    
    # Determine document types
    doc_types = None
    if args.type:
        doc_types = [args.type]
    
    # Determine years
    years = None
    if args.year:
        years = [args.year]
    elif args.years:
        start, end = args.years.split('-')
        years = list(range(int(start), int(end) + 1))
    
    # Run scraper
    print(f"\nCMS Document Scraper")
    print(f"====================")
    print(f"Document types: {doc_types or 'all'}")
    print(f"Years: {years or 'all (2016-2027)'}")
    print(f"Bucket: {args.bucket}")
    
    results = scraper.scrape_all(doc_types=doc_types, years=years)
    
    print(f"\n\nCompleted!")
    print(f"Total documents processed: {len(results)}")
    print(f"Documents with text: {sum(1 for d in results if d.full_text)}")


if __name__ == "__main__":
    main()
