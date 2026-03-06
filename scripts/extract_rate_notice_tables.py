"""
Comprehensive Rate Notice Table Extraction
==========================================

Extracts ALL tables from CMS Rate Announcements (Advance & Final) for all years.

Tables include:
- USPCC projections (Part A, Part B, combined) by calendar year
- ESRD rates and adjustment factors
- Service type cost breakdowns
- Demographic adjustment factors
- HCC/RxHCC hierarchies and coefficients
- Benchmark parameters
- Growth rate projections
- Part D parameters

Each extracted table includes full audit trail:
- source_document: PDF filename
- source_page: Page number in PDF
- source_table_num: Table number on page
- extracted_at: Timestamp
"""

import os
import sys
import json
import boto3
import pdfplumber
import pandas as pd
from io import BytesIO
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict, field
import re
import hashlib

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")


@dataclass
class ExtractedTable:
    """A table extracted from a rate notice PDF."""
    table_id: str  # Unique ID based on content hash
    year: int
    notice_type: str  # "advance" or "final"
    category: str  # "uspcc", "esrd", "hcc", "benchmark", "demographic", etc.
    
    # Table content
    headers: List[str]
    data: List[List[Any]]
    
    # Audit fields
    source_document: str
    source_page: int
    source_table_num: int
    extracted_at: str
    row_count: int
    col_count: int
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert to pandas DataFrame."""
        if not self.data:
            return pd.DataFrame()
        return pd.DataFrame(self.data, columns=self.headers)


class RateNoticeTableExtractor:
    """
    Extracts all tables from CMS Rate Notice PDFs.
    """
    
    CATEGORIES = {
        'uspcc': ['per capita', 'uspcc', 'cumulative', 'fee-for-service'],
        'esrd': ['esrd', 'dialysis', 'transplant', 'end-stage'],
        'growth_rates': ['growth', 'rate change', 'percentage change'],
        'part_d': ['part d', 'deductible', 'coverage limit', 'troop', 'catastrophic'],
        'hcc': ['hcc', 'rxhcc', 'coefficient', 'hierarchy', 'disease group'],
        'benchmark': ['benchmark', 'quartile', 'applicable percentage', 'bid'],
        'service_type': ['service type', 'inpatient', 'outpatient', 'physician', 'hospital'],
        'demographic': ['age', 'sex', 'demographic', 'adjustment factor', 'frailty'],
        'normalization': ['normalization', 'coding intensity', 'coding pattern'],
    }
    
    def __init__(self, bucket: str = None):
        self.bucket = bucket or S3_BUCKET
        self.s3 = boto3.client('s3')
    
    def list_available_pdfs(self) -> Dict[str, List[int]]:
        """List available rate notice PDFs by type and year."""
        pdfs = {'advance': [], 'final': []}
        
        for notice_type in ['advance', 'final']:
            prefix = f"documents/pdf/rate_notice_{notice_type}/"
            try:
                response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
                for obj in response.get('Contents', []):
                    key = obj['Key']
                    # Extract year from filename
                    match = re.search(r'/(\d{4})\.pdf$', key)
                    if match:
                        pdfs[notice_type].append(int(match.group(1)))
            except Exception as e:
                print(f"Error listing {notice_type} PDFs: {e}")
        
        return {k: sorted(v) for k, v in pdfs.items()}
    
    def download_pdf(self, year: int, notice_type: str) -> Optional[bytes]:
        """Download rate notice PDF from S3."""
        key = f"documents/pdf/rate_notice_{notice_type}/{year}.pdf"
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            return response['Body'].read()
        except Exception as e:
            print(f"Error downloading {year} {notice_type}: {e}")
            return None
    
    def categorize_table(self, headers: List[str], first_row: List[str] = None) -> str:
        """Categorize a table based on its headers."""
        header_text = ' '.join(str(h).lower() for h in headers if h)
        if first_row:
            header_text += ' ' + ' '.join(str(c).lower() for c in first_row if c)
        
        for category, keywords in self.CATEGORIES.items():
            if any(kw in header_text for kw in keywords):
                return category
        
        return 'other'
    
    def clean_header(self, header: str) -> str:
        """Clean up a header value."""
        if not header:
            return ''
        # Remove newlines, extra spaces
        cleaned = re.sub(r'\s+', ' ', str(header)).strip()
        # Truncate long headers
        return cleaned[:100]
    
    def clean_cell(self, cell: Any) -> Any:
        """Clean up a cell value."""
        if cell is None:
            return None
        if isinstance(cell, (int, float)):
            return cell
        
        s = str(cell).strip()
        
        # Try to convert to number
        # Remove currency symbols and commas
        clean_s = re.sub(r'[$,]', '', s)
        try:
            if '.' in clean_s:
                return float(clean_s)
            return int(clean_s)
        except ValueError:
            pass
        
        # Return cleaned string
        return re.sub(r'\s+', ' ', s)
    
    def extract_tables_from_pdf(self, pdf_bytes: bytes, year: int, notice_type: str) -> List[ExtractedTable]:
        """Extract all tables from a PDF."""
        tables = []
        source_doc = f"rate_notice_{notice_type}_{year}.pdf"
        
        try:
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    page_tables = page.extract_tables()
                    
                    for table_num, raw_table in enumerate(page_tables, 1):
                        if not raw_table or len(raw_table) < 2:
                            continue
                        
                        # Clean headers
                        headers = [self.clean_header(h) or f'col_{i}' 
                                   for i, h in enumerate(raw_table[0])]
                        
                        # Skip tables with too few columns
                        if len(headers) < 2:
                            continue
                        
                        # Clean data rows
                        data = []
                        for row in raw_table[1:]:
                            if row and any(c for c in row):
                                cleaned_row = [self.clean_cell(c) for c in row]
                                data.append(cleaned_row)
                        
                        if not data:
                            continue
                        
                        # Categorize
                        category = self.categorize_table(headers, data[0] if data else None)
                        
                        # Generate unique ID
                        content_hash = hashlib.md5(
                            json.dumps([headers, data[:3]], default=str).encode()
                        ).hexdigest()[:12]
                        table_id = f"{year}_{notice_type}_{page_num}_{table_num}_{content_hash}"
                        
                        tables.append(ExtractedTable(
                            table_id=table_id,
                            year=year,
                            notice_type=notice_type,
                            category=category,
                            headers=headers,
                            data=data,
                            source_document=source_doc,
                            source_page=page_num,
                            source_table_num=table_num,
                            extracted_at=datetime.now(timezone.utc).isoformat(),
                            row_count=len(data),
                            col_count=len(headers),
                        ))
                
        except Exception as e:
            print(f"Error extracting tables from {source_doc}: {e}")
        
        return tables
    
    def extract_year(self, year: int, notice_type: str = "final") -> List[ExtractedTable]:
        """Extract all tables from a specific year's rate notice."""
        print(f"Extracting {year} {notice_type} rate notice...")
        
        pdf_bytes = self.download_pdf(year, notice_type)
        if not pdf_bytes:
            return []
        
        tables = self.extract_tables_from_pdf(pdf_bytes, year, notice_type)
        print(f"  Extracted {len(tables)} tables")
        
        return tables
    
    def extract_all(self, years: List[int] = None, notice_types: List[str] = None) -> Dict[str, List[ExtractedTable]]:
        """Extract tables from all available rate notices."""
        available = self.list_available_pdfs()
        
        if years is None:
            years = sorted(set(available['advance'] + available['final']))
        if notice_types is None:
            notice_types = ['advance', 'final']
        
        all_tables = {}
        
        for notice_type in notice_types:
            for year in years:
                if year in available.get(notice_type, []):
                    key = f"{year}_{notice_type}"
                    all_tables[key] = self.extract_year(year, notice_type)
        
        return all_tables
    
    def save_to_s3(self, tables: Dict[str, List[ExtractedTable]], prefix: str = "gold/rate_notice_tables"):
        """Save extracted tables to S3."""
        
        # Save as JSON (preserves structure)
        all_tables_json = {}
        for key, table_list in tables.items():
            all_tables_json[key] = [t.to_dict() for t in table_list]
        
        json_key = f"{prefix}/all_tables.json"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=json_key,
            Body=json.dumps(all_tables_json, indent=2, default=str),
            ContentType='application/json'
        )
        print(f"Saved all tables to s3://{self.bucket}/{json_key}")
        
        # Save categorized tables as parquet
        by_category = {}
        for table_list in tables.values():
            for t in table_list:
                if t.category not in by_category:
                    by_category[t.category] = []
                
                # Add metadata to each row
                df = t.to_dataframe()
                df['_year'] = t.year
                df['_notice_type'] = t.notice_type
                df['_source_document'] = t.source_document
                df['_source_page'] = t.source_page
                df['_extracted_at'] = t.extracted_at
                
                by_category[t.category].append(df)
        
        # Combine and save by category
        for category, dfs in by_category.items():
            if dfs:
                try:
                    combined = pd.concat(dfs, ignore_index=True)
                    parquet_key = f"{prefix}/{category}_tables.parquet"
                    buffer = BytesIO()
                    combined.to_parquet(buffer, index=False)
                    buffer.seek(0)
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=parquet_key,
                        Body=buffer.getvalue()
                    )
                    print(f"Saved {category} ({len(combined)} rows) to s3://{self.bucket}/{parquet_key}")
                except Exception as e:
                    print(f"Warning: Could not save {category} as parquet: {e}")
        
        # Save audit summary
        audit_summary = {
            "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
            "documents_processed": list(tables.keys()),
            "tables_by_category": {cat: len(dfs) if dfs else 0 for cat, dfs in by_category.items()},
            "total_tables": sum(len(tl) for tl in tables.values()),
            "categories": list(by_category.keys()),
        }
        
        audit_key = f"{prefix}/_audit.json"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=audit_key,
            Body=json.dumps(audit_summary, indent=2),
            ContentType='application/json'
        )
        print(f"Saved audit summary to s3://{self.bucket}/{audit_key}")


# =============================================================================
# SPECIALIZED EXTRACTORS FOR KEY DATA
# =============================================================================

class USPCCTableBuilder:
    """
    Builds clean USPCC (per capita cost) tables from extracted data.
    """
    
    def __init__(self, extractor: RateNoticeTableExtractor):
        self.extractor = extractor
    
    def build_uspcc_timeseries(self, tables: Dict[str, List[ExtractedTable]]) -> pd.DataFrame:
        """
        Build a clean USPCC time series from extracted tables.
        
        The rate notices contain tables showing projected per capita costs
        for multiple calendar years, which get updated each year.
        """
        records = []
        
        for key, table_list in tables.items():
            for t in table_list:
                if t.category != 'uspcc':
                    continue
                
                # Parse USPCC table
                df = t.to_dataframe()
                
                # Look for columns that suggest this is USPCC data
                part_a_cols = [c for c in df.columns if 'part a' in c.lower()]
                part_b_cols = [c for c in df.columns if 'part b' in c.lower()]
                
                if not (part_a_cols or part_b_cols):
                    continue
                
                # Try to extract year and values
                for idx, row in df.iterrows():
                    # First column is often calendar year
                    cal_year = row.iloc[0] if len(row) > 0 else None
                    
                    try:
                        cal_year = int(float(str(cal_year).replace(',', '')))
                        if cal_year < 2000 or cal_year > 2030:
                            continue
                    except:
                        continue
                    
                    record = {
                        'projection_year': t.year,  # Year the projection was made
                        'calendar_year': cal_year,  # Year being projected
                        'notice_type': t.notice_type,
                        'source_document': t.source_document,
                        'source_page': t.source_page,
                        'extracted_at': t.extracted_at,
                    }
                    
                    # Extract Part A, Part B values
                    for i, val in enumerate(row[1:], 1):
                        col_name = df.columns[i].lower() if i < len(df.columns) else ''
                        if isinstance(val, (int, float)):
                            if 'part a' in col_name:
                                if 'current' in col_name:
                                    record['part_a_current'] = val
                                elif 'last' in col_name or 'prior' in col_name:
                                    record['part_a_prior'] = val
                            elif 'part b' in col_name:
                                if 'current' in col_name:
                                    record['part_b_current'] = val
                                elif 'last' in col_name or 'prior' in col_name:
                                    record['part_b_prior'] = val
                            elif 'a + b' in col_name or 'combined' in col_name or 'total' in col_name:
                                if 'current' in col_name:
                                    record['combined_current'] = val
                                elif 'last' in col_name or 'prior' in col_name:
                                    record['combined_prior'] = val
                    
                    if any(k.startswith('part_') or k.startswith('combined_') for k in record):
                        records.append(record)
        
        return pd.DataFrame(records)


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract Rate Notice Tables")
    parser.add_argument("--year", type=int, help="Extract specific year")
    parser.add_argument("--type", choices=['advance', 'final'], default='final')
    parser.add_argument("--all", action="store_true", help="Extract all years")
    parser.add_argument("--list", action="store_true", help="List available PDFs")
    parser.add_argument("--save", action="store_true", help="Save to S3")
    
    args = parser.parse_args()
    
    extractor = RateNoticeTableExtractor()
    
    if args.list:
        available = extractor.list_available_pdfs()
        print("Available Rate Notice PDFs:")
        for notice_type, years in available.items():
            print(f"  {notice_type}: {years}")
    
    elif args.all:
        print("Extracting ALL rate notice tables...")
        all_tables = extractor.extract_all()
        
        # Summary
        print("\nExtraction Summary:")
        total = 0
        by_category = {}
        for key, tables in all_tables.items():
            print(f"  {key}: {len(tables)} tables")
            total += len(tables)
            for t in tables:
                by_category[t.category] = by_category.get(t.category, 0) + 1
        
        print(f"\nTotal tables: {total}")
        print("\nBy category:")
        for cat, count in sorted(by_category.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {count}")
        
        if args.save:
            extractor.save_to_s3(all_tables)
    
    elif args.year:
        tables = extractor.extract_year(args.year, args.type)
        
        print(f"\nExtracted {len(tables)} tables from {args.year} {args.type}:")
        by_category = {}
        for t in tables:
            by_category[t.category] = by_category.get(t.category, 0) + 1
        
        for cat, count in sorted(by_category.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {count}")
        
        if args.save:
            extractor.save_to_s3({f"{args.year}_{args.type}": tables})
    
    else:
        parser.print_help()
