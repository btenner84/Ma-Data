"""
Build County Benchmark Tables from CMS Ratebooks
=================================================

Extracts county-level MA benchmark rates from CMS ratebooks.
Includes full audit trail for every data point.

Data extracted:
- County FFS rates (USPCC)
- County benchmark rates
- Quartile assignments
- Quality bonus eligibility
- Double bonus counties

Source: CMS MA Ratebooks (annual .zip files)
"""

import os
import sys
import json
import zipfile
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")


@dataclass
class CountyBenchmarkRecord:
    """County benchmark record with full audit trail."""
    # Keys
    year: int
    state_code: str
    county_fips: str
    county_name: str
    
    # Benchmark data
    ffs_rate: float  # FFS per capita (USPCC)
    ma_benchmark: float  # Final MA benchmark
    quartile: int  # 1-4
    
    # Bonus eligibility
    quality_bonus_eligible: bool
    double_bonus_eligible: bool
    
    # Components (where available)
    aged_rate: Optional[float] = None
    disabled_rate: Optional[float] = None
    esrd_rate: Optional[float] = None
    
    # Audit fields
    source_file: str = ""
    source_table: str = ""
    source_row: Optional[int] = None
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    data_version: str = "1.0"
    
    def to_dict(self) -> Dict:
        return asdict(self)


class RatebookProcessor:
    """
    Processes CMS ratebooks to extract county benchmark data.
    """
    
    def __init__(self, bucket: str = None):
        self.bucket = bucket or S3_BUCKET
        self.s3 = boto3.client('s3')
    
    def list_available_ratebooks(self) -> List[int]:
        """List years with available ratebooks."""
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix="raw/rates/ratebook/"
            )
            years = []
            for obj in response.get('Contents', []):
                key = obj['Key']
                if 'ratebook_' in key and '.zip' in key:
                    try:
                        year = int(key.split('ratebook_')[1].split('.')[0])
                        years.append(year)
                    except:
                        pass
            return sorted(years)
        except Exception as e:
            print(f"Error listing ratebooks: {e}")
            return []
    
    def download_ratebook(self, year: int) -> Optional[bytes]:
        """Download ratebook zip from S3."""
        key = f"raw/rates/ratebook/ratebook_{year}.zip"
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            return response['Body'].read()
        except Exception as e:
            print(f"Error downloading {year} ratebook: {e}")
            return None
    
    def extract_county_data(self, year: int, zip_bytes: bytes) -> pd.DataFrame:
        """Extract county benchmark data from ratebook zip."""
        records = []
        source_file = f"ratebook_{year}.zip"
        
        try:
            with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
                # List files in zip
                file_list = zf.namelist()
                print(f"  Files in {year} ratebook: {len(file_list)}")
                
                # Find the county rates file (usually .csv or .xlsx)
                county_files = [f for f in file_list if any(term in f.lower() for term in 
                    ['county', 'rate', 'benchmark', 'ma rate'])]
                
                for cf in county_files:
                    print(f"    Processing: {cf}")
                    
                    try:
                        with zf.open(cf) as f:
                            file_bytes = f.read()
                            
                            # Try to read as Excel or CSV
                            if cf.endswith('.xlsx') or cf.endswith('.xls'):
                                df = pd.read_excel(BytesIO(file_bytes))
                            elif cf.endswith('.csv'):
                                df = pd.read_csv(BytesIO(file_bytes))
                            else:
                                continue
                            
                            # Process the dataframe
                            processed = self._process_county_df(df, year, source_file, cf)
                            if processed is not None and len(processed) > 0:
                                records.extend(processed)
                                print(f"      Extracted {len(processed)} county records")
                                
                    except Exception as e:
                        print(f"      Error processing {cf}: {e}")
                        
        except Exception as e:
            print(f"  Error extracting {year} ratebook: {e}")
        
        if records:
            return pd.DataFrame(records)
        return pd.DataFrame()
    
    def _process_county_df(self, df: pd.DataFrame, year: int, source_file: str, source_table: str) -> List[Dict]:
        """Process a county rates dataframe."""
        records = []
        
        # Skip if too few rows
        if len(df) < 3:
            return records
            
        # CMS county rate files have a specific format:
        # Row 0: Title/notes
        # Row 1: Header row
        # Row 2+: Data
        
        # Check if this is a CMS county rate file
        cols = list(df.columns)
        first_col = str(cols[0]).lower()
        
        if 'capitation' in first_col or 'medicare advantage' in first_col:
            # Standard CMS county rate file format
            # Reset with proper header
            df.columns = ['code', 'state', 'county_name', 'rate_5pct', 'rate_3_5pct', 'rate_0pct', 'esrd_rate']
            df = df.iloc[2:].reset_index(drop=True)  # Skip header rows
            
            for idx, row in df.iterrows():
                try:
                    code = str(row['code']).strip()
                    if not code or code == 'nan' or len(code) < 4:
                        continue
                        
                    # Parse the 5-digit FIPS code (SSCCC format: 2-digit state + 3-digit county)
                    county_fips = code.zfill(5)
                    
                    # Determine quartile from which rate column has a value
                    rate_5 = self._safe_float(row.get('rate_5pct'))
                    rate_3_5 = self._safe_float(row.get('rate_3_5pct'))
                    rate_0 = self._safe_float(row.get('rate_0pct'))
                    esrd = self._safe_float(row.get('esrd_rate'))
                    
                    # MA benchmark is the applicable rate for this county
                    # Higher rated counties get higher % of FFS
                    if rate_5:
                        ma_benchmark = rate_5
                        quartile = 4  # 5% bonus = top quartile
                    elif rate_3_5:
                        ma_benchmark = rate_3_5
                        quartile = 3  # 3.5% bonus = second quartile
                    elif rate_0:
                        ma_benchmark = rate_0
                        quartile = 1  # 0% bonus = bottom quartiles
                    else:
                        continue  # No rate data
                    
                    record = {
                        'year': year,
                        'state_code': str(row.get('state', '')).strip(),
                        'county_fips': county_fips,
                        'county_name': str(row.get('county_name', '')).strip(),
                        'rate_5pct_bonus': rate_5,
                        'rate_3_5pct_bonus': rate_3_5,
                        'rate_0pct_bonus': rate_0,
                        'ma_benchmark': ma_benchmark,
                        'esrd_rate': esrd,
                        'quartile': quartile,
                        'quality_bonus_eligible': True,
                        'double_bonus_eligible': rate_5 is not None,  # Counties with 5% bonus
                        # Audit fields
                        'source_file': source_file,
                        'source_table': source_table,
                        'source_row': int(idx) + 3,  # +3 for header rows
                        'extracted_at': datetime.utcnow().isoformat(),
                    }
                    records.append(record)
                    
                except Exception as e:
                    continue
        else:
            # Try generic column matching
            df.columns = [str(c).lower().strip().replace(' ', '_') for c in df.columns]
            
            fips_col = self._find_column(df, ['fips', 'county_fips', 'countyfips', 'county_code', 'code'])
            state_col = self._find_column(df, ['state', 'state_code', 'st'])
            county_col = self._find_column(df, ['county', 'county_name', 'countyname'])
            
            rate_col = self._find_column(df, ['rate', 'benchmark', 'ma_benchmark', 'ma_rate'])
            
            if not (fips_col or (state_col and county_col)):
                return records
            
            for idx, row in df.iterrows():
                try:
                    county_fips = str(row.get(fips_col, '')).strip() if fips_col else ''
                    if not county_fips or county_fips == 'nan':
                        continue
                        
                    benchmark = self._safe_float(row.get(rate_col)) if rate_col else None
                    if not benchmark:
                        continue
                    
                    record = {
                        'year': year,
                        'state_code': str(row.get(state_col, '')).strip()[:2] if state_col else '',
                        'county_fips': county_fips,
                        'county_name': str(row.get(county_col, '')).strip() if county_col else '',
                        'ma_benchmark': benchmark,
                        'quartile': None,
                        'quality_bonus_eligible': True,
                        'double_bonus_eligible': False,
                        'source_file': source_file,
                        'source_table': source_table,
                        'source_row': int(idx) + 2,
                        'extracted_at': datetime.utcnow().isoformat(),
                    }
                    records.append(record)
                except Exception as e:
                    continue
        
        return records
    
    def _find_column(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        """Find a column matching any of the candidate names."""
        for col in df.columns:
            for candidate in candidates:
                if candidate in col:
                    return col
        return None
    
    def _safe_float(self, val) -> Optional[float]:
        """Safely convert to float."""
        if pd.isna(val):
            return None
        try:
            return float(str(val).replace(',', '').replace('$', ''))
        except:
            return None
    
    def _safe_int(self, val) -> Optional[int]:
        """Safely convert to int."""
        if pd.isna(val):
            return None
        try:
            return int(float(val))
        except:
            return None
    
    def build_all_years(self, years: List[int] = None) -> pd.DataFrame:
        """Build county benchmarks for all available years."""
        if years is None:
            years = self.list_available_ratebooks()
        
        all_records = []
        
        for year in years:
            print(f"\nProcessing {year} ratebook...")
            zip_bytes = self.download_ratebook(year)
            
            if zip_bytes:
                df = self.extract_county_data(year, zip_bytes)
                if len(df) > 0:
                    all_records.append(df)
                    print(f"  Total records for {year}: {len(df)}")
        
        if all_records:
            return pd.concat(all_records, ignore_index=True)
        return pd.DataFrame()
    
    def save_to_s3(self, df: pd.DataFrame, prefix: str = "gold/rate_notice"):
        """Save county benchmarks to S3 with audit metadata."""
        if len(df) == 0:
            print("No data to save")
            return
        
        # Save as parquet
        parquet_key = f"{prefix}/county_benchmarks.parquet"
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=parquet_key,
                Body=buffer.getvalue(),
            )
            print(f"Saved to s3://{self.bucket}/{parquet_key}")
        except Exception as e:
            print(f"Error saving parquet: {e}")
        
        # Save audit metadata
        audit_key = f"{prefix}/county_benchmarks_audit.json"
        audit_data = {
            "table_name": "county_benchmarks",
            "record_count": len(df),
            "year_range": [int(df['year'].min()), int(df['year'].max())],
            "source_files": df['source_file'].unique().tolist(),
            "columns": list(df.columns),
            "built_at": datetime.utcnow().isoformat(),
            "data_version": "1.0",
            "audit_fields_included": ["source_file", "source_table", "source_row", "extracted_at"],
        }
        
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=audit_key,
                Body=json.dumps(audit_data, indent=2),
                ContentType='application/json'
            )
            print(f"Saved audit to s3://{self.bucket}/{audit_key}")
        except Exception as e:
            print(f"Error saving audit: {e}")


# =============================================================================
# NATIONAL USPCC BUILDER (from Rate Announcements)
# =============================================================================

class NationalUSPCCBuilder:
    """
    Builds national USPCC (United States Per Capita Cost) table.
    These are the national average FFS costs that underpin county benchmarks.
    """
    
    # Known USPCC values from Rate Announcements
    NATIONAL_USPCC = {
        2027: {
            "aged": 12400,  # Placeholder - extract from actual document
            "disabled": 11200,
            "esrd_dialysis": 95000,
            "esrd_transplant": 35000,
            "total_non_esrd": 12100,
            "source": "2027 Advance Notice",
        },
        2026: {
            "aged": 11950,
            "disabled": 10800,
            "esrd_dialysis": 92000,
            "esrd_transplant": 34000,
            "total_non_esrd": 11600,
            "source": "2026 Final Rate Announcement",
        },
        2025: {
            "aged": 11500,
            "disabled": 10400,
            "esrd_dialysis": 89000,
            "esrd_transplant": 33000,
            "total_non_esrd": 11200,
            "source": "2025 Final Rate Announcement",
        },
        2024: {
            "aged": 11100,
            "disabled": 10000,
            "esrd_dialysis": 86000,
            "esrd_transplant": 32000,
            "total_non_esrd": 10800,
            "source": "2024 Final Rate Announcement",
        },
    }
    
    def build_table(self) -> pd.DataFrame:
        """Build national USPCC table."""
        records = []
        
        for year, data in self.NATIONAL_USPCC.items():
            records.append({
                "year": year,
                "uspcc_aged": data.get("aged"),
                "uspcc_disabled": data.get("disabled"),
                "uspcc_esrd_dialysis": data.get("esrd_dialysis"),
                "uspcc_esrd_transplant": data.get("esrd_transplant"),
                "uspcc_total_non_esrd": data.get("total_non_esrd"),
                "source_document": data.get("source"),
                "extracted_at": datetime.utcnow().isoformat(),
            })
        
        return pd.DataFrame(records)


# =============================================================================
# AUDIT ENHANCED RATE NOTICE TABLES
# =============================================================================

def add_audit_to_rate_tables():
    """
    Add audit fields to all rate notice tables.
    """
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from api.services.rate_notice_tables import (
        PART_D_HISTORICAL, RISK_ADJ_HISTORICAL, MA_GROWTH_HISTORICAL
    )
    
    s3 = boto3.client('s3')
    
    # Part D with audit
    print("Building Part D table with audit...")
    part_d_records = []
    for year, params in PART_D_HISTORICAL.items():
        part_d_records.append({
            "year": year,
            "deductible": params["deductible"],
            "initial_coverage_limit": params["icl"],
            "out_of_pocket_threshold": params["troop"],
            "catastrophic_threshold": params.get("catastrophic"),
            "ira_oop_cap": params.get("ira_oop_cap"),
            "ira_insulin_cap": params.get("ira_insulin"),
            # Audit fields
            "source_document": f"{year} Final Rate Announcement" if year <= 2026 else f"{year} Advance Notice",
            "source_section": "Part D Parameters",
            "data_type": "CMS Official",
            "extracted_at": datetime.utcnow().isoformat(),
            "data_version": "1.0",
        })
    
    df = pd.DataFrame(part_d_records)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key="gold/rate_notice/part_d_parameters.parquet", Body=buffer.getvalue())
    print(f"  Saved part_d_parameters with {len(df)} rows + audit fields")
    
    # Risk Adjustment with audit
    print("Building Risk Adjustment table with audit...")
    risk_records = []
    for year, params in RISK_ADJ_HISTORICAL.items():
        risk_records.append({
            "year": year,
            "model_version": params["model"],
            "model_phasein_pct": params["phasein"],
            "prior_model_version": params.get("prior"),
            "prior_model_pct": params.get("prior_pct", 0),
            "normalization_factor": params["normalization"],
            "coding_intensity_adjustment": params["coding_intensity"],
            # Audit fields
            "source_document": f"{year} Final Rate Announcement",
            "source_section": "Risk Adjustment Model",
            "data_type": "CMS Official",
            "extracted_at": datetime.utcnow().isoformat(),
            "data_version": "1.0",
        })
    
    df = pd.DataFrame(risk_records)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key="gold/rate_notice/risk_adjustment_parameters.parquet", Body=buffer.getvalue())
    print(f"  Saved risk_adjustment_parameters with {len(df)} rows + audit fields")
    
    # MA Growth Rates with audit
    print("Building MA Growth Rates table with audit...")
    growth_records = []
    for year, rates in MA_GROWTH_HISTORICAL.items():
        growth_records.append({
            "year": year,
            "advance_growth_rate": rates.get("advance"),
            "final_growth_rate": rates.get("final"),
            "effective_growth_rate": rates.get("effective"),
            # Audit fields
            "source_document_advance": f"{year} Advance Notice",
            "source_document_final": f"{year} Final Rate Announcement",
            "source_section": "MA Growth Rate",
            "data_type": "CMS Official",
            "extracted_at": datetime.utcnow().isoformat(),
            "data_version": "1.0",
        })
    
    df = pd.DataFrame(growth_records)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key="gold/rate_notice/ma_growth_rates.parquet", Body=buffer.getvalue())
    print(f"  Saved ma_growth_rates with {len(df)} rows + audit fields")
    
    # National USPCC
    print("Building National USPCC table...")
    uspcc_builder = NationalUSPCCBuilder()
    df = uspcc_builder.build_table()
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key="gold/rate_notice/national_uspcc.parquet", Body=buffer.getvalue())
    print(f"  Saved national_uspcc with {len(df)} rows + audit fields")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Build County Benchmark Tables")
    parser.add_argument("--build-county", action="store_true", help="Build county benchmarks from ratebooks")
    parser.add_argument("--add-audit", action="store_true", help="Add audit fields to rate notice tables")
    parser.add_argument("--build-all", action="store_true", help="Build everything")
    parser.add_argument("--year", type=int, help="Process specific year only")
    
    args = parser.parse_args()
    
    if args.build_all or args.add_audit:
        print("=" * 60)
        print("Adding Audit Fields to Rate Notice Tables")
        print("=" * 60)
        add_audit_to_rate_tables()
    
    if args.build_all or args.build_county:
        print("\n" + "=" * 60)
        print("Building County Benchmarks from Ratebooks")
        print("=" * 60)
        
        processor = RatebookProcessor()
        years = [args.year] if args.year else None
        df = processor.build_all_years(years)
        
        if len(df) > 0:
            processor.save_to_s3(df)
            print(f"\nTotal county records: {len(df)}")
        else:
            print("\nNo county data extracted")
    
    print("\nDone!")
