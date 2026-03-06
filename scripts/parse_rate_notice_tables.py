"""
Parse Extracted Rate Notice Tables into Clean Structured Data
=============================================================

Takes the 1,658 raw tables extracted from rate notice PDFs and parses them
into clean, queryable tables with full audit trails.

Output Tables:
1. uspcc_projections - Per capita cost projections by calendar year
2. hcc_coefficients_all - HCC coefficients across all model versions
3. part_d_parameters_full - Complete Part D benefit parameters
4. esrd_rates - ESRD dialysis/transplant rates and factors
5. demographic_factors - Age/sex adjustment factors
6. benchmark_parameters - Applicable percentages and quartile thresholds
7. service_type_costs - Cost breakdown by service category
8. growth_rate_projections - MA growth rate projections

Every row includes audit fields:
- source_document, source_page, source_table_num, extracted_at
"""

import os
import json
import re
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")


def clean_numeric(val: Any) -> Optional[float]:
    """Convert value to float, handling currency and percentages."""
    if val is None or val == '' or val == 'None':
        return None
    if isinstance(val, (int, float)):
        return float(val)
    
    s = str(val).strip()
    # Remove currency, commas, parentheses for negatives
    s = re.sub(r'[$,()]', '', s)
    s = s.replace('−', '-').replace('–', '-')  # Various dash types
    
    # Handle percentages
    if s.endswith('%'):
        try:
            return float(s[:-1])
        except:
            return None
    
    try:
        return float(s)
    except:
        return None


class RateNoticeTableParser:
    """
    Parses raw extracted tables into clean structured data.
    """
    
    def __init__(self, bucket: str = None):
        self.bucket = bucket or S3_BUCKET
        self.s3 = boto3.client('s3')
        self.raw_tables = None
    
    def load_raw_tables(self) -> Dict:
        """Load the extracted raw tables from S3."""
        if self.raw_tables is None:
            response = self.s3.get_object(
                Bucket=self.bucket, 
                Key='gold/rate_notice_tables/all_tables.json'
            )
            self.raw_tables = json.loads(response['Body'].read())
        return self.raw_tables
    
    def get_tables_by_category(self, category: str) -> List[Dict]:
        """Get all tables for a specific category."""
        tables = self.load_raw_tables()
        result = []
        for doc_key, table_list in tables.items():
            for t in table_list:
                if t['category'] == category:
                    result.append(t)
        return result
    
    # =========================================================================
    # USPCC PARSER
    # =========================================================================
    
    def parse_uspcc_tables(self) -> pd.DataFrame:
        """
        Parse USPCC (per capita cost) tables into clean time series.
        
        These tables show projected FFS costs by calendar year.
        """
        print("Parsing USPCC tables...")
        tables = self.get_tables_by_category('uspcc')
        records = []
        
        for t in tables:
            year = t['year']
            notice_type = t['notice_type']
            
            # Parse table data
            headers = [str(h).lower() for h in t['headers']]
            
            for row_idx, row in enumerate(t['data']):
                # Try to find calendar year in first column
                cal_year = None
                if row and len(row) > 0:
                    first_val = str(row[0]).strip()
                    # Extract 4-digit year
                    year_match = re.search(r'\b(20\d{2})\b', first_val)
                    if year_match:
                        cal_year = int(year_match.group(1))
                
                # Extract values from row
                for col_idx, (header, val) in enumerate(zip(headers, row)):
                    if val is None:
                        continue
                    
                    val_str = str(val)
                    
                    # Look for USPCC values (dollar amounts)
                    uspcc_matches = re.findall(r'\$?([\d,]+\.?\d*)', val_str)
                    
                    for match in uspcc_matches:
                        numeric_val = clean_numeric(match)
                        if numeric_val and numeric_val > 100:  # USPCC values are typically > $100
                            records.append({
                                'projection_year': year,
                                'notice_type': notice_type,
                                'calendar_year': cal_year,
                                'column_header': header[:100],
                                'uspcc_value': numeric_val,
                                'raw_text': val_str[:200],
                                # Audit fields
                                'source_document': t['source_document'],
                                'source_page': t['source_page'],
                                'source_table_num': t['source_table_num'],
                                'source_row': row_idx + 1,
                                'extracted_at': t['extracted_at'],
                            })
        
        df = pd.DataFrame(records)
        print(f"  Extracted {len(df)} USPCC data points")
        return df
    
    # =========================================================================
    # HCC COEFFICIENTS PARSER
    # =========================================================================
    
    def parse_hcc_tables(self) -> pd.DataFrame:
        """
        Parse HCC coefficient tables across all model versions.
        """
        print("Parsing HCC coefficient tables...")
        tables = self.get_tables_by_category('hcc')
        records = []
        
        for t in tables:
            headers = t['headers']
            year = t['year']
            
            # Detect model version from headers or content
            header_text = ' '.join(str(h) for h in headers).lower()
            content_text = ' '.join(str(c) for row in t['data'][:5] for c in row if c).lower()
            
            model_version = None
            if 'v28' in header_text or 'v28' in content_text:
                model_version = 'V28'
            elif 'v24' in header_text or 'v24' in content_text:
                model_version = 'V24'
            elif 'v22' in header_text or 'v22' in content_text:
                model_version = 'V22'
            elif 'v21' in header_text or 'v21' in content_text:
                model_version = 'V21'
            elif 'rxhcc' in header_text:
                model_version = 'RxHCC'
            
            # Find coefficient columns
            coef_cols = []
            for idx, h in enumerate(headers):
                h_lower = str(h).lower()
                if any(term in h_lower for term in ['community', 'institutional', 'aged', 'disabled', 'coefficient', 'non-low']):
                    coef_cols.append((idx, h))
            
            for row_idx, row in enumerate(t['data']):
                if not row or len(row) < 2:
                    continue
                
                # First column often has HCC code
                first_val = str(row[0]).strip() if row[0] else ''
                
                # Look for HCC pattern
                hcc_match = re.search(r'(HCC|RxHCC)?\s*(\d{1,3})', first_val, re.IGNORECASE)
                hcc_code = None
                if hcc_match:
                    prefix = hcc_match.group(1) or 'HCC'
                    num = hcc_match.group(2).zfill(3)
                    hcc_code = f"{prefix.upper()}{num}"
                
                # Get label from second column or first column
                hcc_label = ''
                if len(row) > 1 and row[1]:
                    hcc_label = str(row[1])[:200]
                elif not hcc_code and first_val:
                    hcc_label = first_val[:200]
                
                # Extract coefficients
                for col_idx, col_name in coef_cols:
                    if col_idx < len(row):
                        coef_val = clean_numeric(row[col_idx])
                        if coef_val is not None and -5 < coef_val < 10:  # Reasonable coefficient range
                            records.append({
                                'model_version': model_version,
                                'model_year': year,
                                'hcc_code': hcc_code,
                                'hcc_label': hcc_label,
                                'segment': col_name[:100],
                                'coefficient': coef_val,
                                # Audit
                                'source_document': t['source_document'],
                                'source_page': t['source_page'],
                                'source_table_num': t['source_table_num'],
                                'source_row': row_idx + 1,
                                'extracted_at': t['extracted_at'],
                            })
        
        df = pd.DataFrame(records)
        print(f"  Extracted {len(df)} HCC coefficients")
        
        # Summary by model version
        if len(df) > 0:
            by_version = df.groupby('model_version').size()
            print(f"  By model version: {dict(by_version)}")
        
        return df
    
    # =========================================================================
    # PART D PARAMETERS PARSER
    # =========================================================================
    
    def parse_part_d_tables(self) -> pd.DataFrame:
        """
        Parse Part D benefit parameter tables.
        """
        print("Parsing Part D parameter tables...")
        tables = self.get_tables_by_category('part_d')
        records = []
        
        for t in tables:
            headers = t['headers']
            year = t['year']
            notice_type = t['notice_type']
            
            # Look for year columns (2015, 2016, etc.)
            year_cols = []
            for idx, h in enumerate(headers):
                h_str = str(h)
                year_match = re.search(r'\b(20\d{2})\b', h_str)
                if year_match:
                    year_cols.append((idx, int(year_match.group(1))))
            
            for row_idx, row in enumerate(t['data']):
                if not row or len(row) < 2:
                    continue
                
                # First column is parameter name
                param_name = str(row[0]).strip() if row[0] else ''
                param_lower = param_name.lower()
                
                # Identify parameter type
                param_type = None
                if 'deductible' in param_lower:
                    param_type = 'deductible'
                elif 'initial coverage' in param_lower or 'icl' in param_lower:
                    param_type = 'initial_coverage_limit'
                elif 'out-of-pocket' in param_lower or 'troop' in param_lower or 'oop' in param_lower:
                    param_type = 'out_of_pocket_threshold'
                elif 'catastrophic' in param_lower:
                    param_type = 'catastrophic_threshold'
                elif 'premium' in param_lower:
                    param_type = 'base_premium'
                
                if not param_type:
                    continue
                
                # Extract values for each year column
                for col_idx, col_year in year_cols:
                    if col_idx < len(row):
                        value = clean_numeric(row[col_idx])
                        if value is not None:
                            records.append({
                                'parameter_year': col_year,
                                'projection_year': year,
                                'notice_type': notice_type,
                                'parameter_name': param_type,
                                'parameter_label': param_name[:100],
                                'value': value,
                                # Audit
                                'source_document': t['source_document'],
                                'source_page': t['source_page'],
                                'source_table_num': t['source_table_num'],
                                'source_row': row_idx + 1,
                                'extracted_at': t['extracted_at'],
                            })
        
        df = pd.DataFrame(records)
        print(f"  Extracted {len(df)} Part D parameters")
        return df
    
    # =========================================================================
    # ESRD RATES PARSER
    # =========================================================================
    
    def parse_esrd_tables(self) -> pd.DataFrame:
        """
        Parse ESRD rate and coefficient tables.
        """
        print("Parsing ESRD tables...")
        tables = self.get_tables_by_category('esrd')
        records = []
        
        for t in tables:
            headers = t['headers']
            year = t['year']
            notice_type = t['notice_type']
            
            # Find coefficient columns
            coef_cols = []
            for idx, h in enumerate(headers):
                h_lower = str(h).lower()
                if idx > 0:  # Skip first column (variable names)
                    coef_cols.append((idx, str(h)[:100]))
            
            for row_idx, row in enumerate(t['data']):
                if not row or len(row) < 2:
                    continue
                
                # First column is variable name
                var_name = str(row[0]).strip() if row[0] else ''
                if not var_name or var_name == 'None':
                    continue
                
                # Determine variable type
                var_lower = var_name.lower()
                var_type = None
                if 'dialysis' in var_lower:
                    var_type = 'dialysis'
                elif 'transplant' in var_lower:
                    var_type = 'transplant'
                elif 'graft' in var_lower:
                    var_type = 'graft'
                elif 'age' in var_lower or 'sex' in var_lower:
                    var_type = 'demographic'
                elif 'hcc' in var_lower:
                    var_type = 'hcc_coefficient'
                else:
                    var_type = 'other'
                
                # Extract coefficients
                for col_idx, col_name in coef_cols:
                    if col_idx < len(row):
                        value = clean_numeric(row[col_idx])
                        if value is not None:
                            records.append({
                                'year': year,
                                'notice_type': notice_type,
                                'variable_name': var_name[:200],
                                'variable_type': var_type,
                                'segment': col_name,
                                'value': value,
                                # Audit
                                'source_document': t['source_document'],
                                'source_page': t['source_page'],
                                'source_table_num': t['source_table_num'],
                                'source_row': row_idx + 1,
                                'extracted_at': t['extracted_at'],
                            })
        
        df = pd.DataFrame(records)
        print(f"  Extracted {len(df)} ESRD data points")
        return df
    
    # =========================================================================
    # DEMOGRAPHIC FACTORS PARSER
    # =========================================================================
    
    def parse_demographic_tables(self) -> pd.DataFrame:
        """
        Parse demographic adjustment factor tables.
        """
        print("Parsing demographic factor tables...")
        tables = self.get_tables_by_category('demographic')
        records = []
        
        for t in tables:
            headers = t['headers']
            year = t['year']
            notice_type = t['notice_type']
            
            # Look for trend/factor patterns
            for row_idx, row in enumerate(t['data']):
                if not row or len(row) < 2:
                    continue
                
                # First value
                first_val = str(row[0]).strip() if row[0] else ''
                
                # Look for percentage trends
                if 'trend' in first_val.lower() or 'percentage' in first_val.lower():
                    for col_idx, val in enumerate(row[1:], 1):
                        pct_val = clean_numeric(val)
                        if pct_val is not None and -20 < pct_val < 50:  # Reasonable percentage range
                            records.append({
                                'year': year,
                                'notice_type': notice_type,
                                'factor_type': 'trend',
                                'factor_name': first_val[:200],
                                'value': pct_val,
                                'column': str(headers[col_idx])[:100] if col_idx < len(headers) else f'col_{col_idx}',
                                # Audit
                                'source_document': t['source_document'],
                                'source_page': t['source_page'],
                                'source_table_num': t['source_table_num'],
                                'source_row': row_idx + 1,
                                'extracted_at': t['extracted_at'],
                            })
                
                # Look for year columns with factors
                year_match = re.search(r'\b(20\d{2})\b', first_val)
                if year_match:
                    factor_year = int(year_match.group(1))
                    for col_idx, val in enumerate(row[1:], 1):
                        factor_val = clean_numeric(val)
                        if factor_val is not None:
                            records.append({
                                'year': year,
                                'notice_type': notice_type,
                                'factor_type': 'annual',
                                'factor_year': factor_year,
                                'factor_name': str(headers[col_idx])[:100] if col_idx < len(headers) else 'value',
                                'value': factor_val,
                                # Audit
                                'source_document': t['source_document'],
                                'source_page': t['source_page'],
                                'source_table_num': t['source_table_num'],
                                'source_row': row_idx + 1,
                                'extracted_at': t['extracted_at'],
                            })
        
        df = pd.DataFrame(records)
        print(f"  Extracted {len(df)} demographic factors")
        return df
    
    # =========================================================================
    # BENCHMARK PARAMETERS PARSER
    # =========================================================================
    
    def parse_benchmark_tables(self) -> pd.DataFrame:
        """
        Parse benchmark parameter tables (applicable percentages, quartiles).
        """
        print("Parsing benchmark parameter tables...")
        tables = self.get_tables_by_category('benchmark')
        records = []
        
        for t in tables:
            headers = t['headers']
            year = t['year']
            notice_type = t['notice_type']
            
            for row_idx, row in enumerate(t['data']):
                if not row or len(row) < 2:
                    continue
                
                first_val = str(row[0]).strip() if row[0] else ''
                first_lower = first_val.lower()
                
                # Look for applicable percentage entries
                if 'applicable' in first_lower or re.match(r'^0\.\d+', first_val):
                    for col_idx, val in enumerate(row):
                        pct_val = clean_numeric(val)
                        if pct_val is not None:
                            # Determine if this is applicable percentage or bid ratio
                            param_type = 'applicable_percentage' if pct_val < 2 else 'bid_ratio'
                            if '%' in str(val):
                                param_type = 'bid_ratio'
                            
                            records.append({
                                'year': year,
                                'notice_type': notice_type,
                                'parameter_type': param_type,
                                'parameter_label': first_val[:100],
                                'value': pct_val,
                                'column': str(headers[col_idx])[:100] if col_idx < len(headers) else f'col_{col_idx}',
                                # Audit
                                'source_document': t['source_document'],
                                'source_page': t['source_page'],
                                'source_table_num': t['source_table_num'],
                                'source_row': row_idx + 1,
                                'extracted_at': t['extracted_at'],
                            })
                
                # Look for quartile entries
                if 'quartile' in first_lower or re.match(r'^[1-4]$', first_val):
                    for col_idx, val in enumerate(row[1:], 1):
                        val_parsed = clean_numeric(val)
                        if val_parsed is not None:
                            records.append({
                                'year': year,
                                'notice_type': notice_type,
                                'parameter_type': 'quartile_threshold',
                                'parameter_label': first_val[:100],
                                'value': val_parsed,
                                'column': str(headers[col_idx])[:100] if col_idx < len(headers) else f'col_{col_idx}',
                                # Audit
                                'source_document': t['source_document'],
                                'source_page': t['source_page'],
                                'source_table_num': t['source_table_num'],
                                'source_row': row_idx + 1,
                                'extracted_at': t['extracted_at'],
                            })
        
        df = pd.DataFrame(records)
        print(f"  Extracted {len(df)} benchmark parameters")
        return df
    
    # =========================================================================
    # SERVICE TYPE COSTS PARSER
    # =========================================================================
    
    def parse_service_type_tables(self) -> pd.DataFrame:
        """
        Parse service type cost breakdown tables.
        """
        print("Parsing service type cost tables...")
        tables = self.get_tables_by_category('service_type')
        records = []
        
        for t in tables:
            headers = t['headers']
            year = t['year']
            notice_type = t['notice_type']
            
            for row_idx, row in enumerate(t['data']):
                if not row or len(row) < 2:
                    continue
                
                service_type = str(row[0]).strip() if row[0] else ''
                if not service_type or service_type == 'None':
                    continue
                
                for col_idx, val in enumerate(row[1:], 1):
                    cost_val = clean_numeric(val)
                    if cost_val is not None and cost_val > 0:
                        records.append({
                            'year': year,
                            'notice_type': notice_type,
                            'service_type': service_type[:100],
                            'cost_category': str(headers[col_idx])[:100] if col_idx < len(headers) else 'value',
                            'value': cost_val,
                            # Audit
                            'source_document': t['source_document'],
                            'source_page': t['source_page'],
                            'source_table_num': t['source_table_num'],
                            'source_row': row_idx + 1,
                            'extracted_at': t['extracted_at'],
                        })
        
        df = pd.DataFrame(records)
        print(f"  Extracted {len(df)} service type costs")
        return df
    
    # =========================================================================
    # MASTER PARSE FUNCTION
    # =========================================================================
    
    def parse_all(self) -> Dict[str, pd.DataFrame]:
        """Parse all table categories."""
        print("=" * 70)
        print("PARSING ALL RATE NOTICE TABLES")
        print("=" * 70)
        
        results = {
            'uspcc_projections': self.parse_uspcc_tables(),
            'hcc_coefficients_all': self.parse_hcc_tables(),
            'part_d_parameters_full': self.parse_part_d_tables(),
            'esrd_rates': self.parse_esrd_tables(),
            'demographic_factors': self.parse_demographic_tables(),
            'benchmark_parameters': self.parse_benchmark_tables(),
            'service_type_costs': self.parse_service_type_tables(),
        }
        
        print("\n" + "=" * 70)
        print("PARSING COMPLETE")
        print("=" * 70)
        
        total_rows = sum(len(df) for df in results.values())
        print(f"\nTotal parsed rows: {total_rows:,}")
        
        for name, df in results.items():
            print(f"  {name}: {len(df):,} rows")
        
        return results
    
    def save_to_s3(self, tables: Dict[str, pd.DataFrame], prefix: str = "gold/rate_notice_parsed"):
        """Save parsed tables to S3."""
        print(f"\nSaving to S3 ({prefix})...")
        
        for name, df in tables.items():
            if len(df) == 0:
                print(f"  Skipping {name} (empty)")
                continue
            
            # Save as parquet
            key = f"{prefix}/{name}.parquet"
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            try:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=buffer.getvalue()
                )
                print(f"  ✓ {name}: {len(df):,} rows -> s3://{self.bucket}/{key}")
            except Exception as e:
                print(f"  ✗ {name}: {e}")
        
        # Save audit summary
        audit = {
            'parsed_at': datetime.now(timezone.utc).isoformat(),
            'source': 'gold/rate_notice_tables/all_tables.json',
            'tables_created': {name: len(df) for name, df in tables.items()},
            'total_rows': sum(len(df) for df in tables.values()),
            'audit_columns': [
                'source_document',
                'source_page', 
                'source_table_num',
                'source_row',
                'extracted_at'
            ]
        }
        
        self.s3.put_object(
            Bucket=self.bucket,
            Key=f"{prefix}/_audit.json",
            Body=json.dumps(audit, indent=2),
            ContentType='application/json'
        )
        print(f"  ✓ Audit summary saved")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Parse Rate Notice Tables")
    parser.add_argument("--parse-all", action="store_true", help="Parse all tables")
    parser.add_argument("--save", action="store_true", help="Save to S3")
    parser.add_argument("--category", type=str, help="Parse specific category")
    
    args = parser.parse_args()
    
    parser_obj = RateNoticeTableParser()
    
    if args.parse_all:
        tables = parser_obj.parse_all()
        
        if args.save:
            parser_obj.save_to_s3(tables)
    
    elif args.category:
        method_name = f"parse_{args.category}_tables"
        if hasattr(parser_obj, method_name):
            df = getattr(parser_obj, method_name)()
            print(df.head(20).to_string())
        else:
            print(f"Unknown category: {args.category}")
    
    else:
        parser.print_help()
