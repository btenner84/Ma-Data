#!/usr/bin/env python3
"""
Fix Measure Stars Data
======================
Properly parse CMS "Measure Stars" files which are in WIDE format
(each measure is a column, values are star ratings).

This merges star ratings with existing measure performance data.
"""

import boto3
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
import sys

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def find_stars_csv_in_zip(zf) -> tuple:
    """Find measure stars CSV file in a zip, handling nested zips.
    Returns (content_bytes, filename) or (None, None) if not found."""
    
    # First check for direct CSVs
    stars_files = [f for f in zf.namelist() if 'Measure Stars' in f and f.endswith('.csv')]
    if not stars_files:
        stars_files = [f for f in zf.namelist() if '_stars.csv' in f.lower()]
    
    if stars_files:
        with zf.open(stars_files[0]) as csvf:
            return csvf.read(), stars_files[0]
    
    # Check nested zips
    nested_zips = [f for f in zf.namelist() if f.endswith('.zip') and 'Star Rating' in f]
    if not nested_zips:
        nested_zips = [f for f in zf.namelist() if f.endswith('.zip') and 'star' in f.lower()]
    
    for nested_name in nested_zips:
        with zf.open(nested_name) as nested_f:
            nested_zip = ZipFile(BytesIO(nested_f.read()), 'r')
            nested_stars = [f for f in nested_zip.namelist() if 'Measure Stars' in f and f.endswith('.csv')]
            if not nested_stars:
                nested_stars = [f for f in nested_zip.namelist() if '_stars.csv' in f.lower()]
            
            if nested_stars:
                with nested_zip.open(nested_stars[0]) as csvf:
                    return csvf.read(), nested_stars[0]
    
    return None, None


def parse_measure_stars_file(zip_key: str, year: int) -> pd.DataFrame:
    """Parse a CMS Measure Stars file from wide format to long format."""
    
    response = s3.get_object(Bucket=S3_BUCKET, Key=zip_key)
    zip_data = BytesIO(response['Body'].read())
    
    with ZipFile(zip_data, 'r') as zf:
        content, stars_file = find_stars_csv_in_zip(zf)
        
        if content is None:
            print(f"  No measure stars file found in {zip_key}")
            return pd.DataFrame()
        
        print(f"  Parsing: {stars_file}")
        
        # Try different encodings
        for enc in ['latin-1', 'cp1252', 'utf-8']:
            try:
                df_raw = pd.read_csv(BytesIO(content), dtype=str, encoding=enc, header=None)
                break
            except:
                continue
        
        # Find header rows - look for CONTRACT_ID or Contract Number
        header_row = None
        measure_row = None
        for i in range(min(10, len(df_raw))):
            row_text = ' '.join(str(v) for v in df_raw.iloc[i].values if pd.notna(v))
            if 'CONTRACT' in row_text.upper() and ('ID' in row_text.upper() or 'NUMBER' in row_text.upper()):
                header_row = i
                measure_row = i + 1  # Measure codes usually in next row
                break
        
        if header_row is None:
            print(f"  Could not find header row")
            return pd.DataFrame()
        
        # Find where contract columns end and measure columns begin
        # Usually first 5-6 columns are contract info
        measure_start = 5
        for i, val in enumerate(df_raw.iloc[measure_row].values):
            if pd.notna(val) and str(val).strip().startswith(('C0', 'C1', 'C2', 'C3', 'D0', 'D1')):
                measure_start = i
                break
        
        # Extract measure IDs from measure_row
        measure_ids = []
        for val in df_raw.iloc[measure_row, measure_start:].values:
            if pd.notna(val):
                val_str = str(val).strip()
                # Extract measure code (C01, D01, etc.)
                if ':' in val_str:
                    code = val_str.split(':')[0].strip()
                else:
                    import re
                    match = re.match(r'^([CD]\d{2})', val_str)
                    code = match.group(1) if match else val_str[:10]
                measure_ids.append(code)
            else:
                measure_ids.append(None)
        
        # Read data rows (skip header and measure rows)
        data_start = measure_row + 2  # Usually 2 rows after measure codes (dates row in between)
        
        # Check if there's a date row
        date_row_text = ' '.join(str(v) for v in df_raw.iloc[measure_row + 1].values if pd.notna(v))
        if '/' in date_row_text or '20' in date_row_text:  # Date pattern
            data_start = measure_row + 2
        else:
            data_start = measure_row + 1
        
        df = df_raw.iloc[data_start:].copy()
        
        # Set column names
        contract_cols = ['contract_id', 'org_type', 'contract_name', 'marketing_name', 'parent_org']
        all_cols = contract_cols[:measure_start] + measure_ids
        df.columns = all_cols[:len(df.columns)]
        
        # Clean contract_id
        df['contract_id'] = df['contract_id'].astype(str).str.strip()
        
        # Filter valid contracts
        import re
        df = df[df['contract_id'].str.match(r'^[HERS]\d{4}$', na=False)]
        
        # Get measure columns (C01, D01, etc.)
        measure_cols = [c for c in df.columns if c and (str(c).startswith('C') or str(c).startswith('D')) and len(str(c)) <= 4]
        
        if not measure_cols:
            print(f"  No measure columns found")
            return pd.DataFrame()
        
        # Melt to long format
        id_cols = [c for c in ['contract_id', 'parent_org'] if c in df.columns]
        df_long = df.melt(
            id_vars=id_cols,
            value_vars=measure_cols,
            var_name='measure_id',
            value_name='star_rating'
        )
        
        # Clean star ratings
        def clean_star(val):
            if pd.isna(val):
                return None
            val_str = str(val).strip()
            if val_str in ['1', '2', '3', '4', '5']:
                return int(val_str)
            try:
                num = float(val_str)
                if 1 <= num <= 5 and num == int(num):
                    return int(num)
            except:
                pass
            return None
        
        df_long['star_rating'] = df_long['star_rating'].apply(clean_star)
        df_long = df_long[df_long['star_rating'].notna()]
        df_long['year'] = year
        df_long['_source_file'] = stars_file
        
        return df_long


def main():
    print("=" * 70)
    print("FIX MEASURE STARS DATA")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    
    # Map of years to ZIP files - include both raw and docs paths
    star_files = {
        # Raw stars files (older format with _stars.csv)
        2008: 'raw/stars/2008_ratings.zip',
        2009: 'raw/stars/2009_ratings.zip',
        2010: 'raw/stars/2010_combined.zip',
        # 2011-2013 don't have _stars.csv files in CMS releases
        2014: 'raw/stars/2014_combined.zip',
        2015: 'raw/stars/2015_combined.zip',
        2016: 'raw/stars/2016_combined.zip',
        2017: 'raw/stars/2017_combined.zip',
        2018: 'raw/stars/2018_combined.zip',
        # Newer format with "Measure Stars" CSV (in nested zips)
        2019: 'docs/stars/data_tables/2019_star_ratings.zip',
        2020: 'docs/stars/data_tables/2020_star_ratings.zip',
        2021: 'docs/stars/data_tables/2021_star_ratings.zip',
        2022: 'docs/stars/data_tables/2022_star_ratings.zip',
        2023: 'docs/stars/data_tables/2023_star_ratings.zip',
        2024: 'docs/stars/data_tables/2024_star_ratings_data.zip',
        2025: 'docs/stars/data_tables/2025_star_ratings.zip',
        2026: 'docs/stars/data_tables/2026_star_ratings.zip',
    }
    
    all_stars = []
    
    for year, zip_key in sorted(star_files.items()):
        print(f"\n[{year}] Processing {zip_key}")
        try:
            df = parse_measure_stars_file(zip_key, year)
            if not df.empty:
                print(f"  Parsed {len(df):,} rows, {df['contract_id'].nunique()} contracts")
                all_stars.append(df)
            else:
                print(f"  No data extracted")
        except Exception as e:
            print(f"  Error: {e}")
    
    if all_stars:
        combined = pd.concat(all_stars, ignore_index=True)
        print(f"\n{'='*70}")
        print(f"TOTAL: {len(combined):,} measure star ratings")
        print(f"Years: {combined['year'].min()} - {combined['year'].max()}")
        print(f"Contracts: {combined['contract_id'].nunique()}")
        print(f"Measures: {combined['measure_id'].nunique()}")
        
        # Save to S3
        output_key = 'processed/unified/measure_stars_all_years.parquet'
        buffer = BytesIO()
        combined.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=buffer.getvalue())
        print(f"\nSaved to s3://{S3_BUCKET}/{output_key}")
        
        # Print summary by year
        print("\nBy year:")
        for year in sorted(combined['year'].unique()):
            year_df = combined[combined['year'] == year]
            print(f"  {year}: {len(year_df):,} rows, {year_df['contract_id'].nunique()} contracts")


if __name__ == "__main__":
    main()
