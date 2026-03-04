#!/usr/bin/env python3
"""
Build Gold Layer: dim_geography
================================

Creates the geography dimension table from CPSC enrollment data.

Source: s3://ma-data123/silver/enrollment/cpsc/*/enrollment.parquet
Output: s3://ma-data123/gold/dim_geography.parquet

Columns:
- geo_key: STRING (FIPS code)
- state_code: STRING (2-letter state)
- state_name: STRING
- county_name: STRING
- fips_code: STRING (5-digit)
- ssa_code: STRING (SSA county code)
- cbsa_code: STRING (metro area, if applicable)
- region: STRING (Census region)
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import os

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
SILVER_PREFIX = "silver/enrollment/cpsc"
OUTPUT_KEY = "gold/dim_geography.parquet"

STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
    'PR': 'Puerto Rico', 'VI': 'Virgin Islands', 'GU': 'Guam',
    'AS': 'American Samoa', 'MP': 'Northern Mariana Islands',
}

CENSUS_REGIONS = {
    'CT': 'Northeast', 'ME': 'Northeast', 'MA': 'Northeast', 'NH': 'Northeast',
    'RI': 'Northeast', 'VT': 'Northeast', 'NJ': 'Northeast', 'NY': 'Northeast', 'PA': 'Northeast',
    'IL': 'Midwest', 'IN': 'Midwest', 'MI': 'Midwest', 'OH': 'Midwest', 'WI': 'Midwest',
    'IA': 'Midwest', 'KS': 'Midwest', 'MN': 'Midwest', 'MO': 'Midwest',
    'NE': 'Midwest', 'ND': 'Midwest', 'SD': 'Midwest',
    'DE': 'South', 'FL': 'South', 'GA': 'South', 'MD': 'South', 'NC': 'South',
    'SC': 'South', 'VA': 'South', 'DC': 'South', 'WV': 'South',
    'AL': 'South', 'KY': 'South', 'MS': 'South', 'TN': 'South',
    'AR': 'South', 'LA': 'South', 'OK': 'South', 'TX': 'South',
    'AZ': 'West', 'CO': 'West', 'ID': 'West', 'MT': 'West', 'NV': 'West',
    'NM': 'West', 'UT': 'West', 'WY': 'West',
    'AK': 'West', 'CA': 'West', 'HI': 'West', 'OR': 'West', 'WA': 'West',
    'PR': 'Territories', 'VI': 'Territories', 'GU': 'Territories',
    'AS': 'Territories', 'MP': 'Territories',
}

s3 = boto3.client('s3')


def list_silver_files() -> list:
    """List all silver enrollment files."""
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=SILVER_PREFIX):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('/enrollment.parquet'):
                files.append(obj['Key'])
    return sorted(files)


def load_parquet(key: str) -> pd.DataFrame:
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"  Error loading {key}: {e}")
        return pd.DataFrame()


def main():
    print("=" * 70)
    print("BUILD GOLD LAYER: dim_geography")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    
    silver_files = list_silver_files()
    print(f"Found {len(silver_files)} silver enrollment files")
    
    if not silver_files:
        print("No silver files found. Checking existing dim_county...")
        try:
            df = load_parquet('processed/unified/dim_county.parquet')
            if not df.empty:
                print(f"Using existing dim_county: {len(df)} rows")
                buffer = BytesIO()
                df.to_parquet(buffer, index=False, compression='snappy')
                buffer.seek(0)
                s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
                print(f"Copied to {OUTPUT_KEY}")
                return
        except:
            pass
        print("ERROR: No source data found")
        return
    
    all_geo = []
    for f in silver_files[:5]:
        df = load_parquet(f)
        if not df.empty:
            geo_cols = [c for c in ['state', 'county', 'fips_code', 'ssa_code'] if c in df.columns]
            if geo_cols:
                all_geo.append(df[geo_cols].drop_duplicates())
    
    if not all_geo:
        print("ERROR: No geography data found")
        return
    
    geo_df = pd.concat(all_geo, ignore_index=True).drop_duplicates()
    
    geo_df['state_code'] = geo_df['state'].str.strip().str.upper()
    geo_df['county_name'] = geo_df['county'].str.strip().str.title()
    geo_df['state_name'] = geo_df['state_code'].map(STATE_NAMES)
    geo_df['region'] = geo_df['state_code'].map(CENSUS_REGIONS)
    
    if 'fips_code' in geo_df.columns:
        geo_df['geo_key'] = geo_df['fips_code'].str.strip()
    else:
        geo_df['geo_key'] = geo_df['state_code'] + '_' + geo_df['county_name'].str.replace(' ', '_')
    
    geo_df = geo_df.drop_duplicates(subset=['geo_key'])
    
    final_cols = ['geo_key', 'state_code', 'state_name', 'county_name', 'fips_code', 'ssa_code', 'region']
    for col in final_cols:
        if col not in geo_df.columns:
            geo_df[col] = None
    
    result = geo_df[final_cols].copy()
    
    print(f"Generated {len(result):,} geography dimension rows")
    print(f"States: {result['state_code'].nunique()}")
    
    buffer = BytesIO()
    result.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
    
    print(f"Saved to s3://{S3_BUCKET}/{OUTPUT_KEY}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
