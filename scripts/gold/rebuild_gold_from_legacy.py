#!/usr/bin/env python3
"""
Rebuild Gold Layer from Legacy Tables
======================================

Rebuilds the Gold layer fact tables from the validated legacy tables
in processed/unified/ which have all dimension columns populated.

This ensures all filtering works: plan_type, product_type, snp_type, group_type, state, county.
"""

import boto3
import pandas as pd
import duckdb
from io import BytesIO
from datetime import datetime
import os
import sys
import configparser

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
PIPELINE_RUN_ID = f"gold_rebuild_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

s3 = boto3.client('s3')

def get_duckdb_conn():
    """Create DuckDB connection with S3 access."""
    conn = duckdb.connect(':memory:')
    conn.execute('INSTALL httpfs; LOAD httpfs;')
    
    # Get AWS credentials
    aws_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
    aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    
    if not aws_key:
        try:
            creds = configparser.ConfigParser()
            creds.read(os.path.expanduser('~/.aws/credentials'))
            aws_key = creds['default'].get('aws_access_key_id', '')
            aws_secret = creds['default'].get('aws_secret_access_key', '')
        except:
            pass
    
    conn.execute(f"""
        SET s3_region = 'us-east-1';
        SET s3_access_key_id = '{aws_key}';
        SET s3_secret_access_key = '{aws_secret}';
    """)
    return conn


def load_parquet(key: str) -> pd.DataFrame:
    """Load parquet file from S3."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        print(f"  Error loading {key}: {e}")
        return pd.DataFrame()


def save_parquet(df: pd.DataFrame, key: str):
    """Save dataframe to S3 as parquet."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    print(f"  Saved to s3://{S3_BUCKET}/{key} ({len(df):,} rows)")


def rebuild_fact_enrollment_national():
    """
    Rebuild gold/fact_enrollment_national from Monthly Enrollment by Contract.
    
    Two data sources:
    - 2019-2026: Monthly Enrollment by Contract (exact totals, source='monthly')
    - 2013-2018: CPSC aggregated as fallback (source='cpsc_fallback')
    
    The 'data_source' column tracks which source was used.
    """
    print("\n" + "=" * 70)
    print("REBUILDING: gold/fact_enrollment_national")
    print("  - 2019-2026: Monthly Enrollment by Contract (exact)")
    print("  - 2013-2018: CPSC fallback (suppressed)")
    print("=" * 70)
    
    import zipfile
    from io import BytesIO
    
    # Load dimension lookups for enrichment
    print("\n1. Loading dimension lookups...")
    contract_dim = load_parquet("processed/unified/dim_contract_v2.parquet")
    snp_lookup = load_parquet("processed/unified/snp_lookup.parquet")
    
    # Build lookup dicts for fast access
    contract_dict = {}
    if not contract_dim.empty:
        for _, row in contract_dim.iterrows():
            key = (row['contract_id'], row['year'])
            contract_dict[key] = {
                'plan_type': row.get('plan_type'),
                'product_type': row.get('product_type'),
            }
    print(f"   Loaded {len(contract_dict):,} contract mappings")
    
    snp_dict = {}
    if not snp_lookup.empty:
        for _, row in snp_lookup.iterrows():
            key = (row['contract_id'], row['year'])
            snp_dict[key] = row.get('snp_type', 'Non-SNP')
    print(f"   Loaded {len(snp_dict):,} SNP mappings")
    
    all_rows = []
    
    # --- PART 1: Load CPSC data for 2013-2018 ---
    print("\n2. Loading CPSC data for 2013-2018 (aggregated to national)...")
    cpsc_source = load_parquet("processed/unified/fact_enrollment_all_years.parquet")
    
    if not cpsc_source.empty:
        cpsc_2013_2018 = cpsc_source[(cpsc_source['year'] >= 2013) & (cpsc_source['year'] <= 2018)].copy()
        print(f"   Found {len(cpsc_2013_2018):,} rows for 2013-2018")
        
        for _, row in cpsc_2013_2018.iterrows():
            all_rows.append({
                'time_key': int(row['year']) * 100 + int(row['month']),
                'entity_id': row.get('contract_id', ''),
                'contract_id': row.get('contract_id', ''),
                'plan_id': '000',
                'year': int(row['year']),
                'month': int(row['month']),
                'enrollment': float(row.get('enrollment', 0)),
                'plan_count': int(row.get('plan_count', 1)),
                'parent_org': row.get('parent_org', ''),
                'state': row.get('state'),
                'plan_type': row.get('plan_type', ''),
                'product_type': row.get('product_type', ''),
                'snp_type': row.get('snp_type', 'Non-SNP'),
                'group_type': row.get('group_type', 'Individual'),
                '_source_file': 'cpsc_aggregated',
                'data_source': 'cpsc_fallback',  # Track that this is a fallback, not exact
            })
        
        print(f"   Added {len(all_rows):,} rows from CPSC (fallback for 2013-2018)")
    
    # --- PART 2: Process Monthly Enrollment by Contract for 2019+ ---
    print("\n3. Processing Monthly Enrollment by Contract files (2019+)...")
    
    # List all available files - only process valid zips
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='raw/enrollment/by_contract/', MaxKeys=500)
    all_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.zip')]
    
    # Filter to only valid zip files
    zip_files = []
    for key in all_keys:
        try:
            head = s3.get_object(Bucket=S3_BUCKET, Key=key, Range='bytes=0-4')
            if head['Body'].read().startswith(b'PK'):
                zip_files.append(key)
        except:
            pass
    
    print(f"   Found {len(zip_files)} valid zip files")
    
    monthly_contract_rows = []
    for zip_key in sorted(zip_files):
        # Extract year-month from path (e.g., raw/enrollment/by_contract/2024-01/...)
        parts = zip_key.split('/')
        year_month = parts[3] if len(parts) > 3 else None
        if not year_month or '-' not in year_month:
            continue
        
        year, month = map(int, year_month.split('-'))
        
        # Skip years we already have from CPSC
        if year < 2019:
            continue
        
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=zip_key)
            zip_data = BytesIO(obj['Body'].read())
            
            with zipfile.ZipFile(zip_data) as z:
                for name in z.namelist():
                    if name.endswith('.csv'):
                        with z.open(name) as f:
                            # Try multiple encodings
                            csv_data = f.read()
                            df = None
                            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                                try:
                                    df = pd.read_csv(BytesIO(csv_data), encoding=encoding)
                                    break
                                except:
                                    continue
                            
                            if df is None:
                                print(f"   Could not decode {zip_key}")
                                continue
                            
                            # Process enrollment data
                            for _, row in df.iterrows():
                                contract_id = str(row.get('Contract Number', ''))
                                parent_org = row.get('Parent Organization', '')
                                plan_type_raw = row.get('Plan Type', '')
                                enrollment_val = row.get('Enrollment', 0)
                                
                                # Skip suppressed values
                                if enrollment_val == '*' or pd.isna(enrollment_val):
                                    enrollment = 0
                                else:
                                    try:
                                        enrollment = int(float(enrollment_val))
                                    except:
                                        enrollment = 0
                                
                                # Derive group_type from plan_type
                                group_type = 'Group' if 'Employer' in str(plan_type_raw) else 'Individual'
                                
                                # Derive product_type
                                if 'PDP' in str(plan_type_raw) or contract_id.startswith('S'):
                                    product_type = 'PDP'
                                else:
                                    product_type = 'MAPD'
                                
                                # Check if SNP
                                snp_type = snp_dict.get((contract_id, year), 'Non-SNP')
                                
                                # Map plan_type to standardized categories
                                if 'HMO' in str(plan_type_raw):
                                    plan_type = 'HMO/HMOPOS'
                                elif 'Local PPO' in str(plan_type_raw):
                                    plan_type = 'Local PPO'
                                elif 'Regional PPO' in str(plan_type_raw):
                                    plan_type = 'Regional PPO'
                                elif 'PFFS' in str(plan_type_raw):
                                    plan_type = 'PFFS'
                                elif 'PDP' in str(plan_type_raw) or 'Prescription Drug' in str(plan_type_raw):
                                    plan_type = 'PDP'
                                elif 'PACE' in str(plan_type_raw):
                                    plan_type = 'National PACE'
                                elif 'Cost' in str(plan_type_raw):
                                    plan_type = '1876 Cost'
                                else:
                                    plan_type = plan_type_raw
                                
                                all_rows.append({
                                    'time_key': year * 100 + month,
                                    'entity_id': contract_id,
                                    'contract_id': contract_id,
                                    'plan_id': '000',
                                    'year': year,
                                    'month': month,
                                    'enrollment': enrollment,
                                    'plan_count': 1,
                                    'parent_org': parent_org,
                                    'state': None,  # National level - no state
                                    'plan_type': plan_type,
                                    'product_type': product_type,
                                    'snp_type': snp_type,
                                    'group_type': group_type,
                                    '_source_file': zip_key,
                                    'data_source': 'monthly',  # Exact Monthly Enrollment data
                                })
                        break  # Only process first CSV in zip
            
            print(f"   Processed {year_month}: {sum(1 for r in all_rows if r['year'] == year and r['month'] == month):,} contracts")
            
        except Exception as e:
            print(f"   Error processing {zip_key}: {e}")
    
    print(f"\n4. Building final table...")
    print(f"   CPSC rows (2013-2018): {len([r for r in all_rows if r['year'] <= 2018]):,}")
    print(f"   Monthly rows (2019+): {len([r for r in all_rows if r['year'] >= 2019]):,}")
    result = pd.DataFrame(all_rows)
    result['_source_row'] = range(len(result))
    result['_pipeline_run_id'] = PIPELINE_RUN_ID
    result['_loaded_at'] = datetime.now()
    
    print(f"   Total rows: {len(result):,}")
    
    # Validate
    print("\n4. Validating dimension columns...")
    for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
        non_null = result[col].notna().sum()
        pct = non_null / len(result) * 100
        print(f"   {col}: {non_null:,} / {len(result):,} ({pct:.1f}% populated)")
    
    # Check totals
    dec_2024 = result[(result['year'] == 2024) & (result['month'] == 12)]
    print(f"\n   Dec 2024 enrollment: {dec_2024['enrollment'].sum():,.0f}")
    
    print("\n5. Saving to Gold layer...")
    output_key = "gold/fact_enrollment_national.parquet"
    save_parquet(result, output_key)
    
    return True


def rebuild_dim_geography():
    """Rebuild gold/dim_geography from fact table."""
    print("\n" + "=" * 70)
    print("REBUILDING: gold/dim_geography")
    print("=" * 70)
    
    source_key = "processed/unified/fact_enrollment_all_years.parquet"
    output_key = "gold/dim_geography.parquet"
    
    print(f"\n1. Loading source: {source_key}")
    df = load_parquet(source_key)
    
    if df.empty or 'state' not in df.columns:
        print("  ERROR: No state data in source")
        return False
    
    print("\n2. Building geography dimension...")
    geo_df = df[['state']].drop_duplicates()
    geo_df['county'] = None
    geo_df['fips_code'] = None
    geo_df = geo_df[geo_df['state'].notna()]
    
    print(f"   Found {len(geo_df)} unique states")
    
    print("\n3. Saving to Gold layer...")
    save_parquet(geo_df, output_key)
    
    return True


def rebuild_dim_plan():
    """Rebuild gold/dim_plan from fact table."""
    print("\n" + "=" * 70)
    print("REBUILDING: gold/dim_plan")
    print("=" * 70)
    
    source_key = "processed/unified/fact_enrollment_all_years.parquet"
    output_key = "gold/dim_plan.parquet"
    
    print(f"\n1. Loading source: {source_key}")
    df = load_parquet(source_key)
    
    if df.empty:
        print("  ERROR: Source table is empty")
        return False
    
    print("\n2. Building plan dimension...")
    
    plan_cols = ['contract_id', 'year', 'plan_type', 'product_type', 'snp_type', 'group_type']
    plan_cols = [c for c in plan_cols if c in df.columns]
    
    plan_df = df[plan_cols].drop_duplicates()
    plan_df['plan_id'] = '000'
    plan_df['plan_key'] = plan_df['contract_id'] + '_000_' + plan_df['year'].astype(str)
    plan_df['plan_name'] = None
    plan_df['plan_type_category'] = plan_df['plan_type']
    plan_df['is_snp'] = plan_df['snp_type'].apply(lambda x: x not in ['Non-SNP', None] if pd.notna(x) else False)
    plan_df['is_eghp'] = plan_df['group_type'] == 'Group'
    plan_df['offers_part_d'] = plan_df['product_type'] != 'MA-only'
    
    final_cols = [
        'plan_key', 'contract_id', 'plan_id', 'year',
        'plan_name', 'plan_type', 'plan_type_category', 'product_type',
        'snp_type', 'group_type', 'is_snp', 'is_eghp', 'offers_part_d'
    ]
    
    result = plan_df[[c for c in final_cols if c in plan_df.columns]]
    
    print(f"   Generated {len(result):,} plan dimension rows")
    
    print("\n3. Saving to Gold layer...")
    save_parquet(result, output_key)
    
    return True


def rebuild_fact_enrollment_geographic():
    """Rebuild gold/fact_enrollment_geographic with dimension columns from CPSC + lookups."""
    print("\n" + "=" * 70)
    print("REBUILDING: gold/fact_enrollment_geographic")
    print("=" * 70)
    
    conn = get_duckdb_conn()
    
    print("\n1. Loading CPSC data with dimension lookups...")
    
    # Query to join CPSC data with dimension lookups
    sql = f"""
    WITH cpsc AS (
        SELECT 
            year, month, contract_id, plan_id, fips_code, state, county, 
            enrollment, parent_org, plan_type, is_snp
        FROM read_parquet('s3://{S3_BUCKET}/processed/fact_enrollment/*/*/data.parquet')
    ),
    snp AS (
        SELECT contract_id, plan_id, year, snp_type
        FROM read_parquet('s3://{S3_BUCKET}/processed/unified/snp_lookup.parquet')
    ),
    contracts AS (
        SELECT contract_id, year, product_type
        FROM read_parquet('s3://{S3_BUCKET}/processed/unified/dim_contract_v2.parquet')
    )
    SELECT 
        c.year * 100 + c.month as time_key,
        c.contract_id as entity_id,
        c.contract_id,
        c.plan_id,
        c.fips_code as geo_key,
        c.year,
        c.month,
        c.state,
        c.county,
        c.fips_code,
        c.enrollment,
        CASE WHEN c.enrollment < 10 THEN true ELSE false END as is_suppressed,
        1 as plan_count,
        c.parent_org,
        c.plan_type,
        COALESCE(ct.product_type, 
                 CASE WHEN c.contract_id LIKE 'S%' THEN 'PDP' 
                      ELSE 'MAPD' END) as product_type,
        COALESCE(s.snp_type, 
                 CASE WHEN c.is_snp = 'Yes' OR c.is_snp = true THEN 'SNP-Unknown' 
                      ELSE 'Non-SNP' END) as snp_type,
        CASE WHEN CAST(c.plan_id AS INTEGER) >= 800 THEN 'Group' ELSE 'Individual' END as group_type,
        'cpsc_rebuild' as _source_file,
        ROW_NUMBER() OVER () as _source_row,
        '{PIPELINE_RUN_ID}' as _pipeline_run_id,
        CURRENT_TIMESTAMP as _loaded_at
    FROM cpsc c
    LEFT JOIN snp s ON c.contract_id = s.contract_id AND c.plan_id = s.plan_id AND c.year = s.year
    LEFT JOIN contracts ct ON c.contract_id = ct.contract_id AND c.year = ct.year
    WHERE c.year >= 2013
    """
    
    print("   Running query (this may take a few minutes)...")
    sys.stdout.flush()
    
    try:
        result = conn.execute(sql).fetchdf()
        print(f"   Loaded {len(result):,} rows")
        
        print("\n2. Validating dimension columns...")
        for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
            non_null = result[col].notna().sum()
            pct = non_null / len(result) * 100
            print(f"   {col}: {non_null:,} / {len(result):,} ({pct:.1f}% populated)")
        
        print("\n3. Saving to Gold layer (partitioned)...")
        
        # Save as single file for simplicity (could partition by year)
        buffer = BytesIO()
        result.to_parquet(buffer, index=False, compression='snappy')
        buffer.seek(0)
        
        output_key = "gold/fact_enrollment_geographic.parquet"
        s3.put_object(Bucket=S3_BUCKET, Key=output_key, Body=buffer.getvalue())
        print(f"   Saved to s3://{S3_BUCKET}/{output_key} ({len(result):,} rows)")
        
        return True
        
    except Exception as e:
        print(f"   ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_gold_layer():
    """Validate the rebuilt Gold layer."""
    print("\n" + "=" * 70)
    print("VALIDATING GOLD LAYER")
    print("=" * 70)
    
    print("\n1. Checking fact_enrollment_national...")
    df = load_parquet("gold/fact_enrollment_national.parquet")
    if df.empty:
        print("  FAILED: Table is empty")
        return False
    
    dec_2024 = df[(df['year'] == 2024) & (df['month'] == 12)]
    print(f"   Dec 2024 rows: {len(dec_2024):,}")
    print(f"   Dec 2024 enrollment: {dec_2024['enrollment'].sum():,.0f}")
    
    print("\n2. Checking dimension column coverage (national)...")
    for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
        non_null = dec_2024[col].notna().sum()
        pct = non_null / len(dec_2024) * 100 if len(dec_2024) > 0 else 0
        status = "OK" if pct > 50 else "WARN"
        print(f"   [{status}] {col}: {pct:.1f}% populated")
    
    print("\n3. Checking fact_enrollment_geographic...")
    geo_fact = load_parquet("gold/fact_enrollment_geographic.parquet")
    if not geo_fact.empty:
        dec_2024_geo = geo_fact[(geo_fact['year'] == 2024) & (geo_fact['month'] == 12)]
        print(f"   Dec 2024 rows: {len(dec_2024_geo):,}")
        print(f"   Dec 2024 counties: {dec_2024_geo['county'].nunique():,}")
        for col in ['plan_type', 'product_type', 'snp_type', 'group_type']:
            non_null = dec_2024_geo[col].notna().sum()
            pct = non_null / len(dec_2024_geo) * 100 if len(dec_2024_geo) > 0 else 0
            status = "OK" if pct > 50 else "WARN"
            print(f"   [{status}] {col}: {pct:.1f}% populated")
    else:
        print("   Not rebuilt (optional)")
    
    print("\n4. Checking dim_geography...")
    geo_df = load_parquet("gold/dim_geography.parquet")
    print(f"   States: {len(geo_df)}")
    
    print("\n5. Checking dim_plan...")
    plan_df = load_parquet("gold/dim_plan.parquet")
    print(f"   Plan dimension rows: {len(plan_df):,}")
    
    return True


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--geographic', action='store_true', help='Also rebuild geographic table (slow)')
    parser.add_argument('--all', action='store_true', help='Rebuild everything including geographic')
    args = parser.parse_args()
    
    print("=" * 70)
    print("REBUILD GOLD LAYER FROM LEGACY TABLES")
    print("=" * 70)
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()
    
    success = True
    
    if not rebuild_fact_enrollment_national():
        success = False
    
    if not rebuild_dim_geography():
        success = False
    
    if not rebuild_dim_plan():
        success = False
    
    # Optionally rebuild geographic table (takes longer)
    if args.geographic or args.all:
        if not rebuild_fact_enrollment_geographic():
            success = False
    
    if success:
        validate_gold_layer()
    
    print("\n" + "=" * 70)
    print(f"REBUILD {'COMPLETED' if success else 'FAILED'}")
    print(f"Finished: {datetime.now()}")
    print("=" * 70)
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
