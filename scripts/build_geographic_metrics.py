#!/usr/bin/env python3
"""
Build Geographic Metrics with Full Dimension Filtering

Creates:
1. gold/dim_county.parquet - County dimension with Medicare eligibles (TAM)
2. gold/fact_geographic_metrics.parquet - Pre-aggregated county metrics with all dimensions

All data is traceable back to source files via _source_file column.
"""

import os
import sys
import duckdb
import boto3
from datetime import datetime

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

def setup_duckdb():
    """Create DuckDB connection with S3 access."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs")
    
    # Get credentials from env or boto3 session
    access_key = AWS_ACCESS_KEY
    secret_key = AWS_SECRET_KEY
    
    if not access_key or not secret_key:
        # Try boto3 session credentials
        session = boto3.Session()
        creds = session.get_credentials()
        if creds:
            access_key = creds.access_key
            secret_key = creds.secret_key
    
    if access_key and secret_key:
        con.execute(f"""
            SET s3_region = 'us-east-1';
            SET s3_access_key_id = '{access_key}';
            SET s3_secret_access_key = '{secret_key}';
        """)
        print(f"S3 credentials configured")
    else:
        print("WARNING: No S3 credentials found")
    
    return con

def build_dim_county(con):
    """
    Build county dimension table with Medicare eligibles from penetration data.
    
    Schema:
    - fips: 5-digit FIPS code (string)
    - state: State name
    - county: County name
    - eligibles: Medicare eligible population (TAM)
    - year: Data year
    - month: Data month
    - _source_file: Source file path for audit
    """
    print("\n" + "="*60)
    print("BUILDING: gold/dim_county.parquet")
    print("="*60)
    
    # Get available penetration files
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    
    penetration_files = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='raw/penetration/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                penetration_files.append(obj['Key'])
    
    print(f"Found {len(penetration_files)} penetration files")
    
    # Process each file
    all_counties = []
    
    for file_path in penetration_files:
        # Extract year/month from path: raw/penetration/2026/02/ma_penetration.zip
        parts = file_path.split('/')
        year = int(parts[2])
        month = int(parts[3])
        
        print(f"  Processing: {year}-{month:02d}")
        
        # Download and read the CSV inside the ZIP
        import tempfile
        import zipfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, 'data.zip')
            s3.download_file(S3_BUCKET, file_path, zip_path)
            
            with zipfile.ZipFile(zip_path, 'r') as zf:
                csv_name = [n for n in zf.namelist() if n.endswith('.csv')][0]
                zf.extract(csv_name, tmpdir)
                csv_path = os.path.join(tmpdir, csv_name)
                
                # Load into DuckDB
                df = con.execute(f"""
                    SELECT 
                        FIPS as fips,
                        "State Name" as state,
                        "County Name" as county,
                        CAST(REPLACE(Eligibles, ',', '') AS INTEGER) as eligibles,
                        {year} as year,
                        {month} as month,
                        '{file_path}' as _source_file
                    FROM read_csv_auto('{csv_path}')
                    WHERE FIPS IS NOT NULL
                """).fetchdf()
                
                all_counties.append(df)
    
    # Combine all data
    import pandas as pd
    combined = pd.concat(all_counties, ignore_index=True)
    
    print(f"\nTotal county records: {len(combined):,}")
    print(f"Unique counties: {combined['fips'].nunique():,}")
    print(f"Years covered: {combined['year'].min()}-{combined['year'].max()}")
    
    # Upload to S3
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO
    
    buffer = BytesIO()
    table = pa.Table.from_pandas(combined)
    pq.write_table(table, buffer)
    buffer.seek(0)
    
    s3.upload_fileobj(buffer, S3_BUCKET, 'gold/dim_county.parquet')
    print(f"Uploaded: s3://{S3_BUCKET}/gold/dim_county.parquet")
    
    return combined

def build_fact_geographic_metrics(con):
    """
    Build geographic metrics fact table with ALL dimension breakdowns.
    
    Processes year-by-year for memory efficiency, uploading partitioned data.
    
    Dimensions available for filtering:
    - plan_type (HMO, PPO, PFFS, etc.)
    - product_type (MAPD, PDP)
    - snp_type (Non-SNP, D-SNP, C-SNP, I-SNP)
    - group_type (Individual, Group)
    - parent_org
    """
    print("\n" + "="*60)
    print("BUILDING: gold/fact_geographic_metrics/ (partitioned by year)")
    print("="*60)
    
    con.execute(f"""
        SET preserve_insertion_order = false;
        SET threads = 2;
        SET memory_limit = '4GB';
    """)
    
    # Get available years
    years = con.execute(f"""
        SELECT DISTINCT year 
        FROM read_parquet('s3://{S3_BUCKET}/gold/fact_enrollment_geographic/*.parquet')
        ORDER BY year
    """).fetchall()
    years = [y[0] for y in years]
    
    print(f"Years to process: {years}")
    
    s3 = boto3.client('s3')
    
    # Delete existing partitions
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='gold/fact_geographic_metrics/'):
            for obj in page.get('Contents', []):
                s3.delete_object(Bucket=S3_BUCKET, Key=obj['Key'])
        print("Cleared existing partitions")
    except:
        pass
    
    total_records = 0
    start = datetime.now()
    
    for year in years:
        print(f"\n  Processing year {year}...")
        year_start = datetime.now()
        
        # Process one year at a time
        sql = f"""
        SELECT 
            fips_code as fips,
            state,
            county,
            {year} as year,
            month,
            contract_id,
            COALESCE(plan_type, 
                CASE WHEN contract_id LIKE 'H%' THEN 'HMO' 
                     WHEN contract_id LIKE 'R%' THEN 'Regional PPO'
                     WHEN contract_id LIKE 'S%' THEN 'PDP'
                     ELSE 'Other' END
            ) as plan_type,
            COALESCE(product_type,
                CASE WHEN contract_id LIKE 'S%' THEN 'PDP' ELSE 'MAPD' END
            ) as product_type,
            COALESCE(snp_type, 'Non-SNP') as snp_type,
            COALESCE(group_type, 'Individual') as group_type,
            COALESCE(parent_org, 'Unknown') as parent_org,
            SUM(enrollment) as enrollment,
            COUNT(DISTINCT plan_id) as plan_count,
            COALESCE(_source_file, 'gold/fact_enrollment_geographic') as _source_file
        FROM read_parquet('s3://{S3_BUCKET}/gold/fact_enrollment_geographic/*.parquet')
        WHERE year = {year}
        GROUP BY 
            fips_code, state, county, month, contract_id,
            plan_type, product_type, snp_type, group_type, parent_org, _source_file
        """
        
        copy_sql = f"""
            COPY (
                {sql}
            ) TO 's3://{S3_BUCKET}/gold/fact_geographic_metrics/year={year}/data.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        
        con.execute(copy_sql)
        
        # Get count for this year
        count = con.execute(f"""
            SELECT COUNT(*) 
            FROM read_parquet('s3://{S3_BUCKET}/gold/fact_geographic_metrics/year={year}/data.parquet')
        """).fetchone()[0]
        
        year_elapsed = (datetime.now() - year_start).total_seconds()
        print(f"    {count:,} records in {year_elapsed:.1f}s")
        total_records += count
    
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n  Total: {total_records:,} records in {elapsed:.1f}s")
    print(f"  Uploaded to: s3://{S3_BUCKET}/gold/fact_geographic_metrics/")
    
    # Verify dimensions
    print("\n  Verifying dimensions...")
    stats = con.execute(f"""
        SELECT 
            COUNT(DISTINCT plan_type) as plan_types,
            COUNT(DISTINCT product_type) as product_types,
            COUNT(DISTINCT snp_type) as snp_types,
            COUNT(DISTINCT group_type) as group_types,
            COUNT(DISTINCT parent_org) as parent_orgs,
            COUNT(DISTINCT fips) as counties
        FROM read_parquet('s3://{S3_BUCKET}/gold/fact_geographic_metrics/*/data.parquet')
    """).fetchone()
    
    print(f"    Plan types: {stats[0]}")
    print(f"    Product types: {stats[1]}")
    print(f"    SNP types: {stats[2]}")
    print(f"    Group types: {stats[3]}")
    print(f"    Parent orgs: {stats[4]}")
    print(f"    Counties: {stats[5]}")

def verify_data(con):
    """Verify the built tables are queryable and dimensions work."""
    print("\n" + "="*60)
    print("VERIFICATION")
    print("="*60)
    
    # Test dim_county
    print("\n1. dim_county sample:")
    result = con.execute(f"""
        SELECT state, county, fips, eligibles, year, month
        FROM read_parquet('s3://{S3_BUCKET}/gold/dim_county.parquet')
        WHERE year = 2026
        LIMIT 5
    """).fetchdf()
    print(result.to_string(index=False))
    
    # Test fact_geographic_metrics with filters
    print("\n2. Geographic metrics - HMO + Individual:")
    result = con.execute(f"""
        SELECT 
            year,
            COUNT(DISTINCT fips) as counties,
            SUM(enrollment) as total_enrollment
        FROM read_parquet('s3://{S3_BUCKET}/gold/fact_geographic_metrics.parquet')
        WHERE plan_type LIKE '%HMO%'
          AND group_type = 'Individual'
          AND product_type = 'MAPD'
        GROUP BY year
        ORDER BY year DESC
        LIMIT 5
    """).fetchdf()
    print(result.to_string(index=False))
    
    # Test SNP filtering
    print("\n3. D-SNP county coverage:")
    result = con.execute(f"""
        SELECT 
            year,
            COUNT(DISTINCT fips) as counties,
            SUM(enrollment) as dsnp_enrollment
        FROM read_parquet('s3://{S3_BUCKET}/gold/fact_geographic_metrics.parquet')
        WHERE snp_type = 'D-SNP'
        GROUP BY year
        ORDER BY year DESC
        LIMIT 5
    """).fetchdf()
    print(result.to_string(index=False))
    
    # Test TAM calculation
    print("\n4. TAM / Market Share calculation (2026):")
    result = con.execute(f"""
        WITH geo AS (
            SELECT 
                g.fips,
                SUM(g.enrollment) as enrolled
            FROM read_parquet('s3://{S3_BUCKET}/gold/fact_geographic_metrics.parquet') g
            WHERE g.year = 2026 AND g.month = 2
            GROUP BY g.fips
        ),
        county AS (
            SELECT fips, eligibles
            FROM read_parquet('s3://{S3_BUCKET}/gold/dim_county.parquet')
            WHERE year = 2026 AND month = 2
        )
        SELECT 
            COUNT(DISTINCT c.fips) as counties,
            SUM(c.eligibles) as total_eligibles,
            SUM(g.enrolled) as total_enrolled,
            ROUND(100.0 * SUM(g.enrolled) / SUM(c.eligibles), 2) as penetration_pct
        FROM county c
        LEFT JOIN geo g ON c.fips = g.fips
    """).fetchdf()
    print(result.to_string(index=False))

def main():
    print("="*60)
    print("GEOGRAPHIC METRICS BUILD")
    print("="*60)
    print(f"Started: {datetime.now()}")
    print(f"S3 Bucket: {S3_BUCKET}")
    
    con = setup_duckdb()
    
    # Build dimension table
    build_dim_county(con)
    
    # Build fact table with all dimensions
    build_fact_geographic_metrics(con)
    
    # Verify
    verify_data(con)
    
    print("\n" + "="*60)
    print("BUILD COMPLETE")
    print("="*60)

if __name__ == "__main__":
    main()
