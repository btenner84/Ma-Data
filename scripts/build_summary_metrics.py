#!/usr/bin/env python3
"""
Build Pre-Aggregated Summary Metrics Table

Creates gold/fact_summary_metrics.parquet with pre-computed:
- County counts
- Enrollment 
- TAM (Medicare eligibles in operating counties)
- Market share

Aggregated by dimension (plan_type, product_type, snp_type, group_type).
This enables instant API queries without expensive joins.

Run daily/weekly to refresh.
"""

import os
import sys
import duckdb
import boto3
import pandas as pd
from datetime import datetime
from io import BytesIO

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")

def get_duckdb_connection():
    """Create DuckDB connection with S3 credentials."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs")
    
    session = boto3.Session()
    creds = session.get_credentials()
    if creds:
        con.execute(f"""
            SET s3_region = 'us-east-1';
            SET s3_access_key_id = '{creds.access_key}';
            SET s3_secret_access_key = '{creds.secret_key}';
        """)
    
    return con

def build_summary_metrics():
    """Build pre-aggregated summary metrics table."""
    
    print("="*60)
    print("BUILDING PRE-AGGREGATED SUMMARY METRICS")
    print("="*60)
    print(f"Started: {datetime.now()}")
    
    con = get_duckdb_connection()
    
    # First, create the enriched geographic data as a temp view
    # This joins fact_enrollment_geographic with dim_plan to get dimensions
    print("\nStep 1: Creating enriched geographic view...")
    
    con.execute(f"""
        CREATE OR REPLACE VIEW enriched_geo AS
        WITH dim_plan_lookup AS (
            SELECT DISTINCT
                contract_id,
                CAST(COALESCE(NULLIF(REGEXP_REPLACE(LTRIM(CAST(plan_id AS VARCHAR), '0'), '\\..*', ''), ''), '0') AS VARCHAR) as plan_id_norm,
                plan_type,
                COALESCE(snp_type, 'Non-SNP') as snp_type,
                COALESCE(group_type, 'Individual') as group_type
            FROM read_parquet('s3://{S3_BUCKET}/gold/dim_plan.parquet')
        )
        SELECT 
            g.fips_code,
            g.state,
            g.year,
            g.month,
            g.contract_id,
            g.enrollment,
            g.parent_org,
            COALESCE(p.plan_type, 
                CASE WHEN g.contract_id LIKE 'H%' THEN 'HMO' 
                     WHEN g.contract_id LIKE 'R%' THEN 'Regional PPO'
                     WHEN g.contract_id LIKE 'S%' THEN 'PDP'
                     ELSE 'Other' END
            ) as plan_type,
            CASE WHEN g.contract_id LIKE 'S%' THEN 'PDP' ELSE 'MAPD' END as product_type,
            COALESCE(p.snp_type, 'Non-SNP') as snp_type,
            COALESCE(p.group_type, 'Individual') as group_type
        FROM read_parquet('s3://{S3_BUCKET}/gold/fact_enrollment_geographic/*.parquet') g
        LEFT JOIN dim_plan_lookup p 
            ON g.contract_id = p.contract_id 
            AND CAST(COALESCE(NULLIF(REGEXP_REPLACE(LTRIM(CAST(g.plan_id AS VARCHAR), '0'), '\\..*', ''), ''), '0') AS VARCHAR) = p.plan_id_norm
    """)
    
    # Load county eligibles
    print("Step 2: Loading county eligibles (TAM)...")
    con.execute(f"""
        CREATE OR REPLACE VIEW county_eligibles AS
        SELECT fips, year, month, eligibles
        FROM read_parquet('s3://{S3_BUCKET}/gold/dim_county.parquet')
    """)
    
    # Get available years/months
    years_months = con.execute("""
        SELECT DISTINCT year, month FROM enriched_geo ORDER BY year, month
    """).fetchall()
    
    print(f"Found {len(years_months)} year-month combinations")
    
    all_results = []
    
    for year, month in years_months:
        print(f"\nProcessing {year}-{month:02d}...")
        
        # For each dimension, calculate aggregates
        dimensions = [
            ('total', 'all', '1=1'),  # Total across all
            ('plan_type', 'plan_type', '1=1'),
            ('product_type', 'product_type', '1=1'),
            ('snp_type', 'snp_type', '1=1'),
            ('group_type', 'group_type', '1=1'),
        ]
        
        for dim_type, dim_col, extra_filter in dimensions:
            if dim_type == 'total':
                # Total summary
                sql = f"""
                    WITH geo_agg AS (
                        SELECT 
                            fips_code,
                            SUM(enrollment) as enrollment
                        FROM enriched_geo
                        WHERE year = {year} AND month = {month}
                        GROUP BY fips_code
                    ),
                    with_eligibles AS (
                        SELECT 
                            g.fips_code,
                            g.enrollment,
                            COALESCE(c.eligibles, 0) as eligibles
                        FROM geo_agg g
                        LEFT JOIN county_eligibles c 
                            ON g.fips_code = c.fips 
                            AND c.year = {year} 
                            AND c.month = {month}
                    )
                    SELECT 
                        {year} as year,
                        {month} as month,
                        'total' as dimension_type,
                        'all' as dimension_value,
                        COUNT(DISTINCT fips_code) as county_count,
                        SUM(enrollment) as enrollment,
                        SUM(eligibles) as eligibles,
                        ROUND(100.0 * SUM(enrollment) / NULLIF(SUM(eligibles), 0), 2) as market_share
                    FROM with_eligibles
                """
            else:
                # By dimension
                sql = f"""
                    WITH geo_agg AS (
                        SELECT 
                            {dim_col} as dim_value,
                            fips_code,
                            SUM(enrollment) as enrollment
                        FROM enriched_geo
                        WHERE year = {year} AND month = {month} AND {extra_filter}
                        GROUP BY {dim_col}, fips_code
                    ),
                    dim_counties AS (
                        SELECT 
                            dim_value,
                            fips_code,
                            enrollment
                        FROM geo_agg
                    ),
                    with_eligibles AS (
                        SELECT 
                            dc.dim_value,
                            dc.fips_code,
                            dc.enrollment,
                            COALESCE(c.eligibles, 0) as eligibles
                        FROM dim_counties dc
                        LEFT JOIN county_eligibles c 
                            ON dc.fips_code = c.fips 
                            AND c.year = {year} 
                            AND c.month = {month}
                    )
                    SELECT 
                        {year} as year,
                        {month} as month,
                        '{dim_type}' as dimension_type,
                        dim_value as dimension_value,
                        COUNT(DISTINCT fips_code) as county_count,
                        SUM(enrollment) as enrollment,
                        SUM(eligibles) as eligibles,
                        ROUND(100.0 * SUM(enrollment) / NULLIF(SUM(eligibles), 0), 2) as market_share
                    FROM with_eligibles
                    WHERE dim_value IS NOT NULL
                    GROUP BY dim_value
                """
            
            try:
                result = con.execute(sql).fetchdf()
                if not result.empty:
                    all_results.append(result)
            except Exception as e:
                print(f"  Warning: {dim_type} failed: {e}")
    
    # Combine all results
    print("\nStep 3: Combining results...")
    if not all_results:
        print("ERROR: No results generated!")
        return
    
    df = pd.concat(all_results, ignore_index=True)
    
    print(f"\nTotal rows: {len(df):,}")
    print(f"Dimensions: {df['dimension_type'].unique().tolist()}")
    print(f"Years: {df['year'].min()}-{df['year'].max()}")
    
    # Sample output
    print("\nSample data:")
    print(df[df['year'] == df['year'].max()].head(10).to_string(index=False))
    
    # Upload to S3
    print("\nStep 4: Uploading to S3...")
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    s3 = boto3.client('s3')
    buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer, compression='zstd')
    buffer.seek(0)
    
    s3.upload_fileobj(buffer, S3_BUCKET, 'gold/fact_summary_metrics.parquet')
    
    print(f"Uploaded: s3://{S3_BUCKET}/gold/fact_summary_metrics.parquet")
    print(f"\nCompleted: {datetime.now()}")
    
    return df

if __name__ == "__main__":
    build_summary_metrics()
