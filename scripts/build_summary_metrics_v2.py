#!/usr/bin/env python3
"""
Build Pre-Aggregated Summary Metrics - Memory Efficient Version

Processes one year at a time and writes directly to S3.
"""

import os
import duckdb
import boto3
from datetime import datetime

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")

def main():
    print("="*60)
    print("BUILDING PRE-AGGREGATED SUMMARY METRICS (v2)")
    print("="*60)
    print(f"Started: {datetime.now()}")
    
    # Get credentials
    session = boto3.Session()
    creds = session.get_credentials()
    
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(f"""
        SET s3_region = 'us-east-1';
        SET s3_access_key_id = '{creds.access_key}';
        SET s3_secret_access_key = '{creds.secret_key}';
        SET memory_limit = '4GB';
        SET threads = 2;
    """)
    
    # Get years with eligibles data (we can only calculate market share for these)
    years_with_eligibles = con.execute(f"""
        SELECT DISTINCT year FROM read_parquet('s3://{S3_BUCKET}/gold/dim_county.parquet')
    """).fetchall()
    years_with_eligibles = [y[0] for y in years_with_eligibles]
    print(f"Years with eligibles data: {years_with_eligibles}")
    
    # Build summary for each year that has eligibles
    all_parts = []
    
    for year in years_with_eligibles:
        print(f"\nProcessing year {year}...")
        
        # Get latest month for this year
        latest_month = con.execute(f"""
            SELECT MAX(month) FROM read_parquet('s3://{S3_BUCKET}/gold/fact_enrollment_geographic/*.parquet')
            WHERE year = {year}
        """).fetchone()[0]
        
        if not latest_month:
            print(f"  No geographic data for {year}")
            continue
        
        print(f"  Using month {latest_month}")
        
        # Single query that computes all aggregations for this year/month
        sql = f"""
        WITH dim_plan_lookup AS (
            SELECT DISTINCT
                contract_id,
                CAST(COALESCE(NULLIF(REGEXP_REPLACE(LTRIM(CAST(plan_id AS VARCHAR), '0'), '\\..*', ''), ''), '0') AS VARCHAR) as plan_id_norm,
                plan_type,
                COALESCE(snp_type, 'Non-SNP') as snp_type,
                COALESCE(group_type, 'Individual') as group_type
            FROM read_parquet('s3://{S3_BUCKET}/gold/dim_plan.parquet')
        ),
        
        enriched AS (
            SELECT 
                g.fips_code,
                g.enrollment,
                COALESCE(p.plan_type, 
                    CASE WHEN g.contract_id LIKE 'H%' THEN 'HMO' 
                         WHEN g.contract_id LIKE 'R%' THEN 'PPO'
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
            WHERE g.year = {year} AND g.month = {latest_month}
        ),
        
        county_elig AS (
            SELECT fips, eligibles
            FROM read_parquet('s3://{S3_BUCKET}/gold/dim_county.parquet')
            WHERE year = {year} AND month = (
                SELECT MAX(month) FROM read_parquet('s3://{S3_BUCKET}/gold/dim_county.parquet') WHERE year = {year}
            )
        ),
        
        -- Total
        total_agg AS (
            SELECT 
                'total' as dimension_type,
                'all' as dimension_value,
                COUNT(DISTINCT e.fips_code) as county_count,
                SUM(e.enrollment) as enrollment
            FROM enriched e
        ),
        total_with_elig AS (
            SELECT 
                t.dimension_type,
                t.dimension_value,
                t.county_count,
                t.enrollment,
                (SELECT SUM(eligibles) FROM county_elig WHERE fips IN (SELECT DISTINCT fips_code FROM enriched)) as eligibles
            FROM total_agg t
        ),
        
        -- By plan_type
        plan_agg AS (
            SELECT 
                'plan_type' as dimension_type,
                plan_type as dimension_value,
                COUNT(DISTINCT fips_code) as county_count,
                SUM(enrollment) as enrollment,
                (SELECT SUM(c.eligibles) FROM county_elig c WHERE c.fips IN (
                    SELECT DISTINCT fips_code FROM enriched WHERE plan_type = e.plan_type
                )) as eligibles
            FROM enriched e
            WHERE plan_type IS NOT NULL
            GROUP BY plan_type
        ),
        
        -- By product_type
        product_agg AS (
            SELECT 
                'product_type' as dimension_type,
                product_type as dimension_value,
                COUNT(DISTINCT fips_code) as county_count,
                SUM(enrollment) as enrollment,
                (SELECT SUM(c.eligibles) FROM county_elig c WHERE c.fips IN (
                    SELECT DISTINCT fips_code FROM enriched WHERE product_type = e.product_type
                )) as eligibles
            FROM enriched e
            GROUP BY product_type
        ),
        
        -- By snp_type
        snp_agg AS (
            SELECT 
                'snp_type' as dimension_type,
                snp_type as dimension_value,
                COUNT(DISTINCT fips_code) as county_count,
                SUM(enrollment) as enrollment,
                (SELECT SUM(c.eligibles) FROM county_elig c WHERE c.fips IN (
                    SELECT DISTINCT fips_code FROM enriched WHERE snp_type = e.snp_type
                )) as eligibles
            FROM enriched e
            GROUP BY snp_type
        ),
        
        -- By group_type  
        group_agg AS (
            SELECT 
                'group_type' as dimension_type,
                group_type as dimension_value,
                COUNT(DISTINCT fips_code) as county_count,
                SUM(enrollment) as enrollment,
                (SELECT SUM(c.eligibles) FROM county_elig c WHERE c.fips IN (
                    SELECT DISTINCT fips_code FROM enriched WHERE group_type = e.group_type
                )) as eligibles
            FROM enriched e
            GROUP BY group_type
        ),
        
        combined AS (
            SELECT * FROM total_with_elig
            UNION ALL SELECT * FROM plan_agg
            UNION ALL SELECT * FROM product_agg
            UNION ALL SELECT * FROM snp_agg
            UNION ALL SELECT * FROM group_agg
        )
        
        SELECT 
            {year} as year,
            {latest_month} as month,
            dimension_type,
            dimension_value,
            county_count,
            enrollment,
            eligibles,
            ROUND(100.0 * enrollment / NULLIF(eligibles, 0), 2) as market_share
        FROM combined
        WHERE dimension_value IS NOT NULL
        """
        
        try:
            result = con.execute(sql).fetchdf()
            print(f"  Generated {len(result)} rows")
            all_parts.append(result)
            
            # Show sample
            print(result.head(3).to_string(index=False))
        except Exception as e:
            print(f"  Error: {e}")
    
    if not all_parts:
        print("ERROR: No data generated!")
        return
    
    # Combine and upload
    print("\nCombining all years...")
    import pandas as pd
    df = pd.concat(all_parts, ignore_index=True)
    
    print(f"Total rows: {len(df)}")
    print(f"Years: {sorted(df['year'].unique())}")
    
    # Upload
    print("\nUploading to S3...")
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO
    
    s3 = boto3.client('s3')
    buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer, compression='zstd')
    buffer.seek(0)
    
    s3.upload_fileobj(buffer, S3_BUCKET, 'gold/fact_summary_metrics.parquet')
    
    print(f"Uploaded: s3://{S3_BUCKET}/gold/fact_summary_metrics.parquet")
    print(f"\nDone: {datetime.now()}")

if __name__ == "__main__":
    main()
