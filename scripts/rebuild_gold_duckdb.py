#!/usr/bin/env python3
"""
Rebuild Gold Enrollment using DuckDB
DuckDB queries S3 directly - no need to load all data into memory!
"""

import duckdb
import boto3
from datetime import datetime

# Get AWS credentials for DuckDB
session = boto3.Session()
creds = session.get_credentials()

BUCKET = 'ma-data123'

def get_connection():
    """Create DuckDB connection with S3 access."""
    con = duckdb.connect(':memory:')
    
    # Configure S3 access
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='us-east-1';")
    con.execute(f"SET s3_access_key_id='{creds.access_key}';")
    con.execute(f"SET s3_secret_access_key='{creds.secret_key}';")
    if creds.token:
        con.execute(f"SET s3_session_token='{creds.token}';")
    
    return con


def rebuild_national(con):
    """Rebuild national enrollment with all linkages using SQL JOINs."""
    print("\n" + "=" * 60)
    print("REBUILDING NATIONAL ENROLLMENT")
    print("=" * 60)
    
    # Check what we have
    print("\nChecking source data...")
    result = con.execute(f"""
        SELECT COUNT(*) as cnt 
        FROM read_parquet('s3://{BUCKET}/gold/fact_enrollment_national.parquet')
    """).fetchone()
    print(f"  Current national rows: {result[0]:,}")
    
    # Load dimensions into DuckDB (these are small)
    print("\nLoading dimension tables...")
    
    con.execute(f"""
        CREATE TABLE dim_entity AS 
        SELECT * FROM read_parquet('s3://{BUCKET}/gold/dim_entity.parquet')
    """)
    print(f"  dim_entity: {con.execute('SELECT COUNT(*) FROM dim_entity').fetchone()[0]:,} rows")
    
    con.execute(f"""
        CREATE TABLE dim_plan AS 
        SELECT 
            contract_id,
            CAST(COALESCE(
                NULLIF(REGEXP_REPLACE(LTRIM(CAST(plan_id AS VARCHAR), '0'), '\\..*', ''), ''),
                '0'
            ) AS VARCHAR) as plan_id_norm,
            year,
            plan_type,
            product_type,
            group_type,
            is_snp,
            is_eghp
        FROM read_parquet('s3://{BUCKET}/gold/dim_plan.parquet')
    """)
    print(f"  dim_plan: {con.execute('SELECT COUNT(*) FROM dim_plan').fetchone()[0]:,} rows")
    
    con.execute(f"""
        CREATE TABLE snp_lookup AS 
        SELECT 
            contract_id,
            CAST(COALESCE(
                NULLIF(REGEXP_REPLACE(LTRIM(CAST(plan_id AS VARCHAR), '0'), '\\..*', ''), ''),
                '0'
            ) AS VARCHAR) as plan_id_norm,
            year,
            snp_type
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_id, plan_id, year ORDER BY month DESC) as rn
            FROM read_parquet('s3://{BUCKET}/processed/unified/snp_lookup.parquet')
        ) WHERE rn = 1
    """)
    print(f"  snp_lookup: {con.execute('SELECT COUNT(*) FROM snp_lookup').fetchone()[0]:,} rows")
    
    # Create entity fallback (latest year per contract)
    con.execute("""
        CREATE TABLE entity_fallback AS
        SELECT contract_id, parent_org, organization_name, organization_type
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY year DESC) as rn
            FROM dim_entity
        ) WHERE rn = 1
    """)
    
    # Create plan fallback
    con.execute("""
        CREATE TABLE plan_fallback AS
        SELECT contract_id, plan_id_norm, plan_type, product_type, group_type
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_id, plan_id_norm ORDER BY year DESC) as rn
            FROM dim_plan
        ) WHERE rn = 1
    """)
    
    # Create SNP fallback
    con.execute("""
        CREATE TABLE snp_fallback AS
        SELECT contract_id, plan_id_norm, snp_type
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY contract_id, plan_id_norm ORDER BY year DESC) as rn
            FROM snp_lookup
        ) WHERE rn = 1
    """)
    
    print("\nRebuilding with full linkages...")
    
    # Main rebuild query - joins all dimensions
    con.execute(f"""
        CREATE TABLE national_enriched AS
        WITH base AS (
            SELECT 
                n.contract_id,
                n.plan_id,
                CAST(COALESCE(
                    NULLIF(REGEXP_REPLACE(LTRIM(CAST(n.plan_id AS VARCHAR), '0'), '\\..*', ''), ''),
                    '0'
                ) AS VARCHAR) as plan_id_norm,
                n.year,
                n.month,
                n.enrollment,
                n.data_source
            FROM read_parquet('s3://{BUCKET}/gold/fact_enrollment_national.parquet') n
        )
        SELECT 
            b.contract_id,
            b.plan_id,
            b.year,
            b.month,
            b.enrollment,
            -- Entity attributes (year-specific then fallback)
            COALESCE(e.parent_org, ef.parent_org) as parent_org,
            COALESCE(e.organization_name, ef.organization_name) as organization_name,
            COALESCE(e.organization_type, ef.organization_type) as organization_type,
            -- Plan attributes (year-specific then fallback)
            COALESCE(p.plan_type, pf.plan_type) as plan_type,
            COALESCE(
                p.product_type, 
                pf.product_type,
                CASE WHEN b.contract_id LIKE 'S%' THEN 'PDP' ELSE 'MAPD' END
            ) as product_type,
            COALESCE(p.group_type, pf.group_type) as group_type,
            -- SNP type (year-specific then fallback)
            COALESCE(s.snp_type, sf.snp_type, 'Non-SNP') as snp_type,
            b.data_source
        FROM base b
        LEFT JOIN dim_entity e ON b.contract_id = e.contract_id AND b.year = e.year
        LEFT JOIN entity_fallback ef ON b.contract_id = ef.contract_id
        LEFT JOIN dim_plan p ON b.contract_id = p.contract_id AND b.plan_id_norm = p.plan_id_norm AND b.year = p.year
        LEFT JOIN plan_fallback pf ON b.contract_id = pf.contract_id AND b.plan_id_norm = pf.plan_id_norm
        LEFT JOIN snp_lookup s ON b.contract_id = s.contract_id AND b.plan_id_norm = s.plan_id_norm AND b.year = s.year
        LEFT JOIN snp_fallback sf ON b.contract_id = sf.contract_id AND b.plan_id_norm = sf.plan_id_norm
    """)
    
    # Check results
    result = con.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(parent_org) as has_parent,
            COUNT(plan_type) as has_plan_type,
            SUM(CASE WHEN snp_type != 'Non-SNP' THEN 1 ELSE 0 END) as snp_count
        FROM national_enriched
    """).fetchone()
    
    print(f"\nResults:")
    print(f"  Total rows: {result[0]:,}")
    print(f"  Has parent_org: {result[1]:,} ({result[1]*100/result[0]:.0f}%)")
    print(f"  Has plan_type: {result[2]:,} ({result[2]*100/result[0]:.0f}%)")
    print(f"  SNP plans: {result[3]:,} ({result[3]*100/result[0]:.1f}%)")
    
    # SNP breakdown
    snp_dist = con.execute("""
        SELECT snp_type, COUNT(*) as cnt, SUM(enrollment) as enrollment
        FROM national_enriched
        GROUP BY snp_type
        ORDER BY enrollment DESC
    """).fetchall()
    print(f"\n  SNP Distribution:")
    for row in snp_dist:
        print(f"    {row[0]}: {row[1]:,} rows, {row[2]:,} enrollment")
    
    # Save to S3
    print("\nSaving to S3...")
    con.execute(f"""
        COPY national_enriched TO 's3://{BUCKET}/gold/fact_enrollment_national_v2.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    print("  Saved: gold/fact_enrollment_national_v2.parquet")
    
    return True


def rebuild_geographic_year(con, year):
    """Rebuild one year of geographic enrollment."""
    
    # Check if files exist for this year (path is year/month/data.parquet)
    try:
        count = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('s3://{BUCKET}/processed/fact_enrollment/{year}/*/*.parquet')
        """).fetchone()[0]
    except:
        return None
    
    if count == 0:
        return None
    
    # Enrich with all dimensions
    con.execute(f"""
        CREATE OR REPLACE TABLE geo_year AS
        WITH base AS (
            SELECT 
                contract_number as cid,
                plan_id as pid,
                CAST(COALESCE(
                    NULLIF(REGEXP_REPLACE(LTRIM(CAST(plan_id AS VARCHAR), '0'), '\\..*', ''), ''),
                    '0'
                ) AS VARCHAR) as plan_id_norm,
                year,
                month,
                state,
                county,
                fips_state_county_code as fips,
                enrollment
            FROM read_parquet('s3://{BUCKET}/processed/fact_enrollment/{year}/*/*.parquet')
        )
        SELECT 
            b.cid as contract_id,
            b.pid as plan_id,
            b.year,
            b.month,
            b.state,
            b.county,
            b.fips,
            b.enrollment,
            COALESCE(e.parent_org, ef.parent_org) as parent_org,
            COALESCE(e.organization_name, ef.organization_name) as organization_name,
            COALESCE(p.plan_type, pf.plan_type) as plan_type,
            COALESCE(p.product_type, pf.product_type, 
                CASE WHEN b.cid LIKE 'S%' THEN 'PDP' ELSE 'MAPD' END) as product_type,
            COALESCE(p.group_type, pf.group_type) as group_type,
            COALESCE(s.snp_type, sf.snp_type, 'Non-SNP') as snp_type,
            'cpsc' as data_source
        FROM base b
        LEFT JOIN dim_entity e ON b.cid = e.contract_id AND b.year = e.year
        LEFT JOIN entity_fallback ef ON b.cid = ef.contract_id
        LEFT JOIN dim_plan p ON b.cid = p.contract_id AND b.plan_id_norm = p.plan_id_norm AND b.year = p.year
        LEFT JOIN plan_fallback pf ON b.cid = pf.contract_id AND b.plan_id_norm = pf.plan_id_norm
        LEFT JOIN snp_lookup s ON b.cid = s.contract_id AND b.plan_id_norm = s.plan_id_norm AND b.year = s.year
        LEFT JOIN snp_fallback sf ON b.cid = sf.contract_id AND b.plan_id_norm = sf.plan_id_norm
    """)
    
    result = con.execute("""
        SELECT COUNT(*), 
               COUNT(parent_org)*100.0/COUNT(*),
               SUM(CASE WHEN snp_type != 'Non-SNP' THEN 1 ELSE 0 END)*100.0/COUNT(*)
        FROM geo_year
    """).fetchone()
    
    # Save
    con.execute(f"""
        COPY geo_year TO 's3://{BUCKET}/gold/fact_enrollment_geographic/year={year}/data_v2.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    
    return (result[0], result[1], result[2])


def rebuild_geographic(con):
    """Rebuild geographic enrollment year by year."""
    print("\n" + "=" * 60)
    print("REBUILDING GEOGRAPHIC ENROLLMENT")
    print("=" * 60)
    
    for year in range(2013, 2027):
        result = rebuild_geographic_year(con, year)
        if result:
            print(f"  {year}: {result[0]:,} rows, {result[1]:.0f}% parent, {result[2]:.1f}% SNP")


def main():
    print("=" * 60)
    print(f"GOLD ENROLLMENT REBUILD (DuckDB) - {datetime.now()}")
    print("=" * 60)
    
    con = get_connection()
    
    rebuild_national(con)
    rebuild_geographic(con)
    
    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)
    
    con.close()


if __name__ == "__main__":
    main()
