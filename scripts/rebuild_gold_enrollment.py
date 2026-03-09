#!/usr/bin/env python3
"""
Rebuild Gold Enrollment Tables - Ultra Memory Efficient
Processes one month at a time, pure vectorized operations
"""

import boto3
import pandas as pd
from io import BytesIO
import zipfile
import gc
from datetime import datetime

s3 = boto3.client('s3')
BUCKET = 'ma-data123'


def normalize_plan_ids(series):
    """Vectorized plan_id normalization."""
    s = series.astype(str)
    s = s.str.lstrip('0').str.split('.').str[0]
    s = s.replace('', '0')
    return s


def load_dims_small():
    """Load minimal dimension data for lookups."""
    print("Loading dimensions...")
    
    # Entity
    obj = s3.get_object(Bucket=BUCKET, Key='gold/dim_entity.parquet')
    entity = pd.read_parquet(BytesIO(obj['Body'].read()))
    entity = entity[['contract_id', 'year', 'parent_org', 'organization_name', 'organization_type']]
    print(f"  entity: {len(entity):,} rows")
    
    # Plan
    obj = s3.get_object(Bucket=BUCKET, Key='gold/dim_plan.parquet')
    plan = pd.read_parquet(BytesIO(obj['Body'].read()))
    plan = plan[['contract_id', 'plan_id', 'year', 'plan_type', 'product_type', 'group_type']]
    plan['plan_id'] = normalize_plan_ids(plan['plan_id'])
    print(f"  plan: {len(plan):,} rows")
    
    # SNP
    obj = s3.get_object(Bucket=BUCKET, Key='processed/unified/snp_lookup.parquet')
    snp = pd.read_parquet(BytesIO(obj['Body'].read()))
    snp = snp.sort_values(['year', 'month']).drop_duplicates(['contract_id', 'plan_id', 'year'], keep='last')
    snp = snp[['contract_id', 'plan_id', 'year', 'snp_type']]
    snp['plan_id'] = normalize_plan_ids(snp['plan_id'])
    print(f"  snp: {len(snp):,} rows")
    
    return entity, plan, snp


def enrich_month(df, year, entity, plan, snp):
    """Enrich a month's data using efficient merges."""
    df = df.copy()
    df['plan_id_norm'] = normalize_plan_ids(df['plan_id'])
    
    # Entity merge
    e_year = entity[entity['year'] == year][['contract_id', 'parent_org', 'organization_name', 'organization_type']]
    e_fb = entity.sort_values('year', ascending=False).drop_duplicates('contract_id')[['contract_id', 'parent_org', 'organization_name', 'organization_type']]
    e_fb.columns = ['contract_id', 'parent_org_fb', 'organization_name_fb', 'organization_type_fb']
    
    df = df.merge(e_year, on='contract_id', how='left')
    df = df.merge(e_fb, on='contract_id', how='left')
    df['parent_org'] = df['parent_org'].fillna(df['parent_org_fb'])
    df['organization_name'] = df['organization_name'].fillna(df['organization_name_fb'])
    df['organization_type'] = df['organization_type'].fillna(df['organization_type_fb'])
    df = df.drop(columns=['parent_org_fb', 'organization_name_fb', 'organization_type_fb'])
    
    # Plan merge
    p_year = plan[plan['year'] == year][['contract_id', 'plan_id', 'plan_type', 'product_type', 'group_type']]
    p_year = p_year.rename(columns={'plan_id': 'plan_id_norm'})
    p_fb = plan.sort_values('year', ascending=False).drop_duplicates(['contract_id', 'plan_id'])[['contract_id', 'plan_id', 'plan_type', 'product_type', 'group_type']]
    p_fb.columns = ['contract_id', 'plan_id_norm', 'plan_type_fb', 'product_type_fb', 'group_type_fb']
    
    df = df.merge(p_year, on=['contract_id', 'plan_id_norm'], how='left')
    df = df.merge(p_fb, on=['contract_id', 'plan_id_norm'], how='left')
    df['plan_type'] = df['plan_type'].fillna(df['plan_type_fb'])
    df['product_type'] = df['product_type'].fillna(df['product_type_fb'])
    df['group_type'] = df['group_type'].fillna(df['group_type_fb'])
    df = df.drop(columns=['plan_type_fb', 'product_type_fb', 'group_type_fb'])
    
    # Derive product_type if missing
    mask = df['product_type'].isna()
    df.loc[mask & df['contract_id'].str.startswith('S'), 'product_type'] = 'PDP'
    df.loc[mask & ~df['contract_id'].str.startswith('S'), 'product_type'] = 'MAPD'
    
    # SNP merge
    s_year = snp[snp['year'] == year][['contract_id', 'plan_id', 'snp_type']]
    s_year = s_year.rename(columns={'plan_id': 'plan_id_norm', 'snp_type': 'snp_type_y'})
    s_fb = snp.sort_values('year', ascending=False).drop_duplicates(['contract_id', 'plan_id'])[['contract_id', 'plan_id', 'snp_type']]
    s_fb.columns = ['contract_id', 'plan_id_norm', 'snp_type_fb']
    
    df = df.merge(s_year, on=['contract_id', 'plan_id_norm'], how='left')
    df = df.merge(s_fb, on=['contract_id', 'plan_id_norm'], how='left')
    df['snp_type'] = df['snp_type_y'].fillna(df['snp_type_fb']).fillna('Non-SNP')
    df = df.drop(columns=['snp_type_y', 'snp_type_fb', 'plan_id_norm'])
    
    return df


def rebuild_national(entity, plan, snp):
    """Rebuild national enrollment."""
    print("\n" + "=" * 60)
    print("NATIONAL ENROLLMENT")
    print("=" * 60)
    
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='raw/enrollment/by_plan/', MaxKeys=500)
    files = {}
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.zip') and obj['Size'] > 50000:
            parts = key.split('/')
            if len(parts) >= 4:
                files[parts[3]] = key
    
    all_dfs = []
    for ym, key in sorted(files.items()):
        try:
            year = int(ym.split('-')[0])
            month = int(ym.split('-')[1])
            
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = obj['Body'].read()
            if data[:2] != b'PK':
                continue
            
            with zipfile.ZipFile(BytesIO(data)) as zf:
                for name in zf.namelist():
                    if name.endswith('.csv'):
                        df = pd.read_csv(BytesIO(zf.read(name)), encoding='latin-1', low_memory=False)
                        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
                        
                        if 'plan_id' not in df.columns:
                            continue
                        
                        df['year'] = year
                        df['month'] = month
                        df['plan_id'] = df['plan_id'].astype(str)
                        df = df.rename(columns={'contract_number': 'contract_id'})
                        
                        enroll_col = next((c for c in df.columns if 'enrollment' in c.lower()), None)
                        if not enroll_col:
                            continue
                        
                        df['enrollment'] = pd.to_numeric(
                            df[enroll_col].astype(str).str.replace(',', '').str.replace('*', '0'),
                            errors='coerce'
                        ).fillna(0).astype(int)
                        
                        df = df[['contract_id', 'plan_id', 'year', 'month', 'enrollment']]
                        df = enrich_month(df, year, entity, plan, snp)
                        df['data_source'] = 'monthly_by_plan'
                        
                        all_dfs.append(df)
                        print(f"  {year}-{month:02d}: {len(df):,} rows")
                        break
            
        except Exception as e:
            print(f"  Error {ym}: {e}")
    
    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)
        print(f"\nTotal: {len(result):,} rows, {result['year'].min()}-{result['year'].max()}")
        print(f"SNP: {result['snp_type'].value_counts().to_dict()}")
        
        buffer = BytesIO()
        result.to_parquet(buffer, index=False, compression='snappy')
        buffer.seek(0)
        s3.put_object(Bucket=BUCKET, Key='gold/fact_enrollment_national.parquet', Body=buffer.getvalue())
        print(f"Saved: {len(buffer.getvalue())/1024/1024:.1f} MB")


def rebuild_geographic_month(key, entity, plan, snp):
    """Process a single CPSC month file."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
        df.columns = df.columns.str.lower()
        
        if 'contract_number' in df.columns:
            df = df.rename(columns={'contract_number': 'contract_id'})
        
        year = df['year'].iloc[0] if 'year' in df.columns else int(key.split('/')[2])
        df['plan_id'] = df['plan_id'].astype(str)
        
        df = enrich_month(df, year, entity, plan, snp)
        df['data_source'] = 'cpsc'
        
        return df
    except Exception as e:
        print(f"    Error: {e}")
        return None


def rebuild_geographic(entity, plan, snp):
    """Rebuild geographic enrollment month by month."""
    print("\n" + "=" * 60)
    print("GEOGRAPHIC ENROLLMENT (CPSC) - Month by Month")
    print("=" * 60)
    
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='processed/fact_enrollment/', MaxKeys=500)
    files_by_year = {}
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.parquet') and obj['Size'] > 100000:
            parts = key.split('/')
            if len(parts) >= 4:
                year = int(parts[2])
                files_by_year.setdefault(year, []).append(key)
    
    for year in sorted(files_by_year.keys()):
        files = sorted(files_by_year[year])
        print(f"\n{year}: {len(files)} months")
        
        year_dfs = []
        for key in files:
            df = rebuild_geographic_month(key, entity, plan, snp)
            if df is not None:
                year_dfs.append(df)
                print(f"  {key.split('/')[-1].replace('.parquet','')}: {len(df):,} rows")
        
        if year_dfs:
            year_df = pd.concat(year_dfs, ignore_index=True)
            
            buffer = BytesIO()
            year_df.to_parquet(buffer, index=False, compression='snappy')
            buffer.seek(0)
            s3.put_object(
                Bucket=BUCKET,
                Key=f'gold/fact_enrollment_geographic/year={year}/data.parquet',
                Body=buffer.getvalue()
            )
            
            snp_pct = (year_df['snp_type'] != 'Non-SNP').mean() * 100
            print(f"  SAVED: {len(year_df):,} rows, {snp_pct:.1f}% SNP")
            
            del year_df, year_dfs
            gc.collect()


def main():
    print("=" * 60)
    print(f"GOLD ENROLLMENT REBUILD - {datetime.now()}")
    print("=" * 60)
    
    entity, plan, snp = load_dims_small()
    
    rebuild_national(entity, plan, snp)
    gc.collect()
    
    rebuild_geographic(entity, plan, snp)
    
    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
