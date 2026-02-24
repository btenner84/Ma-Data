"""
Build SNP type lookup from CMS SNP reports.
Creates a mapping of (contract_id, plan_id, year, month) -> snp_type
"""

import boto3
import pandas as pd
from io import BytesIO
import zipfile
from typing import Dict, Tuple

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


def get_snp_type_code(snp_type: str) -> str:
    """Map full SNP type name to code."""
    if pd.isna(snp_type):
        return None
    snp_type = str(snp_type).strip()
    if 'Dual' in snp_type:
        return 'D-SNP'
    elif 'Chronic' in snp_type or 'Disabling' in snp_type:
        return 'C-SNP'
    elif 'Institutional' in snp_type:
        return 'I-SNP'
    return None


def process_snp_file(s3_key: str, year: int, month: int) -> pd.DataFrame:
    """Process a single SNP zip file and return enrollment by contract/plan with SNP type."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        zf = zipfile.ZipFile(BytesIO(response['Body'].read()))

        for name in zf.namelist():
            if name.endswith('.xlsx'):
                with zf.open(name) as f:
                    wb = pd.ExcelFile(BytesIO(f.read()))

                    # Try SNP_REPORT_PART_17 first (detailed data with SNP type)
                    if 'SNP_REPORT_PART_17' in wb.sheet_names:
                        df = wb.parse('SNP_REPORT_PART_17', header=0)
                    elif 'SNP_REPORT_PART_02' in wb.sheet_names:
                        df = wb.parse('SNP_REPORT_PART_02', header=0)
                    else:
                        # Try to find enrollment sheet
                        for sheet in wb.sheet_names:
                            if 'PART' in sheet and 'REPORT' in sheet:
                                df = wb.parse(sheet, header=0)
                                if 'Contract' in str(df.columns[0]):
                                    break
                        else:
                            print(f"No enrollment sheet found in {s3_key}")
                            return pd.DataFrame()

                    # Normalize column names
                    col_map = {}
                    for col in df.columns:
                        col_lower = col.lower().replace(' ', '_')
                        if 'contract' in col_lower and ('id' in col_lower or 'number' in col_lower):
                            col_map[col] = 'contract_id'
                        elif 'plan_id' in col_lower or col_lower == 'plan_id':
                            col_map[col] = 'plan_id'
                        elif 'snp_type' in col_lower or 'special_needs_plan_type' in col_lower:
                            col_map[col] = 'snp_type_raw'
                        elif col_lower == 'enrollment':
                            col_map[col] = 'enrollment'

                    df = df.rename(columns=col_map)

                    # Filter to required columns
                    required = ['contract_id', 'plan_id', 'snp_type_raw']
                    if not all(c in df.columns for c in required):
                        print(f"Missing columns in {s3_key}: have {df.columns.tolist()}")
                        return pd.DataFrame()

                    # Clean up
                    df = df[df['contract_id'].notna()]
                    df = df[df['contract_id'] != 'Under-11']

                    # Map SNP type
                    df['snp_type'] = df['snp_type_raw'].apply(get_snp_type_code)

                    # Add year/month
                    df['year'] = year
                    df['month'] = month

                    # Format plan_id as string with leading zeros
                    df['plan_id'] = df['plan_id'].apply(lambda x: f"{int(x):03d}" if pd.notna(x) else None)

                    result = df[['contract_id', 'plan_id', 'year', 'month', 'snp_type']].copy()
                    result = result[result['snp_type'].notna()]

                    return result

    except Exception as e:
        print(f"Error processing {s3_key}: {e}")
        return pd.DataFrame()

    return pd.DataFrame()


def build_snp_lookup() -> pd.DataFrame:
    """Build SNP lookup from all available SNP files."""
    # List all SNP files
    paginator = s3.get_paginator('list_objects_v2')

    all_dfs = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='raw/snp/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.zip'):
                continue

            # Parse year/month from path: raw/snp/2025-01/snp_2025_01.zip
            parts = key.split('/')
            if len(parts) >= 3:
                ym = parts[2]  # e.g., "2025-01"
                try:
                    year, month = map(int, ym.split('-'))
                except:
                    continue

                print(f"Processing {key}...")
                df = process_snp_file(key, year, month)
                if not df.empty:
                    all_dfs.append(df)

    if not all_dfs:
        print("No SNP data found!")
        return pd.DataFrame()

    # Combine all
    combined = pd.concat(all_dfs, ignore_index=True)

    # For each contract/plan, use December data (or latest available month)
    # to get annual SNP type
    combined = combined.sort_values(['contract_id', 'plan_id', 'year', 'month'])

    # Take December (month=12) or latest month per year
    annual = combined.groupby(['contract_id', 'plan_id', 'year']).last().reset_index()
    annual = annual[['contract_id', 'plan_id', 'year', 'snp_type']]

    print(f"\nBuilt SNP lookup with {len(annual)} records")
    print(f"SNP types: {annual['snp_type'].value_counts().to_dict()}")
    print(f"Years: {sorted(annual['year'].unique())}")

    return annual


if __name__ == "__main__":
    print("Building SNP lookup table...")
    lookup = build_snp_lookup()

    if not lookup.empty:
        # Save to S3
        buffer = BytesIO()
        lookup.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3.put_object(
            Bucket=S3_BUCKET,
            Key='processed/unified/snp_lookup.parquet',
            Body=buffer.getvalue()
        )
        print(f"\nSaved SNP lookup to s3://{S3_BUCKET}/processed/unified/snp_lookup.parquet")
