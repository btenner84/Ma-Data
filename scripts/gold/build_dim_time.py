#!/usr/bin/env python3
"""
Build Gold Layer: dim_time
===========================

Creates the time dimension table for the star schema.

Output: s3://ma-data123/gold/dim_time.parquet

Columns:
- time_key: INT (YYYYMM format, e.g., 202401)
- year: INT
- month: INT
- month_name: STRING (January, February, etc.)
- quarter: INT (1-4)
- quarter_name: STRING (Q1, Q2, Q3, Q4)
- is_year_end: BOOL (December = True)
- fiscal_year: INT (CMS fiscal year, Oct-Sep)
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import calendar
import os

S3_BUCKET = os.environ.get("S3_BUCKET", "ma-data123")
OUTPUT_KEY = "gold/dim_time.parquet"

s3 = boto3.client('s3')


def generate_time_dimension(start_year: int = 2006, end_year: int = 2030) -> pd.DataFrame:
    """Generate time dimension for all year-month combinations."""
    records = []
    
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            time_key = year * 100 + month
            month_name = calendar.month_name[month]
            quarter = (month - 1) // 3 + 1
            quarter_name = f"Q{quarter}"
            is_year_end = month == 12
            
            fiscal_year = year if month >= 10 else year - 1
            
            records.append({
                'time_key': time_key,
                'year': year,
                'month': month,
                'month_name': month_name,
                'quarter': quarter,
                'quarter_name': quarter_name,
                'is_year_end': is_year_end,
                'fiscal_year': fiscal_year,
            })
    
    return pd.DataFrame(records)


def main():
    print("=" * 70)
    print("BUILD GOLD LAYER: dim_time")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    
    df = generate_time_dimension(2006, 2030)
    print(f"Generated {len(df):,} time dimension rows")
    print(f"Year range: {df['year'].min()} - {df['year'].max()}")
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=buffer.getvalue())
    
    print(f"Saved to s3://{S3_BUCKET}/{OUTPUT_KEY}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    main()
