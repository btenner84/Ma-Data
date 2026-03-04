#!/usr/bin/env python3
"""
UNIFIED STARS PARSER - ALL YEARS 2007-2026
==========================================
Handles every file format variation across all years.
Creates unified output with audit lineage.
"""

import boto3
import pandas as pd
import zipfile
import re
import uuid
from io import BytesIO
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import traceback

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')

PIPELINE_RUN_ID = f"unified_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# =============================================================================
# FILE DETECTION
# =============================================================================

def detect_file_type(filename: str) -> Tuple[str, Optional[str]]:
    """
    Detect file type and part (C/D) from filename.
    Returns (file_type, part)
    """
    fname_lower = filename.lower()

    # 2020+ format
    if 'star ratings data table' in fname_lower:
        if 'measure data' in fname_lower:
            return ('MEASURE_DATA_2020', None)
        elif 'measure star' in fname_lower:
            return ('MEASURE_STARS_2020', None)
        elif 'summary' in fname_lower:
            return ('SUMMARY_2020', None)
        elif 'domain' in fname_lower:
            return ('DOMAIN_2020', None)
        elif 'disenrollment' in fname_lower:
            return ('DISENROLLMENT', None)
        elif 'cai' in fname_lower:
            return ('CAI', None)
        elif 'hpi' in fname_lower or 'high performing' in fname_lower:
            return ('HPI', None)
        elif 'lpi' in fname_lower or 'low performing' in fname_lower:
            return ('LPI', None)

    # 2014-2019 Report Card format - ALL use same structure as 2020
    # Row 0: Title, Row 1: Header+Domains, Row 2: Measure IDs, Row 3: Dates, Row 4+: Data
    if 'report_card_master_table' in fname_lower or 'report card master table' in fname_lower:
        part = 'C' if 'part_c' in fname_lower or 'part c' in fname_lower else None
        part = 'D' if 'part_d' in fname_lower or 'part d' in fname_lower else part

        if '_data.csv' in fname_lower or '_data.csv' in filename:
            return ('MEASURE_DATA_2020', part)  # Use 2020 parser for all
        elif '_star.csv' in fname_lower or '_stars.csv' in fname_lower:
            return ('MEASURE_STARS_2020', part)  # Use 2020 parser for all
        elif '_summary.csv' in fname_lower:
            return ('SUMMARY_LEGACY', part)
        elif '_domain.csv' in fname_lower:
            return ('DOMAIN_LEGACY', part)
        elif '_hpi.csv' in fname_lower:
            return ('HPI', part)
        elif '_lpi.csv' in fname_lower:
            return ('LPI', part)
        elif '_threshold.csv' in fname_lower or '_cutpoint' in fname_lower:
            return ('CUTPOINTS', part)

    # 2007 Performance Metrics
    if 'performance metrics' in fname_lower:
        if 'data.csv' in fname_lower:
            return ('MEASURE_DATA_2007', 'D')
        elif 'star.csv' in fname_lower:
            return ('MEASURE_STARS_2007', 'D')
        elif 'cutpoint' in fname_lower:
            return ('CUTPOINTS', 'D')

    # Cutpoints
    if 'cut' in fname_lower and ('point' in fname_lower or 'threshold' in fname_lower):
        part = 'C' if 'part_c' in fname_lower or 'part c' in fname_lower else None
        part = 'D' if 'part_d' in fname_lower or 'part d' in fname_lower else part
        return ('CUTPOINTS', part)

    # Summary
    if 'summary' in fname_lower:
        part = 'C' if 'part_c' in fname_lower else None
        part = 'D' if 'part_d' in fname_lower else part
        return ('SUMMARY', part)

    # Domain
    if 'domain' in fname_lower:
        part = 'C' if 'part_c' in fname_lower else None
        part = 'D' if 'part_d' in fname_lower else part
        return ('DOMAIN', part)

    # CAI
    if 'cai' in fname_lower:
        return ('CAI', None)

    # Disenrollment
    if 'disenroll' in fname_lower:
        return ('DISENROLLMENT', None)

    return ('UNKNOWN', None)

# =============================================================================
# PARSING HELPERS
# =============================================================================

def read_csv_raw(content: bytes, skip_rows: int = 0) -> pd.DataFrame:
    """Read CSV with multiple encoding attempts."""
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(
                BytesIO(content),
                encoding=encoding,
                header=skip_rows,
                dtype=str,
                on_bad_lines='skip'
            )
            # Remove BOM if present
            if df.columns[0].startswith('\ufeff'):
                df.columns = [df.columns[0].replace('\ufeff', '')] + list(df.columns[1:])
            return df
        except Exception as e:
            continue
    return pd.DataFrame()

def find_contract_col(df: pd.DataFrame) -> Optional[str]:
    """Find the contract ID column."""
    for col in df.columns:
        col_str = str(col).lower()
        if 'contract' in col_str and ('id' in col_str or 'number' in col_str):
            return col
        if col_str == 'contract':
            return col
        if col == 'CONTRACT_ID':
            return col

    # Check first column for contract pattern
    if len(df.columns) > 0 and len(df) > 0:
        first_col = df.columns[0]
        sample = df[first_col].dropna().head(10).astype(str)
        if sample.str.match(r'^[HERS]\d{4}').any():
            return first_col

    return None

def extract_year_from_path(s3_key: str, filename: str) -> Optional[int]:
    """Extract year from S3 key or filename."""
    # Try S3 key first (e.g., raw/stars/2020_combined.zip)
    match = re.search(r'/(\d{4})_', s3_key)
    if match:
        return int(match.group(1))

    # Try filename
    match = re.search(r'(\d{4})', filename)
    if match:
        year = int(match.group(1))
        if 2006 <= year <= 2030:
            return year

    return None

# =============================================================================
# MEASURE DATA PARSERS
# =============================================================================

def parse_measure_data_2020(content: bytes, filename: str, year: int) -> List[dict]:
    """
    Parse measure data files - handles ALL format variations 2009-2026.
    Auto-detects:
      - Header row (contains "Contract Number" or "CONTRACT_ID")
      - Measure ID row (contains C01/C02/D01/D02)
      - Data start row
    """
    records = []

    # Decode content
    text = None
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            text = content.decode(encoding, errors='replace')
            break
        except:
            continue

    if not text:
        return records

    lines = text.split('\n')
    if len(lines) < 5:
        return records

    # Parse all lines into parts
    def parse_csv_line(line):
        parts = []
        current = ''
        in_quotes = False
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(current.strip().strip('"'))
                current = ''
            else:
                current += char
        parts.append(current.strip().strip('"'))
        return parts

    all_parts = [parse_csv_line(line) for line in lines[:20]]

    # Find header row (contains "Contract Number" or "CONTRACT_ID")
    header_row_idx = None
    contract_col_idx = None
    for i, parts in enumerate(all_parts):
        for j, val in enumerate(parts):
            val_lower = val.lower()
            if 'contract' in val_lower and ('number' in val_lower or 'id' in val_lower or val_lower == 'contract'):
                header_row_idx = i
                contract_col_idx = j
                break
        if header_row_idx is not None:
            break

    if header_row_idx is None:
        print(f"    No header row found in {filename}")
        return records

    # Find measure ID row (contains C01, C02, D01, D02, etc.)
    measure_row_idx = None
    for i, parts in enumerate(all_parts):
        for val in parts:
            if re.match(r'^[CD]\d{2}[:\s]', val) or re.match(r'^[CD]\d{2}$', val):
                measure_row_idx = i
                break
        if measure_row_idx is not None:
            break

    if measure_row_idx is None:
        # Try finding measure names directly in header row
        # (some older formats have measure names as column headers)
        for i, parts in enumerate(all_parts):
            for val in parts:
                if 'breast cancer' in val.lower() or 'colorectal' in val.lower():
                    measure_row_idx = i
                    break
            if measure_row_idx is not None:
                break

    if measure_row_idx is None:
        print(f"    No measure row found in {filename}")
        return records

    # Build measure column mapping
    measure_cols = {}  # col_index -> (measure_id, measure_name)
    measure_parts = all_parts[measure_row_idx]

    for i, val in enumerate(measure_parts):
        if not val:
            continue

        # Pattern 1: "C01: Breast Cancer Screening"
        match = re.match(r'^([CD]\d{2})[:\s]+(.+)', val)
        if match:
            measure_cols[i] = (match.group(1), match.group(2).strip())
            continue

        # Pattern 2: Just "C01" (measure names might be in different row)
        match = re.match(r'^([CD]\d{2})$', val)
        if match:
            measure_id = match.group(1)
            # Try to get name from a different row (typically header row has names)
            measure_name = val
            if header_row_idx != measure_row_idx and i < len(all_parts[header_row_idx]):
                potential_name = all_parts[header_row_idx][i]
                if potential_name and 'contract' not in potential_name.lower():
                    measure_name = potential_name
            measure_cols[i] = (measure_id, measure_name)
            continue

        # Pattern 3: Measure name without ID (e.g., "Breast Cancer Screening")
        if any(kw in val.lower() for kw in ['screening', 'vaccine', 'care', 'control', 'adherence']):
            # Generate a measure ID based on position
            measure_id = f"M{i:02d}"
            measure_cols[i] = (measure_id, val)

    if not measure_cols:
        print(f"    No measure columns found in {filename}")
        return records

    # Determine data start row (first row after measure row with contract ID pattern)
    data_start_row = max(header_row_idx, measure_row_idx) + 1
    for i in range(data_start_row, min(len(all_parts), data_start_row + 5)):
        parts = all_parts[i]
        if len(parts) > contract_col_idx:
            val = parts[contract_col_idx]
            # Skip date rows
            if re.search(r'\d{1,2}/\d{1,2}/\d{4}', val):
                data_start_row = i + 1
                continue
            # Found data row
            if re.match(r'^[HERS90]\d{3,4}', val):
                data_start_row = i
                break

    print(f"    Found {len(measure_cols)} measures, header row {header_row_idx}, measure row {measure_row_idx}, data starts row {data_start_row}")

    # Parse data rows
    for line_num in range(data_start_row, len(lines)):
        line = lines[line_num].strip()
        if not line:
            continue

        parts = parse_csv_line(line)

        if len(parts) <= contract_col_idx:
            continue

        contract_id = parts[contract_col_idx].strip()
        if not re.match(r'^[HERS90]\d{3,4}', contract_id):
            continue

        for col_idx, (measure_id, measure_name) in measure_cols.items():
            if col_idx >= len(parts):
                continue

            raw_value = parts[col_idx].strip()
            if not raw_value:
                continue

            # Skip non-data values
            skip_phrases = ['not required', 'not enough', 'no data', 'plan too', 'cms identified',
                           'medicare shows', 'not applicable', 'plan not']
            if any(skip in raw_value.lower() for skip in skip_phrases):
                continue

            # Parse value
            star_rating = None
            numeric_value = None

            if raw_value in ['1', '2', '3', '4', '5']:
                star_rating = int(raw_value)
            else:
                # Try to extract percentage or number
                num_match = re.match(r'^(\d+\.?\d*)%?$', raw_value)
                if num_match:
                    numeric_value = float(num_match.group(1))

            records.append({
                'year': year,
                'contract_id': contract_id,
                'measure_id': measure_id,
                'measure_name': measure_name,
                'star_rating': star_rating,
                'numeric_value': numeric_value,
                'raw_value': raw_value,
                '_source_file': filename,
                '_pipeline_run_id': PIPELINE_RUN_ID
            })

    return records

def parse_measure_stars_2020(content: bytes, filename: str, year: int) -> List[dict]:
    """
    Parse 2020-2026 Measure Stars files.
    Same structure as Measure Data but values are star ratings.
    """
    return parse_measure_data_2020(content, filename, year)

def parse_measure_data_legacy(content: bytes, filename: str, year: int, part: str) -> List[dict]:
    """
    Parse 2008-2019 _data.csv files.
    Structure varies by year - detect header row dynamically.
    """
    records = []

    # Read raw to detect structure
    df_raw = read_csv_raw(content, skip_rows=0)
    if df_raw.empty or len(df_raw) < 5:
        return records

    # Find header row (contains Contract)
    header_row = None
    for i in range(min(10, len(df_raw))):
        row_str = ' '.join(df_raw.iloc[i].astype(str)).lower()
        if 'contract' in row_str and ('number' in row_str or 'id' in row_str):
            header_row = i
            break

    if header_row is None:
        print(f"    Could not find header row in {filename}")
        return records

    # Re-read with correct header
    df = read_csv_raw(content, skip_rows=header_row)
    if df.empty:
        return records

    contract_col = find_contract_col(df)
    if not contract_col:
        return records

    # Find measure columns - look for C01/C02/D01/D02 pattern
    # First check if there's a measure row after header
    measure_row = None
    data_start = 0

    if len(df) > 1:
        first_data_row = df.iloc[0]
        # Check if first row has measure IDs
        for val in first_data_row:
            if pd.notna(val) and re.match(r'^[CD]\d{2}', str(val).strip()):
                measure_row = first_data_row
                data_start = 1
                break

    # Check for date row
    if len(df) > data_start:
        check_row = df.iloc[data_start]
        for val in check_row:
            if pd.notna(val) and re.search(r'\d{1,2}/\d{1,2}/\d{4}', str(val)):
                data_start += 1
                break

    # Build measure mapping
    measure_cols = {}
    if measure_row is not None:
        for col in df.columns:
            val = measure_row[col] if col in measure_row else None
            if pd.notna(val):
                val_str = str(val).strip()
                match = re.match(r'^([CD]\d{2})[:\s]*(.*)', val_str)
                if match:
                    measure_cols[col] = (match.group(1), match.group(2).strip() if match.group(2) else col)

    # If no measure row, try column headers
    if not measure_cols:
        for col in df.columns:
            col_str = str(col)
            match = re.match(r'^([CD]\d{2})[:\s]*(.*)', col_str)
            if match:
                measure_cols[col] = (match.group(1), match.group(2).strip() if match.group(2) else col_str)

    if not measure_cols:
        # Last resort: look for columns with star values
        for col in df.columns:
            if col in [contract_col, '_source_file', '_pipeline_run_id']:
                continue
            sample = df[col].iloc[data_start:data_start+20].dropna().astype(str)
            if sample.str.match(r'^[1-5]$|^\d+\.?\d*%?$').any():
                measure_cols[col] = (col, col)

    if not measure_cols:
        print(f"    No measure columns found in {filename}")
        return records

    # Parse data
    df_data = df.iloc[data_start:].reset_index(drop=True)

    for _, row in df_data.iterrows():
        contract_id = str(row[contract_col]).strip()
        if not re.match(r'^[HERS]\d{4}', contract_id):
            continue

        for col, (measure_id, measure_name) in measure_cols.items():
            value = row[col] if col in row else None
            if pd.isna(value):
                continue

            raw_value = str(value).strip()
            if raw_value == '' or 'not' in raw_value.lower() or 'plan' in raw_value.lower():
                continue

            star_rating = None
            numeric_value = None

            if raw_value in ['1', '2', '3', '4', '5']:
                star_rating = int(raw_value)
            elif re.match(r'^\d+\.?\d*%?$', raw_value):
                numeric_value = float(raw_value.replace('%', ''))

            records.append({
                'year': year,
                'contract_id': contract_id,
                'part': part,
                'measure_id': measure_id,
                'measure_name': measure_name,
                'star_rating': star_rating,
                'numeric_value': numeric_value,
                'raw_value': raw_value,
                '_source_file': filename,
                '_pipeline_run_id': PIPELINE_RUN_ID
            })

    return records

def parse_measure_2007(content: bytes, filename: str) -> List[dict]:
    """
    Parse 2007 Performance Metrics files.
    Very different structure - older CMS format.
    """
    records = []

    df_raw = read_csv_raw(content, skip_rows=0)
    if df_raw.empty:
        return records

    # Find header row
    header_row = None
    for i in range(min(10, len(df_raw))):
        row_str = ' '.join(df_raw.iloc[i].astype(str)).lower()
        if 'contract' in row_str:
            header_row = i
            break

    if header_row is None:
        return records

    df = read_csv_raw(content, skip_rows=header_row)
    if df.empty:
        return records

    contract_col = find_contract_col(df)
    if not contract_col:
        return records

    # 2007 has different column names - extract what we can
    for _, row in df.iterrows():
        contract_id = str(row[contract_col]).strip()
        if not re.match(r'^[HERS]\d{4}', contract_id):
            continue

        for col in df.columns:
            if col == contract_col:
                continue

            value = row[col]
            if pd.isna(value):
                continue

            raw_value = str(value).strip()
            if raw_value == '':
                continue

            records.append({
                'year': 2007,
                'contract_id': contract_id,
                'part': 'D',
                'measure_id': col,
                'measure_name': col,
                'star_rating': None,
                'numeric_value': None,
                'raw_value': raw_value,
                '_source_file': filename,
                '_pipeline_run_id': PIPELINE_RUN_ID
            })

    return records

# =============================================================================
# SUMMARY/DOMAIN PARSERS
# =============================================================================

def parse_summary_rating(content: bytes, filename: str, year: int, part: str) -> List[dict]:
    """Parse summary rating files."""
    records = []

    df_raw = read_csv_raw(content, skip_rows=0)
    if df_raw.empty:
        return records

    # Find header row
    header_row = None
    for i in range(min(10, len(df_raw))):
        row_str = ' '.join(df_raw.iloc[i].astype(str)).lower()
        if 'contract' in row_str:
            header_row = i
            break

    if header_row is None:
        return records

    df = read_csv_raw(content, skip_rows=header_row)
    if df.empty:
        return records

    contract_col = find_contract_col(df)
    if not contract_col:
        return records

    # Find summary column
    summary_col = None
    for col in df.columns:
        col_lower = str(col).lower()
        if 'summary' in col_lower and ('score' in col_lower or 'rating' in col_lower):
            summary_col = col
            break

    if not summary_col:
        # Try to find any column with star values
        for col in df.columns:
            if col == contract_col:
                continue
            sample = df[col].dropna().head(20).astype(str)
            if sample.str.match(r'^[1-5]\.?[05]?$|^\d out of 5').any():
                summary_col = col
                break

    for _, row in df.iterrows():
        contract_id = str(row[contract_col]).strip()
        if not re.match(r'^[HERS]\d{4}', contract_id):
            continue

        summary_value = row[summary_col] if summary_col and summary_col in row else None

        # Parse summary rating
        summary_rating = None
        if pd.notna(summary_value):
            val_str = str(summary_value).strip()
            # Try to extract numeric rating
            match = re.search(r'(\d\.?\d?)\s*(?:out of 5|stars)?', val_str)
            if match:
                summary_rating = float(match.group(1))

        # Get other columns
        org_type = None
        parent_org = None
        org_name = None

        for col in df.columns:
            col_lower = str(col).lower()
            val = row[col] if col in row else None
            if pd.notna(val):
                if 'type' in col_lower and 'org' in col_lower:
                    org_type = str(val).strip()
                elif 'parent' in col_lower:
                    parent_org = str(val).strip()
                elif 'marketing' in col_lower or 'org' in col_lower:
                    org_name = str(val).strip()

        records.append({
            'year': year,
            'contract_id': contract_id,
            'part': part,
            'summary_rating': summary_rating,
            'raw_value': str(summary_value) if pd.notna(summary_value) else None,
            'organization_type': org_type,
            'parent_organization': parent_org,
            'organization_name': org_name,
            '_source_file': filename,
            '_pipeline_run_id': PIPELINE_RUN_ID
        })

    return records

def parse_domain(content: bytes, filename: str, year: int, part: str) -> List[dict]:
    """Parse domain score files - handles all year formats."""
    records = []

    # Decode content
    text = None
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            text = content.decode(encoding, errors='replace')
            break
        except:
            continue

    if not text:
        return records

    lines = text.split('\n')
    if len(lines) < 3:
        return records

    # Parse CSV line helper
    def parse_csv_line(line):
        parts = []
        current = ''
        in_quotes = False
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                parts.append(current.strip().strip('"'))
                current = ''
            else:
                current += char
        parts.append(current.strip().strip('"'))
        return parts

    all_parts = [parse_csv_line(line) for line in lines[:10]]

    # Find header row (contains "Contract")
    header_row_idx = None
    contract_col_idx = None
    for i, parts in enumerate(all_parts):
        for j, val in enumerate(parts):
            val_lower = val.lower()
            if 'contract' in val_lower and ('number' in val_lower or 'id' in val_lower or val_lower == 'contract'):
                header_row_idx = i
                contract_col_idx = j
                break
        if header_row_idx is not None:
            break

    if header_row_idx is None:
        print(f"    No header row found in domain file {filename}")
        return records

    # Find domain columns (HD1:, HD2:, DD1:, DD2: or domain names)
    headers = all_parts[header_row_idx]
    domain_cols = {}  # col_index -> domain_name

    for i, val in enumerate(headers):
        if i == contract_col_idx:
            continue
        # Pattern: "HD1: Staying Healthy..." or "DD1: Drug Plan..."
        match = re.match(r'^([HD]D\d+)[:\s]+(.+)', val)
        if match:
            domain_cols[i] = f"{match.group(1)}: {match.group(2).strip()}"
            continue
        # Also catch domain names without ID
        if any(kw in val.lower() for kw in ['staying healthy', 'managing chronic', 'member experience',
               'member complaints', 'customer service', 'drug plan', 'getting care']):
            domain_cols[i] = val

    if not domain_cols:
        print(f"    No domain columns found in {filename}")
        return records

    print(f"    Found {len(domain_cols)} domain columns")

    # Parse data rows
    data_start = header_row_idx + 1
    for line_num in range(data_start, len(lines)):
        line = lines[line_num].strip()
        if not line:
            continue

        parts = parse_csv_line(line)
        if len(parts) <= contract_col_idx:
            continue

        contract_id = parts[contract_col_idx].strip()
        if not re.match(r'^[HERS90]\d{3,4}', contract_id):
            continue

        for col_idx, domain_name in domain_cols.items():
            if col_idx >= len(parts):
                continue

            raw_value = parts[col_idx].strip()
            if not raw_value:
                continue

            # Skip non-data values
            skip_phrases = ['not required', 'not enough', 'no data', 'plan too', 'not applicable']
            if any(skip in raw_value.lower() for skip in skip_phrases):
                continue

            # Parse star rating
            star_rating = None
            if raw_value in ['1', '2', '3', '4', '5']:
                star_rating = int(raw_value)
            else:
                # Try to extract from "3 out of 5 stars" format
                match = re.search(r'(\d\.?\d?)\s*(?:out of 5|stars)?', raw_value)
                if match:
                    try:
                        star_rating = float(match.group(1))
                    except:
                        pass

            records.append({
                'year': year,
                'contract_id': contract_id,
                'part': part,
                'domain_name': domain_name,
                'star_rating': star_rating,
                'raw_value': raw_value,
                '_source_file': filename,
                '_pipeline_run_id': PIPELINE_RUN_ID
            })

    return records

    for _, row in df.iterrows():
        contract_id = str(row[contract_col]).strip()
        if not re.match(r'^[HERS]\d{4}', contract_id):
            continue

        for col in domain_cols:
            value = row[col] if col in row else None
            if pd.isna(value):
                continue

            raw_value = str(value).strip()
            if raw_value == '' or 'not' in raw_value.lower():
                continue

            # Parse star rating
            star_rating = None
            match = re.search(r'(\d\.?\d?)\s*(?:out of 5|stars)?', raw_value)
            if match:
                star_rating = float(match.group(1))

            records.append({
                'year': year,
                'contract_id': contract_id,
                'part': part,
                'domain_name': col,
                'star_rating': star_rating,
                'raw_value': raw_value,
                '_source_file': filename,
                '_pipeline_run_id': PIPELINE_RUN_ID
            })

    return records

# =============================================================================
# CUTPOINTS PARSER
# =============================================================================

def parse_cutpoints(content: bytes, filename: str, year: int, part: str) -> List[dict]:
    """
    Parse cutpoints files - handles all year variations.
    """
    records = []

    df_raw = read_csv_raw(content, skip_rows=0)
    if df_raw.empty or len(df_raw) < 5:
        return records

    # Find measure row (contains C01:, D01:, etc.)
    measure_row_idx = None
    for i in range(min(15, len(df_raw))):
        row_str = ' '.join(df_raw.iloc[i].astype(str))
        if re.search(r'[CD]\d{2}', row_str):
            measure_row_idx = i
            break

    if measure_row_idx is None:
        # Try finding by measure names
        for i in range(min(15, len(df_raw))):
            row_str = ' '.join(df_raw.iloc[i].astype(str)).lower()
            if 'breast cancer' in row_str or 'colorectal' in row_str:
                measure_row_idx = i
                break

    if measure_row_idx is None:
        print(f"    No measure row found in {filename}")
        return records

    measure_row = df_raw.iloc[measure_row_idx]

    # Find star threshold rows
    star_rows = {}  # star_level -> row_idx
    for i in range(measure_row_idx + 1, min(len(df_raw), measure_row_idx + 20)):
        row = df_raw.iloc[i]
        first_val = str(row.iloc[0]).lower().strip() if len(row) > 0 else ''
        second_val = str(row.iloc[1]).lower().strip() if len(row) > 1 else ''
        combined = first_val + ' ' + second_val

        for star in [1, 2, 3, 4, 5]:
            if f'{star} star' in combined or f'{star}star' in combined:
                if star not in star_rows:
                    star_rows[star] = i

    if not star_rows:
        print(f"    No star rows found in {filename}")
        return records

    # Extract measures and cutpoints
    for col_idx in range(1, len(measure_row)):
        measure_val = measure_row.iloc[col_idx]
        if pd.isna(measure_val):
            continue

        measure_str = str(measure_val).strip()

        # Parse measure ID and name
        match = re.match(r'^([CD]\d{2})[:\s]*(.*)', measure_str)
        if match:
            measure_id = match.group(1)
            measure_name = match.group(2).strip() if match.group(2) else measure_str
        else:
            # Use column as measure name (older format)
            measure_id = f"{part or 'X'}{col_idx:02d}"
            measure_name = measure_str

        for star_level, row_idx in star_rows.items():
            try:
                threshold_val = df_raw.iloc[row_idx, col_idx]
                if pd.isna(threshold_val):
                    continue

                threshold_str = str(threshold_val).strip()

                # Extract numeric threshold
                numbers = re.findall(r'[\d.]+', threshold_str)
                threshold = float(numbers[0]) if numbers else None

                records.append({
                    'year': year,
                    'part': part or measure_id[0],
                    'measure_id': measure_id,
                    'measure_name': measure_name,
                    'star_level': star_level,
                    'threshold': threshold,
                    'threshold_text': threshold_str,
                    '_source_file': filename,
                    '_pipeline_run_id': PIPELINE_RUN_ID
                })
            except Exception:
                continue

    return records

# =============================================================================
# MAIN PROCESSING
# =============================================================================

def extract_files_from_zip(zip_bytes: bytes) -> Dict[str, bytes]:
    """Extract all files from zip, handling nested zips."""
    files = {}
    try:
        zf = zipfile.ZipFile(BytesIO(zip_bytes))
        for name in zf.namelist():
            if name.endswith('.zip'):
                try:
                    inner_bytes = zf.read(name)
                    inner_zf = zipfile.ZipFile(BytesIO(inner_bytes))
                    for inner_name in inner_zf.namelist():
                        if inner_name.endswith(('.csv', '.xlsx')):
                            files[f"{name}/{inner_name}"] = inner_zf.read(inner_name)
                except:
                    pass
            elif name.endswith(('.csv', '.xlsx')):
                files[name] = zf.read(name)
    except:
        pass
    return files

def process_all_stars_data():
    """Process all stars data files across all years."""
    print("=" * 80)
    print(f"UNIFIED STARS PARSER - Processing ALL years 2007-2026")
    print(f"Pipeline Run ID: {PIPELINE_RUN_ID}")
    print("=" * 80)

    # Results containers
    all_measures = []
    all_summary = []
    all_domain = []
    all_cutpoints = []

    # List all stars zip files
    paginator = s3.get_paginator('list_objects_v2')
    zip_files = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='raw/stars/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.zip'):
                zip_files.append(obj['Key'])

    print(f"\nFound {len(zip_files)} zip files to process")

    for s3_key in sorted(zip_files):
        print(f"\n{'='*60}")
        print(f"Processing: {s3_key}")
        print(f"{'='*60}")

        try:
            resp = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            zip_bytes = resp['Body'].read()
            files = extract_files_from_zip(zip_bytes)

            print(f"  Extracted {len(files)} files")

            for filename, content in files.items():
                if not filename.endswith('.csv'):
                    continue

                file_type, part = detect_file_type(filename)
                year = extract_year_from_path(s3_key, filename)

                if year is None:
                    continue

                try:
                    if file_type == 'MEASURE_DATA_2020':
                        records = parse_measure_data_2020(content, filename, year)
                        all_measures.extend(records)
                        if records:
                            print(f"    MEASURE_DATA_2020: {len(records):,} records from {filename.split('/')[-1]}")

                    elif file_type == 'MEASURE_STARS_2020':
                        records = parse_measure_stars_2020(content, filename, year)
                        all_measures.extend(records)
                        if records:
                            print(f"    MEASURE_STARS_2020: {len(records):,} records from {filename.split('/')[-1]}")

                    elif file_type == 'MEASURE_DATA_LEGACY':
                        records = parse_measure_data_legacy(content, filename, year, part)
                        all_measures.extend(records)
                        if records:
                            print(f"    MEASURE_DATA_LEGACY ({year}): {len(records):,} records from {filename.split('/')[-1]}")

                    elif file_type == 'MEASURE_STARS_LEGACY':
                        records = parse_measure_data_legacy(content, filename, year, part)
                        all_measures.extend(records)
                        if records:
                            print(f"    MEASURE_STARS_LEGACY ({year}): {len(records):,} records from {filename.split('/')[-1]}")

                    elif file_type == 'MEASURE_DATA_2007':
                        records = parse_measure_2007(content, filename)
                        all_measures.extend(records)
                        if records:
                            print(f"    MEASURE_2007: {len(records):,} records")

                    elif file_type == 'MEASURE_STARS_2007':
                        records = parse_measure_2007(content, filename)
                        all_measures.extend(records)
                        if records:
                            print(f"    MEASURE_STARS_2007: {len(records):,} records")

                    elif file_type in ['SUMMARY', 'SUMMARY_LEGACY', 'SUMMARY_2020']:
                        records = parse_summary_rating(content, filename, year, part)
                        all_summary.extend(records)
                        if records:
                            print(f"    SUMMARY ({year}): {len(records):,} records")

                    elif file_type in ['DOMAIN', 'DOMAIN_LEGACY', 'DOMAIN_2020']:
                        records = parse_domain(content, filename, year, part)
                        all_domain.extend(records)
                        if records:
                            print(f"    DOMAIN ({year}): {len(records):,} records")

                    elif file_type == 'CUTPOINTS':
                        records = parse_cutpoints(content, filename, year, part)
                        all_cutpoints.extend(records)
                        if records:
                            print(f"    CUTPOINTS ({year}): {len(records):,} records")

                except Exception as e:
                    print(f"    ERROR processing {filename}: {str(e)[:100]}")
                    traceback.print_exc()

        except Exception as e:
            print(f"  ERROR loading {s3_key}: {str(e)[:100]}")

    # Upload results
    print("\n" + "=" * 80)
    print("UPLOADING RESULTS")
    print("=" * 80)

    def upload_parquet(df: pd.DataFrame, s3_key: str):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
        print(f"  Uploaded: {s3_key} ({len(df):,} rows)")

    if all_measures:
        measures_df = pd.DataFrame(all_measures)
        # Show year coverage
        years = sorted(measures_df['year'].unique())
        print(f"\nMeasures: {len(measures_df):,} total records")
        print(f"  Years: {min(years)} - {max(years)}")
        print(f"  Year counts: {measures_df.groupby('year').size().to_dict()}")
        upload_parquet(measures_df, 'processed/unified/measures_all_years.parquet')

    if all_summary:
        summary_df = pd.DataFrame(all_summary)
        years = sorted(summary_df['year'].unique())
        print(f"\nSummary: {len(summary_df):,} total records")
        print(f"  Years: {min(years)} - {max(years)}")
        upload_parquet(summary_df, 'processed/unified/summary_all_years.parquet')

    if all_domain:
        domain_df = pd.DataFrame(all_domain)
        years = sorted(domain_df['year'].unique())
        print(f"\nDomain: {len(domain_df):,} total records")
        print(f"  Years: {min(years)} - {max(years)}")
        upload_parquet(domain_df, 'processed/unified/domain_all_years.parquet')

    if all_cutpoints:
        cutpoints_df = pd.DataFrame(all_cutpoints)
        years = sorted(cutpoints_df['year'].unique())
        print(f"\nCutpoints: {len(cutpoints_df):,} total records")
        print(f"  Years: {min(years)} - {max(years)}")
        upload_parquet(cutpoints_df, 'processed/unified/cutpoints_all_years.parquet')

    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)

    return {
        'measures': len(all_measures),
        'summary': len(all_summary),
        'domain': len(all_domain),
        'cutpoints': len(all_cutpoints)
    }

if __name__ == '__main__':
    results = process_all_stars_data()
    print(f"\nFinal counts: {results}")
