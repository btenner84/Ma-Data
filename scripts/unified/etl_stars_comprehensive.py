#!/usr/bin/env python3
"""
Comprehensive Stars ETL - Full audit lineage from raw to unified.

Handles all CMS Stars file format variations from 2007-2026.
Produces:
- processed/stars/{year}/*.parquet (individual year files)
- processed/unified/stars_*.parquet (combined tables)

Each output includes lineage columns for full traceability.
"""

import os
import sys
import re
import json
import tempfile
import shutil
import zipfile
from io import BytesIO
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict, field

import boto3
import pandas as pd
import numpy as np

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from unified.audit_lineage import create_audit_logger, AuditLogger

S3_BUCKET = "ma-data123"
s3 = boto3.client('s3')


# =============================================================================
# YEAR-SPECIFIC CONFIGURATIONS
# =============================================================================

@dataclass
class YearConfig:
    """Configuration for parsing a specific year's stars data."""
    year: int
    raw_key: str                          # S3 key for raw zip file
    raw_keys: List[str] = field(default_factory=list)  # Multiple zip files (2024+)
    has_nested_zips: bool = False         # 2020+ have zips inside zips

    # File patterns to find specific data types
    summary_pattern: str = None           # Pattern for summary file
    domain_pattern: str = None            # Pattern for domain file
    cutpoints_c_pattern: str = None       # Pattern for Part C cutpoints
    cutpoints_d_pattern: str = None       # Pattern for Part D cutpoints
    measure_stars_pattern: str = None     # Pattern for measure stars
    measure_data_pattern: str = None      # Pattern for measure data/performance
    cai_pattern: str = None               # Pattern for CAI (2017+)
    display_measures_pattern: str = None  # Pattern for display measures

    # Parsing settings
    header_row: int = 0                   # Row containing headers (0-indexed)
    title_row_skip: bool = False          # If True, skip first row (title)
    encoding: str = 'latin-1'             # File encoding

    # Column mappings (source -> standard)
    contract_col: str = 'Contract Number'
    parent_org_col: str = 'Parent Organization'

    # Special handling flags
    part_c_prefix: str = 'Part_C'         # How Part C files are named
    part_d_prefix: str = 'Part_D'         # How Part D files are named

    # Additional metadata
    notes: str = ''


def get_year_config(year: int) -> YearConfig:
    """Get configuration for a specific year."""

    # Determine raw file key
    if year >= 2024:
        raw_key = f"raw/stars/{year}_display.zip"
    elif year >= 2010:
        raw_key = f"raw/stars/{year}_combined.zip"
    else:
        raw_key = f"raw/stars/{year}_ratings.zip"

    # Base config
    config = YearConfig(
        year=year,
        raw_key=raw_key,
    )

    # 2007 - earliest year, limited data
    if year == 2007:
        config.notes = "First year, limited measures"
        config.summary_pattern = None  # No summary in 2007
        config.domain_pattern = None
        config.measure_stars_pattern = r'performance_metrics'

    # 2008-2009 - Early format
    elif year in [2008, 2009]:
        config.summary_pattern = r'summary\.csv$'
        config.domain_pattern = r'domain\.csv$'
        config.cutpoints_c_pattern = r'Part_C.*cutpoints\.csv$'
        config.cutpoints_d_pattern = r'Part_D.*(?:cutpoints|threshold)\.csv$'
        config.measure_stars_pattern = r'stars?\.csv$'
        config.measure_data_pattern = r'_data\.csv$'
        config.title_row_skip = True
        config.notes = "Early format with Part_C/Part_D split"

    # 2010-2013 - Transitional format
    elif year in [2010, 2011, 2012, 2013]:
        config.summary_pattern = r'summary\.csv$'
        config.domain_pattern = r'domain\.csv$'
        config.cutpoints_c_pattern = r'Part_C.*cutpoints\.csv$'
        config.cutpoints_d_pattern = r'Part_D.*(?:cutpoints|threshold)\.csv$'
        config.measure_stars_pattern = r'stars?\.csv$'
        config.measure_data_pattern = r'_data\.csv$'
        config.display_measures_pattern = r'Display.*Measures.*Data.*\.csv$'
        config.title_row_skip = True
        config.notes = "Transitional format, title rows in CSVs"

    # 2014-2016 - Stable middle format
    elif year in [2014, 2015, 2016]:
        config.summary_pattern = r'summary\.csv$'
        config.domain_pattern = r'domain\.csv$'
        config.cutpoints_c_pattern = r'C_cutpoints\.csv$'
        config.cutpoints_d_pattern = r'D_cutpoints\.csv$'
        config.measure_stars_pattern = r'stars?\.csv$'
        config.measure_data_pattern = r'_data\.csv$'
        config.display_measures_pattern = r'Display_Measure_Output.*\.csv$'
        config.title_row_skip = True
        config.notes = "Middle format with Report_Card_Master_Table structure"

    # 2017-2018 - CAI introduced
    elif year in [2017, 2018]:
        config.summary_pattern = r'summary\.csv$'
        config.domain_pattern = r'domain\.csv$'
        config.cutpoints_c_pattern = r'C_cutpoints\.csv$'
        config.cutpoints_d_pattern = r'D_cutpoints\.csv$'
        config.measure_stars_pattern = r'stars?\.csv$'
        config.measure_data_pattern = r'_data\.csv$'
        config.display_measures_pattern = r'Display_Measures.*\.csv$'
        config.cai_pattern = r'(?:CAI|cai).*\.csv$'
        config.title_row_skip = True
        config.notes = "CAI introduced in 2017"

    # 2019 - Nested zips format (transition year)
    elif year == 2019:
        config.has_nested_zips = True
        config.summary_pattern = r'(?:summary|Summary).*\.csv$'
        config.domain_pattern = r'(?:domain|Domain).*\.csv$'
        config.cutpoints_c_pattern = r'(?:C_cutpoints|Part\s*C.*Cut).*\.csv$'
        config.cutpoints_d_pattern = r'(?:D_cutpoints|Part\s*D.*Cut).*\.csv$'
        config.measure_stars_pattern = r'(?:stars?|Stars|Measure.*Star).*\.csv$'
        config.measure_data_pattern = r'(?:_data|Data).*\.csv$'
        config.display_measures_pattern = r'Display.*Measures.*\.csv$'
        config.cai_pattern = r'(?:CAI|cai).*\.csv$'
        config.title_row_skip = True
        config.notes = "2019 transition year with nested zips"

    # 2020-2023 - Nested zips format
    elif year in [2020, 2021, 2022, 2023]:
        config.has_nested_zips = True
        # 2020+ uses "Data Table - Summary", "Data Table - Domain Stars" naming
        config.summary_pattern = r'(?:summary|Summary).*\.csv$'
        config.domain_pattern = r'(?:domain|Domain\s*Stars).*\.csv$'
        config.cutpoints_c_pattern = r'(?:C_cutpoints|Part\s*C.*Cut).*\.csv$'
        config.cutpoints_d_pattern = r'(?:D_cutpoints|Part\s*D.*Cut).*\.csv$'
        config.measure_stars_pattern = r'(?:Measure.*Star|stars|Stars).*\.csv$'
        config.measure_data_pattern = r'(?:Measure.*Data|_data).*\.csv$'
        config.display_measures_pattern = r'Display.*Measures.*\.csv$'
        config.cai_pattern = r'CAI.*\.csv$'
        config.title_row_skip = False  # Modern format has clean headers
        config.notes = "Nested zips, modern format"

    # 2024+ - Flat structure, new naming, separate display and ratings zips
    elif year >= 2024:
        config.has_nested_zips = False
        config.raw_keys = [
            f"raw/stars/{year}_ratings.zip",
            f"raw/stars/{year}_display.zip"
        ]
        config.summary_pattern = r'[Ss]ummary.*\.csv$'
        config.domain_pattern = r'[Dd]omain.*\.csv$'
        config.cutpoints_c_pattern = r'Part\s*C\s*Cut\s*Points.*\.csv$'
        config.cutpoints_d_pattern = r'Part\s*D\s*Cut\s*Points.*\.csv$'
        config.measure_stars_pattern = r'Measure\s*Star.*\.csv$'
        config.measure_data_pattern = r'Measure\s*Data.*\.csv$'
        config.display_measures_pattern = r'HPMS_Display_Measures.*\.csv$'
        config.cai_pattern = r'CAI.*\.csv$'
        config.title_row_skip = False
        config.notes = "Flat structure, separate display and ratings zips"

    return config


# =============================================================================
# PARSING UTILITIES
# =============================================================================

def find_header_row(content: str, max_rows: int = 10) -> int:
    """
    Find the row containing column headers.
    Returns 0-indexed row number.
    """
    lines = content.split('\n')[:max_rows]

    for idx, line in enumerate(lines):
        line_lower = line.lower()
        # Look for common header indicators
        if any(x in line_lower for x in ['contract number', 'contract id', 'organization type']):
            return idx

    return 0  # Default to first row


def detect_encoding(content_bytes: bytes) -> str:
    """Detect file encoding."""
    # Try UTF-8 BOM first
    if content_bytes[:3] == b'\xef\xbb\xbf':
        return 'utf-8-sig'
    # Try UTF-8 on full content (not just first 1000 bytes)
    try:
        content_bytes.decode('utf-8')
        return 'utf-8'
    except:
        pass
    # Default to latin-1 (handles all byte values)
    return 'latin-1'


def parse_star_rating(value) -> Optional[float]:
    """Parse star rating from various formats."""
    if pd.isna(value):
        return None

    val_str = str(value).strip().lower()

    # Skip non-rating values
    skip_patterns = [
        'not enough', 'too new', 'not applicable', 'n/a', 'plan too new',
        'not available', 'under review', 'not required',
        'not calculated', 'not reported'
    ]

    # Also skip if empty or just 'nan'
    if not val_str or val_str == 'nan':
        return None

    if any(p in val_str for p in skip_patterns):
        return None

    # "X out of 5 stars" format
    match = re.search(r'(\d+(?:\.\d+)?)\s*(?:out\s*of\s*5)?\s*star', val_str)
    if match:
        try:
            rating = float(match.group(1))
            if 1 <= rating <= 5:
                return rating
        except:
            pass

    # Direct numeric
    try:
        rating = float(val_str)
        if 1 <= rating <= 5:
            return rating
    except:
        pass

    return None


def normalize_column_name(col: str) -> str:
    """Normalize column name for consistency."""
    col = str(col).strip()
    # Remove common prefixes/suffixes
    col = re.sub(r'^\d{4}\s+', '', col)  # Remove year prefix
    col = re.sub(r'\s+$', '', col)
    return col


def find_contract_column(df: pd.DataFrame) -> str:
    """Find the contract ID column."""
    for col in df.columns:
        col_str = str(col).lower()
        if 'contract' in col_str and ('number' in col_str or 'id' in col_str or col_str == 'contract'):
            return col
    # Fallback: first column with H/R/E contract patterns
    for col in df.columns:
        sample = df[col].dropna().head(10).astype(str)
        if sample.str.match(r'^[HRE]\d{4}').any():
            return col
    return df.columns[0]


# =============================================================================
# FILE EXTRACTION AND LOADING
# =============================================================================

def extract_raw_zip(year: int, config: YearConfig) -> Tuple[Dict[str, bytes], str]:
    """
    Extract raw zip file(s), handling nested zips if needed.
    Returns dict of {filename: content_bytes} and temp directory path.
    """
    files = {}
    temp_dir = tempfile.mkdtemp()

    # Get list of zip files to process
    zip_keys = config.raw_keys if config.raw_keys else [config.raw_key]

    for zip_key in zip_keys:
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=zip_key)
            zip_bytes = BytesIO(response['Body'].read())
        except Exception as e:
            print(f"  WARNING: Cannot download {zip_key}: {e}")
            continue

        try:
            with zipfile.ZipFile(zip_bytes, 'r') as zf:
                zf.extractall(temp_dir)

                # Walk extracted files
                for root, dirs, filenames in os.walk(temp_dir):
                    for fname in filenames:
                        fpath = os.path.join(root, fname)

                        # Handle nested zips
                        if fname.endswith('.zip') and config.has_nested_zips:
                            try:
                                with zipfile.ZipFile(fpath, 'r') as inner_zf:
                                    for inner_name in inner_zf.namelist():
                                        if inner_name.endswith(('.csv', '.xlsx', '.xls')):
                                            files[inner_name] = inner_zf.read(inner_name)
                            except:
                                pass

                        # Regular files
                        elif fname.endswith(('.csv', '.xlsx', '.xls')):
                            with open(fpath, 'rb') as f:
                                files[fname] = f.read()
        except Exception as e:
            print(f"  WARNING: Cannot extract {zip_key}: {e}")

    if not files:
        print(f"  ERROR: No files extracted for {year}")

    return files, temp_dir


def load_csv_smart(content_bytes: bytes, config: YearConfig, filename: str) -> pd.DataFrame:
    """
    Load CSV with smart header detection and encoding handling.
    """
    encoding = detect_encoding(content_bytes)
    try:
        content_str = content_bytes.decode(encoding)
    except UnicodeDecodeError:
        # Fallback to latin-1 which handles all byte values
        encoding = 'latin-1'
        content_str = content_bytes.decode(encoding)

    # Find header row
    header_row = find_header_row(content_str)

    # If title row skip is needed and header is at row 0, skip row 0
    skiprows = header_row
    if config.title_row_skip and header_row == 0:
        # Check if first row looks like a title
        first_line = content_str.split('\n')[0].lower()
        if any(x in first_line for x in ['star', 'report card', 'medicare', 'domain', 'view']):
            skiprows = 1

    try:
        df = pd.read_csv(BytesIO(content_bytes), encoding=encoding, skiprows=skiprows)

        # Validate we got real columns - only retry if columns are COMPLETELY broken
        # (i.e., first few columns are unnamed or look like data values)
        first_cols = [str(c).lower() for c in df.columns[:5]]
        looks_broken = (
            all('unnamed' in c for c in first_cols) or
            any(c.startswith('h0') or c.startswith('r0') or c.startswith('e0') for c in first_cols)
        )

        if looks_broken:
            # Try different skiprows
            best_df = df
            for try_skip in [0, 1, 2, 3]:
                if try_skip != skiprows:
                    try_df = pd.read_csv(BytesIO(content_bytes), encoding=encoding, skiprows=try_skip)
                    try_first = [str(c).lower() for c in try_df.columns[:5]]
                    # Check if this looks better (has real column names)
                    if any('contract' in c or 'organization' in c for c in try_first):
                        best_df = try_df
                        break
            df = best_df

        return df
    except Exception as e:
        print(f"    WARNING: Failed to parse {filename}: {e}")
        return pd.DataFrame()


def find_file_by_pattern(files: Dict[str, bytes], pattern: str, prefer_part_c: bool = True) -> Optional[str]:
    """
    Find a file matching the pattern.
    If multiple matches, prefer Part C over Part D (for summary/domain).
    """
    if not pattern:
        return None

    matches = []
    regex = re.compile(pattern, re.IGNORECASE)

    for fname in files.keys():
        if regex.search(fname):
            matches.append(fname)

    if not matches:
        return None

    if len(matches) == 1:
        return matches[0]

    # Multiple matches - apply preference
    if prefer_part_c:
        # Prefer Part C
        part_c = [m for m in matches if 'part_c' in m.lower() or 'partc' in m.lower()]
        if part_c:
            return part_c[0]
        # Prefer non-Part D
        non_d = [m for m in matches if 'part_d' not in m.lower() and 'partd' not in m.lower()]
        if non_d:
            return non_d[0]

    # Return first match
    return matches[0]


# =============================================================================
# DATA PROCESSORS
# =============================================================================

def process_summary(
    files: Dict[str, bytes],
    config: YearConfig,
    audit: AuditLogger
) -> Optional[pd.DataFrame]:
    """Process summary file."""
    fname = find_file_by_pattern(files, config.summary_pattern, prefer_part_c=True)
    if not fname:
        return None

    df = load_csv_smart(files[fname], config, fname)
    if df.empty:
        return None

    # Register source and log
    file_id = audit.register_source_file(
        s3_key=f"{config.raw_key}#{fname}",
        file_type="stars_summary",
        year=config.year,
        df=df
    )
    audit.log_load(file_id, df, f"Loaded summary from {fname}")

    # Add metadata
    df['rating_year'] = config.year
    df['_source_file'] = fname

    # Standardize column names
    contract_col = find_contract_column(df)
    if contract_col != 'contract_id':
        df = df.rename(columns={contract_col: 'contract_id'})

    df['contract_id'] = df['contract_id'].astype(str).str.strip()

    # Add lineage
    df = audit.add_lineage_column(df, file_id, 'contract_id')

    return df


def process_domain(
    files: Dict[str, bytes],
    config: YearConfig,
    audit: AuditLogger
) -> Optional[pd.DataFrame]:
    """Process domain scores file."""
    fname = find_file_by_pattern(files, config.domain_pattern, prefer_part_c=True)
    if not fname:
        return None

    df = load_csv_smart(files[fname], config, fname)
    if df.empty:
        return None

    # Register source
    file_id = audit.register_source_file(
        s3_key=f"{config.raw_key}#{fname}",
        file_type="stars_domain",
        year=config.year,
        df=df
    )
    audit.log_load(file_id, df, f"Loaded domain from {fname}")

    # Find contract column
    contract_col = find_contract_column(df)

    # Find domain columns
    domain_data = []
    domain_cols = []

    for col in df.columns:
        col_str = str(col)
        col_lower = col_str.lower()

        # Modern format: HD1:, HD2:, DD1:, etc.
        if re.match(r'^[HD]D\d:', col_str):
            domain_id = col_str.split(':')[0]
            domain_cols.append((col, domain_id, col_str))

        # Older format: descriptive names
        elif any(x in col_lower for x in ['staying healthy', 'chronic', 'member experience',
                                           'complaints', 'customer service', 'getting care',
                                           'managing', 'health plan', 'drug plan', 'safety',
                                           'responsiveness', 'appeals']):
            # Map to domain ID
            if 'staying healthy' in col_lower or 'screenings' in col_lower:
                domain_id = 'HD1'
            elif 'chronic' in col_lower or 'managing' in col_lower:
                domain_id = 'HD2'
            elif 'responsiveness' in col_lower or 'getting care' in col_lower:
                domain_id = 'HD3'
            elif ('experience' in col_lower or 'rating' in col_lower) and 'health' in col_lower:
                domain_id = 'HD4'
            elif 'complaints' in col_lower and ('health' in col_lower or 'appeals' in col_lower):
                domain_id = 'HD5'
            elif 'customer service' in col_lower and 'health' in col_lower:
                domain_id = 'HD6'
            elif 'customer service' in col_lower and 'drug' in col_lower:
                domain_id = 'DD1'
            elif 'complaints' in col_lower and 'drug' in col_lower:
                domain_id = 'DD2'
            elif 'experience' in col_lower and 'drug' in col_lower:
                domain_id = 'DD3'
            elif 'safety' in col_lower or 'pricing' in col_lower:
                domain_id = 'DD4'
            else:
                domain_id = f'D{len(domain_cols)+1}'

            domain_cols.append((col, domain_id, col_str))

    # Extract domain scores
    for _, row in df.iterrows():
        contract_id = str(row[contract_col]).strip()
        if not re.match(r'^[HRE]\d{4}', contract_id):
            continue

        for col, domain_id, domain_name in domain_cols:
            rating = parse_star_rating(row[col])
            if rating is not None:
                domain_data.append({
                    'year': config.year,
                    'contract_id': contract_id,
                    'domain_id': domain_id,
                    'domain_name': domain_name,
                    'star_rating': rating,
                    '_source_file': fname
                })

    if not domain_data:
        return None

    result_df = pd.DataFrame(domain_data)
    result_df = audit.add_lineage_column(result_df, file_id, 'contract_id')

    audit.log_derive(
        file_id, 'domain_scores',
        'Extracted domain scores from columns',
        result_df,
        f"Extracted {len(result_df)} domain scores"
    )

    return result_df


def process_cutpoints(
    files: Dict[str, bytes],
    config: YearConfig,
    audit: AuditLogger
) -> Optional[pd.DataFrame]:
    """Process cut points files (Part C and Part D)."""
    all_cutpoints = []

    for cut_type, pattern in [('part_c', config.cutpoints_c_pattern),
                               ('part_d', config.cutpoints_d_pattern)]:
        fname = find_file_by_pattern(files, pattern, prefer_part_c=False)
        if not fname:
            continue

        df = load_csv_smart(files[fname], config, fname)
        if df.empty:
            continue

        file_id = audit.register_source_file(
            s3_key=f"{config.raw_key}#{fname}",
            file_type=f"stars_cutpoints_{cut_type}",
            year=config.year,
            df=df
        )
        audit.log_load(file_id, df, f"Loaded {cut_type} cutpoints from {fname}")

        # Parse cutpoints - handle different formats
        cutpoints_found = 0

        # Strategy 1: Modern format with "1star", "2star" rows
        star_col = None
        for col in df.columns:
            sample = df[col].astype(str).str.lower()
            if sample.str.contains('1star|2star|1 star|2 star', regex=True).any():
                star_col = col
                break

        if star_col:
            # Find measure columns
            measure_cols = []
            for col in df.columns:
                col_str = str(col)
                if re.match(r'^[CD]\d{2}:', col_str):
                    measure_cols.append((col, col_str.split(':')[0]))

            for _, row in df.iterrows():
                star_val = str(row[star_col]).lower().strip()
                star_match = re.search(r'(\d)\s*star', star_val)
                if star_match:
                    star_level = int(star_match.group(1))
                    for col, measure_id in measure_cols:
                        cut_value = row[col]
                        if pd.notna(cut_value) and str(cut_value).strip():
                            all_cutpoints.append({
                                'year': config.year,
                                'cut_type': cut_type,
                                'measure_id': measure_id,
                                'star_level': star_level,
                                'threshold_text': str(cut_value).strip(),
                                '_source_file': fname
                            })
                            cutpoints_found += 1

        # Strategy 2: Older format with star levels in first column
        else:
            star_rows = {}
            for idx, row in df.iterrows():
                first_val = str(row.iloc[0]).strip()
                if first_val in ['5', '4', '3', '2', '1']:
                    star_rows[int(first_val)] = idx

            if star_rows:
                for col_idx, col in enumerate(df.columns[1:], 1):
                    col_str = str(col)
                    if 'unnamed' in col_str.lower():
                        continue

                    measure_id = f"{'C' if cut_type == 'part_c' else 'D'}{col_idx:02d}"

                    for star_level, row_idx in star_rows.items():
                        row = df.iloc[row_idx]
                        if col_idx < len(row):
                            cut_value = row.iloc[col_idx]
                            if pd.notna(cut_value) and str(cut_value).strip():
                                all_cutpoints.append({
                                    'year': config.year,
                                    'cut_type': cut_type,
                                    'measure_id': measure_id,
                                    'star_level': star_level,
                                    'threshold_text': str(cut_value).strip(),
                                    '_source_file': fname
                                })
                                cutpoints_found += 1

        print(f"    {cut_type}: {cutpoints_found} cutpoints")

    if not all_cutpoints:
        return None

    result_df = pd.DataFrame(all_cutpoints)
    return result_df


def load_csv_with_measure_headers(content_bytes: bytes, config: YearConfig, filename: str) -> pd.DataFrame:
    """
    Load CSV specifically for measure files, handling multi-level headers.

    2014+ files have a complex structure:
    - Row 0: Title ("2014 Star View...")
    - Row 1: Contract cols (0-4) + category headers (HD1:, HD2:) for cols 5+
    - Row 2: Blank (0-4) + measure IDs (C01:, C02:, D01:) for cols 5+
    - Row 3: Date ranges
    - Row 4+: Data

    We need to merge row 1 (contract columns) with row 2 (measure columns).
    """
    encoding = detect_encoding(content_bytes)
    try:
        content_str = content_bytes.decode(encoding)
    except UnicodeDecodeError:
        encoding = 'latin-1'
        content_str = content_bytes.decode(encoding)

    # First try: simple single-header loading (works for modern formats)
    for skiprows in [1, 0]:
        try:
            df = pd.read_csv(BytesIO(content_bytes), encoding=encoding, skiprows=skiprows)
            measure_count = sum(1 for col in df.columns if re.match(r'^[CD]\d{2}:', str(col)))
            has_contract = any('contract' in str(col).lower() for col in df.columns)

            if measure_count > 0 and has_contract:
                return df
        except Exception:
            continue

    # Second try: multi-level header merging for 2014+ format
    try:
        # Read without headers to get raw rows
        df_raw = pd.read_csv(BytesIO(content_bytes), encoding=encoding, header=None)

        # Find the header rows
        # Contract row: look for column headers like "Contract Number", "CONTRACT_ID"
        # Measure row: look for measure IDs like "C01:", "D01:"
        contract_row = None
        measure_row = None

        for idx in range(min(5, len(df_raw))):
            row_vals = df_raw.iloc[idx].astype(str).str.lower()
            # Contract header should be in first few columns specifically as "contract number" or "contract_id"
            first_vals = row_vals.iloc[:3]
            if first_vals.str.contains(r'contract\s*(?:number|id|_id)?$', regex=True).any():
                contract_row = idx
            if row_vals.str.match(r'^[cd]\d{2}:').any():
                measure_row = idx

        if contract_row is not None and measure_row is not None and measure_row > contract_row:
            # Merge headers: use contract row for first columns, measure row for the rest
            header1 = df_raw.iloc[contract_row].astype(str)
            header2 = df_raw.iloc[measure_row].astype(str)

            # Build merged headers
            merged_headers = []
            for i, (h1, h2) in enumerate(zip(header1, header2)):
                # Use h1 if it's a real name (not blank/nan), otherwise use h2
                if h1 and h1.lower() not in ['', 'nan', 'unnamed']:
                    merged_headers.append(h1)
                elif h2 and h2.lower() not in ['', 'nan', 'unnamed']:
                    merged_headers.append(h2)
                else:
                    merged_headers.append(f'Unnamed: {i}')

            # Find where data starts (after date ranges row typically)
            data_start = measure_row + 1
            # Look for actual data (starts with H/R/E contract ID)
            for idx in range(measure_row + 1, min(measure_row + 5, len(df_raw))):
                first_val = str(df_raw.iloc[idx, 0])
                if re.match(r'^[HRE]\d{4}', first_val):
                    data_start = idx
                    break

            # Create final dataframe
            df_data = df_raw.iloc[data_start:].copy()
            df_data.columns = merged_headers
            df_data = df_data.reset_index(drop=True)

            # Verify we have what we need
            measure_count = sum(1 for col in df_data.columns if re.match(r'^[CD]\d{2}:', str(col)))
            has_contract = any('contract' in str(col).lower() for col in df_data.columns)

            if measure_count > 0 and has_contract:
                return df_data

    except Exception as e:
        pass

    # Fallback to standard loading
    return load_csv_smart(content_bytes, config, filename)


def process_measure_stars(
    files: Dict[str, bytes],
    config: YearConfig,
    audit: AuditLogger
) -> Optional[pd.DataFrame]:
    """Process measure-level star ratings."""
    # Try multiple patterns
    patterns = [config.measure_stars_pattern, config.measure_data_pattern, config.display_measures_pattern]
    patterns = [p for p in patterns if p]

    all_measures = []

    for pattern in patterns:
        for fname, content in files.items():
            if not re.search(pattern, fname, re.IGNORECASE):
                continue

            # Use specialized loader for measure files (handles multi-level headers)
            df = load_csv_with_measure_headers(content, config, fname)
            if df.empty:
                continue

            file_id = audit.register_source_file(
                s3_key=f"{config.raw_key}#{fname}",
                file_type="stars_measures",
                year=config.year,
                df=df
            )
            audit.log_load(file_id, df, f"Loaded measures from {fname}")

            contract_col = find_contract_column(df)

            # Find measure columns
            measure_cols = []
            for col in df.columns:
                col_str = str(col)
                # Direct measure ID in column name (C01:, C02:, D01:, D02:, etc.)
                if re.match(r'^[CD]\d{2}:', col_str):
                    measure_id = col_str.split(':')[0]
                    measure_cols.append((col, measure_id))

            # If no explicit measure columns, look for star rating columns
            if not measure_cols:
                for col in df.columns:
                    col_lower = str(col).lower()
                    if any(x in col_lower for x in ['contract', 'organization', 'parent', 'type', 'name']):
                        continue
                    # Check if it's a rating column
                    try:
                        vals = df[col].dropna().astype(str)
                        if vals.str.contains(r'\d\s*(?:out\s*of\s*5)?\s*star', case=False, regex=True).any():
                            measure_cols.append((col, str(col)[:30]))
                    except:
                        pass

            # Extract measures
            for _, row in df.iterrows():
                contract_id = str(row[contract_col]).strip()
                if not re.match(r'^[HRE]\d{4}', contract_id):
                    continue

                for col, measure_id in measure_cols:
                    value = row[col]
                    rating = parse_star_rating(value)

                    all_measures.append({
                        'year': config.year,
                        'contract_id': contract_id,
                        'measure_id': measure_id,
                        'star_rating': rating,
                        'raw_value': str(value) if pd.notna(value) else None,
                        '_source_file': fname
                    })

    if not all_measures:
        return None

    result_df = pd.DataFrame(all_measures)
    # Remove rows with no useful data
    result_df = result_df[result_df['star_rating'].notna() | result_df['raw_value'].notna()]
    result_df = result_df.drop_duplicates()

    return result_df


def process_cai(
    files: Dict[str, bytes],
    config: YearConfig,
    audit: AuditLogger
) -> Optional[pd.DataFrame]:
    """Process CAI (Categorical Adjustment Index) data."""
    if config.year < 2017:
        return None  # CAI didn't exist before 2017

    fname = find_file_by_pattern(files, config.cai_pattern, prefer_part_c=False)
    if not fname:
        return None

    df = load_csv_smart(files[fname], config, fname)
    if df.empty:
        return None

    file_id = audit.register_source_file(
        s3_key=f"{config.raw_key}#{fname}",
        file_type="stars_cai",
        year=config.year,
        df=df
    )
    audit.log_load(file_id, df, f"Loaded CAI from {fname}")

    contract_col = find_contract_column(df)

    # Find FAC (factor) columns
    fac_cols = [col for col in df.columns if 'fac' in str(col).lower()]

    cai_data = []
    for _, row in df.iterrows():
        contract_id = str(row[contract_col]).strip()
        if not re.match(r'^[HRE]\d{4}', contract_id):
            continue

        record = {
            'year': config.year,
            'contract_id': contract_id,
            '_source_file': fname
        }

        for col in fac_cols:
            col_name = str(col).lower().replace(' ', '_')
            try:
                record[col_name] = float(row[col])
            except:
                record[col_name] = None

        cai_data.append(record)

    if not cai_data:
        return None

    result_df = pd.DataFrame(cai_data)
    result_df = audit.add_lineage_column(result_df, file_id, 'contract_id')

    return result_df


# =============================================================================
# MAIN ETL FUNCTIONS
# =============================================================================

def process_year(year: int, audit: AuditLogger, test_mode: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Process all stars data for a single year.
    Returns dict of {table_name: dataframe}.
    """
    print(f"\n{'='*60}")
    print(f"PROCESSING YEAR {year}")
    print(f"{'='*60}")

    config = get_year_config(year)
    print(f"  Config: {config.notes}")
    print(f"  Raw file: {config.raw_key}")

    # Extract files
    files, temp_dir = extract_raw_zip(year, config)
    if not files:
        print(f"  ERROR: No files extracted for {year}")
        return {}

    print(f"  Extracted {len(files)} files")

    results = {}

    try:
        # Process each data type
        summary = process_summary(files, config, audit)
        if summary is not None:
            results['summary'] = summary
            print(f"  Summary: {len(summary)} contracts")

        domain = process_domain(files, config, audit)
        if domain is not None:
            results['domain'] = domain
            print(f"  Domain: {len(domain)} scores")

        cutpoints = process_cutpoints(files, config, audit)
        if cutpoints is not None:
            results['cutpoints'] = cutpoints
            print(f"  Cutpoints: {len(cutpoints)} records")

        measures = process_measure_stars(files, config, audit)
        if measures is not None:
            results['measures'] = measures
            print(f"  Measures: {len(measures)} records")

        cai = process_cai(files, config, audit)
        if cai is not None:
            results['cai'] = cai
            print(f"  CAI: {len(cai)} contracts")

    finally:
        # Cleanup
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)

    return results


def upload_parquet(df: pd.DataFrame, s3_key: str):
    """Upload dataframe to S3 as parquet."""
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"    Uploaded: {s3_key} ({len(df):,} rows)")


def run_etl(
    years: List[int] = None,
    test_mode: bool = False,
    output_prefix: str = "processed"
) -> Dict[str, Any]:
    """
    Run the full stars ETL.

    Args:
        years: List of years to process (default: all available)
        test_mode: If True, write to test/ prefix instead of processed/
        output_prefix: S3 prefix for output (default: "processed", use "test" for testing)
    """
    if years is None:
        years = list(range(2007, 2027))

    if test_mode:
        output_prefix = "test"

    # Initialize audit logger
    audit = create_audit_logger("etl_stars_comprehensive")

    # Collect results by table type
    all_summaries = []
    all_domains = []
    all_cutpoints = []
    all_measures = []
    all_cai = []

    for year in years:
        results = process_year(year, audit, test_mode)

        # Save individual year files
        for table_name, df in results.items():
            s3_key = f"{output_prefix}/stars/{year}/{table_name}.parquet"
            upload_parquet(df, s3_key)

        # Accumulate for unified tables
        if 'summary' in results:
            all_summaries.append(results['summary'])
        if 'domain' in results:
            all_domains.append(results['domain'])
        if 'cutpoints' in results:
            all_cutpoints.append(results['cutpoints'])
        if 'measures' in results:
            all_measures.append(results['measures'])
        if 'cai' in results:
            all_cai.append(results['cai'])

    # Build unified tables
    print(f"\n{'='*60}")
    print("BUILDING UNIFIED TABLES")
    print(f"{'='*60}")

    unified_stats = {}

    if all_summaries:
        unified = pd.concat(all_summaries, ignore_index=True)
        upload_parquet(unified, f"{output_prefix}/unified/stars_summary.parquet")
        unified_stats['summary'] = {'rows': len(unified), 'years': sorted(unified['rating_year'].unique().tolist())}

    if all_domains:
        unified = pd.concat(all_domains, ignore_index=True).drop_duplicates()
        upload_parquet(unified, f"{output_prefix}/unified/domain_scores_all_years.parquet")
        unified_stats['domain'] = {'rows': len(unified), 'years': sorted(unified['year'].unique().tolist())}

    if all_cutpoints:
        unified = pd.concat(all_cutpoints, ignore_index=True).drop_duplicates()
        upload_parquet(unified, f"{output_prefix}/unified/cut_points_all_years.parquet")
        unified_stats['cutpoints'] = {'rows': len(unified), 'years': sorted(unified['year'].unique().tolist())}

    if all_measures:
        unified = pd.concat(all_measures, ignore_index=True).drop_duplicates()
        upload_parquet(unified, f"{output_prefix}/unified/measure_performance_all_years.parquet")
        unified_stats['measures'] = {'rows': len(unified), 'years': sorted(unified['year'].unique().tolist())}

    if all_cai:
        unified = pd.concat(all_cai, ignore_index=True).drop_duplicates()
        upload_parquet(unified, f"{output_prefix}/unified/cai_all_years.parquet")
        unified_stats['cai'] = {'rows': len(unified), 'years': sorted(unified['year'].unique().tolist())}

    # Finish audit
    try:
        audit.finish_run(
            success=True,
            output_tables=list(unified_stats.keys()),
            output_row_count=sum(s['rows'] for s in unified_stats.values())
        )
    except Exception as e:
        print(f"  WARNING: Audit save failed: {e}")

    # Print summary
    print(f"\n{'='*60}")
    print("ETL COMPLETE")
    print(f"{'='*60}")
    print(f"Output prefix: {output_prefix}/")
    print(f"Audit run ID: {audit.run_id}")
    print("\nUnified table coverage:")
    for table, stats in unified_stats.items():
        years = stats['years']
        print(f"  {table}: {stats['rows']:,} rows, years {min(years)}-{max(years)} ({len(years)} years)")

    return {
        'audit_run_id': audit.run_id,
        'unified_stats': unified_stats,
        'output_prefix': output_prefix
    }


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Comprehensive Stars ETL')
    parser.add_argument('--years', type=str, help='Years to process (e.g., "2020-2024" or "2020,2021,2022")')
    parser.add_argument('--test', action='store_true', help='Run in test mode (writes to test/ prefix)')
    parser.add_argument('--single-year', type=int, help='Process a single year')

    args = parser.parse_args()

    # Parse years
    years = None
    if args.single_year:
        years = [args.single_year]
    elif args.years:
        if '-' in args.years:
            start, end = args.years.split('-')
            years = list(range(int(start), int(end) + 1))
        elif ',' in args.years:
            years = [int(y.strip()) for y in args.years.split(',')]
        else:
            years = [int(args.years)]

    # Run ETL
    run_etl(years=years, test_mode=args.test)
