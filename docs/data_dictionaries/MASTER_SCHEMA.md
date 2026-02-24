# MA Data Platform - Master Schema Documentation

## Overview

This document provides comprehensive schema documentation for all Medicare Advantage data sources in the platform. Each data type includes column definitions, data types, join keys, and evolution notes.

---

## Key Join Fields (Universal)

| Field | Format | Description | Used In |
|-------|--------|-------------|---------|
| Contract Number | H####, R####, S####, E#### | Unique contract identifier | All datasets |
| Plan ID | 001-999 | Plan within contract | Enrollment, CPSC, SNP, Stars |
| FIPS County Code | 5-digit | County identifier | CPSC, Geographic analysis |
| Parent Organization | Text | Ultimate parent company | CPSC Contract Info |
| Year | YYYY | Reporting year | All datasets |
| Month | MM | Reporting month (01-12) | Monthly datasets |

---

## 1. CPSC (County-Plan-State-Contract) Enrollment

**Source**: `s3://ma-data123/raw/enrollment/cpsc/`
**Frequency**: Monthly
**Years Available**: 2013-2026 (158 files)
**Format**: ZIP containing 2 CSVs

### File Structure
```
CPSC_Enrollment_YYYY_MM/
├── CPSC_Contract_Info_YYYY_MM.csv    # Contract/Plan metadata
├── CPSC_Enrollment_Info_YYYY_MM.csv  # County-level enrollment
└── Read_Me_CPSC_Enrollment_YYYY.txt  # Documentation
```

### CPSC_Contract_Info Schema

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| Contract ID | String | Contract number (H0028, etc.) | Primary key with Plan ID |
| Plan ID | String | Plan identifier (001, 002, etc.) | Empty for contract-level rows |
| Organization Type | String | Type code | See Organization Types below |
| Plan Type | String | Product type (HMO, PPO, etc.) | |
| Offers Part D | String | Yes/No | Part D prescription coverage |
| SNP Plan | String | Yes/No | Special Needs Plan flag |
| EGHP | String | Yes/No | Employer Group Health Plan |
| Organization Name | String | Legal entity name | |
| Organization Marketing Name | String | Consumer-facing name | |
| Plan Name | String | Specific plan name | |
| **Parent Organization** | String | Ultimate parent company | KEY FIELD for aggregation |
| Contract Effective Date | Date | MM/DD/YYYY | When contract started |

### CPSC_Enrollment_Info Schema

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| Contract Number | String | Contract ID | Join to Contract_Info |
| Plan ID | String | Plan identifier | |
| SSA State County Code | String | SSA geographic code | Legacy code system |
| FIPS State County Code | String | FIPS code (5 digit) | Standard geo code |
| State | String | State name | |
| County | String | County name | |
| Enrollment | Integer/String | Member count | "*" = suppressed (1-10 members) |

### Schema Stability
- **2013-2026**: Schema has remained stable
- Column names and order unchanged across all years

---

## 2. Enrollment by Contract

**Source**: `s3://ma-data123/raw/enrollment/by_contract/`
**Frequency**: Monthly
**Years Available**: 2007-2026 (230 files)

### Schema

| Column | Type | Description |
|--------|------|-------------|
| Contract Number | String | Contract ID |
| Contract Name | String | Organization name |
| Plan Type | String | MA, MAPD, PDP, etc. |
| Total Enrollment | Integer | Total members |
| MA Enrollment | Integer | MA-only members |
| MA-PD Enrollment | Integer | MA with Part D |
| PDP Enrollment | Integer | Part D only |
| (varies by year) | | Additional columns added over time |

### Schema Evolution
- **2007-2015**: Basic columns (Contract, Name, Type, Enrollment)
- **2016+**: Added more granular breakdowns
- **2020+**: Added COVID-related fields

---

## 3. Enrollment by Plan

**Source**: `s3://ma-data123/raw/enrollment/by_plan/`
**Frequency**: Monthly
**Years Available**: 2007-2026 (230 files)

### Schema
Similar to Enrollment by Contract but at plan level (Contract + Plan ID)

---

## 4. SNP (Special Needs Plans)

**Source**: `s3://ma-data123/raw/snp/`
**Frequency**: Monthly
**Years Available**: 2007-2026 (219 files)
**Format**: ZIP containing XLSX

### Primary Data Sheet: SNP_REPORT_PART_17

| Column | Type | Description |
|--------|------|-------------|
| Contract Number | String | Contract ID |
| Contract Name | String | Organization name |
| Organization Type | String | Local CCP, Regional CCP, etc. |
| Plan ID | String | Plan identifier |
| SEGMENT_ID | String | Segment within plan |
| Plan Name | String | Marketing name |
| Plan Type | String | HMO, PPO, PFFS, etc. |
| Geographic Name | String | Service area description |
| State(s) | String | States served |
| Enrollment | Integer | Member count |
| Special Needs Plan Type | String | C-SNP, D-SNP, I-SNP |
| Specialty Diseases | String | For C-SNP: condition focus |
| Integration Status | String | CO, FIDE, HIDE (D-SNP only) |
| Applicable Integrated Plan | String | Yes/No |
| Partial Dual | String | Yes/No (added 2025) |
| DSNP Only Contract | String | Yes/No (added 2025) |

### SNP Types
- **C-SNP**: Chronic Condition (diabetes, heart, etc.)
- **D-SNP**: Dual Eligible (Medicare + Medicaid)
- **I-SNP**: Institutional (nursing facility residents)

### Integration Status (D-SNP only)
- **CO**: Coordination-Only
- **FIDE**: Fully Integrated Dual Eligible
- **HIDE**: Highly Integrated Dual Eligible

### Schema Evolution
- **2022**: Added D-SNP summary table
- **2025**: Added Partial Dual and DSNP Only Contract columns

---

## 5. Star Ratings

**Source**: `s3://ma-data123/raw/stars/`
**Frequency**: Yearly (October release)
**Years Available**: 2007-2026 (27 files)

### File Types by Era

#### 2024-2026: Separate Files
- `YYYY_ratings.zip` - Star ratings data tables
- `YYYY_display.zip` - Display measures

#### 2019-2023: Combined Files
- `YYYY_combined.zip` - Ratings + Display in one file

#### 2010-2018: Legacy Format
- Part C and Part D rated separately
- Different file naming conventions

#### 2007-2009: Original Format
- Basic plan ratings only

### Current Schema (2024+) - Summary Ratings

| Column | Type | Description |
|--------|------|-------------|
| Contract Number | String | Contract ID |
| Organization Type | String | Type code |
| Contract Name | String | Organization name |
| Organization Marketing Name | String | Consumer name |
| Parent Organization | String | Ultimate parent |
| SNP | String | Yes/No |
| YYYY-2 Disaster % | Float | Prior year disaster adjustment |
| YYYY-1 Disaster % | Float | Current year disaster adjustment |
| YYYY Part C Summary | Float | Part C star rating (1-5) |
| YYYY Part D Summary | Float | Part D star rating (1-5) |
| YYYY Overall | Float | Overall star rating (1-5) |

### Additional Star Rating Tables

1. **Measure Data**: Individual measure scores
2. **Measure Stars**: Star rating per measure
3. **Domain Stars**: Domain-level ratings
4. **Cut Points**: Thresholds for star levels
5. **CAI**: Categorical Adjustment Index
6. **High/Low Performing**: Contract classifications
7. **Disenrollment Reasons**: Member turnover analysis

### Schema Evolution
- **2010**: Part C and Part D rated separately
- **2012**: Overall rating introduced
- **2015**: Display measures added
- **2019**: Combined file format
- **2021**: Disaster adjustments added (COVID)
- **2024**: Separate ratings/display files again

---

## 6. HEDIS

**Source**: `s3://ma-data123/raw/hedis/`
**Frequency**: Yearly
**Years Available**: 2010-2026 (17 files)

Contains HEDIS measure specifications and technical documentation (not actual performance data).

---

## 7. Ratebooks (County Benchmarks)

**Source**: `s3://ma-data123/raw/rates/ratebook/`
**Frequency**: Yearly (April release)
**Years Available**: 2016-2026 (11 files)

### Schema

| Column | Type | Description |
|--------|------|-------------|
| State | String | State code |
| County | String | County name |
| FIPS | String | FIPS county code |
| Aged Benchmark | Decimal | Aged beneficiary rate |
| Disabled Benchmark | Decimal | Disabled beneficiary rate |
| ESRD Benchmark | Decimal | ESRD beneficiary rate |
| (varies) | | Additional rate components |

---

## 8. Risk Adjustment Model Software

**Source**: `s3://ma-data123/raw/risk_adjustment/model_software/`
**Frequency**: Yearly
**Years Available**: 2016-2026 (11 files)

Contains HCC (Hierarchical Condition Category) model documentation and software.

---

## 9. Plan Crosswalks

**Source**: `s3://ma-data123/raw/crosswalks/`
**Frequency**: Yearly
**Years Available**: 2022-2026 (5 files)

### Schema

| Column | Type | Description |
|--------|------|-------------|
| Old Contract | String | Previous year contract |
| Old Plan | String | Previous year plan |
| New Contract | String | Current year contract |
| New Plan | String | Current year plan |
| Crosswalk Type | String | Renewal, Consolidation, etc. |

**Note**: Pre-2022 crosswalks do not exist in CMS public data.

---

## Organization Types

| Code | Description |
|------|-------------|
| Local CCP | Local Coordinated Care Plan |
| Regional CCP | Regional Coordinated Care Plan |
| MSA | Medical Savings Account |
| PFFS | Private Fee-for-Service |
| National PACE | PACE Program |
| HCPP - 1833 Cost | Cost Plan |
| Employer/Union Direct Contract | EGHP |
| PDP | Prescription Drug Plan |

---

## Plan Types

| Code | Description |
|------|-------------|
| HMO | Health Maintenance Organization |
| HMO-POS | HMO with Point of Service |
| PPO | Preferred Provider Organization |
| LPPO | Local PPO |
| RPPO | Regional PPO |
| PFFS | Private Fee-for-Service |
| MSA | Medical Savings Account |
| HCPP | Health Care Prepayment Plan |

---

## Data Quality Notes

1. **Suppressed Values**: Enrollment counts 1-10 shown as "*" for privacy
2. **Null Handling**: Missing data may be blank, NULL, or "Not Applicable"
3. **Date Formats**: Mix of MM/DD/YYYY and YYYY-MM-DD
4. **Character Encoding**: Some files have BOM markers (UTF-8 with BOM)

---

## Recommended Join Strategy

```
CPSC_Contract_Info (base)
  → Join to CPSC_Enrollment_Info on (Contract ID, Plan ID)
  → Join to Stars on (Contract Number)
  → Join to SNP on (Contract Number, Plan ID)
  → Aggregate by Parent Organization for company-level analysis
  → Aggregate by FIPS for geographic analysis
```
