# CMS Enrollment Data Sources - Complete Guide

## The Three File Types

### 1. CPSC Files (What We Use - County Level)
**Path:** `raw/enrollment/cpsc/`
**Years:** 2013-2026
**Coverage:** ALL 12 months (raw files are complete!)

**Contains 3 files per month:**
```
cpsc_enrollment_YYYY_MM.zip/
├── CPSC_Contract_Info_YYYY_MM.csv     (metadata)
├── CPSC_Enrollment_Info_YYYY_MM.csv   (enrollment - THE BIG FILE)
└── Read_Me_CPSC_Enrollment_YYYY.txt   (documentation)
```

**Contract_Info columns:**
- Contract ID, Plan ID
- Organization Type, Plan Type
- Offers Part D, SNP Plan, EGHP
- Organization Name, Marketing Name, Plan Name
- Parent Organization
- Contract Effective Date

**Enrollment_Info columns:**
- Contract Number, Plan ID
- SSA State County Code, FIPS State County Code
- State, County
- Enrollment

**Use case:** Geographic analysis, full detail

---

### 2. By_Contract Files  
**Path:** `raw/enrollment/by_contract/`
**Years:** 2007-2026
**Status:** ✓ Valid ZIPs

**Contains:**
- Contract-level enrollment totals (aggregated across plans)
- Simpler structure for national-level queries

---

### 3. By_Plan Files (National Level - Exact Counts)
**Path:** `raw/enrollment/by_plan/`
**Years:** 2007-2026
**Status:** ✓ Valid ZIPs

**CSV Columns:**
- Contract Number, Plan ID
- Organization Type, Plan Type  
- Offers Part D
- Organization Name, Organization Marketing Name, Plan Name
- Parent Organization
- Contract Effective Date
- Enrollment (exact count, no suppression)

**Use case:** National totals, plan-level detail, no geography

---

## Current Processing Pipeline

### What We Have Processed
```
Source: CPSC files (2013-2026)
    ↓
Processed: processed/fact_enrollment/YYYY/MM/data.parquet
    ↓
Unified: processed/unified/fact_enrollment_all_years.parquet
```

### Processed File Gaps (need to fix)
| Year | Processed | Missing Months |
|------|-----------|----------------|
| 2013 | 1 month | 02-12 |
| 2014 | 10 months | 07, 09 |
| 2015 | 11 months | 09 |
| 2016 | 6 months | 03, 06-09, 12 |
| 2017 | 1 month | 02-12 |
| 2018 | 12 months | ✓ Complete |
| 2019 | 12 months | ✓ Complete |
| 2020 | 12 months | ✓ Complete |
| 2021 | 12 months | ✓ Complete |
| 2022 | 12 months | ✓ Complete |
| 2023 | 11 months | 07 |
| 2024 | 12 months | ✓ Complete |
| 2025 | 11 months | 03 |
| 2026 | 2 months | (current) |

**Root Cause:** Processing script failed on some files silently

---

## Recommended Architecture

### For National/Annual Views (No Geography)
Use aggregated table WITHOUT county detail:
```sql
-- Pre-aggregated by: year, month, contract, parent_org, plan_type, product_type, snp_type
SELECT * FROM fact_enrollment_all_years
WHERE year = 2026 AND month = 1
```

### For Geographic Views (State/County)
Use geographic table WITH county detail:
```sql
-- Has: year, month, state, county, contract, parent_org, plan_type, product_type
SELECT * FROM fact_enrollment_by_geography
WHERE year = 2026 AND state = 'CA'
```

### Table Relationships
```
┌─────────────────────────────────────────────────────────┐
│ fact_enrollment_all_years                               │
│ (National view - no county, faster queries)             │
│ Columns: year, month, contract_id, parent_org,         │
│          plan_type, product_type, snp_type, enrollment  │
│ Years: 2013-2026                                        │
│ Rows: ~3M                                               │
└─────────────────────────────────────────────────────────┘
                    ↑
                    │ Same source, different aggregation
                    ↓
┌─────────────────────────────────────────────────────────┐
│ fact_enrollment_by_geography                            │
│ (Geographic view - has state/county)                    │
│ Columns: year, month, state, county, contract_id,      │
│          parent_org, plan_type, product_type, enrollment│
│ Years: 2014-2026                                        │
│ Rows: ~5M                                               │
└─────────────────────────────────────────────────────────┘
```

---

## Action Items

### Priority 1: Fix Processing Gaps
Re-process missing months from raw CPSC files:
- 2013: 11 months missing
- 2016: 6 months missing  
- 2017: 11 months missing

### Priority 2: Build Silver Layer from By_Plan
The by_plan files are VALID and ready to process:
- Years: 2007-2026 (monthly)
- Contains exact enrollment counts (no suppression)
- Has parent_org and plan type built-in

### Priority 3: API Logic
```python
def get_enrollment(state=None, county=None, **filters):
    if state or county:
        # Use geographic table
        table = "fact_enrollment_by_geography"
    else:
        # Use national table (faster, more complete)
        table = "fact_enrollment_all_years"
```

---

## Data Quality Notes

1. **HIPAA Suppression:** Counties with <11 enrollees show "*" instead of number
2. **Employer Plans:** Some employer plans have no county (national coverage)
3. **PACE Plans:** May not have standard geographic data
4. **SNP Column:** Only applies to MA plans, NULL for PDP
