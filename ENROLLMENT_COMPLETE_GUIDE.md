# Complete Enrollment Data Guide

*Updated: 2026-03-04*

## All Enrollment Data Sources We Have

### 1. CPSC Files (Main Source - Geographic Detail) ✅ WORKING
**Location:** `raw/enrollment/cpsc/`
**Years:** 2018-2026 (2013-2017 no longer available from CMS)
**Files:** ~97 monthly files valid
**Size:** ~3.8 GB

**What's inside each ZIP:**
```
cpsc_enrollment_YYYY_MM.zip/
├── CPSC_Contract_Info_YYYY_MM.csv   ← Contract metadata
│   • Contract ID, Plan ID
│   • Parent Organization  
│   • Plan Type (HMO, PPO, etc.)
│   • SNP Plan (Yes/No)
│   • Offers Part D
│   • Organization Name/Marketing Name
│
└── CPSC_Enrollment_Info_YYYY_MM.csv ← Enrollment by county (BIG FILE)
    • Contract Number, Plan ID
    • State, County, FIPS Code
    • Enrollment count
```

**Use case:** State/County level analysis, geographic market share
**Status:** 2018+ working, 2013-2017 removed from CMS

---

### 2. By Contract Files ✅ FIXED (2020+)
**Location:** `raw/enrollment/by_contract/`
**Years:** 2020-2026 (72 files)
**Status:** Re-downloaded 2026-03-04

**Columns:**
- Contract Number
- Organization Type
- Plan Type  
- Organization Name
- Organization Marketing Name
- **Parent Organization** ← Key field!
- Contract Effective Date
- Offers Part D
- MAOnly (MA-only enrollment)
- PartD (Part D enrollment)
- Enrollment (total)

**Use case:** National totals by contract, MA vs Part D split, Parent org analysis

---

### 3. By Plan Files ✅ FIXED (2020+)
**Location:** `raw/enrollment/by_plan/`
**Years:** 2020-2026 (72 files)
**Status:** Re-downloaded 2026-03-04

**Columns:**
- Contract Number
- Plan ID
- Organization Type
- Plan Type
- Offers Part D
- Organization Name
- Organization Marketing Name
- Plan Name
- **Parent Organization** ← Key field!
- Contract Effective Date
- Enrollment

**Use case:** Plan-level enrollment (no geography), Parent org analysis

---

### 4. Monthly Medicare Enrollment ✅ WORKING
**Location:** `raw/enrollment/monthly_enrollment/`
**File:** `medicare_monthly_enrollment_nov2025.csv` (195 MB)

**What it contains (60+ columns):**
- National, State, County breakdowns (BENE_GEO_LVL)
- Total beneficiaries, Original Medicare vs MA
- Demographics: Age bands, Race, Gender
- **Dual eligibility**: 
  - DUAL_TOT_BENES (total dual)
  - FULL_DUAL_TOT_BENES
  - PART_DUAL_TOT_BENES
  - QMB_ONLY_BENES, QMB_PLUS_BENES
  - SLMB_ONLY_BENES, SLMB_PLUS_BENES
  - QDWI_QI_BENES
- Part A/B/D enrollment
- LIS (Low Income Subsidy) status

**Use case:** Dual eligible analysis, demographics, market sizing by geography

---

### 5. SNP Files (Special Needs Plans) ✅ WORKING  
**Location:** `raw/snp/`
**Years:** 2007-2026 (219 files)

**Contains:**
- Contract ID, Plan ID
- SNP Type: **D-SNP** (Dual), C-SNP (Chronic), I-SNP (Institutional)
- State
- Enrollment

**Use case:** D-SNP = Dual Special Needs Plans for dual eligible populations

---

### 6. Latest CPSC Files (Loose)
**Location:** `raw/enrollment/2025-12/`, `raw/enrollment/2026-02/`
**Files:** Direct CSVs (not in cpsc/ folder)

These are the most recent files, downloaded separately.

---

## What Each File Type Gives You

| Data Point | CPSC | By Contract | By Plan | SNP | Monthly Medicare |
|------------|------|-------------|---------|-----|------------------|
| Contract ID | ✅ | ✅ | ✅ | ✅ | ❌ |
| Plan ID | ✅ | ❌ | ✅ | ✅ | ❌ |
| Parent Org | ✅ | ✅ | ✅ | ❌ | ❌ |
| Plan Type | ✅ | ✅ | ✅ | ❌ | ❌ |
| State | ✅ | ❌ | ❌ | ✅ | ✅ |
| County | ✅ | ❌ | ❌ | ❌ | ✅ |
| SNP Type | Flag only | ❌ | ❌ | ✅ | ❌ |
| Offers Part D | ✅ | ✅ | ✅ | ❌ | ❌ |
| MA vs Part D Split | ❌ | ✅ | ❌ | ❌ | ✅ |
| Dual Eligible | ❌ | ❌ | ❌ | D-SNP | ✅ (detailed) |
| Demographics | ❌ | ❌ | ❌ | ❌ | ✅ |
| LIS Status | ❌ | ❌ | ❌ | ❌ | ✅ |
| **Years** | 2018+ | 2020+ | 2020+ | 2007+ | varies |
| **Granularity** | County | Contract | Plan | State | County |

---

## Current Gaps and Status

### ✅ FIXED: By Contract & By Plan Files
**Problem:** Downloads were HTML error pages
**Fix:** Re-downloaded 72 contract + 72 plan files on 2026-03-04
**Coverage:** 2020-2026

### ⚠️ UNAVAILABLE: Pre-2018 CPSC Data
**Problem:** CMS removed historical files from their servers
**Impact:** Missing county-level detail for 2013-2017
**Workaround:** Use SNP files (have 2007-2026) for SNP enrollment by state

### ⚠️ UNAVAILABLE: Pre-2020 By Contract/Plan Data  
**Problem:** CMS removed older files
**Impact:** Missing parent org data for 2007-2019
**Workaround:** None currently - CMS data not available

### ⚠️ MISSING: July 2023 Data
**Problem:** CMS missing this month from all file types
**Impact:** Small gap in time series
**Workaround:** Interpolate or skip

### ✅ DUAL ELIGIBLE: Available via Two Sources
**Option 1:** Monthly Medicare Enrollment - has detailed dual breakdowns by geography
**Option 2:** SNP Files - D-SNP = Dual Special Needs Plans (2007-2026)

---

## How to Use Each Table

### For National Dashboard (no geography)
```sql
-- Use aggregated table (faster, complete)
SELECT year, SUM(enrollment) 
FROM fact_enrollment_all_years
WHERE month = 1
GROUP BY year
```

### For State/County Analysis
```sql
-- Use geographic table
SELECT state, county, SUM(enrollment)
FROM fact_enrollment_by_geography  
WHERE year = 2026
GROUP BY state, county
```

### For Dual Eligible Analysis
```sql
-- Join enrollment with monthly medicare data
SELECT 
    e.parent_org,
    m.DUAL_TOT_BENES,
    m.FULL_DUAL_TOT_BENES
FROM fact_enrollment_all_years e
JOIN monthly_medicare_enrollment m 
    ON e.state = m.BENE_STATE_ABRVTN AND e.year = m.YEAR
```

---

## Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RAW DATA LAYER                           │
├─────────────────────────────────────────────────────────────────┤
│ CPSC (2018+)     → Full detail: county, plan, parent org        │
│ By Contract      → National totals (2020+) ✅ FIXED             │
│ By Plan          → Plan totals (2020+) ✅ FIXED                 │
│ SNP (2007+)      → D-SNP/C-SNP/I-SNP by state                   │
│ Monthly Medicare → Demographics, dual status by geography        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     PROCESSED LAYER                              │
├─────────────────────────────────────────────────────────────────┤
│ fact_enrollment_all_years    → National view (from CPSC)        │
│ fact_enrollment_national     → From By Contract (2020+)         │
│ fact_enrollment_by_plan      → From By Plan (2020+)             │
│ fact_snp                     → SNP types (2007+)                │
│ fact_dual_enrollment         → From Monthly Medicare            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         API LAYER                                │
├─────────────────────────────────────────────────────────────────┤
│ National dashboard     → Uses fact_enrollment_national (faster) │
│ State/County view      → Uses fact_enrollment_all_years (CPSC)  │
│ Parent Org analysis    → Uses fact_enrollment_by_plan           │
│ SNP/Dual analysis      → Uses fact_snp + fact_dual_enrollment   │
└─────────────────────────────────────────────────────────────────┘
```

## Smart Table Selection Logic

```python
def get_best_table(view_type: str, needs_geography: bool):
    """Choose optimal data source based on query needs."""
    
    if needs_geography:
        # State/County analysis - must use CPSC
        return "fact_enrollment_all_years"  # From CPSC, has county
    
    elif view_type == "parent_org":
        # Parent org analysis - use By Plan (has parent org)
        return "fact_enrollment_by_plan"
    
    elif view_type == "national":
        # National totals - use By Contract (faster, complete)
        return "fact_enrollment_national"
    
    elif view_type == "snp":
        # SNP analysis - use SNP files (goes back to 2007)
        return "fact_snp"
```
