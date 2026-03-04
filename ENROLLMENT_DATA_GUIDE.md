# Enrollment Data Guide

## Overview

We have **3 types** of CMS enrollment files, each with different granularity:

---

## 1. Enrollment by Contract (`by_contract/`)

**Granularity:** Contract-level totals
**Years Available:** 2007-2026
**Files:** 230 (monthly for all years)
**Size:** 6.6 MB total

**What it contains:**
- Total enrollment per contract per month
- Contract-level metadata (parent org, plan type)
- No county or plan-level detail

**Use case:** Quick contract-level analysis, historical trends

---

## 2. Enrollment by Plan (`by_plan/`)

**Granularity:** Plan-level (contract + plan_id)
**Years Available:** 2007-2026
**Files:** 230 (monthly for all years)
**Size:** 6.6 MB total

**What it contains:**
- Enrollment per plan (contract_id + plan_id)
- Plan-level details
- No county detail

**Use case:** Plan-level analysis, SNP joins (requires plan_id)

---

## 3. CPSC Enrollment (`cpsc/`)

**Granularity:** County-Plan-State-County level
**Years Available:** 2013-2026
**Files:** 160 (monthly where available)
**Size:** 4,156 MB total (this is the big one!)

**What it contains:**
- Full geographic detail (state, county, FIPS code)
- Plan-level detail (contract_id, plan_id)
- All metadata (parent org, plan type, SNP flag)

**Use case:** Geographic analysis, full enrollment picture

---

## Monthly Data Coverage

| Year | Contract | Plan | CPSC | Notes |
|------|----------|------|------|-------|
| 2007 | 12 mo | 12 mo | - | No CPSC before 2013 |
| 2008 | 12 mo | 12 mo | - | |
| 2009 | 12 mo | 12 mo | - | |
| 2010 | 12 mo | 12 mo | - | |
| 2011 | 12 mo | 12 mo | - | |
| 2012 | 12 mo | 12 mo | - | |
| 2013 | 12 mo | 12 mo | 1 mo | CPSC starts |
| 2014 | 12 mo | 12 mo | 10 mo | |
| 2015 | 12 mo | 12 mo | 11 mo | |
| 2016 | 12 mo | 12 mo | 6 mo | Gaps in CPSC |
| 2017 | 12 mo | 12 mo | 1 mo | Only Jan CPSC |
| 2018 | 12 mo | 12 mo | 12 mo | Full year |
| 2019 | 12 mo | 12 mo | 12 mo | Full year |
| 2020 | 12 mo | 12 mo | 12 mo | Full year |
| 2021 | 12 mo | 12 mo | 12 mo | Full year |
| 2022 | 12 mo | 12 mo | 12 mo | Full year |
| 2023 | 12 mo | 12 mo | 11 mo | |
| 2024 | 12 mo | 12 mo | 12 mo | Full year |
| 2025 | 12 mo | 12 mo | 11 mo | Current |
| 2026 | 2 mo | 2 mo | 2 mo | Latest |

---

## What We Process

We primarily use **CPSC files** because they have the most detail:
- Geographic breakdown (state/county)
- Plan-level detail for SNP joins
- All contract metadata

**Our processed data:** 125 monthly files covering 2013-2026

---

## Crosswalk Data

**Purpose:** Track plan changes year-over-year

**Files:** 26 crosswalk files (2006-2026)

**What crosswalk tells us:**

| Status | Meaning |
|--------|---------|
| Renewal Plan | Same plan, no changes |
| Terminated/Non-renewed | Plan ended |
| New Plan | Brand new plan |
| Consolidated Renewal | Plans merged together |
| Renewal with SAR | Service Area Reduction |
| Renewal with SAE | Service Area Expansion |
| Initial Contract | New contract entering market |

**Sample crosswalk record:**
```
PREVIOUS: H0028/050 (Humana Gold Choice PFFS)
CURRENT:  TERMINATED/TERMINATED
STATUS:   Terminated/Non-renewed Contract
```

**How we use crosswalk:**
1. Link historical contracts to current contracts
2. Track mergers and acquisitions
3. Understand market exits/entries
4. Build entity chains (dim_entity table)

---

## Data Flow

```
Raw Files (S3)
    │
    ├── by_contract/     → Quick totals (2007+)
    ├── by_plan/         → Plan detail (2007+)
    └── cpsc/            → Full detail (2013+) ← WE USE THIS
            │
            ▼
    Processed Parquet (S3: processed/fact_enrollment/)
            │
            ▼
    Unified Table (fact_enrollment_all_years)
            │
            ├── Aggregated by: year, month, contract, state, parent_org
            ├── Dimensions: plan_type, product_type, group_type, snp_type
            └── Audit: _source_file, _pipeline_run_id
            │
            ▼
    API (enrollment_service.py)
            │
            ▼
    Frontend Dashboard
```

---

## Current Limitations

1. **No CPSC data before 2013** - Only contract/plan level data
2. **Some months missing** - 2016-2017 have gaps
3. **County detail aggregated** - We sum by state in unified table
4. **Crosswalk not fully integrated** - Entity chains exist but not joined to enrollment

---

## Recommended Improvements

1. **Fill monthly gaps** - Re-download missing CPSC months
2. **Integrate crosswalk** - Join historical contracts via entity chains
3. **Keep county detail** - Use fact_enrollment_by_geography for county queries
4. **Add quarterly aggregates** - Pre-compute Q1/Q2/Q3/Q4 rollups
