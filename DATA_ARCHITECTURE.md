# MA Data Platform - Complete Architecture

## Overview

This document maps the complete data flow from CMS source files to the API.

---

## 1. RAW DATA SOURCES (S3: `ma-data123/raw/`)

| Source | Path | Years | Frequency | Description |
|--------|------|-------|-----------|-------------|
| **CPSC Enrollment** | `raw/enrollment/cpsc/` | 2013-2026 | Monthly | Contract-Plan-State-County enrollment |
| **Enrollment by Plan** | `raw/enrollment/by_plan/` | 2007-2026 | Monthly | Totals by contract/plan (no suppression) |
| **SNP Reports** | `raw/snp/` | 2007-2026 | Monthly | Special Needs Plan details (D-SNP, C-SNP, I-SNP) |
| **Stars Ratings** | `raw/stars/` | 2007-2026 | Annual | Star ratings, measures, cutpoints |
| **Risk Scores** | `raw/risk-scores/` | 2006-2024 | Annual | MA risk adjustment scores |
| **Crosswalks** | `raw/crosswalks/` | 2006-2026 | Annual | Contract ID changes (mergers, renames) |
| **HEDIS** | `raw/hedis/` | varies | Annual | Quality measures |
| **Penetration** | `raw/penetration/` | varies | Quarterly | MA penetration rates |

---

## 2. PROCESSED DATA (S3: `ma-data123/processed/`)

### 2.1 Enrollment Tables

| Table | Path | Years | Grain | Key Columns |
|-------|------|-------|-------|-------------|
| `fact_enrollment/*` | `processed/fact_enrollment/YYYY/MM/` | 2013-2026 | contract+plan+county+month | contract_id, plan_id, fips_code, state, enrollment, is_snp, parent_org, plan_type |
| `fact_enrollment_all_years` | `processed/unified/fact_enrollment_all_years.parquet` | 2013-2026 | contract+state+month | **AGGREGATED** - missing plan_id, county |

### 2.2 Dimension Tables

| Table | Path | Description |
|-------|------|-------------|
| `dim_contract_v2` | `processed/unified/dim_contract_v2.parquet` | Contract metadata (parent_org, plan_type, product_type) |
| `dim_county` | `processed/unified/dim_county.parquet` | County FIPS codes and names |
| `dim_parent_org` | `processed/unified/dim_parent_org_v6.parquet` | Parent organization names |
| `snp_lookup` | `processed/unified/snp_lookup.parquet` | Contract+Plan → SNP Type mapping |

### 2.3 Stars Tables

| Table | Path | Years | Grain |
|-------|------|-------|-------|
| `measures_all_years` | `processed/unified/measures_all_years.parquet` | 2008-2026 | contract+measure+year |
| `summary_all_years` | `processed/unified/summary_all_years.parquet` | 2009-2026 | contract+year |
| `cutpoints_all_years` | `processed/unified/cutpoints_all_years.parquet` | 2011-2026 | measure+year |
| `domain_all_years` | `processed/unified/domain_all_years.parquet` | 2008-2026 | contract+domain+year |

### 2.4 Risk Score Tables

| Table | Path | Years | Grain |
|-------|------|-------|-------|
| `fact_risk_scores_unified` | `processed/unified/fact_risk_scores_unified.parquet` | 2006-2024 | contract+year |

### 2.5 SNP Tables

| Table | Path | Years | Issues |
|-------|------|-------|--------|
| `fact_snp` | registered in DuckDB | 2023-2026 | **MISSING 2007-2022** |
| `snp_lookup` | `processed/unified/snp_lookup.parquet` | 2013-2026 | Recently rebuilt with 13K records |

---

## 3. JOIN RELATIONSHIPS

```
                    ┌─────────────────────────────────────┐
                    │         dim_contract_v2             │
                    │  (contract_id, year)                │
                    │                                     │
                    │  - parent_org                       │
                    │  - plan_type                        │
                    │  - product_type                     │
                    │  - org_type                         │
                    └──────────────┬──────────────────────┘
                                   │
       ┌───────────────────────────┼───────────────────────────┐
       │                           │                           │
       ▼                           ▼                           ▼
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  ENROLLMENT      │    │     STARS        │    │   RISK SCORES    │
│                  │    │                  │    │                  │
│ fact_enrollment  │    │ summary_all_yrs  │    │ fact_risk_scores │
│ JOIN ON:         │    │ JOIN ON:         │    │ JOIN ON:         │
│ - contract_id    │    │ - contract_id    │    │ - contract_id    │
│ - year           │    │ - year           │    │ - year           │
└────────┬─────────┘    └──────────────────┘    └──────────────────┘
         │
         │ JOIN ON contract_id + plan_id + year
         ▼
┌──────────────────┐
│   snp_lookup     │
│                  │
│ - contract_id    │
│ - plan_id        │
│ - year           │
│ - snp_type       │
└──────────────────┘
```

---

## 4. CURRENT ISSUES

### 4.1 SNP Type is Broken
- **Problem**: `fact_enrollment_all_years` aggregates away `plan_id` BEFORE joining to `snp_lookup`
- **Result**: 98% of enrollment has NULL snp_type
- **Fix**: Must join to `snp_lookup` at plan level, THEN aggregate

### 4.2 Group Type is Missing
- **Problem**: `group_type` column doesn't exist in fact table
- **Source**: Can derive from `plan_id` (800+ = Group, <800 = Individual)
- **Fix**: Add during build process

### 4.3 Enrollment Months Incomplete
- **Problem**: Some years have missing months (2013, 2016, 2017 especially)
- **Cause**: CMS removed historical monthly data, only January snapshots remain
- **Workaround**: Use January for YoY comparisons

### 4.4 SNP Lookup Incomplete
- **Problem**: Only processed 2013-2026 (raw data goes back to 2007)
- **Cause**: Raw files before 2013 have different format or are corrupted
- **Status**: Rebuilt with 13,013 records covering 2013-2026

---

## 5. IDEAL ARCHITECTURE

### 5.1 Base Fact Table (Most Granular)
```sql
fact_enrollment_base (
    -- Keys
    contract_id,
    plan_id,
    fips_code,      -- County
    year,
    month,
    
    -- Enrollment
    enrollment,
    
    -- From source
    parent_org,
    plan_type,
    is_snp,
    
    -- Derived
    state,          -- From fips_code
    group_type,     -- From plan_id (>=800 = Group)
    
    -- Joined
    snp_type,       -- From snp_lookup
    product_type,   -- From dim_contract
    
    -- Audit
    _source_file,
    _pipeline_run_id
)
```

### 5.2 Aggregated Views
```sql
-- December snapshot for YoY
CREATE VIEW fact_enrollment_yearly AS
SELECT * FROM fact_enrollment_base WHERE month = 12;

-- State level (aggregates county)
CREATE VIEW fact_enrollment_by_state AS
SELECT 
    contract_id, plan_id, state, year, month,
    SUM(enrollment) as enrollment, ...
FROM fact_enrollment_base
GROUP BY contract_id, plan_id, state, year, month;

-- Contract level (aggregates plan)  
CREATE VIEW fact_enrollment_by_contract AS
SELECT
    contract_id, state, year, month,
    SUM(enrollment) as enrollment,
    COUNT(DISTINCT plan_id) as plan_count, ...
FROM fact_enrollment_base
GROUP BY contract_id, state, year, month;
```

### 5.3 Cross-Domain Joins
```sql
-- Enrollment with Stars
SELECT 
    e.contract_id,
    e.parent_org,
    e.enrollment,
    s.overall_rating,
    s.part_c_summary,
    s.part_d_summary
FROM fact_enrollment_by_contract e
LEFT JOIN summary_all_years s 
    ON e.contract_id = s.contract_id 
    AND e.year = s.year
WHERE e.month = 12;  -- December snapshot

-- With Risk Scores
SELECT
    e.contract_id,
    e.enrollment,
    r.risk_score
FROM fact_enrollment_by_contract e
LEFT JOIN fact_risk_scores_unified r
    ON e.contract_id = r.contract_id
    AND e.year = r.year;
```

---

## 6. BUILD PIPELINE ORDER

1. **Build `snp_lookup`** from raw SNP files (all years)
2. **Build `fact_enrollment_base`** from raw CPSC files
   - Join `snp_lookup` at plan level
   - Derive `group_type` from `plan_id`
   - Add `product_type` from `dim_contract`
3. **Build aggregated views** (state, contract level)
4. **Validate MECE** for each dimension
5. **Register in DuckDB** layer

---

## 7. FILTER DIMENSIONS (MECE)

Each of these should sum to 100% of total enrollment:

| Dimension | Values | Source |
|-----------|--------|--------|
| **Plan Type** | HMO, PPO, RPPO, PFFS, MSA, PACE, Cost, PDP | plan_type column |
| **Product Type** | MA-only, MAPD, PDP | contract prefix + offers_part_d |
| **SNP Type** | D-SNP, C-SNP, I-SNP, Non-SNP | snp_lookup join + is_snp flag |
| **Group Type** | Individual, Group | plan_id (>=800 = Group) |

---

## 8. API ENDPOINTS

| Endpoint | Table Used | Filters |
|----------|------------|---------|
| `/api/v3/enrollment/timeseries` | fact_enrollment_all_years | year, plan_type, product_type, snp_type, state, parent_org |
| `/api/v3/enrollment/filters` | fact_enrollment_all_years | (returns available filter options) |
| `/api/v3/stars/summary` | summary_all_years | year, contract_id |
| `/api/v3/risk-scores` | fact_risk_scores_unified | year, contract_id |

---

## 9. AUDIT TRAIL

Every query should be traceable:
- `_source_file`: Original S3 path
- `_pipeline_run_id`: Build run that created the row
- `audit_id`: Query audit ID (returned by API)

---

*Last Updated: 2026-03-03*
