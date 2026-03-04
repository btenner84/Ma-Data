# Complete Filter System

## All Possible Filters

| Filter | Enrollment | Stars | Risk Scores | Source |
|--------|------------|-------|-------------|--------|
| **year** | ✅ | ✅ | ✅ | All tables |
| **month** | ✅ | ❌ | ❌ | Enrollment only (Stars/Risk annual) |
| **contract_id** | ✅ | ✅ | ✅ | All tables |
| **parent_org** | ✅ | ✅ | ✅ | All tables |
| **plan_type** | ✅ | ❌* | ✅ | Need to join |
| **product_type** | ✅ | ❌ | ❌ | Enrollment only |
| **snp_type** | ✅ | ❌* | ✅ | Need to join |
| **group_type** | ⚠️ partial | ❌ | ✅ | Risk scores |
| **state** | ✅ | ❌ | ❌ | Enrollment only |
| **county** | ✅ | ❌ | ❌ | Enrollment only |
| **overall_rating** | ❌ | ✅ | ❌ | Stars only |

*Stars has SNP flag in raw data but not normalized in unified tables

---

## Current Table Dimensions

### Enrollment Tables

**fact_enrollment_all_years** (national aggregates):
```
year, month, contract_id, state, parent_org, plan_type, 
product_type, group_type, snp_type, enrollment, plan_count
```

**fact_enrollment_by_geography** (NEEDS REBUILD):
```
year, state, county, parent_org, plan_type, product_type, enrollment
⚠️ MISSING: month, snp_type, plan_count
```

### Stars Tables

**stars_summary** (by contract):
```
contract_id, rating_year, Parent Organization, Overall Rating, 
Part C Summary, Part D Summary, SNP flag (text)
⚠️ MISSING: plan_type, snp_type (normalized)
```

**stars_by_parent_org** (aggregated):
```
parent_org, rating_year, contract_count, avg_overall_rating
⚠️ MISSING: plan_type, snp_type, enrollment
```

### Risk Score Tables

**risk_scores_by_parent_dims** (has dimensions!):
```
year, parent_org, plan_type, snp_type, group_type,
wavg_risk_score, enrollment, contract_count
✅ Has all key dimensions
```

---

## How Filters Should Flow

### Example: Filter by D-SNP + California

```
User selects: snp_type=D-SNP, state=CA

Enrollment:
  → Query fact_enrollment_by_geography WHERE snp_type='D-SNP' AND state='CA'
  ✅ Works if geography table has snp_type

Stars:
  → Get contract_ids from enrollment WHERE snp_type='D-SNP' AND state='CA'
  → Query stars WHERE contract_id IN (those contracts)
  ✅ Works via join

Risk Scores:
  → Query risk_scores_by_parent_dims WHERE snp_type='D-SNP'
  → Filter to parent_orgs with CA presence from enrollment
  ⚠️ Risk scores don't have state, need to filter via enrollment
```

### Example: Filter by HMO + UnitedHealth

```
User selects: plan_type=HMO, parent_org=UnitedHealth Group

Enrollment:
  → Query fact_enrollment_all_years WHERE plan_type IN ('HMO/HMOPOS',...) AND parent_org='UnitedHealth Group'
  ✅ Works

Stars:
  → Get contract_ids from enrollment with those filters
  → Query stars WHERE contract_id IN (...)
  ⚠️ Stars doesn't have plan_type, must join via enrollment

Risk Scores:
  → Query risk_scores_by_parent_dims WHERE plan_type='HMO' AND parent_org='UnitedHealth Group'
  ✅ Works (has plan_type dimension)
```

---

## Required Fixes

### 1. Rebuild fact_enrollment_by_geography
Add missing columns:
- `month` (for time filtering)
- `snp_type` (for D-SNP filtering)
- `plan_count` (for metrics)

### 2. Create Stars Dimension Table
Build `stars_by_contract_dims` with:
- contract_id, rating_year
- parent_org, plan_type, snp_type (from enrollment)
- overall_rating, part_c_rating, part_d_rating
- enrollment (from enrollment)

### 3. Unified Query Layer
API should handle cross-domain filters by:
1. Getting relevant contract_ids from enrollment (using all filters)
2. Using those contract_ids to filter stars/risk scores
3. Joining results back together

---

## Filter Flow Architecture

```
                    USER FILTERS
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
   ┌─────────┐     ┌─────────┐     ┌─────────┐
   │Enrollment│     │  Stars  │     │  Risk   │
   │ Filters │     │ Filters │     │ Scores  │
   └────┬────┘     └────┬────┘     └────┬────┘
        │                │                │
        │   year ✓       │   year ✓       │   year ✓
        │   month ✓      │                │
        │   parent_org ✓ │   parent_org ✓ │   parent_org ✓
        │   plan_type ✓  │   (via join)   │   plan_type ✓
        │   snp_type ✓   │   (via join)   │   snp_type ✓
        │   state ✓      │                │
        │   county ✓     │                │
        │                │   rating ✓     │
        │                │                │   group_type ✓
        │                │                │
        ▼                ▼                ▼
   ┌─────────────────────────────────────────┐
   │         MASTER CONTRACT LIST            │
   │  (contracts matching ALL active filters)│
   └─────────────────────────────────────────┘
        │
        │ contract_id list used to filter
        │ stars & risk scores when they
        │ don't have the filter natively
        │
        ▼
   ┌─────────────────────────────────────────┐
   │              UNIFIED RESULTS            │
   │  Enrollment + Stars + Risk Scores       │
   │  for matching contracts                 │
   └─────────────────────────────────────────┘
```

---

## Summary: What Filters Work Where

| Filter | Works Directly | Needs Join | Not Available |
|--------|---------------|------------|---------------|
| **year** | Enrollment, Stars, Risk | - | - |
| **parent_org** | Enrollment, Stars, Risk | - | - |
| **plan_type** | Enrollment, Risk | Stars | - |
| **snp_type** | Enrollment, Risk | Stars | - |
| **state** | Enrollment | Stars, Risk | - |
| **county** | Enrollment | Stars, Risk | - |
| **rating** | Stars | - | Enrollment, Risk |
| **group_type** | Risk | - | Enrollment (partial), Stars |
| **month** | Enrollment | - | Stars, Risk (annual only) |

**Key Insight**: Enrollment is the "master" dimension table. Stars and Risk Scores join to it via `contract_id` to get plan_type, snp_type, state, etc.
