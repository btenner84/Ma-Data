# MA Intelligence Platform - Data Architecture

## Overview

This document defines every piece of data in the MA Intelligence Platform, where it comes from, and how sources link together.

---

## Section 1: Raw CMS Data Sources

### 1.1 CPSC Enrollment File
- **Location**: `raw/enrollment/YYYY-MM/CPSC_Enrollment_Info_YYYY_MM.csv`
- **Frequency**: Monthly
- **Granularity**: `contract_id + plan_id + county + month`

| Column | Description |
|--------|-------------|
| Contract Number | CMS contract ID (H1234, R5678) |
| Plan ID | Plan benefit package (001, 002) |
| State | State code |
| County | County name |
| FIPS State County Code | FIPS code |
| Enrollment | Member count (or * for <11) |

**Provides**: Enrollment by geography at plan level
**Missing**: parent_org, plan_type, group_type, snp_type

---

### 1.2 SNP Enrollment Report
- **Location**: `raw/snp/YYYY-MM/snp_YYYY_MM.zip`
- **Frequency**: Monthly
- **Granularity**: `contract_id + plan_id`
- **Coverage**: SNP plans ONLY (D-SNP, C-SNP, I-SNP)

| Column | Description |
|--------|-------------|
| Contract Number | CMS contract ID |
| Plan ID | Plan benefit package |
| Contract Name | Organization name |
| Plan Type | HMO, HMOPOS, PPO, etc. |
| Special Needs Plan Type | Dual-Eligible, Chronic, Institutional |
| Enrollment | Member count |
| State(s) | State coverage |

**Provides**: Specific SNP type (D-SNP, C-SNP, I-SNP), plan_type, enrollment
**Missing**: parent_org, group_type, Non-SNP plans

---

### 1.3 Plan Payment File (Risk Scores)
- **Location**: `raw/plan_payment/YYYY/plan_payment_YYYY.zip`
- **Frequency**: Annual
- **Granularity**: `contract_id + plan_id + year`

| Column | Description |
|--------|-------------|
| Contract Number | CMS contract ID |
| Plan ID | Plan benefit package |
| Average Part C Risk Score | Risk adjustment score (0.5-3.0 typical) |
| Average A/B PM/PM Payment | Per-member payment |
| Average Rebate PM/PM Payment | Per-member rebate |

**Provides**: risk_score, payment, rebate
**Missing**: parent_org, group_type, snp_type, enrollment

---

### 1.4 Star Ratings File
- **Location**: `raw/stars/YYYY_combined.zip`
- **Frequency**: Annual
- **Granularity**: `contract_id + year` (with segment breakouts)

| Column | Description |
|--------|-------------|
| Contract ID | CMS contract ID |
| Organization Name | Parent organization |
| Contract Type | Plan type category |
| SNP | Yes/No (is this a SNP contract) |
| Overall Rating | Star rating (1-5) |
| Enrollment | Member count |

**Key Insight**: Stars data provides enrollment BROKEN OUT by:
- **Segment**: Individual vs Group (called "group_type")
- **SNP Status**: SNP vs Non-SNP

This is the ONLY source that has group_type at contract level.

**Provides**: star_rating, parent_org, plan_type, group_type, snp_type (SNP/Non-SNP), enrollment
**Missing**: plan_id detail, specific SNP type (D/C/I)

---

### 1.5 Plan Crosswalk File
- **Location**: `raw/crosswalks/crosswalk_YYYY.zip`
- **Frequency**: Annual
- **Granularity**: `contract_id + plan_id`

| Column | Description |
|--------|-------------|
| PREVIOUS_CONTRACT_ID | Prior year contract |
| PREVIOUS_PLAN_ID | Prior year plan |
| CURRENT_CONTRACT_ID | Current year contract |
| CURRENT_PLAN_ID | Current year plan |
| CURRENT_SNP_TYPE | SNP classification |

**Provides**: Year-over-year plan mapping, SNP type
**Missing**: enrollment, parent_org, group_type

---

## Section 2: Data Linkage

### Primary Key: `contract_id`
All sources can be linked by contract_id. Some sources also have plan_id.

```
                         contract_id
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   plan_id level         plan_id level        contract level
        │                     │                     │
   ┌────┴────┐           ┌────┴────┐          ┌────┴────┐
   │  CPSC   │           │  Risk   │          │  Stars  │
   │Enrollmt │           │ Scores  │          │ Ratings │
   │         │           │         │          │         │
   │+county  │           │+risk_scr│          │+group   │
   │+enroll  │           │+rebate  │          │+snp Y/N │
   └─────────┘           └─────────┘          │+enroll  │
        │                     │               │+stars   │
        │                     │               │+parent  │
        └──────────┬──────────┘               └─────────┘
                   │
              ┌────┴────┐
              │   SNP   │
              │ Report  │
              │         │
              │+D/C/I   │
              │+enroll  │
              └─────────┘
```

### The Granularity Problem

| Source | Granularity | Has group_type? |
|--------|-------------|-----------------|
| CPSC | contract + plan + county | NO |
| SNP Report | contract + plan | NO |
| Risk Scores | contract + plan | NO |
| Stars | contract + group_type + snp_type | YES |
| Crosswalk | contract + plan | NO |

**Critical Insight**: `group_type` (Individual vs Group) is ONLY available from Stars Ratings, and ONLY at the contract level (not plan level).

We CANNOT determine which specific plan_id is Individual vs Group.

---

## Section 3: Unified Data Model

### Fact Table 1: `fact_enrollment_plan`
**Purpose**: Plan-level enrollment with geography
**Granularity**: `contract_id + plan_id + year + month + state + county`
**Use Case**: Geographic analysis, plan-level detail

| Column | Source | Notes |
|--------|--------|-------|
| contract_id | CPSC | Primary key component |
| plan_id | CPSC | Primary key component |
| year | CPSC | Primary key component |
| month | CPSC | Primary key component |
| state | CPSC | Primary key component |
| county | CPSC | Primary key component |
| fips_code | CPSC | |
| enrollment | CPSC | |
| parent_org | Stars (lookup) | JOIN on contract_id |
| plan_type | CPSC/SNP Report | |
| snp_type | SNP Report | D-SNP/C-SNP/I-SNP or "Non-SNP" |
| risk_score | Risk Scores | JOIN on contract+plan+year |

**Cannot include**: group_type (not available at plan level)

---

### Fact Table 2: `fact_enrollment_contract`
**Purpose**: Contract-level enrollment with all dimensions
**Granularity**: `contract_id + year + group_type + snp_type`
**Use Case**: Payer comparison, group_type/snp_type filtering
**Source**: `stars_enrollment_unified.parquet` (already exists!)

| Column | Source | Notes |
|--------|--------|-------|
| contract_id | Stars | Primary key component |
| year | Stars (star_year) | Primary key component |
| group_type | Stars | Individual, Group |
| snp_type | Stars | SNP, Non-SNP |
| parent_org | Stars | |
| plan_type | Stars | HMO/HMOPOS, Local PPO, etc. |
| enrollment | Stars | |
| overall_rating | Stars | Star rating 1-5 |
| risk_score | Risk Scores | JOIN on contract+year, AVG across plans |

**Cannot include**: plan_id (aggregated to contract level), specific SNP type (D/C/I)

---

### Aggregation Tables (Derived)

| Table | Derived From | Granularity |
|-------|--------------|-------------|
| agg_by_parent_year | fact_enrollment_contract | parent_org + year |
| agg_by_parent_dims | fact_enrollment_contract | parent_org + year + group_type + snp_type |
| agg_by_state_year | fact_enrollment_plan | state + year |
| agg_by_county_year | fact_enrollment_plan | county + year |

---

## Section 4: Current Issues

### Issue 1: Different Data Sources Return Different Totals
- `fact_enrollment_v6` includes PDP (Part D standalone) = ~23M extra
- `stars_enrollment_unified` only includes MA plans with Star ratings = ~33M
- When API switches between sources, totals don't match

**Fix**: Use consistent source per query type, or filter PDP from fact_enrollment

### Issue 2: Risk Score Enrollment Weighting Bug
- `build_unified_risk_scores.py` uses `'first'` aggregation for group_type
- This randomly assigns one group_type to contracts with BOTH Individual and Group
- Causes massive misattribution of enrollment (20M+ wrong)

**Fix**: Don't aggregate away dimensions - keep separate rows per group_type

### Issue 3: Missing Specific SNP Type in Filters
- Stars only has SNP vs Non-SNP (not D-SNP/C-SNP/I-SNP)
- SNP Report has specific types but only for SNP plans
- No way to filter D-SNP + Individual (different sources)

**Fix**: Use SNP Report for D-SNP/C-SNP/I-SNP filtering (without group_type), use Stars for group_type filtering (without specific SNP)

---

## Section 5: Recommended Architecture

### For Enrollment Page (group_type/snp_type filtering)
**Source**: `stars_enrollment_unified`
- Has correct group_type + snp_type at contract level
- Enrollment totals are internally consistent
- Use for: payer comparison, group/individual filtering, SNP/Non-SNP filtering

### For Risk Scores Page
**Source**: `stars_enrollment_unified` + `risk_scores_by_plan`
- Join at CONTRACT level (not plan level)
- Average risk scores across plans in each contract
- Keep group_type/snp_type dimensions separate (don't collapse)

### For Geographic Analysis
**Source**: `fact_enrollment_plan` (CPSC-based)
- Has state/county detail at plan level
- No group_type available at this granularity

### For D-SNP/C-SNP/I-SNP Analysis
**Source**: SNP Report
- Has specific SNP type at plan level
- No group_type available
- SNP enrollment only (not Non-SNP)

---

## Section 6: Data Validation Rules

### Rule 1: Totals Must Tie Within Each Source
```
For stars_enrollment_unified:
  HMO Non-SNP + HMO SNP = HMO Total ✓
  Individual + Group = Total ✓
  Non-SNP + SNP = Total ✓
```

### Rule 2: Never Mix Sources for Same Query
```
Bad:  HMO Total (from fact_enrollment) vs HMO Non-SNP (from stars)
Good: HMO Total (from stars) vs HMO Non-SNP (from stars)
```

### Rule 3: Document When Sources Don't Match
```
stars_enrollment (MA with ratings): 33M
fact_enrollment (all MA + PDP):     57M
Difference: PDP (23M) + MA without ratings (~1M)
```
