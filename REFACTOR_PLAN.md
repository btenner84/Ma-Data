# API Refactor Plan: Wire Up Gold Layer

## Problem
- Built robust Gold layer star schema with dimension tables
- API endpoints bypass it, doing raw SQL to legacy tables
- Filters work inconsistently across pages
- No contract crosswalk tracking
- Audit trail incomplete

## Solution
Create v5 API endpoints that properly use UnifiedDataService + Gold layer

## Gold Layer Tables
| Table | Type | Purpose |
|-------|------|---------|
| gold_dim_entity | Dimension | Parent orgs, contract tracking across mergers |
| gold_dim_plan | Dimension | plan_type, snp_type, group_type, product_type |
| gold_dim_geography | Dimension | state, county, fips_code |
| gold_dim_time | Dimension | year, month, quarter |
| gold_fact_enrollment_national | Fact | Enrollment by contract/plan |
| gold_fact_enrollment_geographic | Fact | Enrollment by county |
| gold_fact_stars | Fact | Star ratings |
| gold_fact_risk_scores | Fact | Risk scores |

## New v5 Endpoints

### Core Query Endpoint
```
GET /api/v5/query
- domain: enrollment | stars | risk
- metrics: [enrollment, plan_count, avg_risk, pct_fourplus]
- dimensions: [year, parent_org, plan_type, state, etc]
- filters: {parent_org: "UNH", year_gte: 2020, state: ["CA", "TX"]}
```

### Convenience Endpoints
```
GET /api/v5/summary
- Single payer or industry summary with all metrics

GET /api/v5/timeseries
- Time series for any metric with full filter support

GET /api/v5/filters
- Get all available filter options

GET /api/v5/geographic
- County-level data with TAM
```

## Pages to Update
1. [ ] Summary Page (new) - Currently broken
2. [ ] Enrollment Page - Uses legacy endpoints
3. [ ] Stars Page - Uses legacy endpoints  
4. [ ] Risk Scores Page - Uses legacy endpoints
5. [ ] Data Sources Page - May need updates

## Testing Checklist
For each page, verify:
- [ ] All filters work (product_type, plan_type, snp_type, group_type, state)
- [ ] Year range works
- [ ] Payer selection works
- [ ] Charts load correctly
- [ ] Tables load correctly
- [ ] No console errors

## Progress
- [x] Create v5 endpoints
- [x] Update Summary page
- [x] Test Summary page
- [x] Update Enrollment page
- [x] Test Enrollment page
- [x] Update Stars page
- [x] Test Stars page
- [x] Update Risk page
- [x] Test Risk page

## Changes Made

### v5 API Endpoints (api/main.py)
- `GET /api/v5/filters` - Returns filter options from Gold layer dimension tables
- `GET /api/v5/enrollment/timeseries` - Enrollment timeseries with full filter support
- `GET /api/v5/stars/timeseries` - 4+ star % timeseries with full filter support  
- `GET /api/v5/risk/timeseries` - Risk score timeseries with full filter support
- `GET /api/v5/summary` - Comprehensive single-year summary

### UnifiedDataService (api/services/data_service.py)
- Enhanced with v5 methods that properly join to dimension tables
- `get_filters_v5()` - Query dimension tables for filter options
- `get_enrollment_timeseries_v5()` - Full filter support
- `get_stars_timeseries_v5()` - Full filter support
- `get_risk_timeseries_v5()` - Full filter support
- `get_summary_v5()` - Combined metrics

### UI Updates
- Summary page: Complete rewrite using v5 endpoints
- Enrollment page: v5 filters with fallback to v3
- Stars page: v5 filters with fallback to v3
- Risk page: v5 filters with fallback to v3
- Fixed ResponsiveContainer explicit heights across all pages
