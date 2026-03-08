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
- [ ] Create v5 endpoints
- [ ] Update Summary page
- [ ] Test Summary page
- [ ] Update Enrollment page
- [ ] Test Enrollment page
- [ ] Update Stars page
- [ ] Test Stars page
- [ ] Update Risk page
- [ ] Test Risk page
