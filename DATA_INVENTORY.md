# MA Data Platform - Data Inventory

## Core Data Tables

### 1. Enrollment Data
| Table | Description | Key Columns | Years |
|-------|-------------|-------------|-------|
| `fact_enrollment_unified` | Plan-level enrollment | contract_id, plan_id, year, month, enrollment, parent_org, plan_type | 2015-2026 |
| `fact_enrollment_all_years` | CPSC county-level | contract_id, plan_id, state, county, enrollment | 2015-2026 |
| `gold_fact_enrollment_national` | National totals | contract_id, plan_id, enrollment, parent_org | 2015-2026 |

### 2. Star Ratings Data
| Table | Description | Key Columns | Years |
|-------|-------------|-------------|-------|
| `summary_all_years` | Contract star ratings | contract_id, star_year, overall_rating, part_c, part_d | 2015-2026 |
| `stars_enrollment_unified` | Pre-joined stars+enrollment | contract_id, star_year, enrollment, star_band, is_fourplus, parent_org | 2015-2026 |

### 3. Risk Scores Data
| Table | Description | Key Columns | Years |
|-------|-------------|-------------|-------|
| `fact_risk_scores_unified` | Plan-level risk scores | contract_id, plan_id, year, avg_risk_score, enrollment, plan_type | 2006-2024 |

### 4. SNP Data
| Table | Description | Key Columns | Years |
|-------|-------------|-------------|-------|
| `fact_snp` | SNP classifications | contract_id, plan_id, snp_type (D-SNP, C-SNP, I-SNP) | 2023-2026 |
| `fact_snp_historical` | Historical SNP | contract_id, plan_id, snp_type | 2020-2022 |

---

## Working API Endpoints for Summary Page

### Enrollment
- `GET /api/v3/enrollment/timeseries` - Time series with filters (parent_org, product_type, snp_type, group_type, state)
- `GET /api/v3/enrollment/filters` - Get filter options (years, parent_orgs, product_types, etc.)
- `GET /api/v3/enrollment/by-parent` - Enrollment by parent org for a year

### Stars
- `GET /api/stars/fourplus-timeseries` - % in 4+ stars over time (supports parent_orgs, plan_types, snp_types, group_types)
- `GET /api/stars/distribution` - Star rating distribution by enrollment (supports parent_orgs, star_year)
- `GET /api/stars/distribution-timeseries` - Distribution over all years

### Risk Scores
- `GET /api/v3/risk/timeseries` - Risk score over time (supports parent_orgs, plan_types, snp_types, group_types)
- `GET /api/v3/risk/by-dimensions` - Breakdown by plan_type x snp_type x group_type

---

## Data Gaps / Issues

### Missing for Summary Page:
1. **Counties Operating In** - No endpoint returns # of counties a payer operates in over time
2. **TAM (Total Addressable Market)** - Need county-level benchmark data to calculate
3. **Enrollment by Product Type** - No direct endpoint; using risk/by-dimensions as workaround
4. **Risk data years** - Only goes to 2024, not 2025/2026

### Filter Limitations:
- Stars fourplus-timeseries: No year range filter (returns all years)
- Risk timeseries: No year range filter (returns all years)
- State filtering only works with geographic enrollment data source

---

## Summary Page Requirements vs. Reality

| Component | Needed Data | Available? | Endpoint |
|-----------|-------------|------------|----------|
| Enrollment chart | Time series by payer | ✓ | /api/v3/enrollment/timeseries |
| Stars 4+ chart | % in 4+ over time | ✓ | /api/stars/fourplus-timeseries |
| Risk chart | Avg risk over time | ✓ | /api/v3/risk/timeseries |
| Enrollment table (by year) | Enrollment, counties, TAM | ⚠️ Partial | Need new endpoint |
| Stars distribution table | By star rating over time | ✓ | /api/stars/distribution-timeseries |
| Risk table | By plan type | ✓ | /api/v3/risk/by-dimensions |
