# MA Data Platform - Comprehensive System Audit

**Date:** March 3, 2026  
**Auditor:** System Audit  

## Executive Summary

This audit documents all data sources, toggles, and calculations in the MA Intelligence Platform. Key findings:

1. **Stars 4+ Star % was inflated by ~6 percentage points** due to excluding non-rated contracts
2. **New national data sources created** to provide accurate calculations
3. **API now supports data_source toggle** for switching between national (accurate) and geographic (detailed) views

---

## 1. Data Source Architecture

### 1.1 Enrollment Data Sources

| Data Source | Table | Years | Filters | Geography | Use Case |
|-------------|-------|-------|---------|-----------|----------|
| **National** | `fact_enrollment_unified` | 2013-2026 | All filters | No | Aggregated national view |
| **Geographic** | `fact_enrollment_unified` | 2013-2026 | All filters + state/county | Yes | Geographic analysis |

**Key Update (March 2026):** Both data sources now use the same underlying CPSC data (`fact_enrollment_unified`) ensuring:
- **Same year coverage (2013-2026)** for both sources
- **All filters work on both sources** (SNP type, Group type, Plan type, Product type)
- Only difference: "Geographic" enables state/county filtering

**Note:** County-level data may have ~2% suppression for counties with <10 enrollees.

### 1.2 Stars Data Sources

| Table | Source | Coverage | Contracts | Use Case |
|-------|--------|----------|-----------|----------|
| `stars_enrollment_national` | By-Contract + Stars | **100% MA** | All MA contracts | **CORRECT 4+ star %** |
| `stars_enrollment_unified` | CPSC + Stars | ~95% MA | Only rated contracts | Legacy, geographic filters |

**Critical Finding:** `stars_enrollment_unified` only includes contracts WITH star ratings, inflating 4+ star % by ~6pp.

### 1.3 Risk Scores Data Sources

| Table | Source | Coverage | Use Case |
|-------|--------|----------|----------|
| `risk_scores_national` | Plan Payment + National Enrollment | **97%+** | Accurate weighted averages |
| `risk_scores_by_parent_dims` | Plan Payment + CPSC | ~97% | Legacy, with dimension filters |

---

## 2. Key Metrics Comparison (2024)

### 2.1 4+ Star Percentage

| Calculation Method | 4+ Star % | Status |
|-------------------|-----------|--------|
| National (ALL MA enrollment) | **75.8%** | ✓ CORRECT |
| Unified (rated contracts only) | 81.7% | ✗ Inflated by 5.9pp |

**Formula:**
- Correct: `4+ enrollment / ALL MA enrollment`
- Incorrect (legacy): `4+ enrollment / RATED enrollment only`

### 2.2 Enrollment Coverage

| Source | 2024 Enrollment | Coverage |
|--------|-----------------|----------|
| National (by-contract) | 34,589,688 | **100%** |
| CPSC (geographic) | 33,850,000 | ~98% |
| Stars National | 34,589,688 | **100%** |
| Stars Unified | 32,997,728 | 95.4% |

---

## 3. API Endpoints & Toggles

### 3.1 Enrollment API (`/api/v3/enrollment/timeseries`)

| Parameter | Values | Effect |
|-----------|--------|--------|
| `data_source` | `national` (default) | Uses `fact_enrollment_national` - exact totals |
| `data_source` | `geographic` | Uses `fact_enrollment_unified` - has state/county filters |

When `data_source=national`:
- State/county filters are **disabled** (not applicable)
- Returns exact enrollment totals

When `data_source=geographic`:
- State/county filters are **enabled**
- Enrollment may be ~1-2% understated due to suppression

### 3.2 Stars API (`/api/v3/stars/distribution`)

| Parameter | Values | Effect |
|-----------|--------|--------|
| `data_source` | `national` (default) | Uses ALL MA enrollment (correct 4+ star %) |
| `data_source` | `rated` | Uses only rated contracts (legacy, inflated %) |

### 3.3 Filter Combinations

| Domain | Product Type | Plan Type | Group Type | SNP Type | State | County | Data Source |
|--------|-------------|-----------|------------|----------|-------|--------|-------------|
| Enrollment | ✓ | ✓ | ✓ | ✓ | Geographic only | Geographic only | National or Geographic |
| Stars | ✓ | ✓ | ✓ | ✓ | - | - | National or Rated |
| Risk Scores | ✓ | ✓ | ✓ | ✓ | - | - | National |

**Notes (Updated March 2026):**
- **All filters now work on BOTH National and Geographic sources** for enrollment
- State and County filters are only available with Geographic data source
- Both National and Geographic enrollment show the same years (2013-2026)
- Product Type mapping: UI "MA" → API "MAPD" (underlying table uses MAPD)

---

## 4. Data Lineage

### 4.1 Source Files (CMS)

| File Type | Frequency | Content | Used For |
|-----------|-----------|---------|----------|
| Monthly Enrollment by Contract | Monthly | Contract-level enrollment | National totals |
| CPSC (County-Plan-State-Contract) | Monthly | Geographic enrollment | State/county analysis |
| Star Ratings | Annual (Oct) | Contract star ratings | Quality metrics |
| Plan Payment Data | Annual | Risk scores, payments | Risk score analysis |
| SNP Comprehensive Report | Monthly | SNP type details | SNP classification |

### 4.2 Processing Pipeline

```
CMS Source Files (S3: raw/)
    │
    ▼
Processing Scripts (scripts/)
    │
    ▼
Processed Tables (S3: processed/unified/)
    │
    ▼
DuckDB Views (db/duckdb_layer.py)
    │
    ▼
API Services (api/services/)
    │
    ▼
Frontend (web/app/)
```

---

## 5. Historical Data Coverage

### 5.1 Enrollment

| Year Range | Data Available | Filters |
|------------|----------------|---------|
| 2013-2026 | ✓ Full coverage | All filters work (SNP, Group, Plan type) |
| Before 2013 | ✗ Not available | - |

**Note:** Both National and Geographic data sources show the same year range (2013-2026).

### 5.2 Stars

| Year Range | Ratings | Measures | Cutpoints |
|------------|---------|----------|-----------|
| 2008-2010 | Partial | Partial | ✗ |
| 2011-2026 | ✓ Full | ✓ Full | ✓ Full |

### 5.3 Risk Scores

| Year Range | Coverage |
|------------|----------|
| 2006-2024 | ✓ Full |
| 2025 | ✗ Not yet published |

---

## 6. Known Limitations

1. **Geographic data suppression**: Counties with <10 enrollees are suppressed in CPSC files
2. **Star rating lag**: Star ratings released in October apply to the following payment year
3. **New contracts**: New contracts may not have star ratings for 1-2 years
4. **SNP classification**: SNP subtypes (D-SNP, C-SNP, I-SNP) only available from 2014+

---

## 7. Recommendations

1. **Use national data sources by default** for accurate industry metrics
2. **Use geographic sources only when** state/county analysis is required
3. **Document data source** in any reports or exports
4. **Monitor for discrepancies** between national and geographic totals

---

## 8. Comprehensive Testing

### 8.1 Automated Audit Scripts

The system includes comprehensive audit scripts that validate all filter combinations:

| Script | Purpose | Tests |
|--------|---------|-------|
| `scripts/comprehensive_audit.py` | Full system validation | 38 tests |
| `scripts/audit_ui_combinations.py` | UI filter combinations | 41 tests |
| `scripts/unified/reconcile_and_validate.py` | Data reconciliation | Per-month validation |

### 8.2 Test Coverage

**Enrollment Tests (All Pass ✓):**
- Year coverage: Both national and geographic show 2013-2026
- Product types: MA, PDP, combined
- Plan types: HMO, PPO, PFFS, MSA, PACE, Cost
- SNP types: Non-SNP, D-SNP, C-SNP, I-SNP
- Group types: Individual, Group
- State filters: All 50 states + territories
- Complex combinations: Multi-filter combinations

**Stars Tests (All Pass ✓):**
- National data source: 75.8% 4+ star (2024)
- Rated-only data source: 81.7% 4+ star (2024)
- Difference validation: 5.9pp (expected due to non-rated contracts)

**Risk Scores Tests (All Pass ✓):**
- Distribution endpoint: All years working
- Timeseries endpoint: 19 years of data

**Data Consistency Tests (All Pass ✓):**
- SNP sum equals total: 0.0% difference
- Group type sum equals total: 0.0% difference

### 8.3 Running Audits

```bash
# Full system audit
python scripts/comprehensive_audit.py

# UI filter combinations audit
python scripts/audit_ui_combinations.py

# Data reconciliation
python scripts/unified/reconcile_and_validate.py
```

### 8.4 Latest Audit Results

| Audit | Tests | Passed | Failed | Pass Rate |
|-------|-------|--------|--------|-----------|
| Comprehensive | 38 | 38 | 0 | **100%** |
| UI Combinations | 41 | 41 | 0 | **100%** |

---

## 9. Audit Trail

All queries are logged with audit_id for lineage tracing:
- Query timestamp
- Tables accessed
- Filters applied
- Row counts returned

Use `engine.trace_query_lineage(audit_id)` to trace any result back to source files.
