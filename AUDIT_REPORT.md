# System Audit Report
**Date:** March 3, 2026

---

## 1. Data Inventory Summary

### Raw Data (S3: raw/)
| Data Type | Files | Size | Years |
|-----------|-------|------|-------|
| enrollment | 622 | 4,364 MB | 2007-2026 |
| stars | 35 | 277 MB | 2007-2026 |
| snp | 219 | 94 MB | 2007-2026 |
| crosswalks | 26 | 15 MB | 2006-2026 |
| hedis | 17 | 30 MB | 2010-2026 |
| plan_payment | 19 | 27 MB | 2006-2024 |
| risk_adjustment | 11 | 4 MB | 2016-2026 |
| rates | 12 | 4 MB | 2016-2026 |
| **TOTAL** | **972** | **4,836 MB** | |

### Processed Data (S3: processed/)
| Data Type | Files | Size | Years |
|-----------|-------|------|-------|
| fact_enrollment | 125 | 796 MB | 2013-2026 |
| unified | 92 | 34 MB | varies |
| stars | 163 | 11 MB | 2007-2026 |
| snp | 125 | 5 MB | 2013-2026 |
| audit | 935 | 3 MB | - |
| **TOTAL** | **1,490** | **853 MB** | |

---

## 2. Current Data Quality (Enrollment)

### What Works ✓
- **Total Enrollment:** 59.5M (Jan 2026)
- **plan_type:** Complete (100% coverage)
  - Medicare Prescription Drug Plan: 24.5M (41%)
  - HMO/HMOPOS: 20.2M (34%)
  - Local PPO: 14.2M (24%)
- **group_type:** Complete (100% coverage)
  - Individual: 47.7M (80%)
  - Group: 11.8M (20%)
- **snp_type:** Complete (100% coverage)
  - Non-SNP: 51.4M (86.5%)
  - D-SNP: 6.3M (10.6%)
  - C-SNP: 1.6M (2.7%)
  - I-SNP: 125K (0.2%)

### What's Missing ✗
| Column | Status | Impact |
|--------|--------|--------|
| `product_type` | MISSING | Can't filter by MAPD/PDP/MA-only |
| `county` | MISSING | Can't filter by county |
| `org_type` | MISSING | Can't distinguish MA-only vs MAPD orgs |

---

## 3. Known Issues

### Data Pipeline Issues
1. **product_type not derived** - Build script doesn't derive MAPD vs PDP vs MA-only
2. **County aggregated away** - Geographic detail lost during aggregation
3. **fact_snp out of sync** - Only has 2023-2026 at parent level, while fact_enrollment_all_years has plan-level SNP for 2013-2026

### API Issues (Fixed)
1. ~~Plan type mapping~~ - Fixed: API now maps "HMO" → "HMO/HMOPOS"
2. ~~Combined filters~~ - Fixed: HMO + D-SNP now works together
3. ~~SNP filter used wrong table~~ - Fixed: Now uses unified enrollment table

### Frontend Issues
1. Product type filter will fail (missing column)
2. Needs testing after API restart

---

## 4. Table Relationships

```
                    ┌─────────────────────┐
                    │ fact_enrollment_    │
                    │ all_years           │
                    │ (3M rows)           │
                    │ Years: 2013-2026    │
                    └─────────┬───────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ stars_summary │    │ fact_risk_    │    │ snp_lookup    │
│ (13K rows)    │    │ scores_unified│    │ (13K rows)    │
│ Years: 2009+  │    │ (66K rows)    │    │ Years: 2013+  │
└───────────────┘    │ Years: 2006+  │    └───────────────┘
                     └───────────────┘
        
Join Keys:
- Enrollment ↔ Stars: contract_id, year
- Enrollment ↔ Risk: contract_id, year
- Enrollment ↔ SNP: contract_id, plan_id, year
```

---

## 5. Recommendations

### Priority 1: Fix product_type
Add product_type derivation to build script:
- S* contracts → PDP
- H*/R* contracts → MA (default to MAPD)
- Plan type "Medicare Prescription Drug Plan" → PDP

### Priority 2: Rebuild with all columns
Ensure unified table has:
- product_type (MAPD, PDP, MA-only)
- org_type (from contract dimension)
- Keep county in a separate geographic table (already exists)

### Priority 3: Validate filters
Test all filter combinations:
- Plan type: HMO, PPO, RPPO, PFFS, MSA
- Product type: MAPD, PDP, MA-only
- SNP type: Non-SNP, D-SNP, C-SNP, I-SNP
- Combined filters: Any combination

### Priority 4: Update documentation
- Document all table schemas
- Document join relationships
- Document filter mappings

---

## 6. File Inventory (Key Scripts)

### Build Scripts
| Script | Purpose | Status |
|--------|---------|--------|
| `build_enrollment_fast.py` | Main enrollment build | Needs product_type |
| `build_snp_lookup_all_years.py` | SNP mapping table | ✓ Working |
| `build_unified_enrollment.py` | Alternative build | Deprecated |

### API Services
| Service | Purpose | Status |
|---------|---------|--------|
| `enrollment_service.py` | Enrollment queries | ✓ Fixed |
| `stars_service.py` | Stars queries | Needs review |
| `risk_scores_service.py` | Risk score queries | Needs review |

---

## 7. Next Steps

1. [ ] Add product_type to build_enrollment_fast.py
2. [ ] Run complete rebuild
3. [ ] Restart API and test all filters
4. [ ] Test frontend filters
5. [ ] Document the complete system
