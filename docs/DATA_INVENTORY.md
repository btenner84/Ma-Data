# MA Data Platform - Master Data Inventory

## Current Status: ~900+ files, 4.3+ GB

## Data Categories

### 1. ENROLLMENT DATA (Monthly)

| Type | Files | Years | Frequency | Status |
|------|-------|-------|-----------|--------|
| CPSC (County/State/Plan/Contract) | 150+ | 2013-2026 | Monthly | In Progress |
| Enrollment by Contract | 230 | 2007-2026 | Monthly | Complete |
| Enrollment by Plan | 230 | 2007-2026 | Monthly | Complete |
| Medicare Monthly Enrollment | 1 | 2013-2025 | Aggregated | Complete |

### 2. SNP DATA (Monthly)

| Type | Files | Years | Frequency | Status |
|------|-------|-------|-----------|--------|
| SNP Comprehensive Reports | 219 | 2007-2026 | Monthly | Complete |

### 3. STAR RATINGS (Yearly)

| Type | Files | Years | Frequency | Status |
|------|-------|-------|-----------|--------|
| Star Ratings Data Tables | 8+ | 2007-2026 | Yearly | In Progress |
| Display Measures | 6+ | 2024-2026 | Yearly | In Progress |
| Cut Point Trends | 1 | Historical | One-time | In Progress |
| Tukey Simulations | 1 | Historical | One-time | In Progress |

### 4. HEDIS DATA (Yearly)

| Type | Files | Years | Frequency | Status |
|------|-------|-------|-----------|--------|
| HEDIS Instructions | 17 | 2010-2026 | Yearly | In Progress |

### 5. RATES & BENCHMARKS (Yearly)

| Type | Files | Years | Frequency | Status |
|------|-------|-------|-----------|--------|
| Ratebooks (County Benchmarks) | 12 | 2016-2026 | Yearly | Complete |

### 6. RISK ADJUSTMENT (Yearly)

| Type | Files | Years | Frequency | Status |
|------|-------|-------|-----------|--------|
| Risk Adjustment Model Software | 11 | 2016-2026 | Yearly | Complete |

### 7. CROSSWALKS (Yearly)

| Type | Files | Years | Frequency | Status |
|------|-------|-------|-----------|--------|
| Plan Crosswalks | 6 | 2022-2026 | Yearly | Complete |
| Pre-2022 Crosswalks | 0 | - | - | Not Available |

## Missing/Needed Data

### High Priority
- [ ] Penetration Data (MA State/County Penetration)
- [ ] Service Area Data (MA Contract Service Area)
- [ ] Benefits Data (PBP files)
- [ ] LIS Enrollment Data

### Lower Priority
- [ ] MA HEDIS Public Use Files (actual HEDIS data, not instructions)
- [ ] Corrective Action Plans
- [ ] Plan Directory files
- [ ] Historical crosswalks (pre-2022 may not exist)

## S3 Bucket Structure

```
s3://ma-data123/
├── raw/
│   ├── enrollment/
│   │   ├── cpsc/           # Monthly by county
│   │   ├── by_contract/    # Monthly by contract
│   │   ├── by_plan/        # Monthly by plan
│   │   └── monthly_enrollment/  # Aggregated data
│   ├── snp/                # Special Needs Plans
│   ├── stars/              # Star Ratings
│   │   └── ratings/        # Original location
│   ├── hedis/              # HEDIS instructions
│   ├── rates/              # Ratebooks
│   │   └── ratebook/
│   ├── risk_adjustment/    # Model software
│   │   └── model_software/
│   └── crosswalks/         # Plan crosswalks
├── processed/              # Future: Parquet files
└── exports/                # Future: Client exports
```

## Join Keys

- **Contract ID**: H1234 format (links enrollment, stars, SNP)
- **Plan ID**: 001-999 (links within contract)
- **FIPS County Code**: Links geography across datasets
- **Year/Month**: Links time series data

## Update Schedule

- **Monthly**: CPSC, Enrollment files (by 15th of month)
- **Yearly (October)**: Star Ratings, Display Measures
- **Yearly (April)**: Ratebooks, Risk Adjustment
- **Yearly (Fall)**: Crosswalks
