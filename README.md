# MA Data Platform

Unified Medicare Advantage data intelligence platform consolidating all CMS public data sources.

## Quick Stats

- **908 files** across 9 data categories
- **~4.5 GB** raw data
- **2007-2026** coverage (19 years)
- **Monthly + Yearly** data frequencies

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA HIERARCHY                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  PARENT ORGANIZATION (UnitedHealth, Humana, Aetna, Kaiser, etc.)        │
│    └── CONTRACT (H0028, H1234, R5678, etc.)                             │
│          └── PLAN (001, 002, 003, etc.)                                 │
│                └── PRODUCT TYPE (HMO, PPO, SNP, etc.)                   │
│                      └── GEOGRAPHY (State → County → FIPS)              │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Sources

| Category | Files | Years | Frequency | Key Fields |
|----------|-------|-------|-----------|------------|
| Enrollment by Contract | 230 | 2007-2026 | Monthly | Contract, Enrollment |
| Enrollment by Plan | 230 | 2007-2026 | Monthly | Contract, Plan, Enrollment |
| CPSC (County/Plan) | 158 | 2013-2026 | Monthly | Contract, Plan, County, Parent Org |
| SNP | 219 | 2007-2026 | Monthly | Contract, Plan, SNP Type, Integration |
| Star Ratings | 27 | 2007-2026 | Yearly | Contract, Overall Rating, Measures |
| HEDIS | 17 | 2010-2026 | Yearly | Measure specifications |
| Ratebooks | 11 | 2016-2026 | Yearly | County, Benchmark Rates |
| Risk Adjustment | 11 | 2016-2026 | Yearly | HCC model software |
| Crosswalks | 5 | 2022-2026 | Yearly | Old Contract/Plan → New Contract/Plan |

## S3 Structure

```
s3://ma-data123/
├── raw/                          # Original CMS downloads
│   ├── enrollment/
│   │   ├── cpsc/                # County-level (158 files)
│   │   ├── by_contract/         # Contract-level (230 files)
│   │   └── by_plan/             # Plan-level (230 files)
│   ├── snp/                     # Special Needs Plans (219 files)
│   ├── stars/                   # Star Ratings (27 files)
│   ├── hedis/                   # HEDIS specs (17 files)
│   ├── rates/ratebook/          # Benchmarks (11 files)
│   ├── risk_adjustment/         # HCC models (11 files)
│   └── crosswalks/              # Plan crosswalks (5 files)
├── processed/                    # Unified data layer (Parquet)
│   ├── dimensions/
│   │   ├── dim_entity.parquet   # Stable entity IDs via crosswalk
│   │   └── dim_parent_org.parquet # Canonical parent orgs + M&A
│   ├── facts/
│   │   ├── fact_enrollment_unified/    # All dimensions, no suppression
│   │   ├── fact_enrollment_geographic/ # County-level with suppression tracking
│   │   ├── fact_star_ratings/
│   │   └── fact_risk_scores/
│   ├── aggregations/
│   │   ├── agg_by_parent_year.parquet
│   │   ├── agg_by_state_year.parquet
│   │   └── agg_industry_totals.parquet
│   ├── audit/                    # Full lineage tracking
│   │   ├── source_files/         # MD5 hashes, row counts
│   │   ├── transformations/      # Join/derive/aggregate logs
│   │   └── queries/              # All API queries logged
│   └── catalog/                  # Data documentation
└── config/
    └── semantic_model.yaml       # AI query configuration
```

## Key Join Strategy

```
CPSC_Contract_Info (Parent Organization is HERE)
    │
    ├── Join CPSC_Enrollment_Info ON (Contract ID, Plan ID)
    │   → Get county-level enrollment
    │
    ├── Join Stars ON (Contract Number)
    │   → Get quality ratings
    │
    ├── Join SNP ON (Contract Number, Plan ID)
    │   → Get SNP details (D-SNP, C-SNP, I-SNP)
    │
    └── Aggregate BY Parent Organization
        → Company-level analysis
```

## Documentation

- **[MASTER_SCHEMA.md](docs/data_dictionaries/MASTER_SCHEMA.md)** - Complete schema documentation
- **[SCHEMA_EVOLUTION.md](docs/data_dictionaries/SCHEMA_EVOLUTION.md)** - How schemas changed over time
- **[DATA_INVENTORY.md](docs/DATA_INVENTORY.md)** - File counts and coverage

## Key Insights

### Parent Organization Field
The **Parent Organization** field in CPSC Contract Info is the key to aggregating by payer:
- UnitedHealth Group
- Humana Inc.
- CVS Health (Aetna)
- Cigna Healthcare
- Kaiser Foundation
- etc.

### Crosswalk Importance
Plan crosswalks (2022-2026) are critical for:
- Tracking plans across years (mergers, renewals)
- Historical enrollment trending
- Avoiding duplicate counting

### Star Ratings Structure
Stars come in multiple tables:
- Summary (Overall, Part C, Part D ratings)
- Measure-level (individual measure scores)
- Domain-level (grouped by category)
- Cut points (thresholds for 1-5 stars)

## Unified Data Architecture

All CMS sources are unified into a queryable data model with **full audit lineage**.

### How Data Connects

```
                    CROSSWALK (2006-2026)
                    ═══════════════════
                    Anchors entity identity across years
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       dim_entity                                     │
│                 (stable entity_id per plan)                          │
└─────────────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ Enrollment    │   │     CPSC      │   │  SNP Report   │
│ by Plan       │   │               │   │               │
│               │   │ • Parent Org  │   │ • D-SNP/C-SNP │
│ • Enrollment  │   │ • Plan Type   │   │   /I-SNP type │
│   (no supp)   │   │ • EGHP        │   │               │
│               │   │ • County      │   │               │
└───────────────┘   └───────────────┘   └───────────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    fact_enrollment_unified                           │
│                                                                      │
│  contract_id + plan_id + year + month                               │
│  + parent_org + plan_type + product_type + group_type + snp_type    │
│  + enrollment + group_type_confidence + snp_type_source             │
└─────────────────────────────────────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            │               │               │
            ▼               ▼               ▼
     ┌──────────┐    ┌──────────┐    ┌──────────┐
     │  Stars   │    │  Risk    │    │  Geo     │
     │  (JOIN:  │    │  Scores  │    │  (JOIN:  │
     │ contract │    │  (JOIN:  │    │ contract │
     │ + year)  │    │ contract │    │ + plan + │
     │          │    │ + plan   │    │ county)  │
     │          │    │ + year)  │    │          │
     └──────────┘    └──────────┘    └──────────┘
```

### Query Any Combination

With this architecture, you can query ANY filter combination:
- Stars by State
- Risk Score by D-SNP vs Non-SNP
- Enrollment by Plan Type + Group Type + State
- Market Share by Parent Org over Time

### Full Audit Trail

Every data point traces back to source:
```
Query Result
  └── audit_id: "abc-123"
        └── Tables Accessed: [fact_enrollment_unified, fact_star_ratings]
              └── Source Files:
                    ├── CMS Monthly Enrollment by Plan (2026-01)
                    ├── CMS CPSC Contract Info (2026-01)
                    └── CMS Star Ratings (2026)
                          └── MD5 Hash: "d41d8cd98f00b204e9800998ecf8427e"
```

## Running the Build Pipeline

```bash
# Build everything (dimensions → facts → aggregations → catalog)
python -m scripts.unified.run_full_build --all

# Or run individual phases
python -m scripts.unified.run_full_build --dimensions
python -m scripts.unified.run_full_build --facts
python -m scripts.unified.run_full_build --aggregations
```

## Deployment (Railway)

```bash
# Connect to Railway
railway link

# Deploy
railway up
```

Environment variables needed:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET`

## Scripts

```
scripts/
├── unified/                       # Unified build pipeline
│   ├── run_full_build.py         # Master orchestrator
│   ├── build_entity_chains.py    # Crosswalk → entity IDs
│   ├── build_parent_org_dimension.py
│   ├── build_fact_enrollment_unified.py
│   ├── build_fact_enrollment_geographic.py
│   ├── build_aggregation_tables.py
│   ├── reconcile_and_validate.py
│   ├── audit_lineage.py          # Lineage tracking
│   └── build_data_catalog.py     # Documentation
│
├── download_cpsc.py               # CPSC enrollment downloader
├── download_stars_complete.py     # Star ratings downloader
├── download_snp.py                # SNP downloader
├── download_enrollment_*.py       # Enrollment downloaders
├── download_ratebooks.py          # Ratebook downloader
├── download_risk_adjustment.py    # Risk adjustment downloader
├── download_crosswalks.py         # Crosswalk downloader
└── download_hedis*.py             # HEDIS downloaders
```
