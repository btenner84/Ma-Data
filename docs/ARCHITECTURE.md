# MA Data Platform Architecture

## Vision
A unified Medicare Advantage intelligence platform that consolidates all CMS data sources into one queryable system. Enables AI-powered data analysis, UI generation, and custom software development.

## Core Principles

### 1. Enrollment is the Base (Spine)
Everything links to enrollment data via `Contract ID` + `Plan ID`. The CPSC (Contract/Plan/State/County) file is the atomic unit - most granular enrollment data available.

### 2. Crosswalks Enable Time Travel
Plans change IDs year-to-year (mergers, consolidations, renumbering). Crosswalks map: `H1234-001 (2024) → H9999-003 (2025)`

Without crosswalks: Can't track plan history
With crosswalks: Full longitudinal analysis

### 3. Join Keys

| Dataset | Primary Key | Joins To |
|---------|-------------|----------|
| CPSC Enrollment | Contract + Plan + County + Month | Everything |
| Stars | Contract | Enrollment via Contract |
| Risk Scores | Contract | Enrollment via Contract |
| Rates/Benchmarks | County FIPS | Enrollment via County |
| Crosswalks | Contract + Plan + Year | Prior/Next year enrollment |
| SNP | Contract + Plan | Enrollment |
| Benefits | Contract + Plan | Enrollment |

### 4. Data Layers

```
┌─────────────────────────────────────────────────────────────┐
│  RAW LAYER (S3: raw/)                                       │
│  - Original CMS ZIP/CSV files                               │
│  - Never modified, audit trail                              │
│  - Source of truth for reprocessing                         │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  PROCESSED LAYER (S3: processed/)                           │
│  - Parquet format (columnar, compressed)                    │
│  - Normalized schemas                                       │
│  - Partitioned by year/month                                │
│  - Queryable via Athena/DuckDB/Spark                        │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  UNIFIED LAYER (S3: processed/unified/)                     │
│  - Pre-joined tables                                        │
│  - Enrollment + Stars + Risk + Rates                        │
│  - Optimized for common queries                             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  EXPORT LAYER (S3: exports/)                                │
│  - Client-ready downloads                                   │
│  - CSV/Excel for humans                                     │
│  - Parquet/JSON for systems                                 │
└─────────────────────────────────────────────────────────────┘
```

## File Format Strategy

| Layer | Format | Reason |
|-------|--------|--------|
| Raw | ZIP/CSV | Preserve original CMS format |
| Processed | Parquet | 10-50x compression, columnar, fast |
| Exports | CSV/Excel/Parquet | Flexible client delivery |

### Why Parquet?
- Columnar storage = query only columns you need
- Built-in compression (snappy/zstd)
- Schema enforcement
- Partition pruning (skip irrelevant files)
- Works with: Athena, Spark, DuckDB, Pandas, Polars

## Partitioning Strategy

| Dataset | Partition Scheme | Reason |
|---------|------------------|--------|
| Enrollment (monthly) | `year=YYYY/month=MM/` | Query by time period |
| Stars (annual) | `year=YYYY/` | Annual ratings |
| Rates (annual) | `year=YYYY/` | Annual benchmarks |
| Crosswalks | `from_year=YYYY/to_year=YYYY/` | Year-to-year mapping |

## S3 Bucket Structure

```
s3://ma-data123/
├── raw/                          # Original CMS files
│   ├── enrollment/
│   │   ├── cpsc/                 # Contract/Plan/State/County (BASE)
│   │   ├── by_contract/
│   │   ├── by_plan/
│   │   ├── by_state/
│   │   ├── ma_state_county_contract/
│   │   ├── pdp_state_county_contract/
│   │   └── summary/
│   ├── penetration/
│   │   ├── ma/
│   │   └── pdp/
│   ├── service_area/
│   │   ├── ma_contract/
│   │   └── state/
│   ├── snp/
│   ├── lis/
│   │   ├── by_plan/
│   │   └── by_county/
│   ├── crosswalks/
│   ├── benefits/
│   ├── hedis/
│   │   ├── ma/
│   │   └── snp/
│   ├── stars/
│   │   ├── ratings/
│   │   ├── display_measures/
│   │   ├── cut_points/
│   │   └── cat_adj_index/
│   ├── rates/
│   │   ├── ratebook/
│   │   ├── part_d/
│   │   └── ffs/
│   └── risk_adjustment/
│       ├── model_software/
│       └── icd_mappings/
├── processed/                    # Parquet, normalized
│   └── [mirrors raw structure]
└── exports/                      # Client downloads
```

## AI Agent Integration (Future)

The platform is designed to support an AI agent that can:

1. **Natural Language Queries**
   - "Show me all 4+ star plans in Florida with >10k members"
   - "Compare UHC vs Humana enrollment growth 2020-2025"

2. **Dynamic UI Generation**
   - Generate charts, tables, dashboards on demand
   - Custom views per client need

3. **Data Pipeline Creation**
   - Agent can write SQL/Python to create new derived datasets
   - Automate recurring analysis

### Agent Architecture (Planned)
```
User Query → LLM → SQL/Code Generation → Execute on Data → Visualize/Export
```

## Data Quality Rules

1. **HIPAA Compliance**: Enrollment counts ≤10 suppressed as "*"
2. **Temporal Consistency**: Use crosswalks for year-over-year comparisons
3. **Contract vs Plan**: Stars are contract-level, benefits are plan-level
4. **County Codes**: SSA and FIPS codes both present, prefer FIPS for joins
