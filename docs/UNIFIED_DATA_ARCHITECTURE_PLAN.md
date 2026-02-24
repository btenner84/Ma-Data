# MA Intelligence Platform - Unified Data Architecture Plan

## Executive Summary

This document defines the complete data architecture for building a unified, AI-queryable Medicare Advantage data platform. The goal is to enable ANY filter combination query while maintaining data integrity, tracking discrepancies, and supporting longitudinal analysis across 20 years of data.

---

## Part 1: Data Sources Inventory

### 1.1 Primary Data Sources

| Source | Files | Years | Grain | Key Fields |
|--------|-------|-------|-------|------------|
| **CPSC Enrollment** | 158 | 2013-2026 | contract+plan+county+month | enrollment, geography |
| **Enrollment by Plan** | 230 | 2007-2026 | contract+plan+month | enrollment (no suppression) |
| **Enrollment by Contract** | 230 | 2007-2026 | contract+month | enrollment totals |
| **SNP Report** | 219 | 2007-2026 | contract+plan+month | D-SNP/C-SNP/I-SNP, enrollment |
| **Stars Ratings** | 27 | 2007-2026 | contract+year | ratings, parent_org, group breakdown |
| **Risk Scores (Plan Payment)** | 19 | 2006-2024 | contract+plan+year | risk_score, payments |
| **Crosswalks** | 21 | 2006-2026 | contract+plan (year N-1 → N) | plan ID mappings |
| **Ratebooks** | 11 | 2016-2026 | county+year | benchmark rates |

### 1.2 What Each Source Provides

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE CAPABILITIES                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ENROLLMENT BY PLAN (Authoritative for totals)                              │
│  ├── enrollment        ✓ (no suppression)                                   │
│  ├── contract_id       ✓                                                    │
│  ├── plan_id           ✓                                                    │
│  ├── parent_org        ✗                                                    │
│  ├── plan_type         ✗                                                    │
│  ├── group_type        ✗                                                    │
│  ├── snp_type          ✗                                                    │
│  └── geography         ✗                                                    │
│                                                                             │
│  CPSC (Authoritative for geography + dimensions)                            │
│  ├── enrollment        ✓ (suppressed < 11)                                  │
│  ├── contract_id       ✓                                                    │
│  ├── plan_id           ✓                                                    │
│  ├── parent_org        ✓ (Contract_Info sheet)                              │
│  ├── plan_type         ✓ (Contract_Info sheet)                              │
│  ├── group_type        ✓ (EGHP field in Contract_Info)                      │
│  ├── snp_type          ~ (Yes/No only, not D/C/I)                           │
│  └── geography         ✓ (state, county, FIPS)                              │
│                                                                             │
│  SNP REPORT (Authoritative for SNP detail)                                  │
│  ├── enrollment        ✓ (SNP plans only)                                   │
│  ├── contract_id       ✓                                                    │
│  ├── plan_id           ✓                                                    │
│  ├── parent_org        ~ (org name, not normalized)                         │
│  ├── plan_type         ✓                                                    │
│  ├── group_type        ✗                                                    │
│  ├── snp_type          ✓ (D-SNP, C-SNP, I-SNP)                              │
│  └── geography         ~ (states served, not county)                        │
│                                                                             │
│  STARS (Authoritative for quality + parent org)                             │
│  ├── enrollment        ✓ (by group_type + snp_type segments)                │
│  ├── contract_id       ✓                                                    │
│  ├── plan_id           ✗ (contract level only)                              │
│  ├── parent_org        ✓ (best source, normalized)                          │
│  ├── plan_type         ✓                                                    │
│  ├── group_type        ✓ (Individual vs Group segments)                     │
│  ├── snp_type          ~ (SNP vs Non-SNP only)                              │
│  ├── geography         ✗                                                    │
│  └── star_rating       ✓                                                    │
│                                                                             │
│  CROSSWALKS (Authoritative for entity tracking)                             │
│  ├── prev_contract_id  ✓                                                    │
│  ├── prev_plan_id      ✓                                                    │
│  ├── curr_contract_id  ✓                                                    │
│  ├── curr_plan_id      ✓                                                    │
│  └── snp_type          ~ (in newer files)                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Part 2: Known Data Quality Issues

### 2.1 CPSC Suppression

**Problem**: CPSC shows "*" for enrollment counts < 11 (HIPAA requirement)

**Impact**:
- CPSC county-level sum < actual total enrollment
- Estimated loss: 1-3% of total enrollment
- Higher loss in rural counties with small plans

**Solution**:
- Use Enrollment by Plan for authoritative totals
- Track suppression loss by state/county
- Allocate suppressed enrollment proportionally when needed

### 2.2 Product Type Summation

**Problem**: MA-only + MAPD + PDP may not equal 100% of total

**Causes**:
- PACE plans (separate category)
- Cost plans (1876 Cost, HCPP)
- Demonstration plans
- Employer direct contracts with ambiguous classification

**Solution**:
- Track "Other" category for edge cases
- Log discrepancy percentage
- Document known exclusions

### 2.3 Group Type Availability

**Problem**: Group vs Individual only explicitly available at contract level (Stars)

**Sources for group_type**:
1. **EGHP field** (CPSC Contract_Info) - Best, plan-level, but not always populated
2. **Plan ID >= 800** - Heuristic, ~90% accurate
3. **Stars segments** - Contract level only, need proportional allocation

**Solution**:
- Use EGHP first, plan_id heuristic second
- Track confidence score for each derivation method
- Validate against Stars contract-level totals

### 2.4 SNP Type Fragmentation

**Problem**: Specific SNP types (D/C/I) only in SNP Report, which only has SNP plans

**Solution**:
- SNP Report for D-SNP/C-SNP/I-SNP
- CPSC SNP flag for SNP vs Non-SNP when detail not available
- Join on contract+plan+year+month

### 2.5 Parent Organization Inconsistencies

**Problem**: Name variations, M&A changes, trailing whitespace

**Known M&A Events**:
```
2018: Aetna → CVS Health Corporation
2020: WellCare → Centene Corporation
2022: Magellan Health → Centene Corporation
2022: Anthem → Elevance Health (rebrand)
2023: CIGNA → The Cigna Group (rebrand)
```

**Solution**:
- Canonical name mapping table
- Year-specific parent_org tracking
- Fuzzy matching for historical alignment

### 2.6 Crosswalk Schema Evolution

**Problem**: Crosswalk file format varies by year

**Schema Variations**:
- 2006-2012: Old format, different column names
- 2013-2020: Intermediate format
- 2021-2026: New format with additional fields

**Solution**:
- Schema adapter per era
- Unified output format regardless of input

---

## Part 3: Target Data Model

### 3.1 Dimension Tables

```sql
-- dim_entity: Stable entity tracking across years via crosswalk chains
CREATE TABLE dim_entity (
    entity_id           UUID PRIMARY KEY,
    entity_type         VARCHAR(20),      -- 'plan', 'contract', 'payer'

    -- Current identifiers (latest year)
    current_contract_id VARCHAR(10),
    current_plan_id     VARCHAR(5),

    -- Lifecycle
    first_year          INT,
    last_year           INT,
    is_active           BOOLEAN,

    -- Identity chain JSON: [{year, contract_id, plan_id, source}, ...]
    identity_chain      JSONB,

    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

-- dim_parent_org: Canonical parent organization with M&A history
CREATE TABLE dim_parent_org (
    parent_org_id       UUID PRIMARY KEY,
    canonical_name      VARCHAR(200),     -- Current/final name

    -- Name history: [{year, name, source}, ...]
    name_history        JSONB,

    -- M&A history: [{year, event_type, acquired_org, source}, ...]
    ma_history          JSONB,

    is_active           BOOLEAN,
    created_at          TIMESTAMP
);

-- dim_geography: State/County reference
CREATE TABLE dim_geography (
    geo_id              UUID PRIMARY KEY,
    state_code          VARCHAR(2),
    state_name          VARCHAR(50),
    county_fips         VARCHAR(5),
    county_name         VARCHAR(100),
    cbsa_code           VARCHAR(10),
    cbsa_name           VARCHAR(100),
    urban_rural         VARCHAR(10)
);

-- dim_time: Time reference with MA-specific attributes
CREATE TABLE dim_time (
    time_id             INT PRIMARY KEY,  -- YYYYMM format
    year                INT,
    month               INT,
    quarter             INT,
    is_star_year_start  BOOLEAN,          -- October = new star year
    is_aca_year_start   BOOLEAN,          -- January = new AEP year
    payment_year        INT               -- Stars payment year alignment
);
```

### 3.2 Fact Tables

```sql
-- fact_enrollment_unified: Master enrollment fact (plan+month grain)
-- Source: Enrollment by Plan (totals) + CPSC Contract_Info (dimensions)
CREATE TABLE fact_enrollment_unified (
    -- Keys
    entity_id           UUID REFERENCES dim_entity,
    time_id             INT REFERENCES dim_time,

    -- Identifiers
    contract_id         VARCHAR(10),
    plan_id             VARCHAR(5),
    year                INT,
    month               INT,

    -- Dimensions (all available at this grain)
    parent_org_id       UUID REFERENCES dim_parent_org,
    plan_type           VARCHAR(50),
    plan_type_simplified VARCHAR(20),     -- HMO, PPO, PFFS, MSA, Other
    product_type        VARCHAR(20),      -- MA-only, MAPD, PDP
    group_type          VARCHAR(20),      -- Individual, Group
    group_type_source   VARCHAR(30),      -- EGHP, plan_id_heuristic, stars_proportional
    group_type_confidence DECIMAL(3,2),   -- 0.0-1.0
    snp_type            VARCHAR(20),      -- D-SNP, C-SNP, I-SNP, Non-SNP
    snp_type_source     VARCHAR(30),      -- snp_report, cpsc_flag, inferred

    -- Measures
    enrollment          INT,
    enrollment_source   VARCHAR(30),      -- enrollment_by_plan, cpsc, stars

    -- Metadata
    data_version        VARCHAR(20),
    created_at          TIMESTAMP,

    PRIMARY KEY (entity_id, time_id)
);

-- fact_enrollment_geographic: Geographic detail (plan+county+month grain)
-- Source: CPSC (with suppression tracking)
CREATE TABLE fact_enrollment_geographic (
    -- Keys
    entity_id           UUID REFERENCES dim_entity,
    geo_id              UUID REFERENCES dim_geography,
    time_id             INT REFERENCES dim_time,

    -- Identifiers
    contract_id         VARCHAR(10),
    plan_id             VARCHAR(5),
    year                INT,
    month               INT,
    state_code          VARCHAR(2),
    county_fips         VARCHAR(5),

    -- Dimensions (inherited from unified)
    parent_org_id       UUID REFERENCES dim_parent_org,
    plan_type           VARCHAR(50),
    product_type        VARCHAR(20),
    group_type          VARCHAR(20),
    snp_type            VARCHAR(20),

    -- Measures
    enrollment          INT,              -- NULL if suppressed
    is_suppressed       BOOLEAN,          -- True if original was "*"

    -- Metadata
    created_at          TIMESTAMP,

    PRIMARY KEY (entity_id, geo_id, time_id)
);

-- fact_star_ratings: Quality ratings (contract+year grain)
-- Source: Stars files
CREATE TABLE fact_star_ratings (
    -- Keys
    contract_id         VARCHAR(10),
    star_year           INT,              -- Rating year (applies to next payment year)

    -- Dimensions
    parent_org_id       UUID REFERENCES dim_parent_org,
    plan_type           VARCHAR(50),

    -- Star Ratings
    overall_rating      DECIMAL(2,1),
    part_c_rating       DECIMAL(2,1),
    part_d_rating       DECIMAL(2,1),

    -- Enrollment by Segment (from Stars file)
    enrollment_total    INT,
    enrollment_individual INT,
    enrollment_group    INT,
    enrollment_snp      INT,
    enrollment_non_snp  INT,

    -- Metadata
    created_at          TIMESTAMP,

    PRIMARY KEY (contract_id, star_year)
);

-- fact_risk_scores: Risk adjustment (plan+year grain)
-- Source: Plan Payment files
CREATE TABLE fact_risk_scores (
    -- Keys
    entity_id           UUID REFERENCES dim_entity,
    contract_id         VARCHAR(10),
    plan_id             VARCHAR(5),
    year                INT,

    -- Measures
    avg_risk_score      DECIMAL(4,3),
    avg_ab_pmpm         DECIMAL(10,2),
    avg_rebate_pmpm     DECIMAL(10,2),

    -- Metadata
    created_at          TIMESTAMP,

    PRIMARY KEY (entity_id, year)
);
```

### 3.3 Reconciliation Tables

```sql
-- reconciliation_totals: Track discrepancies between sources
CREATE TABLE reconciliation_totals (
    recon_id            UUID PRIMARY KEY,
    year                INT,
    month               INT,
    metric              VARCHAR(50),      -- total_ma, total_mapd, etc.

    -- Source values
    enrollment_by_plan  BIGINT,
    cpsc_sum            BIGINT,
    stars_sum           BIGINT,
    snp_report_sum      BIGINT,

    -- Discrepancies
    cpsc_suppression_loss BIGINT,
    cpsc_suppression_pct DECIMAL(5,2),
    stars_unrated_loss  BIGINT,

    -- Validation
    is_valid            BOOLEAN,
    validation_notes    TEXT,

    created_at          TIMESTAMP
);

-- reconciliation_dimensions: Track dimension breakdowns summing correctly
CREATE TABLE reconciliation_dimensions (
    recon_id            UUID PRIMARY KEY,
    year                INT,
    month               INT,
    dimension           VARCHAR(50),      -- product_type, group_type, snp_type

    -- Component values
    component_values    JSONB,            -- {"MA-only": 1000, "MAPD": 2000, ...}
    component_sum       BIGINT,
    expected_total      BIGINT,

    -- Discrepancy
    discrepancy         BIGINT,
    discrepancy_pct     DECIMAL(5,2),
    discrepancy_reason  TEXT,

    created_at          TIMESTAMP
);
```

### 3.4 Aggregation Tables (Pre-computed)

```sql
-- agg_enrollment_by_parent_year: Fast payer comparison
CREATE TABLE agg_enrollment_by_parent_year (
    parent_org_id       UUID,
    year                INT,

    -- Totals
    total_enrollment    BIGINT,
    ma_enrollment       BIGINT,
    mapd_enrollment     BIGINT,
    pdp_enrollment      BIGINT,

    -- By Group Type
    individual_enrollment BIGINT,
    group_enrollment    BIGINT,

    -- By SNP Type
    dsnp_enrollment     BIGINT,
    csnp_enrollment     BIGINT,
    isnp_enrollment     BIGINT,
    non_snp_enrollment  BIGINT,

    -- Counts
    contract_count      INT,
    plan_count          INT,
    state_count         INT,

    -- Quality
    avg_star_rating     DECIMAL(2,1),
    wavg_star_rating    DECIMAL(3,2),

    -- Risk
    avg_risk_score      DECIMAL(4,3),
    wavg_risk_score     DECIMAL(4,3),

    PRIMARY KEY (parent_org_id, year)
);

-- agg_enrollment_by_state_year: Geographic rollup
CREATE TABLE agg_enrollment_by_state_year (
    state_code          VARCHAR(2),
    year                INT,

    -- Totals
    total_enrollment    BIGINT,
    suppressed_estimate BIGINT,           -- Estimated suppressed enrollment

    -- By Product Type
    ma_enrollment       BIGINT,
    mapd_enrollment     BIGINT,
    pdp_enrollment      BIGINT,

    -- By Group Type
    individual_enrollment BIGINT,
    group_enrollment    BIGINT,

    -- Counts
    contract_count      INT,
    plan_count          INT,
    county_count        INT,
    parent_org_count    INT,

    PRIMARY KEY (state_code, year)
);

-- agg_enrollment_by_dimensions: All dimension combinations
CREATE TABLE agg_enrollment_by_dimensions (
    year                INT,
    month               INT,
    plan_type_simplified VARCHAR(20),
    product_type        VARCHAR(20),
    group_type          VARCHAR(20),
    snp_type            VARCHAR(20),

    -- Measures
    enrollment          BIGINT,
    contract_count      INT,
    plan_count          INT,
    parent_org_count    INT,

    PRIMARY KEY (year, month, plan_type_simplified, product_type, group_type, snp_type)
);
```

---

## Part 4: Build Pipeline

### 4.1 Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BUILD PIPELINE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  PHASE 1: EXTRACT & LOAD RAW                                                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  download_*.py scripts → S3 raw/ bucket                             │   │
│  │  Already complete: 900+ files in S3                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  PHASE 2: BUILD DIMENSION TABLES                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 2.1: build_entity_chains.py                                   │   │
│  │            Load all crosswalks → Build entity_id chains             │   │
│  │            Output: dim_entity.parquet                               │   │
│  │                                                                     │   │
│  │  Step 2.2: build_parent_org_dimension.py                           │   │
│  │            Load Stars + CPSC Contract_Info → Normalize names        │   │
│  │            Output: dim_parent_org.parquet                           │   │
│  │                                                                     │   │
│  │  Step 2.3: build_geography_dimension.py                            │   │
│  │            Load CPSC → Extract unique state/county/FIPS             │   │
│  │            Output: dim_geography.parquet                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  PHASE 3: BUILD FACT TABLES                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 3.1: build_fact_enrollment_unified.py                         │   │
│  │            Source: Enrollment by Plan + CPSC Contract_Info          │   │
│  │            Join: SNP Report for snp_type detail                     │   │
│  │            Derive: group_type from EGHP/plan_id                     │   │
│  │            Output: fact_enrollment_unified.parquet                  │   │
│  │                                                                     │   │
│  │  Step 3.2: build_fact_enrollment_geographic.py                      │   │
│  │            Source: CPSC Enrollment_Info                             │   │
│  │            Join: Dimensions from Step 3.1                           │   │
│  │            Track: Suppressed values                                 │   │
│  │            Output: fact_enrollment_geographic.parquet               │   │
│  │                                                                     │   │
│  │  Step 3.3: build_fact_star_ratings.py                               │   │
│  │            Source: Stars files                                      │   │
│  │            Output: fact_star_ratings.parquet                        │   │
│  │                                                                     │   │
│  │  Step 3.4: build_fact_risk_scores.py                                │   │
│  │            Source: Plan Payment files                               │   │
│  │            Join: entity_id from dim_entity                          │   │
│  │            Output: fact_risk_scores.parquet                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  PHASE 4: RECONCILIATION & VALIDATION                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 4.1: reconcile_totals.py                                      │   │
│  │            Compare: Enrollment by Plan vs CPSC vs Stars             │   │
│  │            Track: Suppression loss, unrated contracts               │   │
│  │            Output: reconciliation_totals.parquet                    │   │
│  │                                                                     │   │
│  │  Step 4.2: validate_dimensions.py                                   │   │
│  │            Verify: Dimension breakdowns sum to totals               │   │
│  │            Log: Discrepancies with explanations                     │   │
│  │            Output: reconciliation_dimensions.parquet                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  PHASE 5: BUILD AGGREGATION TABLES                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 5.1: build_agg_by_parent_year.py                              │   │
│  │  Step 5.2: build_agg_by_state_year.py                               │   │
│  │  Step 5.3: build_agg_by_dimensions.py                               │   │
│  │            All validated against reconciliation tables              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  PHASE 6: SEMANTIC LAYER                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Step 6.1: generate_semantic_config.py                              │   │
│  │            Create: semantic_model.yaml                              │   │
│  │            Define: Entities, measures, dimensions, constraints      │   │
│  │            Output: For AI system training/querying                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Output Files Structure

```
s3://ma-data123/
├── raw/                          # Original CMS files (existing)
│   ├── enrollment/
│   ├── snp/
│   ├── stars/
│   ├── plan_payment/
│   └── crosswalks/
│
├── processed/
│   ├── dimensions/               # Dimension tables
│   │   ├── dim_entity.parquet
│   │   ├── dim_parent_org.parquet
│   │   ├── dim_geography.parquet
│   │   └── dim_time.parquet
│   │
│   ├── facts/                    # Fact tables
│   │   ├── fact_enrollment_unified/
│   │   │   └── year=YYYY/month=MM/data.parquet
│   │   ├── fact_enrollment_geographic/
│   │   │   └── year=YYYY/month=MM/state=XX/data.parquet
│   │   ├── fact_star_ratings/
│   │   │   └── year=YYYY/data.parquet
│   │   └── fact_risk_scores/
│   │       └── year=YYYY/data.parquet
│   │
│   ├── aggregations/             # Pre-computed aggregations
│   │   ├── agg_by_parent_year.parquet
│   │   ├── agg_by_state_year.parquet
│   │   └── agg_by_dimensions.parquet
│   │
│   ├── reconciliation/           # Data quality tracking
│   │   ├── reconciliation_totals.parquet
│   │   └── reconciliation_dimensions.parquet
│   │
│   └── semantic/                 # AI configuration
│       ├── semantic_model.yaml
│       ├── entity_definitions.yaml
│       └── query_routing.yaml
│
└── exports/                      # Client-ready exports
    └── (future)
```

---

## Part 5: Implementation Details

### 5.1 Entity Chain Building (Crosswalks)

```python
# build_entity_chains.py - Core algorithm

def build_entity_chains():
    """
    Build stable entity IDs by chaining crosswalks from 2026 back to 2006.

    Algorithm:
    1. Start with all (contract_id, plan_id) pairs from most recent year
    2. For each pair, walk backwards through crosswalks
    3. Build chain: [(year, contract, plan), (year-1, contract, plan), ...]
    4. Assign stable UUID to each chain
    """

    # Load all crosswalks
    crosswalks = {}
    for year in range(2007, 2027):
        try:
            cw = load_crosswalk(year)
            # Normalize column names across eras
            cw = normalize_crosswalk_schema(cw, year)
            crosswalks[year] = cw
        except FileNotFoundError:
            continue

    # Get all current-year plans
    current_plans = get_plans_for_year(2026)

    entities = []
    for contract_id, plan_id in current_plans:
        entity_id = uuid.uuid4()
        chain = [(2026, contract_id, plan_id, 'current')]

        curr_contract, curr_plan = contract_id, plan_id

        # Walk backwards through years
        for year in range(2026, 2006, -1):
            if year not in crosswalks:
                # No crosswalk, assume stable ID
                if year > 2006:
                    chain.append((year-1, curr_contract, curr_plan, 'assumed_stable'))
                continue

            cw = crosswalks[year]
            match = cw[
                (cw['current_contract_id'] == curr_contract) &
                (cw['current_plan_id'] == curr_plan)
            ]

            if not match.empty:
                prev_contract = match.iloc[0]['previous_contract_id']
                prev_plan = match.iloc[0]['previous_plan_id']
                chain.append((year-1, prev_contract, prev_plan, 'crosswalk'))
                curr_contract, curr_plan = prev_contract, prev_plan
            else:
                # Plan didn't exist or no mapping
                break

        entities.append({
            'entity_id': str(entity_id),
            'entity_type': 'plan',
            'current_contract_id': contract_id,
            'current_plan_id': plan_id,
            'first_year': chain[-1][0],
            'last_year': 2026,
            'is_active': True,
            'identity_chain': chain
        })

    return pd.DataFrame(entities)
```

### 5.2 Unified Enrollment Building

```python
# build_fact_enrollment_unified.py - Core algorithm

def build_fact_enrollment_unified():
    """
    Build the master unified enrollment fact table.

    Source hierarchy:
    1. Enrollment by Plan (authoritative totals)
    2. CPSC Contract_Info (dimensions: parent_org, plan_type, EGHP)
    3. SNP Report (snp_type detail)
    4. Stars (validation, secondary parent_org)
    """

    results = []

    for year in range(2007, 2027):
        for month in range(1, 13):
            print(f"Processing {year}-{month:02d}")

            # 1. Load enrollment totals (authoritative)
            enrollment = load_enrollment_by_plan(year, month)
            if enrollment is None:
                continue

            # 2. Load CPSC Contract_Info for dimensions
            cpsc_info = load_cpsc_contract_info(year, month)

            # 3. Join dimensions
            df = enrollment.merge(
                cpsc_info[[
                    'contract_id', 'plan_id', 'parent_org', 'plan_type',
                    'offers_part_d', 'snp_plan', 'eghp'
                ]],
                on=['contract_id', 'plan_id'],
                how='left'
            )

            # 4. Derive group_type with confidence tracking
            df['group_type'], df['group_type_source'], df['group_type_confidence'] = \
                zip(*df.apply(derive_group_type, axis=1))

            # 5. Derive product_type
            df['product_type'] = df.apply(derive_product_type, axis=1)

            # 6. Simplify plan_type
            df['plan_type_simplified'] = df['plan_type'].apply(simplify_plan_type)

            # 7. Load and join SNP Report for specific SNP types
            snp = load_snp_report(year, month)
            if snp is not None:
                df = df.merge(
                    snp[['contract_id', 'plan_id', 'snp_type']],
                    on=['contract_id', 'plan_id'],
                    how='left',
                    suffixes=('', '_snp')
                )
                df['snp_type'] = df['snp_type_snp'].fillna(
                    df['snp_plan'].apply(lambda x: 'SNP-Unknown' if x == 'Yes' else 'Non-SNP')
                )
                df['snp_type_source'] = df['snp_type_snp'].apply(
                    lambda x: 'snp_report' if pd.notna(x) else 'cpsc_flag'
                )
            else:
                df['snp_type'] = df['snp_plan'].apply(
                    lambda x: 'SNP-Unknown' if x == 'Yes' else 'Non-SNP'
                )
                df['snp_type_source'] = 'cpsc_flag'

            # 8. Join entity_id
            entities = load_dim_entity()
            df = df.merge(
                entities[['entity_id', 'current_contract_id', 'current_plan_id']],
                left_on=['contract_id', 'plan_id'],
                right_on=['current_contract_id', 'current_plan_id'],
                how='left'
            )

            # 9. Join parent_org_id
            parent_orgs = load_dim_parent_org()
            df['parent_org_normalized'] = df['parent_org'].apply(normalize_parent_org_name)
            df = df.merge(
                parent_orgs[['parent_org_id', 'canonical_name']],
                left_on='parent_org_normalized',
                right_on='canonical_name',
                how='left'
            )

            # 10. Add metadata
            df['year'] = year
            df['month'] = month
            df['enrollment_source'] = 'enrollment_by_plan'
            df['data_version'] = 'v1.0'
            df['created_at'] = datetime.now()

            results.append(df)

    return pd.concat(results, ignore_index=True)

def derive_group_type(row):
    """
    Derive group_type with source tracking and confidence score.

    Returns: (group_type, source, confidence)
    """
    # Priority 1: EGHP field (explicit)
    if pd.notna(row.get('eghp')):
        if str(row['eghp']).lower() == 'yes':
            return ('Group', 'EGHP_explicit', 1.0)
        elif str(row['eghp']).lower() == 'no':
            return ('Individual', 'EGHP_explicit', 1.0)

    # Priority 2: Plan ID heuristic
    plan_id = row.get('plan_id', 0)
    try:
        plan_id_int = int(plan_id)
        if plan_id_int >= 800:
            return ('Group', 'plan_id_heuristic', 0.9)
        else:
            return ('Individual', 'plan_id_heuristic', 0.85)
    except (ValueError, TypeError):
        pass

    # Priority 3: Unknown
    return ('Unknown', 'unknown', 0.0)

def derive_product_type(row):
    """
    Derive product_type from contract prefix and Offers Part D.
    """
    contract_id = str(row.get('contract_id', ''))
    offers_part_d = str(row.get('offers_part_d', '')).lower()

    # PDP contracts start with S
    if contract_id.startswith('S'):
        return 'PDP'

    # MA contracts start with H or R
    if contract_id.startswith(('H', 'R')):
        if offers_part_d == 'yes':
            return 'MAPD'
        else:
            return 'MA-only'

    # E contracts are employer
    if contract_id.startswith('E'):
        return 'Employer'

    return 'Other'

def simplify_plan_type(plan_type):
    """
    Simplify plan_type to standard categories.
    """
    if pd.isna(plan_type):
        return 'Unknown'

    plan_type = str(plan_type).upper()

    if 'HMO' in plan_type:
        return 'HMO'
    elif 'PPO' in plan_type or 'LPPO' in plan_type:
        return 'PPO'
    elif 'RPPO' in plan_type or 'REGIONAL' in plan_type:
        return 'RPPO'
    elif 'PFFS' in plan_type:
        return 'PFFS'
    elif 'MSA' in plan_type:
        return 'MSA'
    elif 'PACE' in plan_type:
        return 'PACE'
    elif 'COST' in plan_type or 'HCPP' in plan_type:
        return 'Cost'
    elif 'PDP' in plan_type:
        return 'PDP'
    else:
        return 'Other'
```

### 5.3 Reconciliation Logic

```python
# reconcile_totals.py - Data quality tracking

def reconcile_totals():
    """
    Compare totals across sources and track discrepancies.
    """

    reconciliation = []

    for year in range(2007, 2027):
        for month in range(1, 13):
            # Load from each source
            enrollment_by_plan = load_enrollment_by_plan_total(year, month)
            cpsc_sum = load_cpsc_sum(year, month)
            stars_sum = load_stars_sum(year) if month == 1 else None
            snp_sum = load_snp_sum(year, month)

            if enrollment_by_plan is None:
                continue

            # Calculate discrepancies
            cpsc_suppression_loss = enrollment_by_plan - cpsc_sum if cpsc_sum else None
            cpsc_suppression_pct = (cpsc_suppression_loss / enrollment_by_plan * 100) if cpsc_suppression_loss else None

            stars_unrated_loss = enrollment_by_plan - stars_sum if stars_sum else None

            # Validate
            is_valid = True
            validation_notes = []

            if cpsc_suppression_pct and cpsc_suppression_pct > 5:
                is_valid = False
                validation_notes.append(f"CPSC suppression unusually high: {cpsc_suppression_pct:.1f}%")

            reconciliation.append({
                'year': year,
                'month': month,
                'metric': 'total_ma_enrollment',
                'enrollment_by_plan': enrollment_by_plan,
                'cpsc_sum': cpsc_sum,
                'stars_sum': stars_sum,
                'snp_report_sum': snp_sum,
                'cpsc_suppression_loss': cpsc_suppression_loss,
                'cpsc_suppression_pct': cpsc_suppression_pct,
                'stars_unrated_loss': stars_unrated_loss,
                'is_valid': is_valid,
                'validation_notes': '; '.join(validation_notes)
            })

    return pd.DataFrame(reconciliation)

def validate_dimension_sums():
    """
    Verify that dimension breakdowns sum to totals.
    """

    validations = []

    unified = load_fact_enrollment_unified()

    for year in unified['year'].unique():
        for month in unified[unified['year'] == year]['month'].unique():
            df = unified[(unified['year'] == year) & (unified['month'] == month)]
            total = df['enrollment'].sum()

            # Check product_type
            by_product = df.groupby('product_type')['enrollment'].sum()
            product_sum = by_product.sum()
            product_diff = total - product_sum

            validations.append({
                'year': year,
                'month': month,
                'dimension': 'product_type',
                'component_values': by_product.to_dict(),
                'component_sum': product_sum,
                'expected_total': total,
                'discrepancy': product_diff,
                'discrepancy_pct': (product_diff / total * 100) if total > 0 else 0,
                'discrepancy_reason': 'Should be 0 - data issue' if product_diff != 0 else None
            })

            # Check group_type
            by_group = df.groupby('group_type')['enrollment'].sum()
            group_sum = by_group.sum()
            group_diff = total - group_sum

            validations.append({
                'year': year,
                'month': month,
                'dimension': 'group_type',
                'component_values': by_group.to_dict(),
                'component_sum': group_sum,
                'expected_total': total,
                'discrepancy': group_diff,
                'discrepancy_pct': (group_diff / total * 100) if total > 0 else 0,
                'discrepancy_reason': 'Unknown group_type' if group_diff != 0 else None
            })

            # Check snp_type
            by_snp = df.groupby('snp_type')['enrollment'].sum()
            snp_sum = by_snp.sum()
            snp_diff = total - snp_sum

            validations.append({
                'year': year,
                'month': month,
                'dimension': 'snp_type',
                'component_values': by_snp.to_dict(),
                'component_sum': snp_sum,
                'expected_total': total,
                'discrepancy': snp_diff,
                'discrepancy_pct': (snp_diff / total * 100) if total > 0 else 0,
                'discrepancy_reason': 'Unknown snp_type' if snp_diff != 0 else None
            })

    return pd.DataFrame(validations)
```

---

## Part 6: AI Semantic Layer

### 6.1 Semantic Model Configuration

```yaml
# semantic_model.yaml

version: "1.0"
name: "MA Intelligence Platform"
description: "Medicare Advantage data model for AI-powered analytics"

# Entity Definitions
entities:
  payer:
    description: "Medicare Advantage insurer/parent organization"
    table: dim_parent_org
    primary_key: parent_org_id
    display_field: canonical_name
    aliases:
      - "insurer"
      - "carrier"
      - "parent org"
      - "company"
      - "organization"
      - "health plan"

  plan:
    description: "Specific MA plan (contract + plan_id combination)"
    table: dim_entity
    primary_key: entity_id
    display_field: "current_contract_id || '-' || current_plan_id"
    aliases:
      - "MA plan"
      - "Medicare Advantage plan"
      - "benefit package"
      - "plan option"

  contract:
    description: "CMS contract (can have multiple plans)"
    primary_key: contract_id
    aliases:
      - "MA contract"
      - "CMS contract"
      - "H-number"

# Measure Definitions
measures:
  enrollment:
    description: "Number of Medicare beneficiaries enrolled"
    calculation: "SUM(enrollment)"
    fact_table: fact_enrollment_unified
    aliases:
      - "members"
      - "beneficiaries"
      - "lives"
      - "membership"
      - "enrollees"
    format: "number"

  market_share:
    description: "Percentage of total MA enrollment"
    calculation: "(enrollment / total_enrollment) * 100"
    derived_from: enrollment
    unit: "percent"
    format: "percentage"

  risk_score:
    description: "CMS risk adjustment factor (higher = sicker population)"
    calculation: "AVG(avg_risk_score)"
    weighted_calculation: "SUM(avg_risk_score * enrollment) / SUM(enrollment)"
    fact_table: fact_risk_scores
    typical_range: [0.8, 1.5]
    aliases:
      - "RAF"
      - "risk adjustment factor"
      - "HCC score"
      - "acuity"
    format: "decimal"

  star_rating:
    description: "CMS quality rating (1-5 stars, higher is better)"
    calculation: "AVG(overall_rating)"
    weighted_calculation: "SUM(overall_rating * enrollment) / SUM(enrollment)"
    fact_table: fact_star_ratings
    typical_range: [1, 5]
    aliases:
      - "quality rating"
      - "stars"
      - "CMS rating"
      - "quality score"
    format: "decimal"

# Dimension Definitions
dimensions:
  plan_type:
    description: "Network structure of the plan"
    values:
      - HMO: "Health Maintenance Organization - closed network"
      - PPO: "Preferred Provider Organization - open network"
      - RPPO: "Regional PPO - multi-state network"
      - PFFS: "Private Fee-for-Service - any provider"
      - MSA: "Medical Savings Account"
      - PACE: "Program of All-Inclusive Care for the Elderly"
      - Cost: "Cost plan (legacy)"
      - Other: "Other plan types"
    available_in: [fact_enrollment_unified, fact_enrollment_geographic]

  product_type:
    description: "Medicare product category"
    values:
      - MA-only: "Medicare Advantage without drug coverage"
      - MAPD: "Medicare Advantage with Part D drug coverage"
      - PDP: "Standalone Part D drug plan"
      - Employer: "Employer group plans"
    available_in: [fact_enrollment_unified, fact_enrollment_geographic]

  group_type:
    description: "Market segment - individual vs employer group"
    values:
      - Individual: "Individual market enrollment"
      - Group: "Employer/union group market"
      - Unknown: "Unable to determine"
    available_in: [fact_enrollment_unified, fact_enrollment_geographic]
    confidence_field: group_type_confidence
    note: "Derived from EGHP field or plan_id heuristic. Check confidence score."

  snp_type:
    description: "Special Needs Plan type"
    values:
      - D-SNP: "Dual Eligible (Medicare + Medicaid)"
      - C-SNP: "Chronic Condition"
      - I-SNP: "Institutional (nursing facility)"
      - Non-SNP: "Not a Special Needs Plan"
      - SNP-Unknown: "SNP but type not specified"
    available_in: [fact_enrollment_unified, fact_enrollment_geographic]

  geography:
    hierarchy:
      - state: "US State"
      - county: "County within state"
    available_in: [fact_enrollment_geographic]
    note: "Geographic detail only available from CPSC (has suppression for <11 enrollees)"

# Query Routing Rules
query_routing:
  - pattern: "total|overall|industry|national"
    use_table: fact_enrollment_unified
    use_aggregation: agg_by_parent_year
    note: "For total/industry queries, use unified table (no suppression)"

  - pattern: "state|county|geographic|region|local"
    use_table: fact_enrollment_geographic
    use_aggregation: agg_by_state_year
    warning: "Geographic data has ~1-3% suppression for small county/plan combos"

  - pattern: "quality|star|rating"
    use_table: fact_star_ratings
    join_to: fact_enrollment_unified
    note: "Stars are at contract level, will apply to all plans in contract"

  - pattern: "risk|acuity|RAF|HCC"
    use_table: fact_risk_scores
    join_to: fact_enrollment_unified
    note: "Risk scores are annual, at plan level"

# Constraints and Limitations
constraints:
  - name: "suppression_acknowledgment"
    applies_to: [fact_enrollment_geographic]
    description: "County-level data suppresses enrollment <11 for HIPAA"
    user_message: "Note: Geographic totals may be ~1-3% lower than national totals due to HIPAA suppression of small county/plan combinations."

  - name: "group_type_confidence"
    applies_to: [group_type]
    description: "Group type is derived, not always explicit"
    user_message: "Group type is derived from EGHP field or plan ID patterns. Confidence varies by source."

  - name: "snp_detail_coverage"
    applies_to: [snp_type]
    description: "Specific SNP types (D/C/I) only available for SNP plans"
    user_message: "D-SNP/C-SNP/I-SNP detail comes from SNP Report. Some SNP plans may show as 'SNP-Unknown' if not in SNP Report."

  - name: "crosswalk_coverage"
    applies_to: [entity_id]
    description: "Crosswalk data available 2006-2026"
    user_message: "Entity tracking uses CMS crosswalk files. Pre-2006 tracking may rely on stable ID inference."

# Validation Rules
validations:
  - name: "dimension_sums"
    description: "All dimension breakdowns should sum to total"
    checks:
      - "SUM(enrollment by product_type) = total enrollment"
      - "SUM(enrollment by group_type) = total enrollment (excluding Unknown)"
      - "SUM(enrollment by snp_type) = total enrollment"
    tolerance: 0.1  # 0.1% tolerance

  - name: "entity_coverage"
    description: "All enrollment should have entity_id"
    check: "COUNT(enrollment WHERE entity_id IS NULL) / total < 0.01"
    tolerance: 1.0  # 1% tolerance for very new plans
```

---

## Part 7: Implementation Checklist

### Phase 1: Dimensions (Week 1)
- [ ] build_entity_chains.py - Parse all crosswalks, build entity chains
- [ ] build_parent_org_dimension.py - Normalize names, track M&A
- [ ] build_geography_dimension.py - Extract state/county/FIPS
- [ ] build_time_dimension.py - Generate time reference table

### Phase 2: Fact Tables (Week 2)
- [ ] build_fact_enrollment_unified.py - Master enrollment fact
- [ ] build_fact_enrollment_geographic.py - Geographic detail with suppression
- [ ] build_fact_star_ratings.py - Star ratings by contract
- [ ] build_fact_risk_scores.py - Risk scores by plan

### Phase 3: Validation (Week 3)
- [ ] reconcile_totals.py - Compare source totals
- [ ] validate_dimensions.py - Verify dimension sums
- [ ] validate_entities.py - Check entity coverage
- [ ] generate_data_quality_report.py - Summary report

### Phase 4: Aggregations (Week 3)
- [ ] build_agg_by_parent_year.py
- [ ] build_agg_by_state_year.py
- [ ] build_agg_by_dimensions.py

### Phase 5: API Update (Week 4)
- [ ] Update API endpoints to use new tables
- [ ] Add data source metadata to responses
- [ ] Add confidence scores to dimension filters
- [ ] Update filter options from new dimension tables

### Phase 6: Semantic Layer (Week 4)
- [ ] Generate semantic_model.yaml
- [ ] Generate query_routing.yaml
- [ ] Test AI query routing
- [ ] Document limitations and constraints

---

## Appendix A: Parent Organization Name Mapping

```python
PARENT_ORG_CANONICAL_MAP = {
    # Anthem → Elevance (2022 rebrand)
    'Anthem Inc.': 'Elevance Health, Inc.',
    'Anthem, Inc.': 'Elevance Health, Inc.',
    'Anthem Blue Cross': 'Elevance Health, Inc.',

    # CIGNA → The Cigna Group (2023 rebrand)
    'CIGNA': 'The Cigna Group',
    'Cigna Corporation': 'The Cigna Group',
    'CIGNA Corporation': 'The Cigna Group',

    # CVS acquired Aetna (2018)
    'Aetna Inc.': 'CVS Health Corporation',
    'Aetna, Inc.': 'CVS Health Corporation',

    # Centene acquired WellCare (2020)
    'WellCare Health Plans, Inc.': 'Centene Corporation',
    'WellCare Health Plans': 'Centene Corporation',

    # Centene acquired Magellan (2022)
    'Magellan Health, Inc.': 'Centene Corporation',

    # UnitedHealth variations
    'UnitedHealth Group': 'UnitedHealth Group, Inc.',
    'UnitedHealthcare': 'UnitedHealth Group, Inc.',
    'United Healthcare': 'UnitedHealth Group, Inc.',

    # Humana variations
    'Humana': 'Humana Inc.',
    'Humana Insurance Company': 'Humana Inc.',

    # Kaiser variations
    'Kaiser Foundation Health Plan': 'Kaiser Permanente',
    'Kaiser Foundation Health Plan, Inc.': 'Kaiser Permanente',
}
```

---

## Appendix B: Crosswalk Schema by Era

### 2022-2026 (New Format)
```
PREVIOUS_CONTRACT_NUMBER, PREVIOUS_PLAN_ID, PREVIOUS_SEGMENT_ID,
CURRENT_CONTRACT_NUMBER, CURRENT_PLAN_ID, CURRENT_SEGMENT_ID,
PREVIOUS_CONTRACT_MARKETING_NAME, CURRENT_CONTRACT_MARKETING_NAME,
PREVIOUS_PLAN_MARKETING_NAME, CURRENT_PLAN_MARKETING_NAME,
ORGANIZATION_TYPE, PLAN_TYPE, CURRENT_SNP_TYPE, AIP_D_SNP,
SERVICES_COVERED_BY_SNP, DSNP_ONLY_CONTRACT
```

### 2013-2021 (Intermediate Format)
```
Previous Contract ID, Previous Plan ID, Previous Segment ID,
New Contract ID, New Plan ID, New Segment ID,
Previous Contract Name, New Contract Name,
Previous Plan Name, New Plan Name,
Organization Type, Plan Type
```

### 2006-2012 (Old Format)
```
Old Contract Number, Old Plan ID, Old Segment ID,
New Contract Number, New Plan ID, New Segment ID,
Plan Type
```

---

## Appendix C: Suppression Estimation Method

For CPSC suppressed values ("*" = enrollment 1-10):

```python
def estimate_suppressed_enrollment(state, county, year, month):
    """
    Estimate suppressed enrollment using midpoint method.

    For each suppressed cell, estimate 5.5 (midpoint of 1-10).
    Track as separate field, don't add to actual enrollment.
    """
    suppressed_count = count_suppressed_cells(state, county, year, month)
    estimated_suppressed = suppressed_count * 5.5
    return estimated_suppressed
```

---

*Document Version: 1.0*
*Created: 2026-02-24*
*Author: MA Data Platform Team*
