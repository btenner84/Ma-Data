# Schema Evolution Timeline

This document tracks how CMS data schemas have changed over time.

---

## CPSC Data

### 2013 (Initial Release)
```
Contract Info: Contract ID, Plan ID, Organization Type, Plan Type,
               Offers Part D, SNP Plan, EGHP, Organization Name,
               Organization Marketing Name, Plan Name, Parent Organization,
               Contract Effective Date

Enrollment Info: Contract Number, Plan ID, SSA State County Code,
                 FIPS State County Code, State, County, Enrollment
```

### 2013-2026: NO CHANGES
Schema has remained completely stable for 13+ years.

---

## Star Ratings Data

### 2007-2009: Original Format
- Basic plan ratings
- Single file per year
- Part C only initially
- Columns: Contract Number, Organization Type, Parent Organization,
           Organization Marketing Name, Summary Score

### 2010-2012: Part C/D Split
- Separate Part C and Part D files
- Added domain-level ratings
- Added measure-level data
- New files: cutpoints, domain, stars, summary, data

### 2013-2014: Consolidation Begins
- Combined Part C and Part D summary
- Overall rating introduced (2012 for 2013 ratings)
- Added high/low performing contract designations

### 2015-2018: Display Measures Added
- New "Display Measures" supplementary data
- More detailed measure specifications
- Technical notes standardized

### 2019-2023: Combined Era
- Single ZIP file: `YYYY-star-ratings-and-display-measures.zip`
- All tables in one download
- Standardized CSV naming

### 2024+: Separate Files Return
- `YYYY-star-ratings-data-tables.zip` (ratings)
- `YYYY-display-measures.zip` (display)
- Added disaster adjustment columns (COVID legacy)
- New columns: YYYY-2 Disaster %, YYYY-1 Disaster %

### Column Changes Over Time

| Column | Added | Notes |
|--------|-------|-------|
| Overall Rating | 2012 | Combined Part C + D |
| SNP indicator | 2015 | Yes/No flag |
| Disaster % | 2021 | COVID adjustments |
| CAI adjustments | 2016 | Categorical Adjustment Index |

---

## SNP Data

### 2007-2011: Basic Format
- Contract, Plan, Name, Type, Enrollment
- SNP Type (C-SNP, D-SNP, I-SNP)
- Limited columns

### 2012-2021: Expanded
- Added Geographic Name
- Added Specialty Diseases (C-SNP)
- Added Integration Status (D-SNP)

### 2022: D-SNP Summary
- New summary table for D-SNP integration
- Counts by integration type (CO, FIDE, HIDE)

### 2025: New Columns
- Partial Dual (Yes/No)
- DSNP Only Contract (Yes/No)
- AIP D-SNP summary table

---

## Enrollment Files

### 2007-2015: Basic
- Contract/Plan level counts
- Total enrollment
- Basic plan type

### 2016-2019: Enhanced
- Breakdowns by enrollment type
- More plan type granularity
- Added state-level aggregations

### 2020+: COVID Era
- Special enrollment periods tracked
- Additional reporting fields
- Enhanced data quality notes

---

## Ratebooks

### 2016-2020: Original Format
- State, County, FIPS
- Aged, Disabled, ESRD benchmarks
- Single rate components

### 2021+: Enhanced
- Multiple rate components
- Coding intensity adjustments
- Quality bonus payments

---

## File Format Changes

| Data Type | 2007-2015 | 2016-2019 | 2020+ |
|-----------|-----------|-----------|-------|
| CPSC | N/A | CSV in ZIP | CSV in ZIP |
| Enrollment | CSV/XLS | CSV in ZIP | CSV in ZIP |
| SNP | XLS | XLSX | XLSX |
| Stars | XLS/CSV | CSV in ZIP | CSV in ZIP |
| Ratebooks | N/A | CSV in ZIP | CSV in ZIP |

---

## Breaking Changes to Handle

### Stars 2010 vs 2026
```
2010: Contract Number, Organization Type, Parent Organization,
      Organization Marketing Name, Summary Score for Health Plan Quality

2026: Contract Number, Organization Type, Contract Name,
      Organization Marketing Name, Parent Organization, SNP,
      2023 Disaster %, 2024 Disaster %, 2026 Part C Summary,
      2026 Part D Summary, 2026 Overall
```

**Migration Notes**:
- Column order changed
- New columns added
- "Summary Score" → "Part C Summary" + "Part D Summary" + "Overall"
- Parent Organization moved position

### CPSC: No Breaking Changes
Schema stable since 2013 inception.

### SNP 2007 vs 2026
```
2007: Contract, Plan, Name, Type, Enrollment, SNP Type

2026: Contract Number, Contract Name, Organization Type, Plan ID,
      SEGMENT_ID, Plan Name, Plan Type, Geographic Name, State(s),
      Enrollment, Special Needs Plan Type, Specialty Diseases,
      Integration Status, Applicable Integrated Plan, Partial Dual,
      DSNP Only Contract
```

**Migration Notes**:
- Many new columns
- Need null handling for historical data
- Integration Status only applies to D-SNP

---

## Recommended ETL Strategy

1. **Load raw data by year/month**
2. **Apply schema mapping per era**
3. **Standardize column names** (create mapping table)
4. **Handle nulls** for columns that don't exist in older data
5. **Validate join keys** (Contract + Plan ID consistency)
6. **Output to unified Parquet schema** with all columns
