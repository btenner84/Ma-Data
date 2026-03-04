# MASTER FILE STRUCTURES - MA Data Platform

Generated: 2026-03-03

This document defines the EXACT structure of every file type across all years.
Each parser must follow these rules EXACTLY.

---

## FILE CATEGORIES OVERVIEW

| Category | Years | Files | Format | Key |
|----------|-------|-------|--------|-----|
| CAI | 2017-2026 | 22 | CSV | Contract Number |
| CROSSWALK | 2006-2026 | 25 | XLS | Contract ID chains |
| CUTPOINTS | 2007-2026 | 70 | CSV | Measure thresholds |
| DISENROLLMENT | 2020-2026 | 14 | CSV | Contract Number |
| DISPLAY_MEASURES | 2010-2026 | 111 | CSV | Contract Number |
| DOMAIN | 2008-2026 | 50 | CSV | Contract Number |
| ENROLLMENT | 2013-2026 | 208 | CSV | Contract+Plan |
| MEASURE_DATA | 2010-2018 | 45 | CSV | Contract ID |
| MEASURE_STARS | 2020-2026 | 30 | CSV | CONTRACT_ID |
| SNP | 2013-2026 | 132 | XLS | Contract+Plan |
| SUMMARY_RATING | 2009-2026 | 47 | CSV | Contract Number |

---

## 1. CAI (Contract Admin Info)

**Years:** 2017-2026 (10 years)
**Files per year:** 2 versions (different dates)

### Structure (CONSISTENT ALL YEARS):
```
Row 0: TITLE    - "2017 CAI View: Medicare Report Card Master Table"
Row 1: HEADER   - Contract Number, Organization Marketing Name, Contract Name, Parent Organization, Puerto Rico Only, ...
Row 2+: DATA   - E0654, TEAMStar Medicare..., IBT VOLUNTARY..., ...
```

### Parsing Rules:
- Skip row 0 (title)
- Header row = 1
- Data starts row 2
- Key column: "Contract Number"
- 11-12 columns

---

## 2. CROSSWALK

**Years:** 2006-2026 (gaps: 2021, 2025)
**Format:** Excel (.xls)

### Purpose:
Links contract IDs across years when contracts change numbers (H4892 -> H4963)

### Structure:
- Excel workbook with sheets per year
- Columns: Old Contract, New Contract, Effective Date, etc.

### Parsing Rules:
- Use openpyxl or xlrd
- Build entity chain from old -> new mappings
- Create stable entity_id for each chain

---

## 3. CUTPOINTS (STAR THRESHOLDS)

**Years:** 2007-2026 (20 years)
**COMPLEX - FORMAT VARIES BY YEAR**

### Structure Variations:

#### 2007 Format:
```
Row 0: TITLE   - "2007 Part D Performance Metrics Threshold for Star"
Row 1: EMPTY
Row 2: HEADER  - "Org. Type", "Number of Stars Displayed...", "Customer Service", ...
Row 3-5: Complex multi-row header with measure names split across rows
Row 6+: Star threshold data (1 Star, 2 Star, etc.)
```

#### 2008 Format (Part C):
```
Row 0: TITLE      - "2008 Part C Performance Metrics Threshold for Star"
Row 1: TITLE      - "Part C Performance Metrics Threshold for Star Assi..."
Row 2: DOMAIN     - "", "HELPING YOU STAY HEALTHY DOMAIN", ...
Row 3: MIN_MEASURES - "", "Minimum Number of Measures Needed..."
Row 4: DATE_ROW   - "", "01/01/2006-12/31/2006", ...
Row 5: MEASURES   - "Number of Stars Displayed...", "Breast Cancer Screening", "Colorectal Cancer Screening", ...
Row 6+: Star thresholds
```

#### 2009-2010 Format:
```
Row 0: TITLE       - "2009 Part C Performance Metrics Threshold for Star..."
Row 1: TITLE       - "Part C Performance Metrics Threshold for Star Assi..."
Row 2: DOMAIN      - "", "Staying Healthy: Screenings, Tests, and Vaccines", ...
Row 3: MIN_MEASURES
Row 4: DATE_ROW
Row 5: MEASURES    - "Number of Stars Displayed...", "Breast Cancer Screening", ...
Row 6+: Star thresholds
```

#### 2011 Format:
```
Row 0: TITLE
Row 1: TITLE
Row 2: DOMAIN
Row 3: MIN_MEASURES
Row 4: DATE_ROW
Row 5: MEASURE_IDS - "", "C01", "C02", "C03", "C04", ...
Row 6+: Star thresholds
```

#### 2012 Format:
```
Row 0: TITLE
Row 1: EMPTY
Row 2: DATE_ROW
Row 3: MEASURE_IDS - "", "C01", "C02", ...
Row 4: EMPTY
Row 5: STAR_THRESHOLD - "1 Star", values...
Row 6: STAR_THRESHOLD - "2 Star", values...
...
```

#### 2013-2020 Format (MODERN):
```
Row 0: TITLE        - "2013 Part C Performance Metrics..."
Row 1: EMPTY
Row 2: MEASURE_IDS  - "", "C01: Breast Cancer Screening", "C02: Colorectal...", ...
Row 3: DATE_ROW     - "", "1/1/2011 - 12/31/2011", ...
Row 4: STAR_1       - "1 Star", threshold values...
Row 5: STAR_2       - "2 Star", threshold values...
Row 6: STAR_3       - "3 Star", threshold values...
Row 7: STAR_4       - "4 Star", threshold values...
Row 8: STAR_5       - "5 Star", threshold values...
```

#### 2021-2026 Format (CURRENT):
```
Row 0: TITLE
Row 1: EMPTY
Row 2: MEASURE_IDS  - "", "C01: Breast Cancer Screening", "C02: ...", ...
Row 3: DATE_ROW     - "", "Measurement Period", ...
Row 4+: Star thresholds (format may vary)
```

### Parsing Rules:
- Detect year from filename/path
- Use year-specific parser
- Find row containing measure IDs (C01:, C02:, D01:, etc.)
- Find rows with "Star" in first column for thresholds
- Extract: year, part (C/D), measure_id, measure_name, star_level, threshold

---

## 4. DISENROLLMENT

**Years:** 2020-2026 (7 years)

### Structure (CONSISTENT):
```
Row 0: TITLE   - "2020 Disenrollment Reasons View: Medicare Report C..."
Row 1: HEADER  - Contract Number, Organization Marketing Name, Contract Name, Parent Organization, "Problems Getting Needed Care..."
Row 2+: DATA   - E0654, TEAMStar..., Ibt Voluntary..., IBT Voluntary..., Not Applicable
```

### Parsing Rules:
- Skip row 0 (title)
- Header row = 1
- Data starts row 2
- Key column: "Contract Number"

---

## 5. DISPLAY_MEASURES

**Years:** 2010-2026 (with gaps: 2012-2015, 2017-2018)

### Structure Variations:

#### 2010-2011:
- Complex format with appendix data
- Not standard measure format

#### 2019:
- Tab-separated within CSV cells (malformed)

#### 2020-2026 (STANDARD):
```
Row 0: TITLE        - "2020 Medicare Part C and D Display Measures"
Row 1: HEADER       - Contract Number, Organization Marketing Name, Contract Name, Parent Organization, Part C
Row 2: MEASURE_ROW  - "", "", "", "", "Follow-up Visit after Hospital Stay for Mental Ill..."
Row 3+: DATA        - E0654, TEAMStar..., Ibt Voluntary..., IBT Voluntary..., Plan not required...
```

### Parsing Rules:
- For 2020+: Header row = 1, Data starts row 3
- Measure names are in row 2
- Key column: "Contract Number"

---

## 6. DOMAIN

**Years:** 2008-2026 (19 years)

### Structure Variations:

#### 2008:
```
Row 0: TITLE   - "2008 Domain Star View: Medicare Part C Report Card..."
Row 1: HEADER  - Contract, Organization Name, Helping You Stay Healthy, Getting Care..., Getting Timely...
Row 2+: DATA   - H0097, QCC INSURANCE COMPANY, "", "", ""
```

#### 2009-2010:
Multiple variations - some have extra rows before header

#### 2011-2026 (DOMINANT):
```
Row 0: TITLE   - "2012 Domain Star View: Medicare Part C Report Card..."
Row 1: HEADER  - Contract Number, Organization Type, Parent Organization, Organization Marketing Name, "Staying Healthy..."
Row 2+: DATA   - H0084, Local CCP, XLHealth Corporation, Care Improvement Plus, Not enough data...
```

### Parsing Rules:
- Find header row (contains "Contract" in first column)
- Usually row 1 for 2012+
- Domain columns contain star ratings (1-5) or text ("Not enough data...")
- Key column: "Contract Number" or "Contract"

---

## 7. ENROLLMENT (CPSC_Enrollment_Info)

**Years:** 2013-2026 (monthly files)
**Files per year:** 12 (one per month)

### Structure (CONSISTENT):
```
Row 0: HEADER  - "Contract Number", "Plan ID", "SSA State County Code", "FIPS State County Code", "State"
Row 1+: DATA   - "E0654", "801", "62060", "", ""
```

### Parsing Rules:
- NO TITLE ROW - Header is row 0
- Values are quoted
- Key columns: Contract Number + Plan ID
- Contains county-level enrollment counts

---

## 8. MEASURE_DATA (2010-2018 ONLY)

**This category was replaced by MEASURE_STARS in 2020**

### Structure Variations:

#### 2010-2011:
```
Row 0: TITLE       - "CY2010 Medicare Part C and D Display Measures"
Row 1: NOTE        - "* See next worksheet for technical notes."
Row 2: EMPTY
Row 3: HEADER      - Contract Number, Organization Marketing Name, Contract Name, Part D, ""
Row 4: SUBHEADER   - "", "", "", "Access", ""
Row 5: DATE_ROW    - "", "", "", "1/1/2009 - 7/31/2009", ...
Row 6+: DATA
```

#### 2012:
```
Row 0: HEADER      - Contract Number, Contract Name, Parent Organization, Organization Type, Part C
Row 1: SUBHEADER   - "", "", "", "", "Follow-up Visit after Hospital Stay..."
Row 2: MEASURE_IDS - (empty or measure IDs)
Row 3+: DATA       - E0654, IBT VOLUNTARY..., IBT Voluntary..., Employer/Union..., Plan not required...
```

#### 2013:
```
Row 0: HEADER      - Contract Number, Contract Name, Parent Organization, Organization Type, Part C
Row 1: MEASURE_IDS - "", "", "", "", "Follow-up Visit after Hospital Stay..."
Row 2+: DATA       - E0654, IBT VOLUNTARY..., IBT Voluntary..., Employer/Union..., Plan not required...
```

#### 2014-2018 (MODERN):
```
Row 0: TITLE       - "CY2014 Medicare Part C and D Display Measures"
Row 1: HEADER      - Contract Number, Contract Name, Parent Organization, Organization Type, Part C
Row 2: MEASURE_ROW - "", "", "", "", "Follow-up Visit after Hospital Stay..."
Row 3+: DATA       - E0654, IBT VOLUNTARY..., IBT Voluntary..., Employer/Union..., Plan not required...
```

### Parsing Rules:
- Find header row containing "Contract Number"
- Measure names may be in separate row after header
- Data starts after header + measure row
- Key column: "Contract Number"

---

## 9. MEASURE_STARS (2020-2026)

**This is the PRIMARY measure data source for recent years**

### Structure (CONSISTENT 2020-2026):
```
Row 0: TITLE       - "2020 Data View: Medicare Report Card Master Table"
Row 1: HEADER      - CONTRACT_ID, Organization Type, Contract Name, Organization Marketing Name, Parent Organization, [measure columns starting col 5+]
Row 2: MEASURE_IDS - "", "", "", "", "", C01: Breast Cancer Screening, C02: Colorectal..., ...
Row 3: DATE_ROW    - "", "", "", "", "", 1/1/2018-12/31/2018, ...
Row 4+: DATA       - E0654, Employer/Union..., Ibt Voluntary..., TEAMStar..., IBT Voluntary..., [values]
```

### CRITICAL PARSING RULES:
1. Title row = 0 (SKIP)
2. Header row = 1 (CONTRACT_ID, Organization Type, etc.)
3. Measure IDs row = 2 (CONTAINS "C01:", "C02:", "D01:", etc.)
4. Date row = 3 (contains measurement periods)
5. Data starts row = 4

### Measure Column Detection:
- Columns 0-4 are contract info
- Columns 5+ contain measure data
- Row 2 has measure IDs in format: "C01: Breast Cancer Screening"
- Extract measure_id (C01) and measure_name (Breast Cancer Screening)

### Values:
- Star ratings: "1", "2", "3", "4", "5"
- Percentages: "89%", "0.89"
- Text: "Not enough data", "Plan not required to report measure", "Plan too new"

---

## 10. SNP (Special Needs Plans)

**Years:** 2013-2026 (monthly files)
**Format:** Excel (.xls)

### Structure:
- Excel workbooks
- Contains SNP plan data by contract+plan

### Parsing Rules:
- Use openpyxl or xlrd
- Key columns: Contract Number, Plan ID

---

## 11. SUMMARY_RATING

**Years:** 2009-2026 (18 years)

### Structure Variations:

#### 2009-2010:
```
Row 0: TITLE   - "2009 Summary Star View: Medicare Part C Report Car..."
Row 1: HEADER  - Contract Number, Organization Type, Parent Organization, Organization Marketing Name, Summary Score...
Row 2: DATA    - 90091, HCPP - 1833 Cost, UNITED MINE WORKERS..., United Mine Workers..., Not enough data...
```
(Note: Row 2 may contain cost plan IDs like 90091 instead of H/R/E contracts)

#### 2011-2012:
```
Row 0: TITLE
Row 1: EMPTY or HEADER
Row 2: DOMAIN_HEADER (optional)
Row 3: HEADER (if not row 1)
Row 4+: DATA
```

#### 2013-2026 (DOMINANT):
```
Row 0: TITLE   - "2012 Summary Star View: Medicare Part C Report Card..."
Row 1: HEADER  - Contract Number, Organization Type, Parent Organization, Organization Marketing Name, 2012 Part C Summary Rating
Row 2+: DATA   - H0084, Local CCP, XLHealth Corporation, Care Improvement Plus, Not enough data...
```

### Parsing Rules:
- Find header row (contains "Contract Number" or "Contract")
- Usually row 1 for 2013+
- Summary rating column contains: star values ("2.5 out of 5 stars", "3", "3.5") or text
- Key column: "Contract Number"

---

## COMMON PATTERNS

### Contract ID Format:
- MA/MAPD: H0000 - H9999 (starts with H)
- PDP: S0000 - S9999 (starts with S)
- Cost: 90000+ (starts with 9)
- PFFS: R0000 - R9999 (starts with R)
- Employer: E0000 - E9999 (starts with E)

### Star Rating Values:
- Numeric: 1, 2, 3, 4, 5 (integers)
- Decimal: 2.5, 3.5, 4.5 (for summary ratings)
- Text: "2 out of 5 stars", "3.5 out of 5 stars"
- Missing: "Not enough data", "Plan too new", "Not applicable", "Plan not required"

### Audit Columns (ADD TO ALL OUTPUTS):
- `_source_file`: Original filename
- `_pipeline_run_id`: UUID for this ETL run
- `_extracted_at`: Timestamp of extraction

---

## PARSING PRIORITY

### For Stars Data (current system):
1. **MEASURE_STARS** (2020-2026) - PRIMARY
2. **MEASURE_DATA** (2010-2018) - HISTORICAL
3. **CUTPOINTS** (2007-2026) - Thresholds

### For Contract Tracking:
1. **CROSSWALK** (2006-2026) - Entity chains
2. **CAI** (2017-2026) - Contract info

### For Summary Ratings:
1. **SUMMARY_RATING** (2009-2026) - Overall stars
2. **DOMAIN** (2008-2026) - Domain scores

---

## NEXT STEPS

Based on this analysis, each file category needs a dedicated parser that:
1. Detects the exact year/version
2. Applies the correct structure rules
3. Extracts data to a normalized schema
4. Adds audit columns
5. Validates output

The parsers should be built in this order:
1. MEASURE_STARS (primary measure data)
2. MEASURE_DATA (historical measures)
3. CUTPOINTS (star thresholds)
4. SUMMARY_RATING (overall ratings)
5. DOMAIN (domain scores)
6. CROSSWALK (entity chains)
7. CAI, ENROLLMENT, SNP (supporting data)
