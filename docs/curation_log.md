# Curation Log
## Illinois TRI Dataset 2010-2024

Generated: 2026-03-30

This log documents all curation decisions, transformations, and justifications applied to the Illinois Toxics Release Inventory dataset.
## Entry 01 | Phase 1 - Acquisition
**Date:** 2026-03-01

**Action:** Downloaded 15 annual Illinois TRI Basic Data Files from U.S. EPA

**Detail:** Files retrieved from https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-present for years 2010–2024.

**Decision / Justification:** Used 'Basic Data Files' (not 'Basic Plus') as they contain the 100 most-requested fields and are the standard format for longitudinal research.

**Affected files:** 2010_il.csv, 2011_il.csv, 2012_il.csv, 2013_il.csv, 2014_il.csv, 2015_il.csv, 2016_il.csv, 2017_il.csv, 2018_il.csv, 2019_il.csv, 2020_il.csv, 2021_il.csv, 2022_il.csv, 2023_il.csv, 2024_il.csv

---

## Entry 02 | Phase 1 - Acquisition
**Date:** 2026-03-01

**Action:** Logged file-level row and column counts before combining

**Detail:** Row counts range from 3,432 (2024) to 4,130 (2014). All files contain 122 columns.

**Decision / Justification:** File inventory retained in acquisition_log.csv as provenance evidence.

**Affected files:** docs/acquisition_log.csv

---

## Entry 03 | Phase 2 - Schema Harmonisation
**Date:** 2026-03-05

**Action:** Stripped numeric prefixes from column headers

**Detail:** TRI files use column names like '1. YEAR', '37. CHEMICAL'. Prefix stripped to canonical names.

**Decision / Justification:** Standardised names match EPA's field name documentation (Basic Data Files Doc). Avoids downstream pandas errors with numeric-prefixed columns.

---

## Entry 04 | Phase 2 - Schema Harmonisation
**Date:** 2026-03-05

**Action:** Resolved TRIFD / TRIFID naming inconsistency

**Detail:** Some earlier-year files use 'TRIFD' while others use 'TRIFID' for the facility identifier field.

**Decision / Justification:** Renamed 'TRIFD' → 'TRIFID' for all years. EPA documentation uses 'TRIFID' as the canonical name.

---

## Entry 05 | Phase 2 - Schema Harmonisation
**Date:** 2026-03-05

**Action:** Retained all 122 columns including fully empty fields

**Detail:** BIA, TRIBE, PRIMARY SIC, SIC 2–6, HORIZONTAL DATUM are 100% empty for this Illinois dataset.

**Decision / Justification:** RETAINED. These fields are part of the standardised TRI schema. Removing them would break compatibility with the full national TRI data model.

---

## Entry 06 | Phase 3 - Quality Assessment
**Date:** 2026-03-08

**Action:** Analysed zero values in TOTAL RELEASES

**Detail:** 12,578 records (21.6%) show TOTAL_RELEASES == 0. Zero records NOT removed.

**Decision / Justification:** RETAINED AND FLAGGED. Per EPA Basic Data Files Documentation, zeros arise from: (1) 'Not Applicable' responses on Form R, (2) blank fields in pre-electronic paper forms, (3) Form A submissions which do not require quantity data. Removing zeros would discard valid regulatory information and bias statistical summaries upward. FLAG_ZERO_RELEASE and FLAG_FORM_A added to enable user-level filtering.

**Affected files:** outputs/CLEANED_RECORDS.csv

---

## Entry 07 | Phase 3 - Quality Assessment
**Date:** 2026-03-08

**Action:** Applied year-over-year change flag (FLAG_SUDDEN_CHANGE)

**Detail:** Records where |TOTAL_RELEASES YoY % change| > 100% are flagged for analyst attention.

**Decision / Justification:** FLAG ONLY - records not removed. Sudden changes may reflect genuine one-time events (Section 8.8 releases), facility expansions, or reporting corrections. Analysts should review these contextually.

**Affected files:** outputs/CLEANED_RECORDS.csv

---

## Entry 08 | Phase 3 - Quality Assessment
**Date:** 2026-03-08

**Action:** Numeric coercion applied to all quantity fields

**Detail:** RELEASE, TRANSFER, RECYCLE, TREAT, ENERGY, WASTE, RATIO fields coerced to float64 using pd.to_numeric(errors='coerce').

**Decision / Justification:** Non-numeric values converted to NaN (not dropped). This preserves row completeness while enabling arithmetic operations. Original raw files are unchanged.

---

## Entry 09 | Phase 4 - Structuring
**Date:** 2026-03-12

**Action:** Created five derivative datasets from cleaned records

**Detail:** FACILITY_TIME_SERIES, INDUSTRY_TIME_SERIES, CHEMICAL_SUMMARY, CHEMICAL_YEAR_TIME_SERIES, YEAR_STATS.

**Decision / Justification:** Derivative datasets reduce hurdle for common downstream analyses (temporal trends, sector comparisons, chemical ranking) without discarding the full record-level detail in CLEANED_RECORDS.csv. Each dataset preserves enough context to be independently interpretable.

**Affected files:** outputs/FACILITY_TIME_SERIES.csv, outputs/INDUSTRY_TIME_SERIES.csv, outputs/CHEMICAL_SUMMARY.csv, outputs/CHEMICAL_YEAR_TIME_SERIES.csv, outputs/YEAR_STATS.csv

---

## Entry 10 | Phase 4 - Structuring
**Date:** 2026-03-12

**Action:** Added AIR_RELEASE_TOTAL and WATER_RELEASE_TOTAL derived columns

**Detail:** AIR_RELEASE_TOTAL = 5.1 (Fugitive Air) + 5.2 (Stack Air). WATER_RELEASE_TOTAL = 5.3 (Water).

**Decision / Justification:** Common pathway aggregations derived once to avoid repeated manual computation by downstream users.

---

## Entry 11 | Phase 5 - Documentation
**Date:** 2026-03-15

**Action:** Generated data dictionary covering all 122 source fields + curation fields

**Detail:** Dictionary provides field name, category (SOURCE/CURATION), data type, description, and provenance for every column.

**Decision / Justification:** JSON and CSV formats produced. Descriptions draw directly from EPA Basic Data Files Documentation to ensure accuracy.

**Affected files:** docs/data_dictionary.csv, docs/data_dictionary.json

---

## Entry 12 | Phase 5 - Documentation
**Date:** 2026-03-15

**Action:** Noted PFAS data scope limitation

**Detail:** PFAS chemicals were added to TRI in Reporting Year 2020. The 2010–2019 portion of this dataset has no PFAS records by design, not because of data errors.

**Decision / Justification:** Documented as a known temporal scope limitation in README.

---

## Entry 13 | Phase 5 - Documentation
**Date:** 2026-03-15

**Action:** Noted POTW calculation methodology change in RY 2014

**Detail:** Prior to RY 2014, POTW transfers for metals were assumed 100% released. From RY 2014 onward, facilities report percentage breakdowns via TRI-MEweb. This affects comparability of POTW-related fields across the 2010–2024 span.

**Decision / Justification:** Documented in README under 'Known Limitations'. Users performing time-series analysis on POTW fields should be aware of this structural break at 2014.

---
