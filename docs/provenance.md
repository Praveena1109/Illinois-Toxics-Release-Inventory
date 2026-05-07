# Provenance and Data Lineage

## Purpose

This document provides the formal provenance and lineage description for the Illinois TRI curated dataset. It explains where the data came from, how it moved through the workflow, which transformations were applied, and which artifacts document those steps.

## Provenance Summary

| Element | Description |
|---|---|
| Source organization | U.S. Environmental Protection Agency |
| Source program | Toxics Release Inventory Program |
| Source dataset | TRI Basic Data Files |
| Geographic scope | Illinois |
| Temporal scope | 2010–2024 |
| Source format | Annual CSV files |
| Curated version | 1.0.0 |
| Curator | Praveena Acharya, University of Illinois Urbana-Champaign |
| Repository | GitHub and Zenodo Sandbox |

## Lineage Diagram

```text
U.S. EPA TRI Basic Data Files
        │
        │ Manual acquisition of Illinois annual CSV files, 2010–2024
        ↓
data/YYYY_il.csv
        │
        │ 01_acquisition_pipeline.py
        │ - load each annual CSV
        │ - strip numeric prefixes from column names
        │ - add YEAR when needed
        │ - resolve TRIFD / TRIFID inconsistency
        │ - concatenate annual files
        │ - record file-level inventory
        ↓
outputs/BASE_COMBINED.csv
docs/acquisition_log.csv
docs/run_summary.json
        │
        │ 02_quality_assessment.py
        │ - coerce quantity fields to numeric
        │ - assess missingness
        │ - analyze zero release values
        │ - identify Form A records
        │ - flag missing releases
        │ - flag zero releases
        │ - flag sudden year-over-year changes
        ↓
outputs/CLEANED_RECORDS.csv
docs/quality_report.json
        │
        │ 03_structuring_aggregation.py
        │ - create air and water release totals
        │ - create facility-year output
        │ - create industry-year output
        │ - create chemical summary output
        │ - create chemical-year time series
        │ - create annual statistics
        │ - write structuring provenance
        ↓
outputs/FACILITY_TIME_SERIES.csv
outputs/INDUSTRY_TIME_SERIES.csv
outputs/CHEMICAL_SUMMARY.csv
outputs/CHEMICAL_YEAR_TIME_SERIES.csv
outputs/YEAR_STATS.csv
docs/structuring_provenance.json
        │
        │ 04_data_dictionary.py
        │ - combine EPA field descriptions with curation fields
        │ - document source fields, curation flags, and derived fields
        ↓
docs/data_dictionary.csv
docs/data_dictionary.json
        │
        │ 05_curation_log.py
        │ - produce chronological decision log
        │ - document justifications and affected files
        ↓
docs/curation_log.md
docs/curation_log.json
        │
        │ 06_fixity_manifest.py
        │ - compute MD5 and SHA-256 checksums
        │ - record file sizes and generation timestamp
        ↓
docs/fixity_manifest.csv
docs/fixity_manifest.json
        │
        ↓
GitHub repository + Zenodo archival package
```

## Prospective and Retrospective Provenance

### Prospective Provenance

Prospective provenance is represented by the ordered workflow scripts:

1. `01_acquisition_pipeline.py`
2. `02_quality_assessment.py`
3. `03_structuring_aggregation.py`
4. `04_data_dictionary.py`
5. `05_curation_log.py`
6. `06_fixity_manifest.py`

These scripts describe the intended sequence of curation actions.

### Retrospective Provenance

Retrospective provenance is represented by artifacts generated after workflow execution:

- `docs/acquisition_log.csv`
- `docs/run_summary.json`
- `docs/quality_report.json`
- `docs/structuring_provenance.json`
- `docs/curation_log.md`
- `docs/curation_log.json`
- `docs/fixity_manifest.csv`
- `docs/fixity_manifest.json`

These artifacts record what actually happened during the workflow and provide evidence that the outputs were generated.

## Major Transformations

| Step | Transformation | Rationale | Output |
|---|---|---|---|
| Column prefix removal | Removed numeric prefixes such as `1. YEAR` | Creates stable field names for analysis | `BASE_COMBINED.csv` |
| TRIFID harmonization | Resolved `TRIFD` / `TRIFID` inconsistency | Preserves canonical facility identifier | `BASE_COMBINED.csv` |
| Numeric coercion | Converted quantity fields to numeric types | Enables aggregation and quality assessment | `CLEANED_RECORDS.csv` |
| Zero release flag | Added `FLAG_ZERO_RELEASE` | Supports interpretation without removing records | `CLEANED_RECORDS.csv` |
| Form A flag | Added `FLAG_FORM_A` | Identifies certification records where quantities may be zero by design | `CLEANED_RECORDS.csv` |
| Sudden change flag | Added `FLAG_SUDDEN_CHANGE` | Marks records requiring contextual review | `CLEANED_RECORDS.csv` |
| Air total | Added `AIR_RELEASE_TOTAL` | Combines fugitive and stack air releases | `CLEANED_RECORDS.csv`, derivative outputs |
| Water total | Added `WATER_RELEASE_TOTAL` | Standardizes naming for pathway-level analysis | `CLEANED_RECORDS.csv`, derivative outputs |
| Aggregation | Created facility, industry, chemical, chemical-year, and year outputs | Supports common analysis use cases | `outputs/` |

## Provenance Anchors

The main provenance anchor is `outputs/CLEANED_RECORDS.csv`, which preserves the facility × chemical × year grain while adding curation fields. Derivative datasets should be interpreted as convenience views derived from `CLEANED_RECORDS.csv`.

The original raw EPA files in `data/` remain the source anchor and should not be manually edited.

## Version Notes

Version 1.0.0
If future updates are made, a new version should document:

- added reporting years
- changed scripts
- changed output files
- changed metadata
- changed checksums
- changed DOI or archival deposit information
