# Acquisition Instructions

## Purpose

This document addresses the acquisition clarification raised during progress feedback: the `01_acquisition_pipeline.py` script performs loading, schema harmonization, and integration, but a new user also needs clear instructions for obtaining the raw source files before running the script.

## Source

The raw data are the U.S. EPA Toxics Release Inventory (TRI) Basic Data Files.

Source page:

https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-present

## Scope

- State: Illinois
- Reporting years: 2010 through 2024
- File type: TRI Basic Data Files
- Format: CSV
- Expected naming pattern in this repository:

```text
data/2010_il.csv
data/2011_il.csv
data/2012_il.csv
...
data/2024_il.csv
```

## Manual Acquisition Steps

1. Navigate to the EPA TRI Basic Data Files page.
2. Locate the state-level Basic Data File for each reporting year from 2010 through 2024.
3. Download the Illinois file for each year.
4. Save each CSV file into the repository `data/` folder.
5. Rename files using the consistent pattern:

```text
YYYY_il.csv
```

For example:

```text
2010_il.csv
2024_il.csv
```

## Why Manual Acquisition Is Documented

Although the pipeline begins with `01_acquisition_pipeline.py`, the actual acquisition step includes locating, downloading, and preserving the source EPA files. This documentation makes that step explicit so that future users can reproduce the workflow from the source rather than only from already-downloaded files.

## Acquisition Provenance

After the raw files are placed in `data/`, run:

```bash
python scripts/01_acquisition_pipeline.py
```

This script creates:

```text
docs/acquisition_log.csv
docs/run_summary.json
outputs/BASE_COMBINED.csv
```

The acquisition log records file-level provenance including:

- source filename
- reporting year
- row count
- column count
- load status

## Source Data Preservation Decision

Raw EPA files should be preserved unchanged in the `data/` directory. All cleaning, harmonization, and transformation steps are applied only to generated outputs. This supports traceability between the original EPA files and curated datasets.
