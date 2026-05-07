# Data User Guide

## Purpose

This guide explains how future users should interpret the curated Illinois TRI dataset. It is written for researchers, policy analysts, students, and community users who may not be familiar with TRI reporting rules.

## What the Dataset Represents

Each row in `CLEANED_RECORDS.csv` represents a facility reporting a chemical for a specific year. The dataset reflects reported releases and waste management quantities under the EPA TRI program.

The dataset should be interpreted as a regulatory reporting dataset

## Main Analysis Files

| File | Use This When You Want To |
|---|---|
| `CLEANED_RECORDS.csv` | Work at the original facility × chemical × year level |
| `FACILITY_TIME_SERIES.csv` | Compare annual facility-level release patterns |
| `INDUSTRY_TIME_SERIES.csv` | Compare industry sectors over time |
| `CHEMICAL_SUMMARY.csv` | Identify highest total reported chemicals |
| `CHEMICAL_YEAR_TIME_SERIES.csv` | Study chemical trends year by year |
| `YEAR_STATS.csv` | Understand annual dataset-level reporting patterns |

## Key Fields

| Field | Meaning |
|---|---|
| `YEAR` | Reporting year |
| `TRIFID` | EPA TRI facility identifier |
| `FACILITY NAME` | Name of reporting facility |
| `CHEMICAL` | Reported chemical name |
| `CAS#` | Chemical Abstracts Service identifier |
| `FORM TYPE` | Form R or Form A |
| `TOTAL_RELEASES` | Total on-site and off-site releases |
| `INDUSTRY SECTOR` | NAICS-based industry sector |
| `AIR_RELEASE_TOTAL` | Fugitive + stack air releases |
| `WATER_RELEASE_TOTAL` | Surface water releases |

## Curation Flags

| Flag | Meaning | Recommended Use |
|---|---|---|
| `FLAG_MISSING_RELEASE` | `TOTAL_RELEASES` is missing | Review completeness |
| `FLAG_ZERO_RELEASE` | `TOTAL_RELEASES == 0` | Do not assume data error; interpret with `FORM TYPE` |
| `FLAG_FORM_A` | Record submitted using Form A | Consider excluding for quantity-based release analysis |
| `FLAG_SUDDEN_CHANGE` | Large year-over-year change | Review before trend interpretation |

## Important Interpretation Notes

### Zero Does Not Always Mean No Pollution

A zero value in a TRI release field may mean several things. It may reflect no applicable release pathway, Form A reporting, or historical reporting conventions. Users should not automatically remove zeros or treat them as missing.

### Form A Records

Form A is a certification statement used when reporting thresholds and release conditions allow simplified reporting. Form A records do not provide the same quantity detail as Form R records. For quantity-based trend analysis, users may want to filter or separately analyze Form A records.

### PFAS Coverage

PFAS chemicals were added to TRI beginning in Reporting Year 2020. Therefore, the absence of PFAS records before 2020 is a reporting-scope issue, not a data quality problem.

### Facility Turnover

Changes in the number of reporting facilities may reflect openings, closures, mergers, ownership changes, or facilities falling above or below TRI reporting thresholds. Do not interpret record count changes as direct evidence of emissions change without additional context.

### Self-Reported Data

TRI quantities are generally estimated by facilities using EPA-approved reporting methods. The curated dataset preserves and structures these reports, but it does not independently verify the release quantities.

## Suggested Analysis Practices

1. Start with `YEAR_STATS.csv` to understand annual coverage.
2. Use `INDUSTRY_TIME_SERIES.csv` for sector-level trends.
3. Use `CHEMICAL_YEAR_TIME_SERIES.csv` for chemical trends.
4. Use `CLEANED_RECORDS.csv` when detailed filtering is needed.
5. Always check flags before making claims.
6. Document whether Form A and zero-release records were included or excluded.
7. Avoid making health-risk claims without exposure, toxicity, and population data.

## Example Filters

### Exclude Form A records

```python
df = df[df["FLAG_FORM_A"] == False]
```

### Review sudden changes

```python
sudden = df[df["FLAG_SUDDEN_CHANGE"] == True]
```

### Focus on nonzero reported releases

```python
nonzero = df[df["FLAG_ZERO_RELEASE"] == False]
```

## Responsible Use Statement

This curated dataset is suitable for exploratory analysis, environmental reporting trend analysis, policy discussion, and educational use. It should not be used alone to make direct claims about exposure, causation, or health outcomes.
