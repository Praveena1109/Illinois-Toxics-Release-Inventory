# Illinois Toxics Release Inventory (TRI) - Curated Dataset 2010–2024

> A reproducible data curation project transforming 15 years of U.S. EPA annual regulatory files into a structured, documented, preservation-ready, and reusable environmental research resource.

**Dataset scope:** Illinois · 2010-2024 · 58,317 records · 1,533 facilities · 334 chemicals  
**Version:** 1.0.0  
**Repository:** https://github.com/Praveena1109/Illinois-Toxics-Release-Inventory  
**Sandbox deposit:** https://sandbox.zenodo.org/records/493838  

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Use Case and Institutional Scenario](#2-use-case-and-institutional-scenario)
3. [Why 2010–2024?](#3-why-20102024)
4. [Repository Structure](#4-repository-structure)
5. [Dataset Description](#5-dataset-description)
6. [How to Reproduce the Workflow](#6-how-to-reproduce-the-workflow)
7. [Curation Workflow](#7-curation-workflow)
8. [Data Quality Dimensions](#8-data-quality-dimensions)
9. [Curation Decisions](#9-curation-decisions)
10. [Known Limitations](#10-known-limitations)
11. [Metadata and Documentation](#11-metadata-and-documentation)
12. [Provenance and Lineage](#12-provenance-and-lineage)
13. [Fixity and Preservation](#13-fixity-and-preservation)
14. [Dissemination and Citation](#14-dissemination-and-citation)
15. [Ethical, Legal, and Policy Notes](#15-ethical-legal-and-policy-notes)
16. [References](#16-references)

---

## 1. Project Overview

The **Toxics Release Inventory (TRI)** is an annual self-reporting program under the Emergency Planning and Community Right-to-Know Act (EPCRA) Section 313. Facilities that meet EPA reporting thresholds report releases and waste management quantities for listed toxic chemicals. This project curates the **Illinois subset** of the U.S. EPA TRI Basic Data Files for reporting years 2010 through 2024.

Raw TRI files are publicly available, but the annual files are not immediately analysis-ready for longitudinal research. Each year is distributed separately, field names require harmonization, zero values require contextual interpretation, and time-series users need clear documentation about reporting changes, facility turnover, chemical list changes, Form A reporting, PFAS coverage, and other limitations.

This repository transforms the annual Illinois TRI Basic Data Files into a structured curation package with:

- A unified record-level dataset
- Multiple derivative analytical datasets
- A reproducible Python workflow
- File-level acquisition provenance
- Data quality assessment artifacts
- Data dictionary in CSV and JSON
- Dataset-level metadata in schema.org-style JSON
- Curation log in Markdown and JSON
- Fixity manifest using checksums
- Preservation and dissemination documentation

The central goal is not only to clean data, but to make the dataset independently understandable, reusable, traceable, and preservation-ready.

---

## 2. Use Case and Institutional Scenario

### Institutional Context

The institutional setting is an environmental research organization, university research group, or state environmental agency interested in understanding toxic chemical release trends across Illinois. The curation work supports data users who need reliable time-series data but may not have the time or background to interpret raw EPA files and documentation.

### Specific Policy-Oriented Use Case

A concrete use case is supporting Illinois environmental policy analysts and community researchers who want to ask:

- Which Illinois industries reported the largest toxic chemical releases from 2010 to 2024?
- Are reported releases increasing, decreasing, or shifting by industry sector?
- Which chemicals are consistently high-volume or emerging concerns?
- How do reporting changes, Form A submissions, PFAS additions, or facility turnover affect trend interpretation?
- Which records require caution because zeros, sudden changes, or reporting limitations may affect interpretation?

This use case strengthens the project’s connection to environmental justice and public policy.

---

## 3. Why 2010–2024?

The 2010–2024 timeframe provides a balanced 15 year window for analyzing long term trends across facilities, industries, and chemicals while remaining manageable for a course scale curation project. It also captures important policy and reporting changes, including the introduction of PFAS reporting in 2020 and post 2014 POTW methodology updates. Additionally, the period is recent enough to support current environmental research, policy analysis, and educational use while documenting comparability limitations across reporting years.

---

## 4. Repository Structure

The repository keeps the existing project structure and adds documentation around it.

```text
Illinois-Toxics-Release-Inventory/
│
├── data/                              # Raw EPA TRI files
│   ├── 2010_il.csv
│   ├── 2011_il.csv
│   └── ... through 2024_il.csv
│
├── scripts/                           # Ordered curation pipeline
│   ├── 01_acquisition_pipeline.py     # Load and combine annual files
│   ├── 02_quality_assessment.py       # Quality checks and curation flags
│   ├── 03_structuring_aggregation.py  # Derivative dataset creation
│   ├── 04_data_dictionary.py          # Data dictionary generation
│   ├── 05_curation_log.py             # Curation log output
│   └── 06_fixity_manifest.py          # Checksum manifest for preservation
│
├── outputs/                           # Curated datasets generated by scripts
│   ├── BASE_COMBINED.csv
│   ├── CLEANED_RECORDS.csv
│   ├── FACILITY_TIME_SERIES.csv
│   ├── INDUSTRY_TIME_SERIES.csv
│   ├── CHEMICAL_SUMMARY.csv
│   ├── CHEMICAL_YEAR_TIME_SERIES.csv
│   └── YEAR_STATS.csv
│
├── docs/                              # Documentation and provenance artifacts
│   ├── acquisition_log.csv
│   ├── curation_log.md
│   ├── curation_log.json
│   ├── data_dictionary.csv
│   ├── data_dictionary.json
│   ├── quality_report.json
│   ├── structuring_provenance.json
│   ├── fixity_manifest.csv            
│   ├── fixity_manifest.json          
│   ├── provenance.md                  # Formal lineage documentation
│   ├── quality_dimensions.md          # Quality dimensions mapping
│   ├── preservation_plan.md           # Long-term access plan
│   ├── acquisition_instructions.md    # Manual acquisition instructions
│   └── data_user_guide.md             # User-oriented explanation
│  
├── metadata/
│   └── dataset_metadata.json          # dataset metadata
│
├── CITATION.cff                       # Citation metadata for GitHub
├── LICENSE                            # License statement
├── requirements.txt                   # Dependencies
├── environment.yml                    # Conda environment
├── .gitignore                         
└── README.md                          # Project-level documentation
```

---

## 5. Dataset Description

### Source Dataset

- **Source:** U.S. EPA Toxics Release Inventory Basic Data Files
- **Geographic scope:** Illinois, United States
- **Temporal scope:** Reporting years 2010–2024
- **Source format:** Annual CSV files
- **Unit of observation:** Facility × chemical × year
- **Curated record count:** 58,317 records
- **Facilities:** 1,533 unique TRI facility IDs
- **Chemicals:** 334 unique chemicals

### Unit of Observation

Each row in `CLEANED_RECORDS.csv` represents a **facility × chemical × year** record. This preserves the original TRI reporting structure and avoids collapsing data too early in the workflow.

### Main Outputs

| File | Description | Grain |
|---|---|---|
| `BASE_COMBINED.csv` | Combined annual files before curation flags | Facility × Chemical × Year |
| `CLEANED_RECORDS.csv` | Flagged and cleaned record-level dataset | Facility × Chemical × Year |
| `FACILITY_TIME_SERIES.csv` | Annual release totals and flag counts by facility | Facility × Year |
| `INDUSTRY_TIME_SERIES.csv` | Annual totals by NAICS industry sector | Industry Sector × Year |
| `CHEMICAL_SUMMARY.csv` | All-years chemical release totals ranked by volume | Chemical |
| `CHEMICAL_YEAR_TIME_SERIES.csv` | Chemical-level yearly release trends | Chemical × Year |
| `YEAR_STATS.csv` | Dataset-level annual statistics | Year |

---

## 6. How to Reproduce the Workflow

### 6.1 Clone Repository

```bash
git clone https://github.com/Praveena1109/Illinois-Toxics-Release-Inventory.git
cd Illinois-Toxics-Release-Inventory
```

### 6.2 Create Environment

Using pip:

```bash
pip install -r requirements.txt
```

Or using conda:

```bash
conda env create -f environment.yml
conda activate illinois-tri-curation
```

### 6.3 Place Raw Data

Place the 15 raw Illinois TRI Basic Data Files in the `data/` folder using this naming pattern:

```text
data/2010_il.csv
data/2011_il.csv
...
data/2024_il.csv
```

Manual download instructions are documented in:

```text
docs/acquisition_instructions.md
```

### 6.4 Run Scripts in Order

```bash
python scripts/01_acquisition_pipeline.py
python scripts/02_quality_assessment.py
python scripts/03_structuring_aggregation.py
python scripts/04_data_dictionary.py
python scripts/05_curation_log.py
python scripts/06_fixity_manifest.py
```

### 6.5 Expected Outputs

After running the workflow, the `outputs/` folder should contain the curated CSV files and the `docs/` folder should contain logs, metadata documentation, quality summaries, provenance records, and fixity manifests.

---

## 7. Curation Workflow

The workflow follows a dataset-specific adaptation of the USGS Science Data Lifecycle Model: plan, acquire, process, analyze/structure, preserve, and share.

```text
EPA TRI annual Illinois files
        ↓
01_acquisition_pipeline.py
        ↓
BASE_COMBINED.csv
        ↓
02_quality_assessment.py
        ↓
CLEANED_RECORDS.csv
        ↓
03_structuring_aggregation.py
        ↓
FACILITY_TIME_SERIES.csv
INDUSTRY_TIME_SERIES.csv
CHEMICAL_SUMMARY.csv
CHEMICAL_YEAR_TIME_SERIES.csv
YEAR_STATS.csv
        ↓
04_data_dictionary.py
        ↓
data_dictionary.csv / data_dictionary.json
        ↓
05_curation_log.py
        ↓
curation_log.md / curation_log.json
        ↓
06_fixity_manifest.py
        ↓
fixity_manifest.csv / fixity_manifest.json
        ↓
GitHub + Zenodo archival package
```

| Lifecycle Stage | Repository Implementation |
|---|---|
| Plan | Project scope, use case, documentation strategy, quality dimensions |
| Acquire | Raw EPA annual files and `acquisition_log.csv` |
| Process | Schema harmonization, TRIFID correction, numeric conversion |
| Quality Assess | Missingness checks, zero analysis, Form A interpretation, sudden change flags |
| Structure | Facility, industry, chemical, chemical-year, and year-level outputs |
| Document | README, data dictionary, curation log, metadata JSON |
| Preserve | Open formats, checksums, Zenodo package, versioning |
| Share | GitHub repository and Zenodo Sandbox deposit |

---

## 8. Data Quality Dimensions

Quality was assessed as contextual fitness for use rather than as a generic cleaning exercise. The project focuses on whether the curated data can support transparent environmental time-series analysis.

| Dimension | How It Was Operationalized | Repository Evidence |
|---|---|---|
| Completeness | Checked missingness in core fields, especially `TOTAL_RELEASES`, `TRIFID`, `YEAR`, and `CHEMICAL` | `docs/quality_report.json`, `scripts/02_quality_assessment.py` |
| Consistency | Harmonized annual schemas, stripped numeric prefixes, resolved `TRIFD` / `TRIFID` inconsistency | `scripts/01_acquisition_pipeline.py` |
| Interpretability | Created flags for zero releases, Form A records, missing release values, and sudden changes | `outputs/CLEANED_RECORDS.csv`, `docs/data_dictionary.csv` |
| Accuracy Support | Did not claim to verify facility-reported quantities, but preserved EPA fields and flagged values needing caution | README, `docs/data_user_guide.md` |
| Temporal Comparability | Documented PFAS scope, POTW methodology change, facility turnover, and reporting threshold limits | README, `docs/preservation_plan.md`, `docs/data_user_guide.md` |
| Provenance | Logged raw file counts, transformations, curation decisions, and derivative outputs | `docs/acquisition_log.csv`, `docs/curation_log.md`, `docs/provenance.md` |
| Reusability | Provided analysis-ready outputs, documentation, metadata, and reproducible scripts | Full repository |

---

## 9. Curation Decisions

### 9.1 Zero Value Semantics

A major curation decision was to **retain and flag zero release values** rather than remove them.

Zeros in TRI data may indicate different reporting situations:

1. A facility had no applicable physical release pathway.
2. A field was blank in older reporting contexts and represented as zero.
3. A facility submitted Form A, where detailed quantity reporting is not required.

Because zero values are meaningful within the TRI reporting system, they are not treated as generic missing values. They are flagged using:

- `FLAG_ZERO_RELEASE`
- `FLAG_FORM_A`

This allows users to decide whether to include or exclude zero-release records depending on their research question.

### 9.2 Empty Fields Retained

Several fields are fully empty in the Illinois 2010–2024 subset, including fields related to tribal lands and pre-NAICS SIC classification. These fields are retained because they belong to the broader EPA TRI schema. Removing them would make the Illinois subset less compatible with the national data model.

### 9.3 Sudden Change Flag

Records with large year-over-year changes in `TOTAL_RELEASES` for a facility-chemical pair are flagged using `FLAG_SUDDEN_CHANGE`. These records are not removed because sudden changes may reflect real events, facility changes, reporting corrections, or operational shifts.

### 9.4 Derivative Datasets

The project creates derivative datasets to support common reuse scenarios without forcing users to rebuild all aggregations from the record-level table. The full record-level data are preserved as the provenance anchor.

---

## 10. Known Limitations

| Limitation | Detail |
|---|---|
| PFAS scope | PFAS chemicals were added to TRI beginning in Reporting Year 2020. Pre-2020 records have no PFAS data by design. |
| POTW methodology break | POTW release/treatment methodology changed around RY 2014. Comparisons across this break should be made carefully. |
| Self-reported estimates | TRI release quantities are primarily facility-estimated rather than independently measured. |
| Threshold reporting | Only facilities above EPCRA reporting thresholds submit TRI reports. The dataset does not represent all statewide emissions. |
| Facility turnover | Facilities open, close, merge, change ownership, or fall below reporting thresholds. Changes in record count do not necessarily equal environmental change. |
| NAICS consistency | NAICS codes may vary because facilities self-report business classifications. |
| Trade secrets | Some chemical identities may be reported using generic names. CAS `9999999999` indicates a sanitized trade-secret submission. |
| Health risk interpretation | TRI releases alone cannot be used to determine direct exposure or health risk without additional environmental, toxicological, and demographic context. |

---

## 11. Metadata and Documentation

The repository includes both human-readable and machine-readable documentation:

| Artifact | Purpose |
|---|---|
| `README.md` | Project overview, workflow, limitations, reproduction instructions |
| `metadata/dataset_metadata.json` | Schema.org-style dataset-level metadata |
| `docs/data_dictionary.csv` | Field-level documentation in tabular form |
| `docs/data_dictionary.json` | Machine-readable field-level documentation |
| `docs/data_user_guide.md` | User-oriented guide to interpretation and reuse |
| `docs/quality_dimensions.md` | Quality dimensions and operational definitions |
| `docs/curation_log.md` | Chronological curation decisions |
| `docs/provenance.md` | Formal lineage and transformation documentation |
| `docs/preservation_plan.md` | Long-term preservation and dissemination plan |

---

## 12. Provenance and Lineage

The dataset lineage is:

```text
Raw EPA Illinois annual files
        ↓
Schema harmonization and file integration
        ↓
BASE_COMBINED.csv
        ↓
Quality assessment and curation flags
        ↓
CLEANED_RECORDS.csv
        ↓
Derivative aggregation and structuring
        ↓
Facility, industry, chemical, chemical-year, and year-level outputs
        ↓
Metadata, data dictionary, curation log, checksums
        ↓
GitHub repository and Zenodo archival package
```

Detailed provenance is documented in:

- `docs/acquisition_log.csv`
- `docs/curation_log.md`
- `docs/curation_log.json`
- `docs/structuring_provenance.json`
- `docs/provenance.md`
- `docs/fixity_manifest.csv`

---

## 13. Fixity and Preservation

Fixity is addressed through checksums generated by:

```bash
python scripts/06_fixity_manifest.py
```

This creates:

```text
docs/fixity_manifest.csv
docs/fixity_manifest.json
```

The manifest records:

- file path
- file size
- MD5 checksum
- SHA-256 checksum
- timestamp generated

Preservation strategy:

- Use CSV for long-term tabular interoperability
- Use JSON for machine-readable metadata and provenance
- Use Markdown for human-readable documentation
- Use GitHub for version transparency
- Use Zenodo for archival deposit and persistent citation
- Use fixity manifests to verify file integrity over time

---

## 14. Dissemination and Citation

The project is disseminated through:

1. GitHub repository for version-controlled workflow transparency
2. Zenodo Sandbox deposit for repository packaging practice
3. Final report PDF for narrative explanation
4. Supplementary repository artifacts for inspection and rerun

---

## 15. Ethical, Legal, and Policy Notes

The source data are public regulatory data from the U.S. EPA. The dataset does not contain individual-level personal information or human subject research data. However, ethical interpretation remains important.

Responsible reuse requires:

- Clear documentation that TRI data are self-reported estimates
- Avoiding unsupported claims about direct exposure or health risk
- Respecting trade-secret sanitization where chemical identities are generic
- Documenting reporting thresholds and scope limitations
- Explaining that absence from TRI does not mean absence of emissions

---

## 16. References

Bruce, T. R., & Hillmann, D. I. (2004). The continuum of metadata quality: Defining, expressing, exploiting. In D. Hillmann & E. L. Westbrooks (Eds.), *Metadata in practice* (pp. 238–256). ALA Editions.

DataCite Metadata Working Group. (2021). *DataCite metadata schema documentation for the publication and citation of research data and other research outputs* (Version 4.4). DataCite e.V. https://doi.org/10.14454/3w3z-sa82

Peer, L., Green, A., & Stephenson, E. (2014). Committing to data quality review. *IASSIST Quarterly*.

Plale, B., & Kouper, I. (2017). The centrality of data: Data lifecycle and data pipelines.

Riley, J. (2017). *Understanding metadata: What is metadata, and what is it for?* National Information Standards Organization.

Strong, D. M., Lee, Y. W., & Wang, R. Y. (1997). Data quality in context. *Communications of the ACM, 40*(5), 103–110.

Terrizzano, I., Schwarz, P., Roth, M., & Colino, J. E. (2015). Data wrangling: The challenging journey from the wild to the lake.

U.S. Environmental Protection Agency. (2024). *TRI Basic Data Files Documentation*.

Wilkinson, M. D., Dumontier, M., Aalbersberg, I. J., Appleton, G., Axton, M., Baak, A., et al. (2016). The FAIR guiding principles for scientific data management and stewardship. *Scientific Data, 3*, 160018. https://doi.org/10.1038/sdata.2016.18
