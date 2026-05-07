# Data Quality Dimensions and Operationalization

## Purpose

This document explains which data quality dimensions were assessed in the Illinois TRI curation project and how each dimension was operationalized.

## Quality Framing

This does not treat data quality as a single score. Instead, quality is assessed in relation to the project’s use case: creating a structured, transparent, and reusable Illinois TRI time-series dataset for environmental research, policy analysis, and education.

Because TRI data are regulatory self-reports, this project does not claim to verify the true measured amount of each chemical release. Instead, it improves quality for reuse by documenting structure, provenance, limitations, and interpretation rules.

## Quality Dimensions

| Dimension | Definition in This Project | How It Was Operationalized | Evidence |
|---|---|---|---|
| Completeness | Whether core fields needed for analysis are present and usable | Checked missing values in `TOTAL_RELEASES`, `YEAR`, `TRIFID`, `CHEMICAL`, facility fields, and release quantity fields | `scripts/02_quality_assessment.py`, `docs/quality_report.json` |
| Consistency | Whether files across years use a harmonized schema | Stripped numeric prefixes from EPA field names, resolved `TRIFD` / `TRIFID`, standardized key release field names | `scripts/01_acquisition_pipeline.py` |
| Interpretability | Whether users can understand what values mean | Created data dictionary, documented zero semantics, Form A, missing release flags, sudden change flags | `docs/data_dictionary.csv`, `docs/data_user_guide.md` |
| Accuracy Support | Whether the curated dataset preserves source meaning and avoids misleading transformations | Preserved raw EPA fields, did not impute release quantities, retained zeros, retained empty schema fields | `README.md`, `docs/curation_log.md` |
| Temporal Comparability | Whether users can compare values across years responsibly | Documented PFAS addition in 2020, POTW methodology change, facility turnover, and reporting thresholds | `README.md`, `docs/data_user_guide.md` |
| Provenance | Whether origins and processing history are traceable | Created acquisition log, curation log, structuring provenance, and formal lineage documentation | `docs/acquisition_log.csv`, `docs/provenance.md` |
| Reproducibility | Whether another user can rerun or evaluate the workflow | Ordered scripts, requirements file, environment file, and step-by-step README instructions | `scripts/`, `requirements.txt`, `environment.yml` |
| Preservation Readiness | Whether files are prepared for longer-term access | Used CSV, JSON, Markdown, checksums, citation metadata, and Zenodo packaging | `docs/preservation_plan.md`, `docs/fixity_manifest.csv` |

## Key Curation Decisions Based on Quality Assessment

### 1. Zero Values Were Retained

`TOTAL_RELEASES == 0` is not automatically an error. TRI zeros may reflect Form A reporting, not applicable pathways, or historical reporting conventions. Removing zeros would bias results upward and remove valid regulatory records.

### 2. Fully Empty Fields Were Retained

Fields such as `BIA`, `TRIBE`, and older SIC fields may be empty in the Illinois 2010–2024 subset, but they remain part of the broader EPA TRI schema. Retaining them preserves compatibility with the national schema.

### 3. Sudden Changes Were Flagged, Not Removed

A large year-over-year change may represent a real event, facility change, or reporting correction. Since the curation role is to support responsible reuse rather than erase unusual values, these records are flagged for review.

### 4. Multiple Derivative Datasets Were Created

Different users need different grains of analysis. The project provides facility-year, industry-year, chemical-level, chemical-year, and year-level views while preserving the full record-level dataset.
