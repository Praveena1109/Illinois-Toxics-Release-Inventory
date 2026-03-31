# SCRIPT 05: CURATION LOG GENERATOR
# Illinois TRI Dataset Curation | 2010–2024
# PURPOSE: Produce a chronological log of curation decisions, justifications, and transformations applied to the Illinois TRI dataset.

import json
import os
from datetime import datetime

LOG_DIR = "docs/"

CURATION_LOG = [
    {
        "phase"     : "Phase 1 - Acquisition",
        "date"      : "2026-03-01",
        "action"    : "Downloaded 15 annual Illinois TRI Basic Data Files from U.S. EPA",
        "detail"    : "Files retrieved from https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-data-files-calendar-years-1987-present for years 2010–2024.",
        "decision"  : "Used 'Basic Data Files' (not 'Basic Plus') as they contain the 100 most-requested fields and are the standard format for longitudinal research.",
        "files"     : ["2010_il.csv", "2011_il.csv", "2012_il.csv", "2013_il.csv",
                       "2014_il.csv", "2015_il.csv", "2016_il.csv", "2017_il.csv",
                       "2018_il.csv", "2019_il.csv", "2020_il.csv", "2021_il.csv",
                       "2022_il.csv", "2023_il.csv", "2024_il.csv"],
    },
    {
        "phase"     : "Phase 1 - Acquisition",
        "date"      : "2026-03-01",
        "action"    : "Logged file-level row and column counts before combining",
        "detail"    : "Row counts range from 3,432 (2024) to 4,130 (2014). All files contain 122 columns.",
        "decision"  : "File inventory retained in acquisition_log.csv as provenance evidence.",
        "files"     : ["docs/acquisition_log.csv"],
    },
    {
        "phase"     : "Phase 2 - Schema Harmonisation",
        "date"      : "2026-03-05",
        "action"    : "Stripped numeric prefixes from column headers",
        "detail"    : "TRI files use column names like '1. YEAR', '37. CHEMICAL'. Prefix stripped to canonical names.",
        "decision"  : "Standardised names match EPA's field name documentation (Basic Data Files Doc). Avoids downstream pandas errors with numeric-prefixed columns.",
        "files"     : [],
    },
    {
        "phase"     : "Phase 2 - Schema Harmonisation",
        "date"      : "2026-03-05",
        "action"    : "Resolved TRIFD / TRIFID naming inconsistency",
        "detail"    : "Some earlier-year files use 'TRIFD' while others use 'TRIFID' for the facility identifier field.",
        "decision"  : "Renamed 'TRIFD' → 'TRIFID' for all years. EPA documentation uses 'TRIFID' as the canonical name.",
        "files"     : [],
    },
    {
        "phase"     : "Phase 2 - Schema Harmonisation",
        "date"      : "2026-03-05",
        "action"    : "Retained all 122 columns including fully empty fields",
        "detail"    : "BIA, TRIBE, PRIMARY SIC, SIC 2–6, HORIZONTAL DATUM are 100% empty for this Illinois dataset.",
        "decision"  : "RETAINED. These fields are part of the standardised TRI schema. Removing them would break compatibility with the full national TRI data model.",
        "files"     : [],
    },
    {
        "phase"     : "Phase 3 - Quality Assessment",
        "date"      : "2026-03-08",
        "action"    : "Analysed zero values in TOTAL RELEASES",
        "detail"    : "12,578 records (21.6%) show TOTAL_RELEASES == 0. Zero records NOT removed.",
        "decision"  : "RETAINED AND FLAGGED. Per EPA Basic Data Files Documentation, zeros arise from: (1) 'Not Applicable' responses on Form R, (2) blank fields in pre-electronic paper forms, (3) Form A submissions which do not require quantity data. Removing zeros would discard valid regulatory information and bias statistical summaries upward. FLAG_ZERO_RELEASE and FLAG_FORM_A added to enable user-level filtering.",
        "files"     : ["outputs/CLEANED_RECORDS.csv"],
    },
    {
        "phase"     : "Phase 3 - Quality Assessment",
        "date"      : "2026-03-08",
        "action"    : "Applied year-over-year change flag (FLAG_SUDDEN_CHANGE)",
        "detail"    : "Records where |TOTAL_RELEASES YoY % change| > 100% are flagged for analyst attention.",
        "decision"  : "FLAG ONLY - records not removed. Sudden changes may reflect genuine one-time events (Section 8.8 releases), facility expansions, or reporting corrections. Analysts should review these contextually.",
        "files"     : ["outputs/CLEANED_RECORDS.csv"],
    },
    {
        "phase"     : "Phase 3 - Quality Assessment",
        "date"      : "2026-03-08",
        "action"    : "Numeric coercion applied to all quantity fields",
        "detail"    : "RELEASE, TRANSFER, RECYCLE, TREAT, ENERGY, WASTE, RATIO fields coerced to float64 using pd.to_numeric(errors='coerce').",
        "decision"  : "Non-numeric values converted to NaN (not dropped). This preserves row completeness while enabling arithmetic operations. Original raw files are unchanged.",
        "files"     : [],
    },
    {
        "phase"     : "Phase 4 - Structuring",
        "date"      : "2026-03-12",
        "action"    : "Created five derivative datasets from cleaned records",
        "detail"    : "FACILITY_TIME_SERIES, INDUSTRY_TIME_SERIES, CHEMICAL_SUMMARY, CHEMICAL_YEAR_TIME_SERIES, YEAR_STATS.",
        "decision"  : "Derivative datasets reduce hurdle for common downstream analyses (temporal trends, sector comparisons, chemical ranking) without discarding the full record-level detail in CLEANED_RECORDS.csv. Each dataset preserves enough context to be independently interpretable.",
        "files"     : ["outputs/FACILITY_TIME_SERIES.csv", "outputs/INDUSTRY_TIME_SERIES.csv",
                       "outputs/CHEMICAL_SUMMARY.csv", "outputs/CHEMICAL_YEAR_TIME_SERIES.csv",
                       "outputs/YEAR_STATS.csv"],
    },
    {
        "phase"     : "Phase 4 - Structuring",
        "date"      : "2026-03-12",
        "action"    : "Added AIR_RELEASE_TOTAL and WATER_RELEASE_TOTAL derived columns",
        "detail"    : "AIR_RELEASE_TOTAL = 5.1 (Fugitive Air) + 5.2 (Stack Air). WATER_RELEASE_TOTAL = 5.3 (Water).",
        "decision"  : "Common pathway aggregations derived once to avoid repeated manual computation by downstream users.",
        "files"     : [],
    },
    {
        "phase"     : "Phase 5 - Documentation",
        "date"      : "2026-03-15",
        "action"    : "Generated data dictionary covering all 122 source fields + curation fields",
        "detail"    : "Dictionary provides field name, category (SOURCE/CURATION), data type, description, and provenance for every column.",
        "decision"  : "JSON and CSV formats produced. Descriptions draw directly from EPA Basic Data Files Documentation to ensure accuracy.",
        "files"     : ["docs/data_dictionary.csv", "docs/data_dictionary.json"],
    },
    {
        "phase"     : "Phase 5 - Documentation",
        "date"      : "2026-03-15",
        "action"    : "Noted PFAS data scope limitation",
        "detail"    : "PFAS chemicals were added to TRI in Reporting Year 2020. The 2010–2019 portion of this dataset has no PFAS records by design, not because of data errors.",
        "decision"  : "Documented as a known temporal scope limitation in README.",
        "files"     : [],
    },
    {
        "phase"     : "Phase 5 - Documentation",
        "date"      : "2026-03-15",
        "action"    : "Noted POTW calculation methodology change in RY 2014",
        "detail"    : "Prior to RY 2014, POTW transfers for metals were assumed 100% released. From RY 2014 onward, facilities report percentage breakdowns via TRI-MEweb. This affects comparability of POTW-related fields across the 2010–2024 span.",
        "decision"  : "Documented in README under 'Known Limitations'. Users performing time-series analysis on POTW fields should be aware of this structural break at 2014.",
        "files"     : [],
    },
]

# Write human-readable Markdown log
md_lines = [
    "# Curation Log",
    "## Illinois TRI Dataset 2010-2024",
    f"\nGenerated: {datetime.now().strftime('%Y-%m-%d')}",
    "\nThis log documents all curation decisions, transformations, and justifications applied to the Illinois Toxics Release Inventory dataset.",
]

for i, entry in enumerate(CURATION_LOG, 1):
    md_lines.append(f"## Entry {i:02d} | {entry['phase']}")
    md_lines.append(f"**Date:** {entry['date']}")
    md_lines.append(f"\n**Action:** {entry['action']}")
    md_lines.append(f"\n**Detail:** {entry['detail']}")
    md_lines.append(f"\n**Decision / Justification:** {entry['decision']}")
    if entry['files']:
        md_lines.append(f"\n**Affected files:** {', '.join(entry['files'])}")
    md_lines.append("\n---\n")

md_path = f"{LOG_DIR}curation_log.md"
with open(md_path, "w", encoding="utf-8") as f:
    f.write("\n".join(md_lines))
print(f" Curation log (Markdown) : {md_path}")

# Write JSON log
json_path = f"{LOG_DIR}curation_log.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump({
        "dataset"  : "Illinois TRI Curated Dataset 2010-2024",
        "generated": datetime.now().isoformat(),
        "entries"  : CURATION_LOG
    }, f, indent=2)
print(f" Curation log (JSON) : {json_path}")
print(f"\nTotal log entries: {len(CURATION_LOG)}")
