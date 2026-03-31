# SCRIPT 04: DATA DICTIONARY GENERATOR
# Illinois TRI Dataset Curation | 2010–2024
# PURPOSE: Produce a dictionary for the curated CLEANED_RECORDS.csv. Combines EPA field documentation with curation-layer information (flags, derived fields, zero semantics).
# The dictionary covers three field categories:
#   1. SOURCE FIELDS  – from original EPA TRI Basic Data Files
#   2. CURATION FLAGS – added during quality assessment (Script 02)
#   3. DERIVED FIELDS – computed during structuring (Script 03)


import pandas as pd
import json
import os
from datetime import datetime

OUTPUT_DIR = "outputs/"
LOG_DIR    = "docs/"

print("\n" + "="*60)
print("  PHASE 5: DATA DICTIONARY GENERATION")
print("="*60)

# EPA source field descriptions (from Basic Data Files Doc)
EPA_FIELD_DESCRIPTIONS = {
    "YEAR"               : "Calendar year in which the reported activities occurred. Source: TRI_REPORTING_FORM.REPORTING_YEAR",
    "TRIFID"             : "Unique TRI facility identification number. Identifies a specific geographic location; does NOT change with ownership.",
    "FRS ID"             : "EPA Facility Registry Service unique ID. Enables cross-program data linkage.",
    "FACILITY NAME"      : "Name of the reporting facility.",
    "STREET ADDRESS"     : "Street address of the reporting facility.",
    "CITY"               : "City of the reporting facility.",
    "COUNTY"             : "County of the reporting facility.",
    "ST"                 : "Two-letter state code (always IL for this dataset).",
    "ZIP"                : "ZIP code of the reporting facility.",
    "BIA"                : "Bureau of Indian Affairs code. 100% empty in Illinois non-tribal data — retained for schema compliance.",
    "TRIBE"              : "Tribe name on whose land the facility is located. 100% empty in Illinois non-tribal data.",
    "LATITUDE"           : "Facility latitude from EPA Facility Registry System. Note: EPA stopped collecting directly in RY 2005; now sourced from FRS.",
    "LONGITUDE"          : "Facility longitude from EPA Facility Registry System.",
    "HORIZONTAL DATUM"   : "Horizontal datum for lat/lon. Empty for most IL records after RY 2005 transition to FRS coordinates.",
    "PARENT CO NAME"     : "Name of parent company controlling the reporting facility.",
    "STANDARD PARENT CO NAME": "EPA-standardised current ultimate U.S. parent company name.",
    "FEDERAL FACILITY"   : "Flag: YES = federal facility, NO = non-federal.",
    "INDUSTRY SECTOR CODE": "NAICS-based sector code used for TRI industry classification and trend analysis.",
    "INDUSTRY SECTOR"    : "Industry sector label (e.g., Primary Metals, Chemicals, Food). Used for sector-level aggregations.",
    "PRIMARY SIC"        : "Primary 4-digit SIC code. Reported 1987–2005 only. Empty for 2010–2024 data.",
    "PRIMARY NAICS"      : "Primary 6-digit NAICS code. Represents the main business activity at the facility.",
    "CHEMICAL"           : "Name of the reported chemical. May be a generic name if trade secret is claimed.",
    "CAS#"               : "Chemical Abstracts Service identifier. CAS 9999999999 indicates a sanitised trade-secret submission.",
    "CLASSIFICATION"     : "Chemical classification: TRI (general EPCRA 313), PBT (persistent bioaccumulative toxic), or DIOXIN.",
    "METAL"              : "YES if the chemical is a metal with TRI reporting restrictions.",
    "METAL CATEGORY"     : "Metal category 1–4 per EPA classification. Determines how transfers are treated (disposal vs. treatment).",
    "CARCINOGEN"         : "YES if chemical is classified as a carcinogen by OSHA.",
    "PBT"                : "YES if chemical is a persistent, bioaccumulative, and toxic (PBT) substance.",
    "PFAS"               : "YES if chemical is a per- and polyfluoroalkyl substance. PFAS first added to TRI in RY 2020.",
    "FORM TYPE"          : "R = Full Form R (detailed quantities required). A = Form A Certification (no quantities — all release fields are zero by design).",
    "UNIT OF MEASURE"    : "Pounds for all chemicals except dioxin/dioxin-like compounds, which are reported in grams.",
    "5.1 - FUGITIVE AIR" : "Estimated fugitive (non-stack) air emissions in pounds.",
    "5.2 - STACK AIR"    : "Estimated stack (point source) air emissions in pounds.",
    "5.3 - WATER"        : "Estimated surface water discharge in pounds.",
    "ON-SITE RELEASE TOTAL": "Sum of all on-site releases (air + water + land). Sum of fields 5.1 through 5.5.4.",
    "OFF-SITE RELEASE TOTAL": "Total quantity transferred to off-site locations for release or disposal.",
    "POTW - TOTAL TRANSFERS": "Total chemical quantity transferred to a Publicly Owned Treatment Work (POTW). Note: method for calculating POTW release vs. treatment changed in RY 2014.",
    "6.2 - TOTAL TRANSFER": "Total quantity transferred off-site across all disposal, recycling, energy recovery, and treatment codes.",
    "TOTAL_RELEASES"     : "Total on- and off-site releases. Equals ON-SITE RELEASE TOTAL + OFF-SITE RELEASE TOTAL. This is the primary release metric.",
    "PRODUCTION WSTE (8.1-8.7)": "Total production-related waste containing this chemical. Sum of Form R Sections 8.1–8.7.",
    "8.9 - PRODUCTION RATIO": "Ratio of current-year production to prior-year production. Used to contextualise release changes.",
    "PROD_RATIO_OR_ ACTIVITY": "Indicates whether Section 8.9 value is a production ratio or an activity index.",
}

# Curation-added field descriptions
CURATION_FIELD_DESCRIPTIONS = {
    "FLAG_MISSING_RELEASE" : "Curation flag: TRUE if TOTAL_RELEASES is null/missing. In this dataset: 0 records (strong completeness).",
    "FLAG_ZERO_RELEASE"    : "Curation flag: TRUE if TOTAL_RELEASES == 0. Does NOT imply absence of chemical activity — see ZERO VALUE SEMANTICS in README.",
    "FLAG_FORM_A"          : "Curation flag: TRUE if FORM TYPE == 'A'. All release quantities for Form A records are zero by regulatory design.",
    "FLAG_SUDDEN_CHANGE"   : "Curation flag: TRUE if year-over-year % change in TOTAL_RELEASES > 100% for a given facility-chemical pair. May reflect genuine events, reporting corrections, or facility changes.",
    "AIR_RELEASE_TOTAL"    : "Derived: sum of 5.1 (fugitive air) and 5.2 (stack air) releases. Convenience field for air-pathway analysis.",
    "WATER_RELEASE_TOTAL"  : "Derived: alias of 5.3 - WATER. Added for naming consistency with AIR_RELEASE_TOTAL.",
}

DERIVATIVE_FIELD_DESCRIPTIONS = {
    "N_CHEMICALS_REPORTED"        : "Derived (FACILITY_TIME_SERIES): count of distinct chemical reports submitted by this facility in this year.",
    "N_UNIQUE_FACILITIES"         : "Derived (INDUSTRY/YEAR_STATS): count of distinct TRIFID values in this group.",
    "N_UNIQUE_CHEMICALS"          : "Derived (INDUSTRY/YEAR_STATS): count of distinct chemical names in this group.",
    "N_CHEMICAL_REPORTS"          : "Derived (INDUSTRY_TIME_SERIES): total chemical report rows for this sector-year.",
    "N_REPORTING_FACILITIES"      : "Derived (CHEMICAL_YEAR_TIME_SERIES): count of distinct facilities reporting this chemical in this year.",
    "PRODUCTION_NORMALISED_RELEASE": "Derived (CHEMICAL_YEAR_TIME_SERIES): TOTAL_RELEASES / PRODUCTION WSTE (8.1-8.7). Only valid where production waste > 0; NaN otherwise.",
    "N_RECORDS"                   : "Derived (YEAR_STATS): total facility-chemical records in this reporting year.",
    "TOTAL_RELEASE_SUM_LBS"       : "Derived (YEAR_STATS): sum of TOTAL_RELEASES across all records in this year (pounds).",
    "MEAN_RELEASE_LBS"            : "Derived (YEAR_STATS): mean TOTAL_RELEASES per record in this year.",
    "MEDIAN_RELEASE_LBS"          : "Derived (YEAR_STATS): median TOTAL_RELEASES per record in this year.",
    "N_FORM_A"                    : "Derived (YEAR_STATS): count of Form A submissions in this year.",
    "N_ZERO_RELEASE"              : "Derived (YEAR_STATS): count of records with TOTAL_RELEASES == 0 in this year.",
    "N_YEARS_REPORTED"            : "Derived (CHEMICAL_SUMMARY): number of distinct years this chemical appeared in the dataset.",
    "N_FACILITIES"                : "Derived (CHEMICAL_SUMMARY): number of distinct facilities that reported this chemical.",
    "RANK"                        : "Derived (CHEMICAL_SUMMARY): rank by TOTAL_RELEASES descending.",
}

OUTPUT_FILES = {
    "CLEANED_RECORDS.csv"          : "Record-level (facility × chemical × year)",
    "FACILITY_TIME_SERIES.csv"     : "Facility annual aggregation",
    "INDUSTRY_TIME_SERIES.csv"     : "Industry-sector annual aggregation",
    "CHEMICAL_SUMMARY.csv"         : "All-years chemical totals",
    "CHEMICAL_YEAR_TIME_SERIES.csv": "Chemical × year trends",
    "YEAR_STATS.csv"               : "Dataset-level annual statistics",
}

col_registry = {}  
for fname in OUTPUT_FILES:
    fpath = f"{OUTPUT_DIR}{fname}"
    if not os.path.exists(fpath):
        continue
    sample = pd.read_csv(fpath, low_memory=False, nrows=2)
    for col in sample.columns:
        if col not in col_registry:
            col_registry[col] = {"dtype": str(sample[col].dtype), "files": []}
        col_registry[col]["files"].append(fname)

ALL_DESCRIPTIONS = {**EPA_FIELD_DESCRIPTIONS, **CURATION_FIELD_DESCRIPTIONS, **DERIVATIVE_FIELD_DESCRIPTIONS}

dict_records = []
for col, meta in col_registry.items():
    desc = ALL_DESCRIPTIONS.get(col)

    if col.startswith('FLAG_') or col in CURATION_FIELD_DESCRIPTIONS:
        category = "CURATION"
    elif col in DERIVATIVE_FIELD_DESCRIPTIONS:
        category = "DERIVED"
    elif col in EPA_FIELD_DESCRIPTIONS:
        category = "SOURCE"
    else:
        category = "SOURCE"

    dict_records.append({
        "field_name"      : col,
        "category"        : category,
        "data_type"       : meta["dtype"],
        "description"     : desc or "See EPA TRI Basic Data Files Documentation for field details.",
        "source"          : (
            "EPA TRI Basic Data Files"     if category == "SOURCE"  else
            "Derived during curation"      if category == "CURATION" else
            "Computed in aggregation step"
        ),
        "present_in_files": ", ".join(meta["files"]),
    })

dict_df = pd.DataFrame(dict_records)

# Save CSV data dictionary
dict_csv_path = f"{LOG_DIR}data_dictionary.csv"
dict_df.to_csv(dict_csv_path, index=False)
print(f" Data dictionary CSV → {dict_csv_path} ({len(dict_df)} fields)")

# Save JSON data dictionary
dict_json_path = f"{LOG_DIR}data_dictionary.json"
dict_json = {
    "dataset"     : "Illinois TRI Curated Dataset 2010–2024",
    "generated"   : datetime.now().isoformat(),
    "total_fields": len(dict_records),
    "fields"      : dict_records
}
with open(dict_json_path, "w") as f:
    json.dump(dict_json, f, indent=2)
print(f" Data dictionary JSON : {dict_json_path}")

print("\n  PHASE 5 COMPLETE")
print("="*60)
