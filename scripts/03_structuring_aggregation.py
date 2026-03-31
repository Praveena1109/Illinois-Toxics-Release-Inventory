# SCRIPT 03: STRUCTURING & AGGREGATION PIPELINE
# Illinois TRI Dataset Curation | 2010–2024
# PURPOSE: Transform the cleaned record-level dataset into multiple structured, analysis-ready derivative files.
# OUTPUTS PRODUCED:
#   FACILITY_TIME_SERIES.csv    - per-facility annual totals
#   INDUSTRY_TIME_SERIES.csv    - per-NAICS-sector annual totals
#   CHEMICAL_SUMMARY.csv        - all-years chemical totals, ranked
#   CHEMICAL_YEAR_TIME_SERIES.csv - chemical × year release trends
#   YEAR_STATS.csv              - dataset-level annual statistics
# CURATION RATIONALE:
#   Raw TRI data is organized at facility × chemical × year. This granularity is correct for provenance, but creates a hurdle for downstream users who typically need one of:
#     (a) Temporal trends — how did a chemical change over time?
#     (b) Industry comparison — which sectors release the most?
#     (c) Facility profiles — what did a given site report?
#   Each derivative dataset serves a distinct analytical purpose while the original CLEANED_RECORDS.csv preserves full detail.

import pandas as pd
import numpy as np
import os
from datetime import datetime

DATA_DIR   = "outputs/"
OUTPUT_DIR = "outputs/"
LOG_DIR    = "docs/"

print("\n" + "="*60)
print("  PHASE 4: STRUCTURING & AGGREGATION")
print("="*60)

# Load flagged cleaned records
df = pd.read_csv(f"{DATA_DIR}CLEANED_RECORDS.csv", low_memory=False)
print(f"  Input: {len(df):,} records loaded from CLEANED_RECORDS.csv")

# Coerce numeric release columns
RELEASE_COLS = [
    'TOTAL_RELEASES', 'ON_SITE_RELEASE_TOTAL', 'OFF_SITE_RELEASE_TOTAL',
    '5.1 - FUGITIVE AIR', '5.2 - STACK AIR', '5.3 - WATER',
    'OFF-SITE RECYCLED TOTAL', 'OFF-SITE ENERGY RECOVERY T',
    'OFF-SITE TREATED TOTAL', '6.2 - TOTAL TRANSFER',
    'PRODUCTION WSTE (8.1-8.7)',
]
for col in RELEASE_COLS:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Derived composite columns
if '5.1 - FUGITIVE AIR' in df.columns and '5.2 - STACK AIR' in df.columns:
    df['AIR_RELEASE_TOTAL'] = df['5.1 - FUGITIVE AIR'].fillna(0) + df['5.2 - STACK AIR'].fillna(0)
else:
    df['AIR_RELEASE_TOTAL'] = np.nan

if '5.3 - WATER' in df.columns:
    df['WATER_RELEASE_TOTAL'] = df['5.3 - WATER']
else:
    df['WATER_RELEASE_TOTAL'] = np.nan

# 1. FACILITY TIME SERIES
print("\n  Building FACILITY_TIME_SERIES...")
fac_agg_cols = {c: 'sum' for c in ['TOTAL_RELEASES', 'AIR_RELEASE_TOTAL',
                                     'WATER_RELEASE_TOTAL', 'ON_SITE_RELEASE_TOTAL',
                                     'OFF_SITE_RELEASE_TOTAL'] if c in df.columns}
fac_flag_cols = {c: 'sum' for c in df.columns if c.startswith('FLAG_')}
fac_agg_cols.update(fac_flag_cols)

# Add count of chemicals reported per facility-year
df['_N_CHEMICALS'] = 1
fac_agg_cols['_N_CHEMICALS'] = 'sum'

group_cols = ['TRIFID', 'YEAR']
if 'FACILITY NAME' in df.columns:
    group_cols.append('FACILITY NAME')
if 'CITY' in df.columns:
    group_cols.append('CITY')
if 'COUNTY' in df.columns:
    group_cols.append('COUNTY')

facility_ts = df.groupby(group_cols, as_index=False).agg(fac_agg_cols)
facility_ts.rename(columns={'_N_CHEMICALS': 'N_CHEMICALS_REPORTED'}, inplace=True)

path = f"{OUTPUT_DIR}FACILITY_TIME_SERIES.csv"
facility_ts.to_csv(path, index=False)
print(f" {path} — {len(facility_ts):,} rows")

# 2. INDUSTRY TIME SERIES
print("\n  Building INDUSTRY_TIME_SERIES...")
ind_group = ['INDUSTRY SECTOR', 'YEAR'] if 'INDUSTRY SECTOR' in df.columns else ['YEAR']
ind_agg = {c: 'sum' for c in ['TOTAL_RELEASES', 'AIR_RELEASE_TOTAL',
                                'WATER_RELEASE_TOTAL'] if c in df.columns}
ind_agg['TRIFID'] = 'nunique'
ind_agg['CHEMICAL'] = 'nunique'
ind_agg['_N_CHEMICALS'] = 'sum'

industry_ts = df.groupby(ind_group, as_index=False).agg(ind_agg)
industry_ts.rename(columns={
    'TRIFID': 'N_UNIQUE_FACILITIES',
    'CHEMICAL': 'N_UNIQUE_CHEMICALS',
    '_N_CHEMICALS': 'N_CHEMICAL_REPORTS'
}, inplace=True)

path = f"{OUTPUT_DIR}INDUSTRY_TIME_SERIES.csv"
industry_ts.to_csv(path, index=False)
print(f" {path} - {len(industry_ts):,} rows")

# 3. CHEMICAL SUMMARY
print("\n  Building CHEMICAL_SUMMARY...")
chem_agg = {'TOTAL_RELEASES': 'sum', 'TRIFID': 'nunique', 'YEAR': 'nunique'}
for flag_col in ['CARCINOGEN', 'PBT', 'PFAS', 'CLASSIFICATION']:
    if flag_col in df.columns:
        chem_agg[flag_col] = 'first'

chem_summary = df.groupby('CHEMICAL', as_index=False).agg(chem_agg)
chem_summary.rename(columns={
    'TRIFID': 'N_FACILITIES',
    'YEAR':   'N_YEARS_REPORTED'
}, inplace=True)
chem_summary.sort_values('TOTAL_RELEASES', ascending=False, inplace=True)
chem_summary.reset_index(drop=True, inplace=True)
chem_summary.index += 1
chem_summary.index.name = 'RANK'

path = f"{OUTPUT_DIR}CHEMICAL_SUMMARY.csv"
chem_summary.to_csv(path)
print(f" {path} - {len(chem_summary):,} chemicals ranked")

# 4. CHEMICAL × YEAR TIME SERIES
print("\n  Building CHEMICAL_YEAR_TIME_SERIES...")
cy_agg = {c: 'sum' for c in [
    'TOTAL_RELEASES', 'ON_SITE_RELEASE_TOTAL', 'OFF_SITE_RELEASE_TOTAL',
    'AIR_RELEASE_TOTAL', 'WATER_RELEASE_TOTAL',
    'OFF-SITE TREATED TOTAL', '6.2 - TOTAL TRANSFER',
    'PRODUCTION WSTE (8.1-8.7)'
] if c in df.columns}

cy_meta = {c: 'first' for c in ['CARCINOGEN', 'PBT', 'PFAS', 'CLASSIFICATION',
                                   'METAL', 'METAL CATEGORY'] if c in df.columns}
cy_agg.update(cy_meta)
cy_agg['TRIFID'] = 'nunique'

cy_ts = df.groupby(['CHEMICAL', 'YEAR'], as_index=False).agg(cy_agg)
cy_ts.rename(columns={'TRIFID': 'N_REPORTING_FACILITIES'}, inplace=True)

# Production-normalised release - only valid where production waste > 0
if 'TOTAL_RELEASES' in cy_ts.columns and 'PRODUCTION WSTE (8.1-8.7)' in cy_ts.columns:
    cy_ts['PRODUCTION_NORMALISED_RELEASE'] = (
        cy_ts['TOTAL_RELEASES'] /
        cy_ts['PRODUCTION WSTE (8.1-8.7)'].replace(0, np.nan)
    )

path = f"{OUTPUT_DIR}CHEMICAL_YEAR_TIME_SERIES.csv"
cy_ts.to_csv(path, index=False)
print(f" {path} - {len(cy_ts):,} rows")

# 5. YEAR STATS
print("\n  Building YEAR_STATS...")
year_stats_agg = {
    'TOTAL_RELEASES': ['count', 'sum', 'mean', 'median'],
    'TRIFID': 'nunique',
    'CHEMICAL': 'nunique',
}
if 'FLAG_FORM_A' in df.columns:
    year_stats_agg['FLAG_FORM_A'] = 'sum'
if 'FLAG_ZERO_RELEASE' in df.columns:
    year_stats_agg['FLAG_ZERO_RELEASE'] = 'sum'

year_stats = df.groupby('YEAR').agg(year_stats_agg).reset_index()
year_stats.columns = [
    'YEAR', 'N_RECORDS', 'TOTAL_RELEASE_SUM_LBS', 'MEAN_RELEASE_LBS',
    'MEDIAN_RELEASE_LBS', 'N_UNIQUE_FACILITIES', 'N_UNIQUE_CHEMICALS',
    'N_FORM_A', 'N_ZERO_RELEASE'
][:len(year_stats.columns)]

path = f"{OUTPUT_DIR}YEAR_STATS.csv"
year_stats.to_csv(path, index=False)
print(f" {path} — {len(year_stats):,} year rows")

# Provenance log
prov = {
    "script"          : "03_structuring_aggregation.py",
    "run_timestamp"   : datetime.now().isoformat(),
    "input_file"      : "CLEANED_RECORDS.csv",
    "outputs": {
        "FACILITY_TIME_SERIES.csv"     : f"{len(facility_ts):,} rows",
        "INDUSTRY_TIME_SERIES.csv"     : f"{len(industry_ts):,} rows",
        "CHEMICAL_SUMMARY.csv"         : f"{len(chem_summary):,} chemicals",
        "CHEMICAL_YEAR_TIME_SERIES.csv": f"{len(cy_ts):,} rows",
        "YEAR_STATS.csv"               : f"{len(year_stats):,} years",
    }
}
import json
with open(f"{LOG_DIR}structuring_provenance.json", "w") as f:
    json.dump(prov, f, indent=2)

print(f"\n  Provenance log → {LOG_DIR}structuring_provenance.json")

print("\n  Writing derived columns back to CLEANED_RECORDS...")
derived_back = ['AIR_RELEASE_TOTAL', 'WATER_RELEASE_TOTAL']
cr = pd.read_csv(f"{OUTPUT_DIR}CLEANED_RECORDS.csv", low_memory=False)
for col in derived_back:
    if col in df.columns:
        cr[col] = df[col].values
cr.to_csv(f"{OUTPUT_DIR}CLEANED_RECORDS.csv", index=False)
print(f" CLEANED_RECORDS.csv updated — added: {derived_back}")

print("\n  PHASE 4 COMPLETE")
print("="*60)

