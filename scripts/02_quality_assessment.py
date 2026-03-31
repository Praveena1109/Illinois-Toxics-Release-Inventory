# SCRIPT 02: DATA QUALITY ASSESSMENT
# Illinois TRI Dataset Curation | 2010–2024
# PURPOSE: Systematic quality evaluation of the combined TRI dataset.
# QUALITY DIMENSIONS ASSESSED:
#   - Completeness   : missing values, structural gaps
#   - Consistency    : zero-value semantics, Form A vs R
#   - Accuracy flags : sudden year-over-year changes
#   - Interpretability: schema fields with no IL coverage
# CURATOR NOTES ON ZEROS (as per EPA Basic Data Files Doc):
#   Zeros appear for THREE distinct reasons:
#   1. "Not Applicable" (NA) on Form R — facility has no pathway
#   2. Blank fields from pre-electronic paper forms (pre-2014)
#   3. Form A submissions — no quantity required; all zeros by design
#   Zeros are PRESERVED and FLAGGED, not removed.

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

DATA_DIR   = "outputs/"
OUTPUT_DIR = "outputs/"
LOG_DIR    = "docs/"

print("\n" + "="*60)
print("  PHASE 3: DATA QUALITY ASSESSMENT")
print("="*60)

# Load base dataset
df = pd.read_csv(f"{DATA_DIR}BASE_COMBINED.csv", low_memory=False)
print(f"  Loaded: {len(df):,} rows × {df.shape[1]} columns")

# Find a column by keyword
def find_col(keyword, df=df):
    for c in df.columns:
        if keyword.upper() in c.upper():
            return c
    return None

# Standardise key column names
rename_map = {}
for target, keyword in [
    ('TOTAL_RELEASES',        'TOTAL RELEASE'),
    ('ON_SITE_RELEASE_TOTAL', 'ON-SITE RELEASE TOTAL'),
    ('OFF_SITE_RELEASE_TOTAL','OFF-SITE RELEASE TOTAL'),
]:
    col = find_col(keyword)
    if col and col != target:
        rename_map[col] = target

df.rename(columns=rename_map, inplace=True)

# Numeric coercion
NUMERIC_KEYWORDS = ['RELEASE', 'TRANSFER', 'RECYCLE', 'TREAT', 'ENERGY', 'WASTE', 'RATIO']
numeric_cols = [c for c in df.columns if any(k in c.upper() for k in NUMERIC_KEYWORDS)]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Section 1: Core Coverage 
print("\n  ── SECTION 1: CORE COVERAGE ──")
facilities = df['TRIFID'].nunique() if 'TRIFID' in df.columns else 'N/A'
chemicals  = df['CHEMICAL'].nunique() if 'CHEMICAL' in df.columns else 'N/A'
years_list = sorted(df['YEAR'].dropna().unique().astype(int).tolist())
print(f"  Unique facilities : {facilities:,}")
print(f"  Unique chemicals  : {chemicals:,}")
print(f"  Years             : {years_list[0]}–{years_list[-1]} ({len(years_list)} years)")
print(f"  Total records     : {len(df):,}")

# Section 2: Missingness Values
print("\n  ── SECTION 2: MISSINGNESS Values ──")
missing_pct  = (df.isna().mean() * 100).sort_values(ascending=False)
fully_empty  = missing_pct[missing_pct == 100].index.tolist()
high_missing = missing_pct[(missing_pct > 50) & (missing_pct < 100)].index.tolist()
low_missing  = missing_pct[(missing_pct > 0)  & (missing_pct <= 50)].index.tolist()
complete     = missing_pct[missing_pct == 0].index.tolist()

print(f"  Fully empty fields  (100% missing) : {len(fully_empty)}")
print(f"  High missing fields (50–99%)       : {len(high_missing)}")
print(f"  Low missing fields  (1–50%)        : {len(low_missing)}")
print(f"  Complete fields     (0% missing)   : {len(complete)}")
print(f"\n  Fully empty fields (not applicable to IL):")
for f in fully_empty:
    print(f"    • {f}")

# Curation decision - document why empty fields are KEPT
print("\nCURATION DECISION: Fully empty fields are RETAINED to preserve schema fidelity with the full EPA TRI data model. BIA, TRIBE, PRIMARY SIC, SIC 2–6 reflect regulatory fields not applicable to Illinois industrial facilities in this time window.")

# Section 3: Zero-Value Analysis
print("\n  ── SECTION 3: ZERO-VALUE ANALYSIS ──")
if 'TOTAL_RELEASES' in df.columns:
    total_zeros  = (df['TOTAL_RELEASES'] == 0).sum()
    total_na     = df['TOTAL_RELEASES'].isna().sum()
    forma_zeros  = 0
    if 'FORM TYPE' in df.columns:
        forma_zeros = ((df['FORM TYPE'] == 'A') & (df['TOTAL_RELEASES'] == 0)).sum()
    non_forma_zeros = total_zeros - forma_zeros

    print(f"  TOTAL RELEASES missing             : {total_na:,} ({total_na/len(df)*100:.1f}%)")
    print(f"  TOTAL RELEASES = 0                 : {total_zeros:,} ({total_zeros/len(df)*100:.1f}%)")
    print(f"    └ Form A (no quantities required): {forma_zeros:,} ({forma_zeros/len(df)*100:.1f}%)")
    print(f"    └ Form R with zero releases      : {non_forma_zeros:,} ({non_forma_zeros/len(df)*100:.1f}%)")
    print("\nCURATION NOTE: Zero ≠ missing. As per EPA documentation, zeros arise from NA responses, pre-electronic blanks, or Form A. These are FLAGGED (ZERO_RELEASE_FLAG) not imputed or deleted.")
else:
    print("  [WARN] TOTAL_RELEASES column not found.")

# Section 4: Form Type Distribution
print("\n  ── SECTION 4: FORM TYPE DISTRIBUTION ──")
if 'FORM TYPE' in df.columns:
    form_dist = df['FORM TYPE'].value_counts(dropna=False)
    for val, cnt in form_dist.items():
        print(f"  {str(val):5s} : {cnt:6,} records ({cnt/len(df)*100:.1f}%)")
    print("\nCURATION NOTE: Form A records certify release < 500 lbs. All quantity fields are zero by design for Form A.")

# Section 5: Year-over-Year Record Counts 
print("\n  ── SECTION 5: ANNUAL RECORD COUNTS ──")
year_counts = df.groupby('YEAR').size().reset_index(name='RECORDS')
for _, row in year_counts.iterrows():
    bar = "█" * int(row['RECORDS'] / 100)
    print(f"  {int(row['YEAR'])} | {row['RECORDS']:5,} | {bar}")

print("\n  NOTE: Declining counts (2020–2024) may reflect COVID-19 disruptions, facility closures, changes in reporting thresholds, or PFAS additions changing the chemical universe.")

# Section 6: CARCINOGEN / PBT / PFAS Flags
print("\n  ── SECTION 6: CHEMICAL CLASSIFICATION FLAGS ──")
for flag_col in ['CARCINOGEN', 'PBT', 'PFAS']:
    if flag_col in df.columns:
        yes_cnt = (df[flag_col].str.upper() == 'YES').sum() if df[flag_col].dtype == object else 0
        print(f"  {flag_col:12s} flagged YES: {yes_cnt:,} records ({yes_cnt/len(df)*100:.1f}%)")

# Section 7: Top 10 Chemicals by Total Release
print("\n  ── SECTION 7: TOP 10 CHEMICALS BY TOTAL RELEASE ──")
if 'TOTAL_RELEASES' in df.columns and 'CHEMICAL' in df.columns:
    chem_totals = df.groupby('CHEMICAL')['TOTAL_RELEASES'].sum().sort_values(ascending=False).head(10)
    for chem, total in chem_totals.items():
        print(f"  {chem[:100]:100s} : {total:>15,.0f} lbs")

# Section 8: Top 10 Industries
print("\n  ── SECTION 8: TOP 10 INDUSTRY SECTORS BY TOTAL RELEASE ──")
if 'TOTAL_RELEASES' in df.columns and 'INDUSTRY SECTOR' in df.columns:
    ind_totals = df.groupby('INDUSTRY SECTOR')['TOTAL_RELEASES'].sum().sort_values(ascending=False).head(10)
    for ind, total in ind_totals.items():
        print(f"  {str(ind)[:40]:40s} : {total:>15,.0f} lbs")

# Apply Curation Flags
print("\n  ── APPLYING CURATION FLAGS ──")

df['FLAG_MISSING_RELEASE'] = df['TOTAL_RELEASES'].isna() if 'TOTAL_RELEASES' in df.columns else False
df['FLAG_ZERO_RELEASE']    = (df['TOTAL_RELEASES'] == 0)  if 'TOTAL_RELEASES' in df.columns else False

if 'FORM TYPE' in df.columns:
    df['FLAG_FORM_A']      = df['FORM TYPE'].str.upper() == 'A'
else:
    df['FLAG_FORM_A']      = False

# Temporal outlier flag: >100% YoY change in facility-chemical pair
if 'TOTAL_RELEASES' in df.columns and 'TRIFID' in df.columns and 'CHEMICAL' in df.columns:
    df_sorted = df.sort_values(['TRIFID', 'CHEMICAL', 'YEAR'])
    df_sorted['_PREV']   = df_sorted.groupby(['TRIFID','CHEMICAL'])['TOTAL_RELEASES'].shift(1)
    df_sorted['_DELTA']  = df_sorted['TOTAL_RELEASES'] - df_sorted['_PREV']
    df_sorted['_PCT_CHG']= df_sorted['_DELTA'] / df_sorted['_PREV'].replace(0, np.nan)
    df['FLAG_SUDDEN_CHANGE'] = df_sorted['_PCT_CHG'].abs() > 1.0
    n_sudden = df['FLAG_SUDDEN_CHANGE'].sum()
    print(f"  FLAG_SUDDEN_CHANGE   : {n_sudden:,} records (>100% YoY change)")
else:
    df['FLAG_SUDDEN_CHANGE'] = False

flag_cols = [c for c in df.columns if c.startswith('FLAG_')]
print(f"  Flags applied: {flag_cols}")

# Save flagged dataset
flagged_path = f"{OUTPUT_DIR}CLEANED_RECORDS.csv"
df.to_csv(flagged_path, index=False)
print(f"\n Flagged dataset saved : {flagged_path}")

# Save quality report JSON
qreport = {
    "run_timestamp"      : datetime.now().isoformat(),
    "total_records"      : int(len(df)),
    "unique_facilities"  : int(facilities) if isinstance(facilities, (int, np.integer)) else 0,
    "unique_chemicals"   : int(chemicals)  if isinstance(chemicals,  (int, np.integer)) else 0,
    "years_covered"      : years_list,
    "fully_empty_fields" : fully_empty,
    "high_missing_fields": high_missing,
    "flags_applied"      : flag_cols,
    "zero_release_count" : int(total_zeros) if 'total_zeros' in dir() else 0,
    "form_a_count"       : int(forma_zeros) if 'forma_zeros' in dir() else 0,
}

with open(f"{LOG_DIR}quality_report.json", "w") as f:
    json.dump(qreport, f, indent=2)

print(f" Quality report saved : {LOG_DIR}quality_report.json")
print("\n  PHASE 3 COMPLETE")
print("="*60)
