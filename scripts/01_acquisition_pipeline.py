# SCRIPT 01: DATA ACQUISITION & INTEGRATION
# Illinois TRI Dataset Curation | 2010–2024
# PURPOSE: Load all 15 annual Illinois TRI CSV files, harmonize schema, and produce the unified base dataset.
# CURATION NOTES:
#   - Column names include numeric prefixes (e.g., "1. YEAR") 
#     that are stripped for consistency across years.
#   - TRIFD/TRIFID naming inconsistency is resolved here.
#   - YEAR field is added from filename when absent in file.
#   - File-level stats are logged for provenance tracking.

import pandas as pd
import glob
import re
import os
import json
from datetime import datetime

# Configuration
DATA_DIR   = "data/"
OUTPUT_DIR = "outputs/"
LOG_DIR    = "docs/"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

RUN_TS    = datetime.now().isoformat()

# Extract year from filename 
def extract_year(filename):
    match = re.search(r'\d{4}', os.path.basename(filename))
    return int(match.group()) if match else None

# Phase 1: Load & Inventory
files = sorted(glob.glob(f"{DATA_DIR}*.csv"))

if not files:
    raise FileNotFoundError(
        f"No CSV files found in '{DATA_DIR}'. "
        "Place all annual Illinois TRI files (e.g., 2010_il.csv) in this folder."
    )

df_list        = []
file_inventory = []

print("\n" + "="*60)
print("  PHASE 1: DATA ACQUISITION & FILE-LEVEL INVENTORY")
print("="*60)
print(f"  Timestamp : {RUN_TS}")
print(f"  Source dir: {DATA_DIR}")
print("-"*60)

for fpath in files:
    year = extract_year(fpath)
    try:
        temp = pd.read_csv(fpath, low_memory=False)

        # Strip numeric column prefixes: "1. YEAR" → "YEAR"
        temp.columns = temp.columns.str.strip()
        temp.columns = temp.columns.str.replace(r'^\d+\.\s*', '', regex=True)
        temp.columns = temp.columns.str.strip()

        # Ensure YEAR column exists
        if 'YEAR' not in temp.columns and year:
            temp.insert(0, 'YEAR', year)

        row_count = len(temp)
        col_count = len(temp.columns)

        record = {
            "file"      : os.path.basename(fpath),
            "year"      : year,
            "rows"      : row_count,
            "columns"   : col_count,
            "status"    : "OK"
        }

        print(f"  {os.path.basename(fpath):30s} → {row_count:5d} rows | {col_count} cols")
        df_list.append(temp)

    except Exception as e:
        record = {
            "file"   : os.path.basename(fpath),
            "year"   : year,
            "rows"   : 0,
            "columns": 0,
            "status" : f"ERROR: {e}"
        }
        print(f"  [ERROR] {os.path.basename(fpath):28s} → {e}")

    file_inventory.append(record)

# Phase 2: Concatenate
print("\n" + "-"*60)
print("  PHASE 2: COMBINING INTO UNIFIED DATASET")
print("-"*60)

df = pd.concat(df_list, ignore_index=True)

# Deduplicate columns
df = df.loc[:, ~df.columns.duplicated()]

# Harmonise TRIFD → TRIFID (present in some years)
if 'TRIFD' in df.columns and 'TRIFID' not in df.columns:
    df.rename(columns={'TRIFD': 'TRIFID'}, inplace=True)
elif 'TRIFD' in df.columns:
    df.drop(columns=['TRIFD'], inplace=True)

print(f"  Total rows    : {len(df):,}")
print(f"  Total columns : {df.shape[1]}")
print(f"  Years covered : {sorted(df['YEAR'].dropna().unique().astype(int).tolist())}")

# Export base dataset
base_path = f"{OUTPUT_DIR}BASE_COMBINED.csv"
df.to_csv(base_path, index=False)
print(f"\n  Base dataset saved : {base_path}")

# Export acquisition log
inv_df   = pd.DataFrame(file_inventory)
inv_path = f"{LOG_DIR}acquisition_log.csv"
inv_df.to_csv(inv_path, index=False)

# Save summary JSON for downstream scripts
summary = {
    "run_timestamp"  : RUN_TS,
    "total_rows"     : int(len(df)),
    "total_columns"  : int(df.shape[1]),
    "years"          : sorted([int(y) for y in df['YEAR'].dropna().unique()]),
    "files_loaded"   : len([r for r in file_inventory if r['status'] == 'OK']),
    "base_dataset"   : base_path,
}
with open(f"{LOG_DIR}run_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print(f" Acquisition log saved : {inv_path}")
print("\n  PHASE 1 & 2 COMPLETE")
print("="*60)
