# Community Pulse — Pipeline Replication Guide

This document provides step-by-step instructions to fully replicate the Community Pulse data pipelines: ACS Summary Data (Pipeline 1) and PUMS Microdata + ALICE Analysis (Pipeline 2).

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Pipeline 1 — ACS Summary Data](#pipeline-1--acs-summary-data)
- [Pipeline 2 — PUMS Microdata & ALICE Analysis](#pipeline-2--pums-microdata--alice-analysis)
- [Outputs Reference](#outputs-reference)
- [Project Structure](#project-structure)

---

## Prerequisites

- Python 3.10+
- PostgreSQL (running and accessible)
- Git Bash or any bash shell (for `.sh` scripts on Windows)

---

## Environment Setup

**1. Clone the repo**

```bash
git clone <repo-url>
cd Community-Pulse
```

**2. Create and activate a virtual environment**

```bash
python -m venv .venv
source .venv/Scripts/activate     # Windows Git Bash
# source .venv/bin/activate       # macOS/Linux
```

**3. Install dependencies**

```bash
pip install -r requirements.txt
pip install psycopg2-binary
```

**4. Create a `.env` file** in the project root:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

**5. Test your database connection**

```bash
python scripts/postgres-connection.py
```

---

## Pipeline 1 — ACS Summary Data

This pipeline loads ACS 5-year estimate CSVs (2010–2023) for ten demographic categories into PostgreSQL.

### Data Sources

Download from the [US Census Bureau ACS](https://www.census.gov/programs-surveys/acs) (5-year estimates). Place raw downloaded folders under `data/raw/`.

| Dataset | ACS Table | Years |
|---|---|---|
| Age Distribution | S0101 | 2019–2023 |
| Population & Race | DP05 | 2019–2023 |
| Household Composition | DP02 | 2019–2023 |
| Language Spoken at Home | S1601 | 2019–2023 |
| Poverty Status | S1701 | 2019–2023 |
| Income Distribution | S1901 | 2019–2023 |
| Employment Status | S2301 | 2019–2023 |
| Occupation | S2401 | 2019–2023 |
| Household Costs | S2503 | 2019–2023 |
| Economic Characteristics | DP03 | 2019–2023 |

### Steps

**Step 1 — Copy raw files into yearly-data folders**

```bash
bash scripts/copy_files_census.sh
```

**Step 2 — Standardize filenames**

```bash
bash scripts/rename_census_data_files.sh
```

**Step 3 — Combine yearly files**

Open and run [notebooks/combine_yearly_files.ipynb](../notebooks/combine_yearly_files.ipynb).

**Step 4 — Load into PostgreSQL**

```bash
python scripts/load_census_data_db.py
```

### Resulting Tables

| Table | Source |
|---|---|
| `age` | age_combined.csv |
| `populationrace` | populationrace_combined.csv |
| `householdcompostion` | householdcompostion_combined.csv |
| `languagespoken` | languagespoken_combined.csv |
| `poverty` | poverty_combined.csv |
| `incomedistribution` | incomedistribution_combined.csv |
| `employmentstatus` | employmentstatus_combined.csv |
| `occupation` | occupation_combined.csv |
| `houseoldcost` | houseoldcost_combined.csv |
| `economiccharateristic` | economiccharateristic_combined.csv |

---

## Pipeline 2 — PUMS Microdata & ALICE Analysis

This pipeline loads Census PUMS person and housing microdata for Illinois, filters to Champaign County, builds household-level ALICE profiles, and produces Tableau-ready statistical exports.

**Supported years:** 2019, 2020 (experimental), 2021, 2022, 2023
**ALICE analysis years:** 2019, 2021, 2022, 2023 *(2020 experimental excluded from ALICE steps)*

> **Note on 2022/2023 PUMA boundaries:** PUMA definitions changed after 2021. For 2019–2021, Champaign County maps to PUMA 2100. For 2022–2023 it maps to PUMAs 1901 and 1902, but PUMA 1902 extends beyond Champaign County. Steps 4–6 build an ACS5-based allocation factor (alpha) to weight PUMA 1902 households to the Champaign-only portion.

All PUMS pipeline scripts live in `scripts/pums/` and are numbered in execution order.

### Data Sources

Download PUMS CSV files from the [Census PUMS Access page](https://www.census.gov/programs-surveys/acs/microdata/access.html) for Illinois. Place housing (`csv_hil_*`) and person (`csv_pil_*`) folders under `data/raw/`.

---

### Step 1 — Copy and rename PUMS files

```bash
bash scripts/pums/01_copy_files_pums.sh
```

Copies files from `data/raw/` into `data/pums/<year>/housing.csv` and `data/pums/<year>/person.csv`.

---

### Step 2 — Load PUMS data into PostgreSQL

```bash
python scripts/pums/02_load_pums_data_db.py
```

Loads each file using PostgreSQL `COPY` for fast bulk ingestion. Column names are cleaned to snake_case with these standardizations:

| Original | Loaded as |
|---|---|
| `ST` | `state` |
| `TYPE` | `typehugq` |
| `ACCESS` | `accessinet` |
| `YBL` | `yrblt` |

Creates tables: `alice_housing_{y}_raw`, `alice_person_{y}_raw` for each year.

---

### Step 3 — Generate Champaign filter and household profile SQL

```bash
python scripts/pums/03_generate_pums_sql_scripts.py
```

Generates and executes SQL scripts 01–06 under `sql/pums/<year>/` for each year:

| Script | Creates Table | Description |
|---|---|---|
| `01_create_pums_person_{y}_champaign.sql` | `alice_person_{y}_champaign` | Filters person records to Champaign PUMA(s) |
| `02_create_pums_housing_{y}_champaign.sql` | `alice_housing_{y}_champaign` | Filters housing records to Champaign PUMA(s) |
| `03_create_pums_person_student_flags_{y}.sql` | `alice_person_student_flags_{y}` | Per-person student and employment flags |
| `04_create_pums_household_student_agg_{y}.sql` | `alice_household_student_agg_{y}` | Household-level student aggregations |
| `05_create_pums_household_base_{y}.sql` | `alice_household_base_{y}` | Core housing unit fields |
| `06_create_pums_alice_household_profile_{y}.sql` | `alice_household_profile_{y}` | Joins base housing with student aggregation |

> **Optional** — after Step 3, run from `sql/pums/ALICE/`:
> - `01_create_alice_household_profile_all_years.sql` → `alice_household_profile_all_years`
> - `02_create_alice_household_profile_all_years_clean.sql` → `alice_household_profile_all_years_clean`

---

### Step 4 — Load Illinois tract-to-PUMA crosswalk

Place `Tract-Illinois.csv` in `data/pums/`, then run:

```bash
python scripts/pums/04_load_tract_il.py
```

Creates table: `tract_il`

---

### Step 5 — Build PUMA 1902 lookup and household counts

Run this SQL script first in your SQL client:

```sql
\i sql/pums/ALICE/03_create_tract_puma1902_lookup.sql
```

Creates `tract_puma1902_lookup`. Then pull ACS5 household counts from the Census API:

```bash
python scripts/pums/05_pull_acs_tract_households.py
```

Creates: `acs5_2022_b11001_raw`, `acs5_2023_b11001_raw`

Then run the remaining ALICE setup scripts in order:

| Script | Creates Table | Description |
|---|---|---|
| `ALICE/04_create_2022_puma1902_household.sql` | `acs5_2022_puma1902_households` | Joins tract lookup with 2022 ACS5 counts |
| `ALICE/05_create_2023_puma1902_household.sql` | `acs5_2023_puma1902_households` | Joins tract lookup with 2023 ACS5 counts |
| `ALICE/06_create_alpha.sql` | `alice_puma1902_alpha` | Champaign allocation factors (alpha) for PUMA 1902 |

---

### Step 6 — Generate adjusted profile tables (2022 and 2023 only)

```bash
python scripts/pums/06_generate_pums_household_profile_adj_sql_scripts.py
```

Generates and executes `06b_create_pums_alice_household_profile_{y}_adj.sql` for 2022 and 2023. Applies the PUMA 1902 alpha — PUMA 1901 households keep `wgtp` unchanged; PUMA 1902 households are scaled by `wgtp × alpha`.

Creates tables: `alice_household_profile_2022_adj`, `alice_household_profile_2023_adj`

---

### Step 7 — Create household final tables

```bash
python scripts/pums/07_generate_pums_household_final_sql_scripts.py
```

Generates and executes `07_create_pums_alice_household_final_{y}.sql`. Applies population filters (occupied units, non-zero person count, non-null income) and sets `analysis_weight`. Uses `wgtp_adj` for 2022/2023 and raw `wgtp` for 2019/2021.

Creates tables: `alice_household_final_{y}`

---

### Step 8 — Create ALICE threshold tables

```bash
python scripts/pums/08_generate_pums_threshold_sql_scripts.py
```

Generates and executes:

| Script | Action |
|---|---|
| `ALICE/07_create_pums_alice_threshold_tables.sql` | Creates `illinois_essentials_index` and `alice_thresholds` |
| `2023/08_load_pums_alice_thresholds_2023_exact.sql` | Loads 2023 United For ALICE monthly survival budgets |
| `{y}/08_backfill_pums_alice_thresholds_{y}.sql` | Backfills 2019/2021/2022 thresholds using Illinois AEI ratios |

Then run the calibration script manually in your SQL client:

```sql
\i sql/pums/ALICE/08_create_alice_thresholds_calibrated.sql
```

Creates `alice_thresholds_calibrated` — a copy of `alice_thresholds` with upward adjustments to dominant childless household buckets (`1_adult_0_child`, `2_adult_0_child`) to account for local cost of living. All downstream ALICE flag calculations use this calibrated table.

---

### Step 9 — Add derived columns to household final tables

```bash
python scripts/pums/09_generate_pums_household_final_updates_sql_scripts.py
```

Generates and executes `09_update_alter_pums_alice_household_final_{y}.sql`, adding:

| Column | Description |
|---|---|
| `hincp_adj_real` | Inflation-adjusted household income (`hincp × adjinc / 1,000,000`) |
| `hh_comp_key` | Household composition category (e.g. `2_adult_1_child`) |
| `student_heavy_flag` | 1 if majority of household members are likely college students with no employment |
| `below_alice_flag` | 1 if `hincp_adj_real` is below the ALICE threshold for the household's composition type |

---

### Step 10 — Refresh below_alice_flag using calibrated thresholds

```bash
python scripts/pums/10_generate_pums_below_alice_flag_sql_scripts.py
```

Generates and executes `10_update_pums_below_alice_flag_calibrated_{y}.sql`. Refreshes `annual_alice_threshold` on each `alice_household_final_{y}` from `alice_thresholds_calibrated` and recalculates `below_alice_flag`.

---

### Step 11 — Create non-student ALICE household tables

```bash
python scripts/pums/11_generate_pums_nonstudent_sql_scripts.py
```

Generates and executes `11_create_pums_alice_nonstudent_households_{y}.sql`.

Creates tables: `alice_nonstudent_households_{y}` — ALICE households (`below_alice_flag = 1`) that are not student-heavy (`student_heavy_flag = 0`).

---

### Step 12 — Run final validation

```bash
python scripts/pums/12_run_pums_final_validation.py
```

Executes all queries in `sql/pums/Final_Validation_Check.sql` and saves results to `outputs/pums/`. Checks:

| Check | Pass Condition |
|---|---|
| `01_calibrated_threshold_match` | `threshold_mismatch_rows = 0` for all years |
| `02_calibration_spot_check` | Expected uplift amounts per year/bucket |
| `03_final_table_null_check` | All null columns = 0 |
| `04_balance_check` | `below + above = total` (balance_check = 0) |
| `05–07_sheet_compare` | Actuals within expected range of benchmark targets |
| `08_nonstudent_filter_impact` | `nonstudent_below <= all_below` |

---

### Step 13 — Generate statistical profiles and export CSVs

```bash
python scripts/pums/13_generate_pums_alice_statistical_profile_sql_scripts_with_exports.py
```

Generates and executes SQL scripts 12a–12c per year, then exports CSVs to `outputs/pums/tableau-data/`:

| Script | Creates Table | Description |
|---|---|---|
| `12a_create_pums_below_alice_households_{y}.sql` | `alice_below_alice_households_{y}` | All ALICE households (below_alice_flag = 1) |
| `12b_create_pums_below_alice_stat_profile_{y}.sql` | `alice_below_alice_stat_profile_{y}` | Statistical profile of complete ALICE population |
| `12c_create_pums_nonstudent_stat_profile_{y}.sql` | `alice_nonstudent_stat_profile_{y}` | Statistical profile of non-student ALICE population |

Profile metrics include: weighted household counts, average household size, average adult/child count, average and median real income, average ALICE threshold, and student/employment composition breakdowns by household type.

Combined all-years CSVs are also saved to `outputs/pums/tableau-data/`.

---

### Step 14 — Generate nonstudent vs. complete profile comparison

```bash
python scripts/pums/14_generate_pums_alice_profile_comparison_sql_scripts_with_exports.py
```

Generates and executes `13_create_pums_nonstudent_vs_complete_profile_compare_{y}.sql`, joining the two stat profile tables to produce side-by-side metric comparisons with absolute and percentage-point differences.

Creates tables: `alice_nonstudent_vs_complete_profile_compare_{y}`
Exports CSVs to `outputs/pums/tableau-data/{year}/` and combined `alice_nonstudent_vs_complete_profile_compare_all_years.csv`.

---

### Final ALICE Tables Summary

| Table | Description |
|---|---|
| `alice_household_final_{y}` | Analysis-ready households with income, composition key, weight, and ALICE flag |
| `alice_nonstudent_households_{y}` | ALICE households excluding student-heavy households |
| `alice_below_alice_households_{y}` | All below-ALICE households (full record set for Tableau) |
| `alice_below_alice_stat_profile_{y}` | Statistical profile of the complete below-ALICE population |
| `alice_nonstudent_stat_profile_{y}` | Statistical profile of the non-student below-ALICE population |
| `alice_nonstudent_vs_complete_profile_compare_{y}` | Side-by-side comparison of nonstudent vs. complete ALICE profiles |
| `alice_thresholds` | Raw ALICE annual thresholds by year and household composition |
| `alice_thresholds_calibrated` | Calibrated thresholds with upward adjustments for dominant childless buckets |
| `illinois_essentials_index` | Illinois AEI values used for threshold backfilling |
| `alice_puma1902_alpha` | Champaign County allocation factors for PUMA 1902 (2022 and 2023) |
| `alice_household_profile_all_years` | Union of `alice_household_profile_{y}` across all years |
| `alice_household_profile_all_years_clean` | Filtered version (non-null person count, income, and household size) |

---

## Outputs Reference

```
outputs/
└── pums/
    ├── 01_calibrated_threshold_match.csv
    ├── 02_calibration_spot_check.csv
    ├── 03_final_table_null_check.csv
    ├── 04_balance_check.csv
    ├── 05_sheet_compare_total.csv
    ├── 08_nonstudent_filter_impact.csv
    ├── final_validation_combined_<timestamp>.csv
    └── tableau-data/
        ├── {year}/
        │   ├── alice_below_alice_households_{year}.csv
        │   ├── alice_below_alice_stat_profile_{year}.csv
        │   ├── alice_nonstudent_stat_profile_{year}.csv
        │   └── alice_nonstudent_vs_complete_profile_compare_{year}.csv
        ├── below_alice_households_all_years.csv
        ├── below_alice_stat_profile_all_years.csv
        ├── nonstudent_stat_profile_all_years.csv
        └── alice_nonstudent_vs_complete_profile_compare_all_years.csv
```

---

## Project Structure

```
Community-Pulse/
├── data/
│   ├── raw/                          # Downloaded ACS and PUMS source files
│   ├── yearly-data/                  # ACS CSVs organized by category
│   ├── combined-data/                # Merged multi-year ACS CSVs
│   └── pums/                         # Processed PUMS files and crosswalk data
│       └── Tract-Illinois.csv        # Illinois tract-to-PUMA crosswalk (Census)
├── docs/
│   └── REPLICATION.md                # This file — full pipeline replication guide
├── notebooks/
│   ├── combine_yearly_files.ipynb    # Merges yearly ACS files into combined CSVs
│   └── pums_files.ipynb              # Exploratory analysis of PUMS data
├── outputs/
│   └── pums/
│       ├── *.csv                     # Final validation outputs (script 12)
│       └── tableau-data/             # Tableau-ready CSVs (scripts 13 and 14)
├── scripts/
│   ├── copy_files_census.sh          # Copies ACS raw files into yearly-data/
│   ├── rename_census_data_files.sh   # Standardizes ACS filenames
│   ├── load_census_data_db.py        # Loads combined ACS CSVs into PostgreSQL
│   ├── postgres-connection.py        # Tests database connectivity
│   └── pums/                         # PUMS pipeline scripts (run in numbered order)
│       ├── 01_copy_files_pums.sh
│       ├── 02_load_pums_data_db.py
│       ├── 03_generate_pums_sql_scripts.py
│       ├── 04_load_tract_il.py
│       ├── 05_pull_acs_tract_households.py
│       ├── 06_generate_pums_household_profile_adj_sql_scripts.py
│       ├── 07_generate_pums_household_final_sql_scripts.py
│       ├── 08_generate_pums_threshold_sql_scripts.py
│       ├── 09_generate_pums_household_final_updates_sql_scripts.py
│       ├── 10_generate_pums_below_alice_flag_sql_scripts.py
│       ├── 11_generate_pums_nonstudent_sql_scripts.py
│       ├── 12_run_pums_final_validation.py
│       ├── 13_generate_pums_alice_statistical_profile_sql_scripts_with_exports.py
│       └── 14_generate_pums_alice_profile_comparison_sql_scripts_with_exports.py
├── sql/
│   └── pums/
│       ├── ALICE/                    # Shared/cross-year SQL (run manually)
│       │   ├── 01_create_alice_household_profile_all_years.sql
│       │   ├── 02_create_alice_household_profile_all_years_clean.sql
│       │   ├── 03_create_tract_puma1902_lookup.sql
│       │   ├── 04_create_2022_puma1902_household.sql
│       │   ├── 05_create_2023_puma1902_household.sql
│       │   ├── 06_create_alpha.sql
│       │   ├── 07_create_pums_alice_threshold_tables.sql
│       │   └── 08_create_alice_thresholds_calibrated.sql
│       ├── 2019/                     # Per-year generated SQL (scripts 01–13)
│       ├── 2020_exp/                 # Per-year generated SQL (scripts 01–06)
│       ├── 2021/                     # Per-year generated SQL (scripts 01–13)
│       ├── 2022/                     # Per-year generated SQL (scripts 01–13)
│       ├── 2023/                     # Per-year generated SQL (scripts 01–13)
│       └── Final_Validation_Check.sql
├── requirements.txt
└── .env                              # Database credentials (not committed)
```
