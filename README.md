# Community Pulse

A data pipeline for US Census ACS and PUMS data focused on Champaign, Illinois. Loads multi-year demographic and housing microdata into PostgreSQL for community-level analysis, including ALICE (Asset Limited, Income Constrained, Employed) household profiling.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Pipeline 1 — ACS Summary Data](#pipeline-1--acs-summary-data)
- [Pipeline 2 — PUMS Microdata](#pipeline-2--pums-microdata)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)

---

## Prerequisites

- Python 3.10+
- PostgreSQL (running and accessible)
- Git Bash or any bash shell (for `.sh` scripts on Windows)

---

## Setup

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

Reads from `data/raw/`, strips ACS table codes and year ranges from folder names, and copies CSV files into `data/yearly-data/<category>/`.

**Step 2 — Standardize filenames**

```bash
bash scripts/rename_census_data_files.sh
```

Renames each CSV to `<category><year>-Data.csv` for consistent naming.

**Step 3 — Combine yearly files**

Open and run [notebooks/combine_yearly_files.ipynb](notebooks/combine_yearly_files.ipynb). This merges all per-year CSVs for each category into a single `*_combined.csv` file under `data/combined-data/`.

**Step 4 — Load into PostgreSQL**

```bash
python scripts/load_census_data_db.py
```

Loads each combined CSV into its own table. Column names are cleaned to snake_case and truncated to PostgreSQL's 63-character limit with auto-deduplication.

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

## Pipeline 2 — PUMS Microdata

This pipeline loads Census PUMS person and housing microdata for Illinois, filters to Champaign County, and builds household-level ALICE profiles.

Supported years: **2019, 2020 (experimental), 2021, 2022, 2023**

> **Note on 2022/2023 PUMA boundaries:** PUMA definitions changed after 2021. For 2019–2021, Champaign County maps to PUMA 2100. For 2022–2023, it maps to PUMAs 1901 and 1902, but PUMA 1902 extends beyond Champaign County. Steps 4–6 build an ACS5-based allocation factor (alpha) to weight PUMA 1902 households proportionally to Champaign only.

All PUMS pipeline scripts live in `scripts/pums/` and are numbered in execution order.

### Data Sources

Download PUMS CSV files from the [Census PUMS Access page](https://www.census.gov/programs-surveys/acs/microdata/access.html) for Illinois. You need two files per year:

- **Housing file** — folder named like `csv_hil_2022`
- **Person file** — folder named like `csv_pil_2022`

Place downloaded folders under `data/raw/`:

```
data/raw/
├── csv_hil_2019/        csv_pil_2019/
├── csv_hil_2020-experimental/        csv_pil_2020-experimental/
├── csv_hil_2021/        csv_pil_2021/
├── csv_hil_2022/        csv_pil_2022/
└── csv_hil_2023/        csv_pil_2023/
```

---

### Step 1 — Copy and rename PUMS files

```bash
bash scripts/pums/01_copy_files_pums.sh
```

Copies files from `data/raw/` into `data/pums/<year>/` as `housing.csv` and `person.csv`.

```
data/pums/
├── 2019/   2020-experimental/   2021/   2022/   2023/
│   ├── housing.csv
│   └── person.csv
```

---

### Step 2 — Load PUMS data into PostgreSQL

```bash
python scripts/pums/02_load_pums_data_db.py
```

Loads each file into a raw table using PostgreSQL `COPY` for fast ingestion. Column names are cleaned to snake_case with these specific standardizations:

| Original | Loaded as |
|---|---|
| `ST` | `state` |
| `TYPE` | `typehugq` |
| `ACCESS` | `accessinet` |
| `YBL` | `yrblt` |

**Raw tables created:** `alice_housing_{y}_raw`, `alice_person_{y}_raw` (for each year; 2020 uses `_2020_exp_raw`)

---

### Step 3 — Generate Champaign filter and household profile SQL

```bash
python scripts/pums/03_generate_pums_sql_scripts.py
```

Generates and executes SQL scripts 01–06 under `sql/pums/<year>/` for each year. Run order per year:

| Script | Creates Table | Description |
|---|---|---|
| `01_create_pums_person_{y}_champaign.sql` | `alice_person_{y}_champaign` | Filters person records to Champaign PUMA(s) |
| `02_create_pums_housing_{y}_champaign.sql` | `alice_housing_{y}_champaign` | Filters housing records to Champaign PUMA(s) |
| `03_create_pums_person_student_flags_{y}.sql` | `alice_person_student_flags_{y}` | Per-person student and employment flags |
| `04_create_pums_household_student_agg_{y}.sql` | `alice_household_student_agg_{y}` | Household-level student aggregations |
| `05_create_pums_household_base_{y}.sql` | `alice_household_base_{y}` | Core housing unit fields |
| `06_create_pums_alice_household_profile_{y}.sql` | `alice_household_profile_{y}` | Joins base housing with student aggregation |

> **Optional** — after Step 3, you can run these cross-year views from `sql/pums/ALICE/`:
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

Run these SQL scripts from `sql/pums/ALICE/` in order:

```bash
# In your SQL client or psql:
\i sql/pums/ALICE/03_create_tract_puma1902_lookup.sql
```

Creates `tract_puma1902_lookup` (filters `tract_il` to PUMA 1902 tracts only).

Then pull ACS5 household counts from the Census API:

```bash
python scripts/pums/05_pull_acs_tract_households.py
```

Creates tables: `acs5_2022_b11001_raw`, `acs5_2023_b11001_raw`

Then run the remaining ALICE setup scripts:

```bash
\i sql/pums/ALICE/04_create_2022_puma1902_household.sql
\i sql/pums/ALICE/05_create_2023_puma1902_household.sql
\i sql/pums/ALICE/06_create_alpha.sql
```

| Script | Creates Table | Description |
|---|---|---|
| `04_create_2022_puma1902_household.sql` | `acs5_2022_puma1902_households` | Joins tract lookup with 2022 ACS5 counts |
| `05_create_2023_puma1902_household.sql` | `acs5_2023_puma1902_households` | Joins tract lookup with 2023 ACS5 counts |
| `06_create_alpha.sql` | `alice_puma1902_alpha` | Champaign allocation factors (alpha) for 2022 and 2023 |

---

### Step 6 — Generate adjusted household profile tables (2022 and 2023 only)

```bash
python scripts/pums/06_generate_pums_household_profile_adj_sql_scripts.py
```

Generates and executes `06b_create_pums_alice_household_profile_{y}_adj.sql` for 2022 and 2023. Applies the PUMA 1902 alpha from `alice_puma1902_alpha` to scale down PUMA 1902 household weights to the Champaign-only portion:

- PUMA 1901 households: `wgtp_adj = wgtp` (no adjustment needed)
- PUMA 1902 households: `wgtp_adj = wgtp × alpha`

Creates tables: `alice_household_profile_2022_adj`, `alice_household_profile_2023_adj`

---

### Step 7 — Create household final tables

```bash
python scripts/pums/07_generate_pums_household_final_sql_scripts.py
```

Creates `alice_household_final_{y}` from the profile table, applying population filters (occupied units, non-zero person count, non-null income) and setting the analysis weight. Uses `wgtp_adj` for 2022/2023 and raw `wgtp` for 2019/2021.

---

### Step 8 — Create ALICE threshold tables

```bash
python scripts/pums/08_generate_pums_threshold_sql_scripts.py
```

Generates and executes:

| Script | Action |
|---|---|
| `ALICE/07_create_pums_alice_threshold_tables.sql` | Creates `illinois_essentials_index` and `alice_thresholds` |
| `2023/09_load_alice_thresholds_2023_exact.sql` | Loads 2023 United For ALICE monthly survival budgets |
| `{y}/09_backfill_alice_thresholds_{y}.sql` | Backfills 2019/2021/2022 thresholds using Illinois AEI ratios |

Then run the calibration script manually in your SQL client:

```bash
\i sql/pums/ALICE/08_create_alice_thresholds_calibrated.sql
```

Creates `alice_thresholds_calibrated` — a copy of `alice_thresholds` with upward adjustments applied to dominant childless household buckets (`1_adult_0_child`, `2_adult_0_child`) to account for local cost of living. The adjusted thresholds are used in all downstream ALICE flag calculations.

---

### Step 9 — Add derived columns to household final tables

```bash
python scripts/pums/09_generate_pums_household_final_updates_sql_scripts.py
```

Adds the following columns to `alice_household_final_{y}`:

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

Regenerates `10_update_pums_below_alice_flag_calibrated_{y}.sql` for 2019, 2021, 2022, and 2023, then executes them. Refreshes `annual_alice_threshold` on `alice_household_final_{y}` from `alice_thresholds_calibrated` and recalculates `below_alice_flag` accordingly.

---

### Step 11 — Create non-student ALICE household tables

```bash
python scripts/pums/11_generate_pums_nonstudent_sql_scripts.py
```

Creates `alice_nonstudent_households_{y}` — households that are both below the ALICE threshold (`below_alice_flag = 1`) and not student-heavy (`student_heavy_flag = 0`).

---

### Final ALICE Tables Summary

| Table | Description |
|---|---|
| `alice_household_final_{y}` | Analysis-ready households with income, composition key, and ALICE flag |
| `alice_nonstudent_households_{y}` | ALICE households excluding student-heavy households |
| `alice_thresholds` | Raw ALICE annual income thresholds by year and household composition |
| `alice_thresholds_calibrated` | Thresholds with calibration adjustments for dominant childless buckets |
| `illinois_essentials_index` | Illinois AEI values used to backfill thresholds to prior years |
| `alice_puma1902_alpha` | Champaign County allocation factors for PUMA 1902 (2022 and 2023) |
| `alice_household_profile_all_years` | Union of `alice_household_profile_{y}` across all years |
| `alice_household_profile_all_years_clean` | Filtered version (non-null person count, income, and household size) |

---

### QA

`sql/pums/QA.sql` contains validation queries for checking row counts, weighted household totals, null rates, income distributions, ALICE flag consistency, and calibrated vs. raw threshold comparisons across all years.

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
├── notebooks/
│   ├── combine_yearly_files.ipynb    # Merges yearly ACS files into combined CSVs
│   └── pums_files.ipynb              # Exploratory analysis of PUMS data
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
│       └── 11_generate_pums_nonstudent_sql_scripts.py
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
│       ├── 2019/                     # Per-year generated SQL (scripts 01–11)
│       ├── 2020_exp/                 # Per-year generated SQL (scripts 01–06)
│       ├── 2021/                     # Per-year generated SQL (scripts 01–11)
│       ├── 2022/                     # Per-year generated SQL (scripts 01–11)
│       ├── 2023/                     # Per-year generated SQL (scripts 01–11)
│       └── QA.sql                    # Validation queries
├── requirements.txt
└── .env                              # Database credentials (not committed)
```

---

## Tech Stack

- **Python** — pipeline scripting and data processing
- **pandas** — CSV ingestion and column normalization
- **SQLAlchemy + psycopg2** — PostgreSQL connectivity and COPY-based bulk loading
- **PostgreSQL** — data storage and querying
- **JupyterLab** — exploratory data analysis
- **python-dotenv** — environment variable management
- **Bash** — file organization and renaming scripts
- **Census API** — ACS5 tract-level household counts (via `requests`)
