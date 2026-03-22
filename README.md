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
bash scripts/rename_files.sh
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

This pipeline loads Census PUMS person and housing microdata for Illinois, filters to Champaign County, and builds household-level profiles including student population and ALICE indicators.

Supported years: **2019, 2020 (experimental), 2021, 2022, 2023**

> **Note on 2022/2023 PUMA boundaries:** PUMS PUMA definitions changed after 2021. For 2019–2021, Champaign County maps to PUMA 2100. For 2022–2023, it maps to PUMAs 1901 and 1902, but PUMA 1902 extends beyond Champaign County. The pipeline applies an ACS5-based weight adjustment to allocate PUMA 1902 households proportionally to Champaign only. See [Step 5](#step-5--build-puma-1902-weight-adjustment-2022-and-2023-only) below.

### Data Sources

Download PUMS CSV files from the [Census PUMS Access page](https://www.census.gov/programs-surveys/acs/microdata/access.html) for Illinois. You need two files per year:

- **Housing file** — the folder will be named like `csv_hil_2022`
- **Person file** — the folder will be named like `csv_pil_2022`

Place the downloaded folders under `data/raw/`:

```
data/raw/
├── csv_hil_2019/
├── csv_pil_2019/
├── csv_hil_2020-experimental/
├── csv_pil_2020-experimental/
├── csv_hil_2021/
├── csv_pil_2021/
├── csv_hil_2022/
├── csv_pil_2022/
├── csv_hil_2023/
└── csv_pil_2023/
```

### Steps

**Step 1 — Copy and rename PUMS files**

```bash
bash scripts/copy_files_pums.sh
```

Reads from `data/raw/`, extracts the year, and copies the CSV to `data/pums/<year>/` as `housing.csv` (from `hil` folders) or `person.csv` (from `pil` folders).

Output structure:

```
data/pums/
├── 2019/
│   ├── housing.csv
│   └── person.csv
├── 2020-experimental/
├── 2021/
├── 2022/
└── 2023/
```

**Step 2 — Load PUMS data into PostgreSQL**

```bash
python scripts/load_pums_data_db.py
```

Loads each file into a raw table. Column names are cleaned to snake_case with these specific standardizations:

| Original column | Loaded as |
|---|---|
| `ST` | `state` |
| `TYPE` | `typehugq` |
| `ACCESS` | `accessinet` |
| `YBL` | `yrblt` |

### Raw Tables Created

| Table | Source |
|---|---|
| `housing_2019_raw` | data/pums/2019/housing.csv |
| `person_2019_raw` | data/pums/2019/person.csv |
| `housing_2020_exp_raw` | data/pums/2020-experimental/housing.csv |
| `person_2020_exp_raw` | data/pums/2020-experimental/person.csv |
| `housing_2021_raw` | data/pums/2021/housing.csv |
| `person_2021_raw` | data/pums/2021/person.csv |
| `housing_2022_raw` | data/pums/2022/housing.csv |
| `person_2022_raw` | data/pums/2022/person.csv |
| `housing_2023_raw` | data/pums/2023/housing.csv |
| `person_2023_raw` | data/pums/2023/person.csv |

**Step 3 — Generate Champaign analysis SQL scripts**

```bash
python scripts/generate_pums_sql_scripts.py
```

Generates SQL files under `sql/pums/<year>/` that filter data to Champaign County and build analysis tables. Scripts generated per year (run them in order):

| Script | Creates Table |
|---|---|
| `01_create_pums_person_{y}_champaign.sql` | `person_{y}_champaign` |
| `02_create_pums_housing_{y}_champaign.sql` | `housing_{y}_champaign` |
| `03_create_pums_person_student_flags_{y}.sql` | `person_student_flags_{y}` |
| `04_create_pums_household_student_agg_{y}.sql` | `household_student_agg_{y}` |
| `05_create_pums_household_base_{y}.sql` | `household_base_{y}` |
| `06_create_pums_alice_household_profile_{y}.sql` | `alice_household_profile_{y}` |

**Step 4 — Run the SQL scripts**

Run the scripts in order for each year using `psql` or any SQL client:

```bash
for f in sql/pums/2022/*.sql; do
  psql -h localhost -U <DB_USER> -d <DB_NAME> -f "$f"
done
```

Or run them manually one by one in your SQL client.

---

**Step 5 — Build PUMA 1902 weight adjustment (2022 and 2023 only)**

PUMA 1902 straddles Champaign County and adjacent counties. This step builds a proportional allocation factor (alpha) so that PUMA 1902 households are weighted to reflect only the Champaign portion, using ACS5 tract-level household counts as the basis.

**5a — Load the Illinois tract-to-PUMA crosswalk**

Place the `Tract-Illinois.csv` file in `data/pums/`, then run:

```bash
python scripts/load_tract_il.py
```

Creates table: `tract_il`

**5b — Run the ALICE shared SQL scripts (in order)**

These scripts are in `sql/pums/ALICE/` and are run once (not per year):

| Script | Creates Table | Notes |
|---|---|---|
| `03_create_tract_puma1902_lookup.sql` | `tract_puma1902_lookup` | Filters `tract_il` to PUMA 1902 tracts |
| `04_create_2022_puma1902_household.sql` | `acs5_2022_puma1902_households` | Needs step 5c first |
| `05_create_2023_puma1902_household.sql` | `acs5_2023_puma1902_households` | Needs step 5c first |
| `06_create_alpha.sql` | `puma1902_alpha` | Champaign allocation factors for 2022 and 2023 |

**5c — Pull ACS5 household counts from the Census API**

Run after `03_create_tract_puma1902_lookup.sql` (the script reads counties from `tract_puma1902_lookup`):

```bash
python scripts/pull_acs_tract_households.py
```

Creates tables: `acs5_2022_b11001_raw`, `acs5_2023_b11001_raw`

Then run scripts `04` and `05` from `sql/pums/ALICE/`.

---

**Step 6 — Build ALICE cross-year combined tables**

Run these scripts from `sql/pums/ALICE/` in order:

| Script | Creates Table |
|---|---|
| `01_create_alice_household_profile_all_years.sql` | `alice_household_profile_all_years` |
| `02_create_alice_household_profile_all_years_clean.sql` | `alice_household_profile_all_years_clean` |

---

**Step 7 — Create ALICE threshold reference tables**

```bash
python scripts/generate_threshold_sql_scripts.py
```

Generates and executes:

| Script | Action |
|---|---|
| `ALICE/07_create_alice_threshold_tables.sql` | Creates `illinois_essentials_index` and `alice_thresholds` |
| `2023/09_load_alice_thresholds_2023_exact.sql` | Loads 2023 United For ALICE budget thresholds |
| `{y}/09_backfill_alice_thresholds_{y}.sql` | Backfills 2019/2021/2022 thresholds using Illinois AEI ratios |

The `alice_thresholds` table holds monthly and annual survival budgets for 9 household composition types across all analysis years.

---

**Step 8 — Generate and run household final SQL scripts**

```bash
python scripts/generate_pums_household_final_sql_scripts.py
```

Creates `alice_household_final_{y}` from the profile table, applying population filters (occupied units, non-zero person count, non-null income) and setting the analysis weight. For 2022/2023, uses the alpha-adjusted weight (`wgtp_adj`); for 2019/2021, uses raw `wgtp`.

```bash
python scripts/generate_pums_household_final_updates_sql_scripts.py
```

Adds derived columns to `alice_household_final_{y}`:

| Column | Description |
|---|---|
| `hincp_adj_real` | Inflation-adjusted household income (`hincp * adjinc / 1,000,000`) |
| `hh_comp_key` | Household composition category key (e.g. `2_adult_1_child`) |
| `student_heavy_flag` | 1 if household is student-heavy (from `student_heavy_flag_rule_b`) |
| `below_alice_flag` | 1 if `hincp_adj_real` is below the ALICE threshold for the household type |

---

**Step 9 — Create non-student ALICE household tables**

```bash
python scripts/generate_pums_nonstudent_sql_scripts.py
```

Creates `alice_nonstudent_households_{y}` — ALICE households (`below_alice_flag = 1`) that are not student-heavy (`student_heavy_flag = 0`). Years: 2019, 2021, 2022, 2023.

---

**Step 10 — Refresh the below_alice_flag (optional)**

```bash
python scripts/generate_pums_below_alice_flag_sql_scripts.py
```

Regenerates and re-executes the `below_alice_flag` update on `alice_household_final_{y}`, optionally refreshing `annual_alice_threshold` from `alice_thresholds` first. Also generates inline validation SQL that compares stored flag totals against direct income comparisons. Years: 2019, 2021, 2022, 2023.

---

### Final ALICE Tables Summary

| Table | Description |
|---|---|
| `alice_household_final_{y}` | Analysis-ready households with income, composition, and ALICE flag columns |
| `alice_nonstudent_households_{y}` | Subset of ALICE households excluding student-heavy households |
| `alice_thresholds` | ALICE annual income thresholds by year and household composition type |
| `illinois_essentials_index` | Illinois ALICE Essentials Index values used for threshold backfilling |
| `puma1902_alpha` | Champaign County allocation factors for PUMA 1902 (2022 and 2023) |
| `alice_household_profile_all_years` | Union of `alice_household_profile_{y}` across all years |
| `alice_household_profile_all_years_clean` | Filtered version (non-null person count, income, and household size) |

### QA

`sql/pums/QA.sql` contains ad-hoc validation queries for checking row counts, weighted household totals, null rates, income distributions, and ALICE flag consistency across all years.

---

## Project Structure

```
Community-Pulse/
├── data/
│   ├── raw/                        # Downloaded ACS and PUMS source files
│   ├── yearly-data/                # ACS CSVs organized by category (output of copy_files_census.sh)
│   ├── combined-data/              # Merged multi-year ACS CSVs (output of notebook)
│   └── pums/                       # Processed PUMS files and crosswalk data
│       └── Tract-Illinois.csv      # Illinois tract-to-PUMA crosswalk (Census)
├── notebooks/
│   ├── combine_yearly_files.ipynb  # Merges yearly ACS files into combined CSVs
│   └── pums_files.ipynb            # Exploratory analysis of PUMS data
├── scripts/
│   ├── copy_files_census.sh                          # Copies ACS raw files into yearly-data/
│   ├── rename_files.sh                               # Standardizes ACS filenames
│   ├── load_census_data_db.py                        # Loads combined ACS CSVs into PostgreSQL
│   ├── copy_files_pums.sh                            # Copies PUMS files into data/pums/
│   ├── load_pums_data_db.py                          # Loads PUMS CSVs into PostgreSQL
│   ├── load_tract_il.py                              # Loads Illinois tract-PUMA crosswalk into tract_il
│   ├── pull_acs_tract_households.py                  # Pulls ACS5 tract household counts from Census API
│   ├── generate_pums_sql_scripts.py                  # Generates scripts 01–06 (Champaign filter + profile)
│   ├── generate_pums_household_final_sql_scripts.py  # Generates script 07 (household final table)
│   ├── generate_pums_household_final_updates_sql_scripts.py  # Generates script 08 (derived columns)
│   ├── generate_threshold_sql_scripts.py             # Generates ALICE threshold tables and backfill SQL
│   ├── generate_pums_nonstudent_sql_scripts.py       # Generates script 10 (non-student ALICE households)
│   ├── generate_pums_below_alice_flag_sql_scripts.py # Generates script 11 (below_alice_flag refresh)
│   └── postgres-connection.py                        # Tests database connectivity
├── sql/
│   └── pums/                       # Generated SQL scripts
│       ├── ALICE/                  # Cross-year and shared ALICE tables (scripts 01–07)
│       ├── 2019/                   # Per-year scripts 01–11
│       ├── 2020_exp/               # Per-year scripts 01–06
│       ├── 2021/                   # Per-year scripts 01–11
│       ├── 2022/                   # Per-year scripts 01–11
│       ├── 2023/                   # Per-year scripts 01–11
│       └── QA.sql                  # Validation queries
├── requirements.txt
└── .env                            # Database credentials (not committed)
```

---

## Tech Stack

- **Python** — pipeline scripting and data processing
- **pandas** — CSV ingestion and column normalization
- **SQLAlchemy + psycopg2** — PostgreSQL connectivity
- **PostgreSQL** — data storage and querying
- **JupyterLab** — exploratory data analysis
- **python-dotenv** — environment variable management
- **Bash** — file organization and renaming scripts
- **Census API** — ACS5 tract-level household counts (via `requests`)
