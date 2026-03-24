# Community Pulse — Replication Guide

This guide gives the exact dependency-aware execution order for the Community Pulse repo so the project can be cloned and rerun with minimal guesswork.

It is organized in the order you should actually execute things:

1. environment setup
2. source data placement
3. ACS dimension layer
4. ACS fact layer
5. ACS validation
6. PUMS / ALICE pipeline
7. notebook layer
8. optional local extensions

---

## 1. Prerequisites

You need:

- Python 3.10+
- PostgreSQL running locally or remotely
- Git Bash or another shell for `.sh` scripts
- enough disk space for ACS and PUMS raw files
- `geopandas`, `shapely`, `scikit-learn`, and `scipy` for the later notebook layer

Install base dependencies:

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
pip install psycopg2-binary geopandas shapely scikit-learn scipy
```

Create `.env` in the project root:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

Verify the DB connection:

```bash
python scripts/postgres-connection.py
```

---

## 2. Source data placement

### ACS raw files

Place ACS downloads under `data/raw/` using the original folder naming convention expected by the project. The replication guide already expects 2019, 2021, 2022, and 2023 ACS 5-year folders for the tracked tables.

The documented ACS tables are:

- B19013 — Median Household Income
- B25003 — Housing Tenure
- B25070 — Gross Rent as % of Income
- DP04 — Selected Housing Characteristics
- S1101 — Household Composition
- S1701 — Poverty Status
- S1901 — Income Distribution
- S2301 — Employment Status
- S1501 — Educational Attainment
- S2401 — Occupation
- B03002 — Race / Ethnicity
- S0101 — Age Distribution
- S1601 — Language Spoken at Home

### PUMS raw files

Place Illinois housing and person folders under `data/raw/`. The project expects housing (`csv_hil_*`) and person (`csv_pil_*`) source files there.

### Additional geography files

These are needed later, not for the base pipelines:

- `data/pums/Tract-Illinois.csv` for the PUMA 1902 crosswalk step
- TIGER/Line tract, place, and optionally ZCTA shapefiles under `data/geo/` for the geography lookup notebook

---

## 3. ACS pipeline — exact execution order

The ACS scripts are split into:

- `scripts/acs/dim/`
- `scripts/acs/fact/`
- `scripts/acs/validation/`

Run all ACS scripts from the repo root.

### 3A. Dimension layer

Run these in order:

```bash
python scripts/acs/dim/01_inventory_acs_files.py
python scripts/acs/dim/02_extract_dim_tract_from_acs.py
python scripts/acs/dim/03_build_dim_tract.py
python scripts/acs/dim/04_validate_dim_tract.py
python scripts/acs/dim/05_build_final_dim_tract_and_bridge.py
```

What this creates:

- file inventory outputs under `outputs/acs/inventory/`
- `dim_tract`
- `bridge_tract_year`
- the final tract-year bridge used by the fact layer

### 3B. Fact layer

Run these Python scripts in order:

```bash
python scripts/acs/fact/01_generate_acs_staging_files.py
python scripts/acs/fact/02_create_acs_staging_sql_script.py
python scripts/acs/fact/03_create_acs_int_core_housing.py
python scripts/acs/fact/04_create_acs_int_housing_poverty_emp.py
python scripts/acs/fact/05_create_acs_int_demo_edu.py
python scripts/acs/fact/06_create_acs_fact_tract_metric_long.py
python scripts/acs/fact/07_create_acs_facts_tract_profile.py
```

What this creates:

- staging SQL / staging tables
- intermediate ACS views
- `fact_acs_tract_metric_long`
- `fact_acs_tract_profile`

### 3C. ACS SQL objects that downstream notebooks depend on

The repo’s existing documentation shows that the downstream notebooks depend on the revised ACS profile and frozen metric sheet. At minimum, make sure these SQL objects exist before running the notebook layer:

- `fact_acs_tract_profile_v2`
- `acs_frozen_metric_sheet_v2`

The documented SQL files involved are:

- `sql/acs/fact/07a_create_fact_acs_tract_profile.sql`
- `sql/acs/fact/07b_create_fact_acs_tract_profile_v2.sql`
- `sql/acs/fact/07c_rebuild_fact_acs_tract_profile_v2_audited.sql`
- `sql/acs/fact/08_create_metric_sheet.sql`
- `sql/acs/fact/09_create_acs_fact_tract_profile_v2_21101_fix.sql`
- `sql/acs/fact/10_update_metric_sheet.sql`

Recommended order:

1. create or refresh `fact_acs_tract_profile`
2. create or refresh `fact_acs_tract_profile_v2`
3. create the frozen metric sheet
4. apply any documented fixes / updates

If you are working only from the notebook layer, the repo documentation already treats `notebooks/01_build_acs_profile_v2_from_int_tables.ipynb` as a valid way to rebuild `fact_acs_tract_profile_v2`.

### 3D. ACS validation

Run the orchestrator after the dimension and fact layers are complete:

```bash
python scripts/acs/validation/04_run_full_validation.py
```

Or run the three passes individually:

```bash
python scripts/acs/validation/01_validate_stg_tables.py
python scripts/acs/validation/02_validate_int_tables.py
python scripts/acs/validation/03_validate_fact_tables.py
```

---

## 4. PUMS / ALICE pipeline — exact execution order

All PUMS pipeline scripts live under `scripts/pums/` and are already numbered in execution order.

### Step 1 — copy and rename raw PUMS files

```bash
bash scripts/pums/01_copy_files_pums.sh
```

### Step 2 — bulk load PUMS housing and person files

```bash
python scripts/pums/02_load_pums_data_db.py
```

### Step 3 — build Champaign-filtered person, housing, and household profile tables

```bash
python scripts/pums/03_generate_pums_sql_scripts.py
```

This generates and executes SQL scripts under `sql/pums/<year>/` for each year.

### Step 4 — load tract-to-PUMA crosswalk

First place `Tract-Illinois.csv` under `data/pums/`, then run:

```bash
python scripts/pums/04_load_tract_il.py
```

### Step 5 — create PUMA 1902 lookup and alpha prerequisites

Before the Python pull step, run this SQL manually:

```sql
\i sql/pums/ALICE/03_create_tract_puma1902_lookup.sql
```

Then run:

```bash
python scripts/pums/05_pull_acs_tract_households.py
```

Then execute the remaining documented SQL helpers in order:

- `sql/pums/ALICE/04_create_2022_puma1902_household.sql`
- `sql/pums/ALICE/05_create_2023_puma1902_household.sql`
- `sql/pums/ALICE/06_create_alpha.sql`

### Step 6 — create adjusted 2022 / 2023 household profile tables

```bash
python scripts/pums/06_generate_pums_household_profile_adj_sql_scripts.py
```

### Step 7 — create final household tables

```bash
python scripts/pums/07_generate_pums_household_final_sql_scripts.py
```

### Step 8 — create threshold tables and calibrated thresholds

```bash
python scripts/pums/08_generate_pums_threshold_sql_scripts.py
```

Then run the calibration SQL manually:

```sql
\i sql/pums/ALICE/08_create_alice_thresholds_calibrated.sql
```

### Step 9 — add derived columns to final household tables

```bash
python scripts/pums/09_generate_pums_household_final_updates_sql_scripts.py
```

### Step 10 — refresh below-ALICE flags using calibrated thresholds

```bash
python scripts/pums/10_generate_pums_below_alice_flag_sql_scripts.py
```

### Step 11 — create nonstudent ALICE household tables

```bash
python scripts/pums/11_generate_pums_nonstudent_sql_scripts.py
```

### Step 12 — run final PUMS validation

```bash
python scripts/pums/12_run_pums_final_validation.py
```

### Step 13 — generate ALICE statistical profiles and export CSVs

```bash
python scripts/pums/13_generate_pums_alice_statistical_profile_sql_scripts_with_exports.py
```

### Step 14 — generate nonstudent vs complete profile comparison

```bash
python scripts/pums/14_generate_pums_alice_profile_comparison_sql_scripts_with_exports.py
```

At this point, the repo’s committed PUMS export layer under `outputs/pums/` and `outputs/pums/tableau-data/` should be reproducible.

---

## 5. Notebook layer — exact dependency-aware order

The repo documentation already lists the notebook layer and its dependencies. The safest execution order is:

### Notebook 01 — build ACS profile v2

```text
notebooks/01_build_acs_profile_v2_from_int_tables.ipynb
```

Run this if you want to rebuild `fact_acs_tract_profile_v2` directly from the intermediate views before the rest of the notebook layer.

**Dependency:** ACS dimension + fact layers complete.

### Notebook 00 — tract geography lookup

```text
notebooks/00_acs_tract_geography_lookup.ipynb
```

Run this after `fact_acs_tract_profile_v2` exists.

**Dependency:** notebook 01 or SQL-generated `fact_acs_tract_profile_v2`, plus local tract/place/ZCTA shapefiles.

### Notebook 02 — ACS EDA on original fact table

```text
notebooks/02_acs_eda_all_years.ipynb
```

Optional if you want the original fact table EDA.

### Notebook 02b — ACS EDA with geography labels

```text
notebooks/02b_acs_eda_all_years.ipynb
```

**Dependency:** notebook 00 and `fact_acs_tract_profile_v2`

### Notebook 03 — ACS visuals on original fact table

```text
notebooks/03_acs_visuals.ipynb
```

Optional if you want the original visuals layer.

### Notebook 03b — ACS visuals with geography labels

```text
notebooks/03b_acs_visuals_geo.ipynb
```

**Dependency:** notebook 00 and `fact_acs_tract_profile_v2`

### Notebook 04 — ACS clustering with geography labels

```text
notebooks/04_acs_clustering_geo.ipynb
```

**Dependency:** notebook 00, `fact_acs_tract_profile_v2`, and `acs_frozen_metric_sheet_v2`

### Notebook 05 — ACS cluster transitions

```text
notebooks/05_acs_cluster_transitions.ipynb
```

**Dependency:** notebook 04 because it needs `cluster_assignments_all_years.csv`

### Notebook 06 — ACS ↔ PUMS / ALICE bridge

```text
notebooks/06_acs_pums_alice_bridge.ipynb
```

**Dependencies:**

- notebook 00 for geography labels
- notebook 04 if you want cluster labels joined
- year-level county ALICE totals from the PUMS layer

The existing repo documentation treats the county totals file as an input requirement for notebook 06. If you standardize that input later, do it after PUMS step 14 and before notebook 06.

---

## 6. Dependency summary

If you only want the base tables:

1. ACS dimension layer
2. ACS fact layer
3. PUMS pipeline 01–14

If you want the tract analytics layer:

4. ACS validation
5. notebook 01
6. notebook 00
7. notebook 02b
8. notebook 03b
9. notebook 04
10. notebook 05
11. notebook 06

Optional original-table notebooks:

- notebook 02
- notebook 03

---

## 7. Expected outputs by layer

### ACS output areas

- `outputs/acs/inventory/`
- `outputs/acs/validation/`
- `outputs/acs/analysis/eda/`
- `outputs/acs/analysis/geography_lookup/`
- `outputs/acs/analysis/visuals/`
- `outputs/acs/analysis/clustering/`
- `outputs/acs/analysis/transitions/`
- `outputs/acs/analysis/pums_alice_bridge/`
- `outputs/acs/analysis/pums_alice_bridge_capacity_adjusted/` if you are using the extended bridge layer already present in the repo outputs

### PUMS output areas

- `outputs/pums/`
- `outputs/pums/tableau-data/`

Important committed all-years PUMS outputs include:

- `below_alice_households_all_years.csv`
- `below_alice_stat_profile_all_years.csv`
- `nonstudent_stat_profile_all_years.csv`
- `alice_nonstudent_vs_complete_profile_compare_all_years.csv`

---

## 8. Recommended replication checkpoints

Use these checkpoints instead of trying to diagnose everything at the end.

### After ACS dimension layer
Confirm these exist:

- `dim_tract`
- `bridge_tract_year`

### After ACS fact layer
Confirm these exist:

- `fact_acs_tract_metric_long`
- `fact_acs_tract_profile`
- `fact_acs_tract_profile_v2`
- `acs_frozen_metric_sheet_v2`

### After ACS validation
Confirm `outputs/acs/validation/` has fresh run artifacts.

### After PUMS step 12
Confirm:

- `outputs/pums/04_balance_check.csv`
- `outputs/pums/08_nonstudent_filter_impact.csv`
- the validation output produced by step 12

### After PUMS step 14
Confirm:

- `outputs/pums/tableau-data/below_alice_stat_profile_all_years.csv`
- `outputs/pums/tableau-data/nonstudent_stat_profile_all_years.csv`
- `outputs/pums/tableau-data/alice_nonstudent_vs_complete_profile_compare_all_years.csv`

### After notebook layer
Confirm at minimum:

- `outputs/acs/analysis/geography_lookup/data/dim_tract_geography_lookup.csv`
- `outputs/acs/analysis/clustering/assignments/cluster_assignments_all_years.csv`
- `outputs/acs/analysis/transitions/data/aligned_cluster_assignments_all_years.csv`
- `outputs/acs/analysis/pums_alice_bridge/data/tract_alice_estimates_<label>.csv`

---

## 9. Optional local extension layer

If you also keep the extended local notebook layer that produced the already-committed `outputs/acs/analysis/pums_alice_bridge_capacity_adjusted/` outputs, run it **after notebook 06**. That extension is logically downstream of the base bridge and should never be run before the ACS notebook and PUMS profile layers are complete.

---

## 10. One-line replication order

If you want the shortest possible version, this is the exact order:

1. `python scripts/postgres-connection.py`
2. ACS dim `01` → `05`
3. ACS fact `01` → `07`
4. ACS SQL for `fact_acs_tract_profile_v2` and `acs_frozen_metric_sheet_v2`
5. ACS validation `04_run_full_validation.py`
6. PUMS `01` → `14`, including the documented manual SQL steps at steps 5 and 8
7. notebooks `01` → `00` → `02b` → `03b` → `04` → `05` → `06`
8. optional original notebooks `02`, `03`
9. optional local extension notebooks after notebook 06

That order respects the layer dependencies across the whole project.
