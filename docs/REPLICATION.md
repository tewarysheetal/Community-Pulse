# Community Pulse — Replication Guide

This guide gives the exact dependency-aware execution order for the Community Pulse repo so the project can be cloned and rerun with minimal guesswork.

It is organized in the order you should actually execute things:

1. environment setup
2. source data placement
3. ACS pipeline
4. PUMS / ALICE pipeline
5. county totals bridge input
6. notebook layer
7. final outputs
8. ML layer

---

## 1. Prerequisites

You need:

- Python 3.10+
- PostgreSQL running locally or remotely
- Git Bash or another shell for `.sh` scripts
- enough disk space for ACS and PUMS raw files
- `geopandas`, `shapely`, `scikit-learn`, and `scipy` for the geography, bridge, final output, and ML layers

Install the environment:

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

Place ACS downloads under the expected raw folders under `data/raw/`. The project currently expects the four ACS years used throughout the analysis:

- 2019
- 2021
- 2022
- 2023

Tracked ACS tables:

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

Place Illinois housing and person folders under the expected raw directory. The project expects:

- housing files like `csv_hil_*`
- person files like `csv_pil_*`

### Additional geography files

These are needed later for geography lookup and final maps:

- `data/pums/Tract-Illinois.csv` for the PUMA 1902 crosswalk step
- TIGER/Line tract shapefile
- TIGER/Line place shapefile
- optional ZCTA shapefile

A practical local location is:

```text
data/geo/tl_2023_17_tract/
data/geo/tl_2023_17_place/
data/geo/tl_2020_us_zcta520/
```

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

Expected core outputs:

- `dim_tract`
- `bridge_tract_year`
- file inventory outputs under `outputs/acs/inventory/`

### 3B. Fact layer

Run these in order:

```bash
python scripts/acs/fact/01_generate_acs_staging_files.py
python scripts/acs/fact/02_create_acs_staging_sql_script.py
python scripts/acs/fact/03_create_acs_int_core_housing.py
python scripts/acs/fact/04_create_acs_int_housing_poverty_emp.py
python scripts/acs/fact/05_create_acs_int_demo_edu.py
python scripts/acs/fact/06_create_acs_fact_tract_metric_long.py
python scripts/acs/fact/07_create_acs_facts_tract_profile.py
```

Expected core outputs:

- staging tables
- intermediate ACS tables / views
- `fact_acs_tract_metric_long`
- `fact_acs_tract_profile`

### 3C. ACS SQL objects required by downstream notebooks

Before the notebook layer, make sure these objects exist:

- `fact_acs_tract_profile_v2`
- `acs_frozen_metric_sheet_v2`

Relevant SQL files:

- `sql/acs/fact/07a_create_fact_acs_tract_profile.sql`
- `sql/acs/fact/07b_create_fact_acs_tract_profile_v2.sql`
- `sql/acs/fact/07c_rebuild_fact_acs_tract_profile_v2_audited.sql`
- `sql/acs/fact/08_create_metric_sheet.sql`
- `sql/acs/fact/09_create_acs_fact_tract_profile_v2_21101_fix.sql`
- `sql/acs/fact/10_update_metric_sheet.sql`

Recommended order:

1. refresh `fact_acs_tract_profile`
2. refresh `fact_acs_tract_profile_v2`
3. create the frozen metric sheet
4. apply any documented fixes or updates

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

All PUMS scripts live under `scripts/pums/`.

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

### Step 4 — load tract-to-PUMA crosswalk

Place `Tract-Illinois.csv` under `data/pums/`, then run:

```bash
python scripts/pums/04_load_tract_il.py
```

### Step 5 — create PUMA 1902 lookup and alpha prerequisites

Run this SQL manually first:

```sql
\i sql/pums/ALICE/03_create_tract_puma1902_lookup.sql
```

Then run:

```bash
python scripts/pums/05_pull_acs_tract_households.py
```

Then execute the remaining SQL helpers in order:

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

### Step 15 — create county-level ALICE totals bridge input

Run the bridge-input helper after the PUMS statistical profile exports exist:

```bash
python scripts/pums/15_generate_alice_county_totals_bridge_sql_with_exports.py
```

Expected outputs:

- `outputs/pums/bridge/alice_county_totals_bridge.csv`
- `outputs/pums/bridge/alice_county_totals_complete_calibrated.csv`
- `outputs/pums/bridge/alice_county_totals_nonstudent_calibrated.csv`

This is the standard input used by the ACS ↔ PUMS / ALICE bridge notebook.

---

## 5. Notebook layer — exact dependency-aware order

The notebook layer is downstream of both the ACS and PUMS pipelines.

Use this functional order even if local filenames differ slightly.

### Notebook 1 — rebuild ACS profile v2

Suggested file:
- `notebooks/01_build_acs_profile_v2_from_int_tables.ipynb`

Dependency:
- ACS dimension and fact layers complete

Purpose:
- rebuild `fact_acs_tract_profile_v2` from the ACS intermediate layer if needed

### Notebook 2 — tract geography lookup

Suggested file:
- `notebooks/00_acs_tract_geography_lookup.ipynb`
- or local renamed equivalent such as `notebooks/04_acs_tract_geography_lookup.ipynb`

Dependencies:
- `fact_acs_tract_profile_v2`
- local tract / place / optional ZCTA shapefiles

Purpose:
- create the tract geography lookup and map-ready geography layer

### Notebook 3 — ACS EDA with geography labels

Suggested file:
- `notebooks/02b_acs_eda_all_years.ipynb`
- or local geography-updated equivalent

Dependencies:
- notebook 1
- notebook 2

Purpose:
- produce geography-aware EDA summaries and tract-level descriptive outputs

### Notebook 4 — ACS visuals with geography labels

Suggested file:
- `notebooks/03b_acs_visuals_geo.ipynb`
- or local geography-updated equivalent

Dependencies:
- notebook 1
- notebook 2

Purpose:
- produce heatmaps, rankings, change tables, and choropleth-ready ACS exports

### Notebook 5 — ACS clustering with geography labels

Suggested file:
- `notebooks/04_acs_clustering_geo.ipynb`
- or local equivalent such as `03_acs_clustering_geo.ipynb`

Dependencies:
- notebook 1
- notebook 2
- `acs_frozen_metric_sheet_v2`

Purpose:
- build yearly clustering outputs and cluster assignments

### Notebook 6 — ACS cluster transitions

Suggested file:
- `notebooks/05_acs_cluster_transitions.ipynb`

Dependencies:
- notebook 5

Purpose:
- align clusters across years and build stable-tract transition outputs

### Notebook 7 — ACS ↔ PUMS / ALICE bridge

Suggested file:
- `notebooks/06_acs_pums_alice_bridge.ipynb`

Dependencies:
- notebook 2
- notebook 5 if cluster labels are joined
- PUMS step 15 if using standardized county ALICE totals

Purpose:
- allocate county ALICE totals across tracts using ACS tract hardship

### Notebook 8 — capacity-adjusted bridge

Suggested file:
- `notebooks/07_acs_pums_alice_bridge_capacity_adjusted.ipynb`
- or local equivalent such as `acs_pums_alice_bridge_capacity_adjusted.ipynb`

Dependencies:
- notebook 7
- or direct access to `outputs/pums/bridge/alice_county_totals_nonstudent_calibrated.csv`

Purpose:
- produce the capacity-aware tract ALICE estimates used for final outputs

### Notebook 9 — final ALICE outputs

Suggested file:
- `notebooks/08_final_alice_maps_and_summary_outputs.ipynb`
- or local equivalent such as `final_alice_maps_and_summary_outputs_outputs_final.ipynb`

Dependencies:
- notebook 2
- notebook 8

Purpose:
- create final integrated outputs under `outputs/final/`

### Notebook 10 — ML modeling

Suggested file:
- `notebooks/09_acs_alice_ml_modeling.ipynb`
- or local equivalent such as `acs_alice_ml_modeling_fixed.ipynb`

Dependencies:
- notebook 9

Purpose:
- build regression and classification models on the final integrated tract output

---

## 6. Dependency summary

### Base tables only

1. ACS dimension layer
2. ACS fact layer
3. ACS validation
4. PUMS pipeline steps 1–14

### Standardized bridge-ready project

5. PUMS step 15
6. notebook 1
7. notebook 2
8. notebook 3
9. notebook 4
10. notebook 5
11. notebook 6
12. notebook 7

### Final integrated and ML project

13. notebook 8
14. notebook 9
15. notebook 10

---

## 7. Expected outputs by layer

### ACS outputs

- `outputs/acs/inventory/`
- `outputs/acs/validation/`
- `outputs/acs/analysis/eda/`
- `outputs/acs/analysis/geography_lookup/`
- `outputs/acs/analysis/visuals/`
- `outputs/acs/analysis/clustering/`
- `outputs/acs/analysis/transitions/`
- `outputs/acs/analysis/pums_alice_bridge/`
- `outputs/acs/analysis/pums_alice_bridge_capacity_adjusted/`

### PUMS outputs

- `outputs/pums/`
- `outputs/pums/tableau-data/`
- `outputs/pums/bridge/`

Important all-years PUMS outputs include:

- `below_alice_households_all_years.csv`
- `below_alice_stat_profile_all_years.csv`
- `nonstudent_stat_profile_all_years.csv`
- `alice_nonstudent_vs_complete_profile_compare_all_years.csv`

### Final integrated outputs

- `outputs/final/data/`
- `outputs/final/summary/`
- `outputs/final/maps/`
- `outputs/final/plots/`
- `outputs/final/ml/`

---

## 8. Recommended checkpoints

Use these checkpoints to avoid finding issues only at the end.

### After ACS dimension layer

Confirm:

- `dim_tract`
- `bridge_tract_year`

### After ACS fact layer

Confirm:

- `fact_acs_tract_metric_long`
- `fact_acs_tract_profile`
- `fact_acs_tract_profile_v2`
- `acs_frozen_metric_sheet_v2`

### After ACS validation

Confirm fresh validation files exist under `outputs/acs/validation/`.

### After PUMS step 12

Confirm:

- `outputs/pums/04_balance_check.csv`
- `outputs/pums/08_nonstudent_filter_impact.csv`

### After PUMS step 14

Confirm:

- `outputs/pums/tableau-data/below_alice_stat_profile_all_years.csv`
- `outputs/pums/tableau-data/nonstudent_stat_profile_all_years.csv`
- `outputs/pums/tableau-data/alice_nonstudent_vs_complete_profile_compare_all_years.csv`

### After PUMS step 15

Confirm:

- `outputs/pums/bridge/alice_county_totals_bridge.csv`
- `outputs/pums/bridge/alice_county_totals_nonstudent_calibrated.csv`

### After notebook 2

Confirm:

- `outputs/acs/analysis/geography_lookup/data/dim_tract_geography_lookup.csv`

### After notebook 5

Confirm:

- `outputs/acs/analysis/clustering/assignments/cluster_assignments_all_years.csv`

### After notebook 6

Confirm:

- `outputs/acs/analysis/transitions/data/aligned_cluster_assignments_all_years.csv`

### After notebook 8

Confirm:

- `outputs/acs/analysis/pums_alice_bridge_capacity_adjusted/data/tract_alice_estimates_nonstudent_calibrated_capacity_adjusted.csv`

### After notebook 9

Confirm:

- `outputs/final/data/final_tract_alice_outputs_nonstudent_calibrated_capacity_adjusted.csv`
- `outputs/final/summary/top_bottom_alice_areas_nonstudent_calibrated_capacity_adjusted.csv`

### After notebook 10

Confirm:

- `outputs/final/ml/summary/ml_run_summary.csv`
- `outputs/final/ml/summary/regression_model_results.csv`
- `outputs/final/ml/summary/classification_model_results.csv`
- `outputs/final/ml/feature_importance/feature_importance_all_models.csv`

---

## 9. One-line execution order

If you want the shortest possible version, this is the exact order:

1. `python scripts/postgres-connection.py`
2. ACS dim `01` → `05`
3. ACS fact `01` → `07`
4. ACS SQL for `fact_acs_tract_profile_v2` and `acs_frozen_metric_sheet_v2`
5. ACS validation `04_run_full_validation.py`
6. PUMS `01` → `14`, including the documented manual SQL steps at steps 5 and 8
7. PUMS county totals bridge step `15`
8. ACS notebook layer in this order:
   - profile v2 rebuild
   - tract geography lookup
   - EDA
   - visuals
   - clustering
   - transitions
   - ACS ↔ PUMS / ALICE bridge
   - capacity-adjusted bridge
   - final outputs
   - ML modeling

That order respects the dependencies across the whole project and reproduces the current end-to-end workflow.
