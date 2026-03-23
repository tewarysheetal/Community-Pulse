# Community Pulse

Community Pulse is a data pipeline and analysis project that measures economic hardship in Champaign, Illinois using US Census microdata. It combines two Census data products — ACS Summary tables and PUMS microdata — to produce community-level demographic profiles and identify households living below the ALICE threshold.

The outputs power Tableau dashboards used by community organizations to understand who is economically vulnerable, by how much, and how that population has changed year over year.

---

## What We Are Trying to Achieve

Champaign is a university city, which makes standard poverty measures misleading. A large student population suppresses apparent income figures and distorts household composition data. This project's goal is to:

1. **Build an accurate picture of economic hardship** in Champaign's non-student residential population using the ALICE framework — a measure that captures households above the federal poverty line but still unable to afford basic survival budgets.
2. **Track changes across years (2019–2023)** to surface trends in household income, composition, and cost burden.
3. **Separate student-heavy households** from the general population so that downstream analysis reflects the true working-family population.
4. **Produce Tableau-ready exports** that community stakeholders can explore without needing to understand the underlying data engineering.

---

## Key Concepts

### ALICE — Asset Limited, Income Constrained, Employed
ALICE is a framework developed by United For ALICE to measure households that earn above the federal poverty level but still cannot afford basic necessities (housing, food, childcare, transportation, healthcare, technology). The ALICE threshold is calculated per household composition type (e.g., 1 adult + 0 children, 2 adults + 2 children) and varies by year and geography.

This project uses the **United For ALICE Illinois Essentials Index (AEI)** to backfill thresholds for 2019–2022 from 2023 base budgets, and applies a local **calibration** to the two most common childless household types to account for Champaign's cost of living.

### ACS — American Community Survey (Summary Tables)
The ACS 5-year estimates provide aggregate statistics at the Census tract or place level. Pipeline 1 loads ten ACS subject/profile tables covering demographics, income, housing costs, employment, and language. These summary tables give a broad view of community characteristics.

### PUMS — Public Use Microdata Sample
PUMS is the individual-record microdata underlying the ACS. Each row is a person or housing unit record with sample weights (`pwgtp` for persons, `wgtp` for households). Pipeline 2 uses PUMS to build household-level profiles, calculate inflation-adjusted income, classify household composition, and flag ALICE-eligible households.

### PUMA Boundary Change (2022–2023)
PUMA (Public Use Microdata Area) geographies were redrawn after 2021. Champaign County previously mapped to a single PUMA (2100). Starting in 2022 it spans two PUMAs (1901 and 1902), with PUMA 1902 extending into neighboring counties. To isolate Champaign County households in PUMA 1902, the pipeline builds a **tract-level allocation factor (alpha)** from ACS5 household counts and applies it as a weight adjustment.

### Inflation Adjustment
PUMS income (`hincp`) includes a Census-provided adjustment factor (`adjinc`) that normalizes dollar values to a reference year. The pipeline applies this as: `hincp_adj_real = hincp × adjinc / 1,000,000` before comparing household income to ALICE thresholds.

---

## Pipelines at a Glance

### Pipeline 1 — ACS Summary Data
Loads 10 ACS demographic tables (2019–2023) from downloaded CSVs into PostgreSQL via a 4-step process: copy raw files, standardize filenames, combine yearly files, and bulk-load to the database.

### Pipeline 2 — PUMS Microdata & ALICE Analysis
A 14-step pipeline that:
- Loads raw PUMS housing and person data into PostgreSQL
- Filters to Champaign County (with PUMA boundary correction for 2022/2023)
- Builds household profiles with student composition flags
- Applies ALICE thresholds (calibrated) to flag below-ALICE households
- Validates outputs against benchmark targets
- Exports statistical profiles and comparison tables as CSVs for Tableau

---

## Execution Plan

### Phase 1 — Environment
1. Clone repo, create virtual environment, install dependencies
2. Configure `.env` with PostgreSQL credentials
3. Verify connection: `python scripts/postgres-connection.py`

### Phase 2 — ACS Summary Data (Pipeline 1)
4. Download ACS tables from Census Bureau, place in `data/raw/`
5. `bash scripts/copy_files_census.sh`
6. `bash scripts/rename_census_data_files.sh`
7. Run `notebooks/combine_yearly_files.ipynb`
8. `python scripts/load_census_data_db.py`

### Phase 3 — PUMS Baseline (Pipeline 2, Steps 1–3)
9. Download PUMS housing and person CSVs for Illinois, place in `data/raw/`
10. `bash scripts/pums/01_copy_files_pums.sh`
11. `python scripts/pums/02_load_pums_data_db.py`
12. `python scripts/pums/03_generate_pums_sql_scripts.py`

### Phase 4 — PUMA Boundary Correction (Steps 4–6)
13. Place `Tract-Illinois.csv` in `data/pums/`
14. `python scripts/pums/04_load_tract_il.py`
15. Run `sql/pums/ALICE/03_create_tract_puma1902_lookup.sql` manually
16. `python scripts/pums/05_pull_acs_tract_households.py`
17. Run `ALICE/04` through `ALICE/06` SQL scripts manually
18. `python scripts/pums/06_generate_pums_household_profile_adj_sql_scripts.py`

### Phase 5 — ALICE Thresholds & Flags (Steps 7–10)
19. `python scripts/pums/07_generate_pums_household_final_sql_scripts.py`
20. `python scripts/pums/08_generate_pums_threshold_sql_scripts.py`
21. Run `sql/pums/ALICE/08_create_alice_thresholds_calibrated.sql` manually
22. `python scripts/pums/09_generate_pums_household_final_updates_sql_scripts.py`
23. `python scripts/pums/10_generate_pums_below_alice_flag_sql_scripts.py`

### Phase 6 — Filtering, Validation & Export (Steps 11–14)
24. `python scripts/pums/11_generate_pums_nonstudent_sql_scripts.py`
25. `python scripts/pums/12_run_pums_final_validation.py` — all checks must pass
26. `python scripts/pums/13_generate_pums_alice_statistical_profile_sql_scripts_with_exports.py`
27. `python scripts/pums/14_generate_pums_alice_profile_comparison_sql_scripts_with_exports.py`

---

## Tech Stack

| Tool | Role |
|---|---|
| Python | Pipeline scripting and data processing |
| pandas | CSV ingestion and column normalization |
| SQLAlchemy + psycopg2 | PostgreSQL connectivity and COPY-based bulk loading |
| PostgreSQL | Data storage and SQL-based transformations |
| JupyterLab | Exploratory data analysis |
| python-dotenv | Environment variable management |
| Bash | File organization and renaming scripts |
| Census API | ACS5 tract-level household counts (via `requests`) |
| Tableau | Downstream visualization and dashboarding |

---

## Detailed Replication Guide

For step-by-step commands, table schemas, script parameters, and SQL details:

**[docs/REPLICATION.md](docs/REPLICATION.md)**
