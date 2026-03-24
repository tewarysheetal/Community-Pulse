# Community Pulse

Community Pulse is a multi-layer data engineering, analytics, and machine learning project focused on economic hardship measurement in Champaign County, Illinois. It integrates tract-level ACS processing with household-level PUMS / ALICE estimation to move from raw Census files to geography-aware, client-facing insight and supervised ML outputs.

## Project question

**How many working-family households in Champaign County fall below a realistic survival threshold, where are those households concentrated, and which tract characteristics best predict higher ALICE burden?**

## What this repository builds

The repository is organized as a sequence of reproducible layers.

### 1. ACS tract pipeline
The ACS side ingests ACS 5-year CSV downloads, builds a tract dimension and tract-year bridge, stages and reshapes 13 ACS tables, and produces analysis-ready tract fact tables.

Core ACS outputs include:

- `dim_tract`
- `bridge_tract_year`
- `fact_acs_tract_metric_long`
- `fact_acs_tract_profile`
- `fact_acs_tract_profile_v2`
- `acs_frozen_metric_sheet_v2`

### 2. PUMS / ALICE pipeline
The PUMS side loads Illinois housing and person microdata, isolates Champaign County households, calibrates ALICE thresholds, flags below-ALICE households, creates nonstudent variants, and exports year-level and all-years statistical profiles.

Core PUMS outputs include:

- `alice_household_final_{year}`
- `alice_nonstudent_households_{year}`
- `alice_below_alice_stat_profile_{year}`
- `alice_nonstudent_stat_profile_{year}`
- `alice_nonstudent_vs_complete_profile_compare_{year}`

The repo also contains consolidated profile outputs under `outputs/pums/tableau-data/`.

### 3. Integrated ACS analysis layer
The notebook layer builds on the ACS and PUMS tables and adds:

- tract geography lookup
- EDA
- visuals
- clustering
- cluster transitions
- ACS ↔ PUMS / ALICE bridge
- capacity-adjusted ALICE bridge
- final client-facing ALICE maps and summary outputs

### 4. Supervised ML layer
The final ML notebook uses the integrated tract outputs to build:

- a regression task to predict tract-level ALICE burden
- a classification task to identify high-risk tracts
- model comparison across linear and tree-based methods
- feature importance and error analysis

That makes the project not just an ETL or dashboarding project, but also a small-area policy-oriented ML project.

## Repository structure

```text
Community-Pulse/
├── data/                  # raw downloads, processed ACS/PUMS files, shapefiles
├── docs/                  # replication and project documents
├── notebooks/             # ACS, bridge, final output, and ML notebooks
├── outputs/
│   ├── acs/               # ACS pipeline outputs and ACS-side analysis outputs
│   ├── pums/              # PUMS validation outputs and Tableau exports
│   └── final/             # integrated final outputs and ML outputs
├── scripts/
│   ├── acs/               # ACS dimension, fact, and validation scripts
│   └── pums/              # PUMS / ALICE pipeline scripts
├── sql/
│   ├── acs/               # ACS SQL generated or maintained by layer
│   └── pums/              # PUMS SQL folders by year and ALICE helpers
├── requirements.txt
└── README.md
```

## Environment setup

### 1. Clone the repo

```bash
git clone https://github.com/tewarysheetal/Community-Pulse.git
cd Community-Pulse
```

### 2. Create and activate a virtual environment

On Windows:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

On macOS / Linux:

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
pip install psycopg2-binary geopandas shapely scikit-learn scipy
```

### 4. Create `.env`

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

### 5. Test the database connection

```bash
python scripts/postgres-connection.py
```

## Quick execution summary

Use **REPLICATION.md** for the exact dependency-aware run order.

High level:

1. place ACS and PUMS raw files under the expected `data/` folders
2. run ACS dimension scripts
3. run ACS fact scripts
4. run ACS validation
5. run PUMS / ALICE scripts in order
6. generate county-level ALICE totals bridge input
7. run notebooks in dependency order
8. generate final outputs under `outputs/final/`
9. run the ML notebook

## Outputs you can inspect

### ACS-side outputs
Typical ACS analysis output folders include:

- `outputs/acs/inventory/`
- `outputs/acs/validation/`
- `outputs/acs/analysis/eda/`
- `outputs/acs/analysis/geography_lookup/`
- `outputs/acs/analysis/visuals/`
- `outputs/acs/analysis/clustering/`
- `outputs/acs/analysis/transitions/`
- `outputs/acs/analysis/pums_alice_bridge/`
- `outputs/acs/analysis/pums_alice_bridge_capacity_adjusted/`

### PUMS-side outputs
PUMS outputs include:

- validation files such as `outputs/pums/04_balance_check.csv`
- nonstudent filter impact output such as `outputs/pums/08_nonstudent_filter_impact.csv`
- all-years profile exports under `outputs/pums/tableau-data/`

### Final integrated outputs
Final client-facing outputs are stored under:

- `outputs/final/data/`
- `outputs/final/summary/`
- `outputs/final/maps/`
- `outputs/final/plots/`
- `outputs/final/ml/`

## Recommended entry points

If you are exploring the project for the first time:

1. start with `docs/REPLICATION.md`
2. inspect `scripts/acs/` and `scripts/pums/`
3. review `outputs/pums/tableau-data/`
4. review `outputs/final/`
5. then move into the notebooks and ML layer

## Notes

- The ACS and PUMS pipelines are designed to be auditable. Python generates SQL where appropriate, writes SQL worksheets to disk, and can execute them against PostgreSQL.
- Some notebook steps depend on local shapefiles under `data/geo/`, especially the tract geography lookup and final maps layer.
- The tract universe is modeled explicitly as a union geography dimension across 2019, 2021, 2022, and 2023. Downstream strict longitudinal work should use the stable 4-year subset where appropriate.
- Downstream ACS analysis is centered on `fact_acs_tract_profile_v2` and `acs_frozen_metric_sheet_v2`.
- Final tract-level ALICE estimates are derived through a capacity-adjusted bridge between ACS tract indicators and calibrated PUMS-based county ALICE totals.

## Full replication guide

See **REPLICATION.md** for:

- exact script order
- dependencies between layers
- required manual SQL steps
- expected tables and outputs
- notebook run order after the base pipelines complete
- final output generation and ML execution order
