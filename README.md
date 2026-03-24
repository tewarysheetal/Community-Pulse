# Community Pulse

Community Pulse is a two-pipeline data engineering and applied analytics project focused on economic hardship measurement in Champaign County, Illinois. The repository combines tract-level ACS processing with household-level PUMS / ALICE estimation so the final outputs can move from raw Census files to geography-aware, client-facing insight.

## What this project does

The project answers a practical policy question:

**How many working-family households in Champaign County fall below a realistic survival threshold, and where are those households concentrated?**

To answer that, the repo builds:

- an **ACS tract pipeline** that ingests ACS 5-year CSV downloads, creates a tract geography dimension, stages and reshapes 13 ACS tables, and produces analysis-ready tract fact tables
- a **PUMS / ALICE pipeline** that loads Illinois housing and person microdata, isolates Champaign County households, calibrates ALICE thresholds, flags below-ALICE households, and exports Tableau-ready profiles
- an **integrated analysis layer** that supports EDA, visuals, clustering, transitions, geography lookup, and ACS ↔ PUMS / ALICE bridging

## Repository structure

```text
Community-Pulse/
├── data/                  # raw downloads, processed ACS/PUMS files, shapefiles
├── docs/                  # replication and project documents
├── notebooks/             # analysis notebooks
├── outputs/
│   ├── acs/               # ACS pipeline outputs and analysis outputs
│   └── pums/              # PUMS validation outputs and Tableau exports
├── scripts/
│   ├── acs/               # ACS dimension, fact, and validation scripts
│   └── pums/              # PUMS / ALICE pipeline scripts 01–14
├── sql/
│   ├── acs/               # ACS SQL generated or maintained by layer
│   └── pums/              # PUMS SQL folders by year and ALICE helpers
├── requirements.txt
└── README.md
```

## Reproducible layers

### 1. ACS pipeline
The ACS side is split into three layers:

- `scripts/acs/dim/` → tract dimension and tract-year bridge
- `scripts/acs/fact/` → staging, intermediate views, long fact, wide profiles
- `scripts/acs/validation/` → stage/int/fact validation

Key outputs include:

- `dim_tract`
- `bridge_tract_year`
- `fact_acs_tract_metric_long`
- `fact_acs_tract_profile`
- `fact_acs_tract_profile_v2`
- `acs_frozen_metric_sheet_v2`

### 2. PUMS / ALICE pipeline
The PUMS side is already organized in strict execution order under `scripts/pums/01` through `14`.

Key outputs include:

- `alice_household_final_{year}`
- `alice_nonstudent_households_{year}`
- `alice_below_alice_stat_profile_{year}`
- `alice_nonstudent_stat_profile_{year}`
- `alice_nonstudent_vs_complete_profile_compare_{year}`

The repo also contains consolidated PUMS exports under `outputs/pums/tableau-data/`.

### 3. Analysis notebooks
The notebook layer builds on the ACS and PUMS tables. In the committed repo, the documented notebook sequence covers:

- ACS profile v2 rebuild
- tract geography lookup
- EDA
- visuals
- clustering
- cluster transitions
- ACS ↔ PUMS / ALICE bridge

## Quick start

### 1. Clone the repo

```bash
git clone https://github.com/tewarysheetal/Community-Pulse.git
cd Community-Pulse
```

### 2. Create and activate an environment

```bash
python -m venv .venv
source .venv/Scripts/activate
```

On macOS / Linux:

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
pip install psycopg2-binary
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

## What to run

Use **REPLICATION.md** for the exact dependency-aware order. The short version is:

1. prepare raw ACS and PUMS source files under `data/raw/`
2. run ACS dimension scripts
3. run ACS fact scripts
4. run ACS validation
5. run PUMS / ALICE scripts `01` to `14`
6. run notebooks in documented order after the required tables exist

## Outputs you can inspect immediately

### ACS
`outputs/acs/analysis/` currently contains:

- `base/`
- `clustering/`
- `eda/`
- `geography_lookup/`
- `pums_alice_bridge/`
- `pums_alice_bridge_capacity_adjusted/`
- `transitions/`
- `visuals/`

### PUMS
`outputs/pums/` contains:

- validation CSVs such as `04_balance_check.csv`
- nonstudent impact output `08_nonstudent_filter_impact.csv`
- all-years Tableau exports under `outputs/pums/tableau-data/`

## Recommended entry points

If you are exploring the repo for the first time:

- start with `docs/REPLICATION.md`
- inspect `scripts/acs/` and `scripts/pums/`
- review `outputs/pums/tableau-data/`
- then move into the notebooks layer

## Notes

- The ACS and PUMS pipelines are both designed to be auditable: Python generates SQL where appropriate, writes SQL worksheets to disk, and executes them against PostgreSQL.
- Some analysis notebooks depend on local shapefiles under `data/geo/`, especially the geography lookup notebook.
- The repo contains both original and revised ACS profile layers. Downstream analysis is centered on `fact_acs_tract_profile_v2` and `acs_frozen_metric_sheet_v2`.

## Full replication guide

See **REPLICATION.md** for:

- exact script order
- dependencies between layers
- required manual SQL steps
- expected tables and outputs
- notebook run order after the pipelines complete
