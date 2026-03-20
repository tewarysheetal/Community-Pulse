# Community Pulse

A data pipeline project that collects, processes, and loads US Census American Community Survey (ACS) socioeconomic data into a PostgreSQL database for community-level analysis in Illinois.

## Overview

Community Pulse aggregates multi-year ACS 5-year estimates (2010–2023) across ten demographic and economic categories. The goal is to provide a unified, query-ready dataset for tracking community well-being trends over time.

## Data Sources

All raw data is sourced from the [US Census Bureau American Community Survey](https://www.census.gov/programs-surveys/acs) (5-year estimates) and the United Way ALICE report.

| Dataset | ACS Table | Years Available |
|---|---|---|
| Age Distribution | S0101 | 2010–2023 |
| Population & Race | DP05 | 2010–2023 |
| Household Composition | DP02 | 2010–2023 |
| Language Spoken at Home | S1601 | 2010–2023 |
| Poverty Status | S1701 | 2012–2023 |
| Income Distribution | S1901 | 2010–2023 |
| Employment Status | S2301 | 2010–2023 |
| Occupation | S2401 | 2010–2023 |
| Household Costs | S2503 | 2010–2023 |
| Economic Characteristics | DP03 | 2010–2023 |
| ALICE Data | — | 2025 (Illinois) |

## Project Structure

```
Community-Pulse/
├── data/
│   ├── yearly-data/            # Raw ACS CSV files organized by category/year
│   └── combined-data/          # Merged multi-year CSVs (one file per category)
├── notebooks/
│   └── combine_yearly_files.ipynb  # Combines per-year files into single CSVs
├── scripts/
│   ├── load_data_db.py         # Loads combined CSVs into PostgreSQL
│   ├── copy_files.sh           # Copies raw files into project structure
│   ├── rename_files.sh         # Renames files to a consistent naming convention
│   └── postgres-connection.py  # Tests database connectivity
├── sql/
│   └── basic_queries.sql       # Sample queries for the loaded tables
├── requirements.txt
└── .env                        # Database credentials (not committed)
```

## Setup

### Prerequisites

- Python 3.10+
- PostgreSQL (running and accessible)

### Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Configure Environment

Create a `.env` file in the project root:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

### Test Connection

```bash
python scripts/postgres-connection.py
```

## Loading Data

The `load_data_db.py` script reads all `*_combined.csv` files from `data/combined-data/` and loads each into its own PostgreSQL table (e.g., `age_combined.csv` → table `age`).

```bash
python scripts/load_data_db.py
```

Each run replaces existing tables (`if_exists="replace"`). Column names are automatically cleaned to snake_case.

### Resulting Tables

| Table | Source File |
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

## Querying the Data

Connect to your database using `psql` or any SQL client:

```bash
psql -h localhost -U <DB_USER> -d <DB_NAME>
```

Sample queries (also available in [sql/basic_queries.sql](sql/basic_queries.sql)):

```sql
-- Preview rows
SELECT * FROM age LIMIT 10;

-- Check available years
SELECT DISTINCT year FROM age ORDER BY year;

-- Filter by year
SELECT * FROM poverty WHERE year = 2022;

-- List all columns in a table
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'age'
ORDER BY ordinal_position;
```

All tables are loaded into the `public` schema. Column names are cleaned to `snake_case` and truncated to PostgreSQL's 63-character identifier limit with auto-deduplication.

## Tech Stack

- **Python** — data processing and pipeline scripting
- **pandas** — CSV ingestion and column normalization
- **SQLAlchemy + psycopg2** — PostgreSQL connectivity
- **PostgreSQL** — data storage and querying
- **JupyterLab** — exploratory data analysis
- **python-dotenv** — environment variable management
