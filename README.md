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
│   ├── raw/                    # Original ACS CSV files (per year, per table)
│   └── combined-data/          # Merged multi-year CSVs (one file per category)
├── scripts/
│   ├── load_data_db.py         # Loads combined CSVs into PostgreSQL
│   └── postgres-connection.py  # Tests database connectivity
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

## Tech Stack

- **Python** — data processing and pipeline scripting
- **pandas** — CSV ingestion and column normalization
- **SQLAlchemy + psycopg2** — PostgreSQL connectivity
- **PostgreSQL** — data storage and querying
- **JupyterLab** — exploratory data analysis
- **python-dotenv** — environment variable management
