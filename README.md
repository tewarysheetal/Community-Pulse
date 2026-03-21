# Community Pulse

A data pipeline for US Census ACS and PUMS data focused on Champaign, Illinois. Loads multi-year demographic and housing microdata into PostgreSQL for community-level analysis.

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

Generates SQL files under `sql/pums/<year>/` that filter data to Champaign County (PUMA 2100 for 2019–2021; PUMA 1901 and 1902 for 2022–2023) and build analysis tables.

Scripts generated per year (run them in order):

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

## Project Structure

```
Community-Pulse/
├── data/
│   ├── raw/                        # Downloaded ACS and PUMS source files
│   ├── yearly-data/                # ACS CSVs organized by category (output of copy_files_census.sh)
│   ├── combined-data/              # Merged multi-year ACS CSVs (output of notebook)
│   └── pums/                       # Processed PUMS files (output of copy_files_pums.sh)
├── notebooks/
│   ├── combine_yearly_files.ipynb  # Merges yearly ACS files into combined CSVs
│   └── pums_files.ipynb            # Exploratory analysis of PUMS data
├── scripts/
│   ├── copy_files_census.sh        # Copies ACS raw files into yearly-data/
│   ├── rename_files.sh             # Standardizes ACS filenames
│   ├── load_census_data_db.py      # Loads combined ACS CSVs into PostgreSQL
│   ├── copy_files_pums.sh          # Copies PUMS files into data/pums/ with standard names
│   ├── load_pums_data_db.py        # Loads PUMS CSVs into PostgreSQL
│   ├── generate_pums_sql_scripts.py # Generates Champaign analysis SQL scripts
│   └── postgres-connection.py      # Tests database connectivity
├── sql/
│   └── pums/                       # Generated SQL scripts (output of generate_pums_sql_scripts.py)
│       ├── 2019/
│       ├── 2020_exp/
│       ├── 2021/
│       ├── 2022/
│       └── 2023/
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
