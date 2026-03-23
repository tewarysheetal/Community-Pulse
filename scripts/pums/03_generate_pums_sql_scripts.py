from pathlib import Path
import textwrap
from typing import List
from dotenv import load_dotenv
import os
import re

try:
    from sqlalchemy import create_engine, text
except ImportError:
    create_engine = None
    text = None

OUTPUT_DIR = Path(r"D:\Projects\Community-Pulse\sql\pums")
RUN_SQL = True

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

YEARS = {
    "2019": {
        "suffix": "2019",
        "person_raw": "alice_person_2019_raw",
        "housing_raw": "alice_housing_2019_raw",
        "puma_filter_sql": "puma = 2100",
    },
    "2020_exp": {
        "suffix": "2020_exp",
        "person_raw": '"alice_person_2020_exp_raw"',
        "housing_raw": '"alice_housing_2020_exp_raw"',
        "puma_filter_sql": "puma = 2100",
    },
    "2021": {
        "suffix": "2021",
        "person_raw": "alice_person_2021_raw",
        "housing_raw": "alice_housing_2021_raw",
        "puma_filter_sql": "puma = 2100",
    },
    "2022": {
        "suffix": "2022",
        "person_raw": "alice_person_2022_raw",
        "housing_raw": "alice_housing_2022_raw",
        "puma_filter_sql": "puma IN (1901, 1902)",
    },
    "2023": {
        "suffix": "2023",
        "person_raw": "alice_person_2023_raw",
        "housing_raw": "alice_housing_2023_raw",
        "puma_filter_sql": "puma IN (1901, 1902)",
    },
}

def sql_person_champaign(cfg):
    y = cfg["suffix"]
    return textwrap.dedent(f"""    DROP TABLE IF EXISTS alice_person_{y}_champaign;

    CREATE TABLE alice_person_{y}_champaign AS
    SELECT
        *
    FROM {cfg["person_raw"]}
    WHERE state = 17
      AND {cfg["puma_filter_sql"]};
    """)

def sql_housing_champaign(cfg):
    y = cfg["suffix"]
    return textwrap.dedent(f"""    DROP TABLE IF EXISTS alice_housing_{y}_champaign;

    CREATE TABLE alice_housing_{y}_champaign AS
    SELECT
        *
    FROM {cfg["housing_raw"]}
    WHERE state = 17
      AND {cfg["puma_filter_sql"]};
    """)

def sql_person_student_flags(cfg):
    y = cfg["suffix"]
    return textwrap.dedent(f"""    DROP TABLE IF EXISTS alice_person_student_flags_{y};

    CREATE TABLE alice_person_student_flags_{y} AS
    SELECT
        serialno,
        sporder,
        agep,
        relshipp,
        sch,
        schg,
        schl,
        esr,
        CASE WHEN agep >= 18 THEN 1 ELSE 0 END AS is_adult,
        CASE WHEN agep BETWEEN 18 AND 24 THEN 1 ELSE 0 END AS is_college_age,
        CASE WHEN sch IN (2, 3) THEN 1 ELSE 0 END AS is_student_enrolled,
        CASE
            WHEN agep BETWEEN 18 AND 24
             AND sch IN (2, 3)
             AND schg IN (15, 16)
            THEN 1 ELSE 0
        END AS is_likely_college_student,
        CASE WHEN relshipp = 20 THEN 1 ELSE 0 END AS is_reference_person,
        CASE WHEN esr IN (1, 2) THEN 1 ELSE 0 END AS is_employed
    FROM alice_person_{y}_champaign;
    """)

def sql_household_student_agg(cfg):
    y = cfg["suffix"]
    return textwrap.dedent(f"""    DROP TABLE IF EXISTS alice_household_student_agg_{y};

    CREATE TABLE alice_household_student_agg_{y} AS
    SELECT
        serialno,
        COUNT(*) AS person_count,
        SUM(is_adult) AS adult_count,
        SUM(is_college_age) AS college_age_count,
        SUM(is_student_enrolled) AS student_enrolled_count,
        SUM(is_likely_college_student) AS likely_college_student_count,
        SUM(is_employed) AS employed_count,
        SUM(is_reference_person) AS reference_person_count,
        MAX(
            CASE
                WHEN is_reference_person = 1 AND is_likely_college_student = 1
                THEN 1 ELSE 0
            END
        ) AS reference_person_student_flag,
        CASE WHEN SUM(is_likely_college_student) > 0 THEN 1 ELSE 0 END AS has_any_likely_college_student,
        CASE WHEN SUM(is_student_enrolled) > 0 THEN 1 ELSE 0 END AS has_any_student,
        CASE WHEN SUM(is_college_age) > 0 THEN 1 ELSE 0 END AS has_any_college_age_person,
        CASE WHEN SUM(is_employed) > 0 THEN 1 ELSE 0 END AS has_any_employed_person,
        SUM(is_likely_college_student)::numeric / NULLIF(COUNT(*), 0) AS student_share_of_persons,
        SUM(is_likely_college_student)::numeric / NULLIF(SUM(is_adult), 0) AS student_share_of_adults,
        SUM(is_college_age)::numeric / NULLIF(COUNT(*), 0) AS college_age_share_of_persons,
        CASE
            WHEN SUM(is_likely_college_student) > 0
             AND (
                    MAX(CASE WHEN is_reference_person = 1 AND is_likely_college_student = 1 THEN 1 ELSE 0 END) = 1
                    OR
                    SUM(is_likely_college_student)::numeric / NULLIF(SUM(is_adult), 0) >= 0.5
                 )
            THEN 1 ELSE 0
        END AS student_heavy_flag_rule_a,
        CASE
            WHEN SUM(is_likely_college_student)::numeric / NULLIF(COUNT(*), 0) >= 0.5
             AND SUM(is_employed) = 0
            THEN 1 ELSE 0
        END AS student_heavy_flag_rule_b
    FROM alice_person_student_flags_{y}
    GROUP BY serialno;
    """)

def sql_household_base(cfg):
    y = cfg["suffix"]
    return textwrap.dedent(f"""    DROP TABLE IF EXISTS alice_household_base_{y};

    CREATE TABLE alice_household_base_{y} AS
    SELECT
        serialno,
        state,
        puma,
        wgtp,
        np,
        hincp,
        fincp,
        adjinc,
        hht,
        hht2,
        noc,
        npf,
        fparc
    FROM alice_housing_{y}_champaign;
    """)

def sql_alice_household_profile(cfg):
    y = cfg["suffix"]
    return textwrap.dedent(f"""    DROP TABLE IF EXISTS alice_household_profile_{y};

    CREATE TABLE alice_household_profile_{y} AS
    SELECT
        hb.serialno,
        hb.state,
        hb.puma,
        hb.wgtp,
        hb.np,
        hb.hincp,
        hb.fincp,
        hb.adjinc,
        hb.hht,
        hb.hht2,
        hb.noc,
        hb.npf,
        hb.fparc,
        hsa.person_count,
        hsa.adult_count,
        hsa.college_age_count,
        hsa.student_enrolled_count,
        hsa.likely_college_student_count,
        hsa.employed_count,
        hsa.reference_person_count,
        hsa.reference_person_student_flag,
        hsa.has_any_likely_college_student,
        hsa.has_any_student,
        hsa.has_any_college_age_person,
        hsa.has_any_employed_person,
        hsa.student_share_of_persons,
        hsa.student_share_of_adults,
        hsa.college_age_share_of_persons,
        hsa.student_heavy_flag_rule_a,
        hsa.student_heavy_flag_rule_b
    FROM alice_household_base_{y} hb
    LEFT JOIN alice_household_student_agg_{y} hsa
        ON hb.serialno = hsa.serialno;
    """)

TEMPLATES = [
    ("01_create_pums_person_{y}_champaign.sql", sql_person_champaign),
    ("02_create_pums_housing_{y}_champaign.sql", sql_housing_champaign),
    ("03_create_pums_person_student_flags_{y}.sql", sql_person_student_flags),
    ("04_create_pums_household_student_agg_{y}.sql", sql_household_student_agg),
    ("05_create_pums_household_base_{y}.sql", sql_household_base),
    ("06_create_pums_alice_household_profile_{y}.sql", sql_alice_household_profile),
]

def execute_sql_files(files: List[Path]) -> None:
    if not RUN_SQL:
        return
    if create_engine is None:
        raise ImportError("sqlalchemy is required when RUN_SQL = True")
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    with engine.begin() as conn:
        for file_path in files:
            sql_text = file_path.read_text(encoding="utf-8")
            print(f"Executing: {file_path.name}")
            conn.execute(text(sql_text))


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    generated_files = []
    
    for key, cfg in YEARS.items():
        year_dir = OUTPUT_DIR / key
        year_dir.mkdir(exist_ok=True)
        for pattern, fn in TEMPLATES:
            filename = pattern.format(y=cfg["suffix"])
            file_path = year_dir / filename
            file_path.write_text(fn(cfg), encoding="utf-8")
            generated_files.append(file_path)
    
    print(f"Generated SQL files under: {OUTPUT_DIR.resolve()}")

    if RUN_SQL:
        execute_sql_files(generated_files)
        print("All SQL files executed successfully.")

if __name__ == "__main__":
    main()
