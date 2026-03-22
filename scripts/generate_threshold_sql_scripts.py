from pathlib import Path
import textwrap
from typing import List
from dotenv import load_dotenv
import os
import re

try:
    from sqlalchemy import create_engine, text
except ImportError:  # optional unless RUN_SQL = True
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


YEARS = [2019, 2021, 2022, 2023]

ILLINOIS_AEI = {
    2019: 275,
    2021: 286,
    2022: 304,
    2023: 318,
}

THRESHOLDS_2023 = {
    "1_adult_0_child": 2779,
    "1_adult_1_child": 3824,
    "1_adult_2plus_child": 5186,
    "2_adult_0_child": 3874,
    "2_adult_1_child": 5230,
    "2_adult_2plus_child": 6001,
    "3plus_adult_0_child": 5340,
    "3plus_adult_1plus_child": 6108,
    "other": 2779,
}

HH_META = {
    "1_adult_0_child": (1, 0, 0, 0, 0, "Single adult"),
    "1_adult_1_child": (1, 0, 0, 0, 1, "1 adult + 1 school-age child"),
    "1_adult_2plus_child": (1, 0, 0, 0, 2, "v1 uses 2 school-age children"),
    "2_adult_0_child": (2, 0, 0, 0, 0, "Two adults"),
    "2_adult_1_child": (2, 0, 0, 0, 1, "2 adults + 1 school-age child"),
    "2_adult_2plus_child": (2, 0, 0, 0, 2, "v1 uses 2 school-age children"),
    "3plus_adult_0_child": (3, 0, 0, 0, 0, "v1 uses 3 adults"),
    "3plus_adult_1plus_child": (3, 0, 0, 0, 1, "v1 uses 3 adults + 1 school-age child"),
    "other": (1, 0, 0, 0, 0, "review manually"),
}


def sql_create_threshold_tables() -> str:
    values = ",\n".join(
        f"    ({year}, {value})" for year, value in ILLINOIS_AEI.items()
    )
    hh_rows = []
    for year in YEARS:
        for hh_comp_key, meta in HH_META.items():
            adults_18_64, adults_65_plus, infants_0_2, preschoolers_3_4, school_age_5_17, notes = meta
            hh_rows.append(
                f"    ({year}, '{hh_comp_key}', {adults_18_64}, {adults_65_plus}, {infants_0_2}, {preschoolers_3_4}, {school_age_5_17},\n"
                f"     'United For ALICE, Champaign County 2023 budget + Illinois AEI backfill', '{notes}')"
            )
    hh_values = ",\n".join(hh_rows)
    return textwrap.dedent(f"""
    drop table if exists illinois_essentials_index;

    create table illinois_essentials_index (
        year integer primary key,
        alice_essentials_index numeric,
        ratio_to_2023 numeric
    );

    insert into illinois_essentials_index (year, alice_essentials_index)
    values
    {values};

    update illinois_essentials_index i
    set ratio_to_2023 =
        i.alice_essentials_index / b.alice_essentials_index
    from (
        select alice_essentials_index
        from illinois_essentials_index
        where year = 2023
    ) b;

    create table if not exists alice_thresholds (
        year integer not null,
        hh_comp_key text not null,
        adults_18_64 integer not null,
        adults_65_plus integer not null default 0,
        infants_0_2 integer not null default 0,
        preschoolers_3_4 integer not null default 0,
        school_age_5_17 integer not null default 0,
        monthly_survival_budget numeric,
        annual_survival_budget numeric,
        annual_alice_threshold numeric,
        threshold_source text,
        notes text,
        primary key (year, hh_comp_key)
    );

    delete from alice_thresholds;

    insert into alice_thresholds (
        year,
        hh_comp_key,
        adults_18_64,
        adults_65_plus,
        infants_0_2,
        preschoolers_3_4,
        school_age_5_17,
        threshold_source,
        notes
    )
    values
    {hh_values};
    """)


def sql_load_2023_exact() -> str:
    stmts = []
    for hh_comp_key, monthly in THRESHOLDS_2023.items():
        annual = monthly * 12
        stmts.append(textwrap.dedent(f"""
        update alice_thresholds
        set monthly_survival_budget = {monthly},
            annual_survival_budget = {annual},
            annual_alice_threshold = {annual}
        where year = 2023
          and hh_comp_key = '{hh_comp_key}';
        """).strip())
    return "\n\n".join(stmts) + "\n"


def sql_backfill_year(year: int) -> str:
    return textwrap.dedent(f"""
    update alice_thresholds t
    set monthly_survival_budget = round((b.annual_survival_budget * i.ratio_to_2023) / 12.0, 2),
        annual_survival_budget = round(b.annual_survival_budget * i.ratio_to_2023, 2),
        annual_alice_threshold = round(b.annual_alice_threshold * i.ratio_to_2023, 2)
    from alice_thresholds b
    join illinois_essentials_index i
      on i.year = {year}
    where b.year = 2023
      and b.hh_comp_key = t.hh_comp_key
      and t.year = {year};
    """)

def write_file(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents.strip() + "\n", encoding="utf-8")


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


def main() -> None:
    files: List[Path] = []

    f0 = OUTPUT_DIR / "ALICE"/ "07_create_alice_threshold_tables.sql"
    write_file(f0, sql_create_threshold_tables())
    files.append(f0)

    f2023 = OUTPUT_DIR / "2023" / "09_load_alice_thresholds_2023_exact.sql"
    write_file(f2023, sql_load_2023_exact())
    files.append(f2023)

    for year in [2019, 2021, 2022]:
        fy = OUTPUT_DIR / str(year) / f"09_backfill_alice_thresholds_{year}.sql"
        write_file(fy, sql_backfill_year(year))
        files.append(fy)

    print(f"Generated SQL files under: {OUTPUT_DIR.resolve()}")
    execute_sql_files(files)


if __name__ == "__main__":
    main()
