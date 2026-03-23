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

YEARS = [2019, 2021, 2022, 2023]


def sql_add_updates(year: int) -> str:
    table_name = f"alice_household_final_{year}"
    return textwrap.dedent(f"""
    alter table {table_name}
    add column if not exists hincp_adj_real numeric;

    update {table_name}
    set hincp_adj_real = (hincp::numeric * adjinc::numeric) / 1000000.0;

    alter table {table_name}
    add column if not exists student_heavy_flag integer;

    update {table_name}
    set student_heavy_flag = coalesce(student_heavy_flag_rule_b, 0);

    alter table {table_name}
    add column if not exists hh_comp_key text;

    update {table_name}
    set hh_comp_key =
        case
            when adult_count = 1 and noc = 0 then '1_adult_0_child'
            when adult_count = 1 and noc = 1 then '1_adult_1_child'
            when adult_count = 1 and noc >= 2 then '1_adult_2plus_child'
            when adult_count = 2 and noc = 0 then '2_adult_0_child'
            when adult_count = 2 and noc = 1 then '2_adult_1_child'
            when adult_count = 2 and noc >= 2 then '2_adult_2plus_child'
            when adult_count >= 3 and noc = 0 then '3plus_adult_0_child'
            when adult_count >= 3 and noc >= 1 then '3plus_adult_1plus_child'
            else 'other'
        end;

    alter table {table_name}
    add column if not exists below_alice_flag integer;

    update {table_name} h
    set below_alice_flag =
        case
            when h.hincp_adj_real < t.annual_alice_threshold then 1
            else 0
        end
    from alice_thresholds t
    where t.year = {year}
      and t.hh_comp_key = h.hh_comp_key;
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
    for year in YEARS:
        f = OUTPUT_DIR / str(year) / f"09_update_alter_pums_alice_household_final_{year}.sql"
        write_file(f, sql_add_updates(year))
        files.append(f)

    print(f"Generated SQL files under: {OUTPUT_DIR.resolve()}")
    execute_sql_files(files)


if __name__ == "__main__":
    main()
