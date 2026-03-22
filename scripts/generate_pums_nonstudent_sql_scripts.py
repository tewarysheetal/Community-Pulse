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


def sql_create_nonstudent(year: int) -> str:
    return textwrap.dedent(f"""
    drop table if exists alice_nonstudent_households_{year};

    create table alice_nonstudent_households_{year} as
    select *
    from alice_household_final_{year}
    where below_alice_flag = 1
      and student_heavy_flag = 0;
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
        f = OUTPUT_DIR / str(year) / f"10_create_alice_nonstudent_households_{year}.sql"
        write_file(f, sql_create_nonstudent(year))
        files.append(f)

    print(f"Generated SQL files under: {OUTPUT_DIR.resolve()}")
    execute_sql_files(files)


if __name__ == "__main__":
    main()
