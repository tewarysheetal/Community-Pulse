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
        "source_table": "alice_household_profile_2019",
        "weight_expr": "wgtp::numeric",
        "where_sql": "puma = 2100\n  and coalesce(np, 0) > 0\n  and coalesce(person_count, 0) > 0\n  and hincp is not null",
    },
    "2021": {
        "suffix": "2021",
        "source_table": "alice_household_profile_2021",
        "weight_expr": "wgtp::numeric",
        "where_sql": "puma = 2100\n  and coalesce(np, 0) > 0\n  and coalesce(person_count, 0) > 0\n  and hincp is not null",
    },
    "2022": {
        "suffix": "2022",
        "source_table": "alice_household_profile_2022_adj",
        "weight_expr": "wgtp_adj::numeric",
        "where_sql": "puma in (1901, 1902)\n  and coalesce(np, 0) > 0\n  and coalesce(person_count, 0) > 0\n  and hincp is not null",
    },
    "2023": {
        "suffix": "2023",
        "source_table": "alice_household_profile_2023_adj",
        "weight_expr": "wgtp_adj::numeric",
        "where_sql": "puma in (1901, 1902)\n  and coalesce(np, 0) > 0\n  and coalesce(person_count, 0) > 0\n  and hincp is not null",
    },
}


def sql_create_household_final(cfg: dict) -> str:
    y = cfg["suffix"]
    return textwrap.dedent(f"""
    drop table if exists alice_household_final_{y};

    create table alice_household_final_{y} as
    select
        *,
        {cfg['weight_expr']} as analysis_weight
    from {cfg['source_table']}
    where {cfg['where_sql']};
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
    for key, cfg in YEARS.items():
        f = OUTPUT_DIR / key / f"07_create_pums_alice_household_final_{cfg['suffix']}.sql"
        write_file(f, sql_create_household_final(cfg))
        files.append(f)

    print(f"Generated SQL files under: {OUTPUT_DIR.resolve()}")
    execute_sql_files(files)

if __name__ == "__main__":
    main()
