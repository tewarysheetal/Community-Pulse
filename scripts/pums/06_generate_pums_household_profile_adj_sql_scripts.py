from pathlib import Path
import textwrap
from typing import List
from dotenv import load_dotenv
import os

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

YEARS = [2022, 2023]


def sql_create_adj_table(year: int) -> str:
    source_table = f"alice_household_profile_{year}"
    target_table = f"alice_household_profile_{year}_adj"

    return textwrap.dedent(
        f"""
        drop table if exists {target_table};

        create table {target_table} as
        select
            a.*,
            case
                when a.puma = 1901 then a.wgtp::numeric
                when a.puma = 1902 then a.wgtp::numeric * p.alpha
                else null
            end as wgtp_adj
        from {source_table} a
        cross join (
            select alpha
            from alice_puma1902_alpha
            where alloc_year = {year}
        ) p
        where a.puma in (1901, 1902)
          and coalesce(a.np, 0) > 0
          and coalesce(a.person_count, 0) > 0;
        """
    ).strip() + "\n"



def write_file(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")



def validate_db_settings() -> None:
    missing = [
        key for key, value in {
            "DB_HOST": DB_HOST,
            "DB_PORT": DB_PORT,
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError("Missing DB environment variables: " + ", ".join(missing))



def execute_sql_files(files: List[Path]) -> None:
    if not RUN_SQL:
        return

    if create_engine is None or text is None:
        raise ImportError(
            "sqlalchemy is required when RUN_SQL = True. "
            "Install with: pip install sqlalchemy psycopg2-binary"
        )

    validate_db_settings()

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    with engine.begin() as conn:
        for file_path in files:
            sql_text = file_path.read_text(encoding="utf-8")
            print(f"Executing: {file_path}")
            conn.execute(text(sql_text))



def main() -> None:
    files: List[Path] = []

    for year in YEARS:
        file_path = OUTPUT_DIR / str(year) / f"06b_create_pums_alice_household_profile_{year}_adj.sql"
        sql_text = sql_create_adj_table(year)
        write_file(file_path, sql_text)
        files.append(file_path)
        print(f"Generated: {file_path}")

    execute_sql_files(files)
    print(f"Done. SQL files created under: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
