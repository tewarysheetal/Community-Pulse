from pathlib import Path
from typing import List
import textwrap
import os

from dotenv import load_dotenv

try:
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None

OUTPUT_DIR = Path(r"D:\Projects\Community-Pulse\sql\pums")
YEARS = [2019, 2021, 2022, 2023]
RUN_SQL = True
REFRESH_THRESHOLD_COLUMN = True
GENERATE_VALIDATION_SQL = True

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def sql_update_below_alice_flag(year: int, refresh_threshold_column: bool = True) -> str:
    """Generate SQL to refresh below_alice_flag for a given year."""
    table_name = f"alice_household_final_{year}"

    threshold_refresh_sql = ""
    if refresh_threshold_column:
        threshold_refresh_sql = textwrap.dedent(f"""
        alter table {table_name}
        add column if not exists annual_alice_threshold numeric;

        update {table_name} h
        set annual_alice_threshold = t.annual_alice_threshold
        from alice_thresholds t
        where t.year = {year}
          and t.hh_comp_key = h.hh_comp_key;
        """)

    validation_sql = ""
    if GENERATE_VALIDATION_SQL:
        validation_sql = textwrap.dedent(f"""
        -- Validation check for {year}
        -- stored_flag_weighted and direct_compare_weighted should match
        select
            {year} as year,
            round(sum(case when below_alice_flag = 1 then analysis_weight else 0 end), 0) as stored_flag_weighted,
            round(sum(case when hincp_adj_real < annual_alice_threshold then analysis_weight else 0 end), 0) as direct_compare_weighted
        from {table_name};
        """)

    return textwrap.dedent(f"""
    
    alter table {table_name}
    add column if not exists below_alice_flag integer;

    {threshold_refresh_sql}

    update {table_name} h
    set below_alice_flag =
        case
            when h.annual_alice_threshold is null then null
            when h.hincp_adj_real < h.annual_alice_threshold then 1
            else 0
        end;

    {validation_sql}
    """).strip() + "\n"



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
        raise ValueError(
            "Missing DB environment variables: " + ", ".join(missing)
        )



def execute_sql_files(files: List[Path]) -> None:
    if not RUN_SQL:
        return

    if create_engine is None:
        raise ImportError(
            "sqlalchemy is required when RUN_SQL = True. "
            "Install with: pip install sqlalchemy psycopg2-binary"
        )

    validate_db_settings()

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        for file_path in files:
            sql_text = file_path.read_text(encoding="utf-8")
            print(f"Executing: {file_path}")
            cursor.execute(sql_text)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()
        engine.dispose()



def main() -> None:
    files: List[Path] = []

    for year in YEARS:
        file_path = OUTPUT_DIR / str(year) / f"11_update_below_alice_flag_{year}.sql"
        sql_text = sql_update_below_alice_flag(
            year=year,
            refresh_threshold_column=REFRESH_THRESHOLD_COLUMN,
        )
        write_file(file_path, sql_text)
        files.append(file_path)
        print(f"Generated: {file_path}")

    execute_sql_files(files)
    print(f"Done. SQL files created under: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
