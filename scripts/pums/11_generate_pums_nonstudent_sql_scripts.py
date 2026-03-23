from pathlib import Path
import textwrap
from typing import List
from dotenv import load_dotenv
import os

try:
    import psycopg2
except ImportError:
    psycopg2 = None

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
    table_name = f"alice_nonstudent_households_{year}"
    source_table = f"alice_household_final_{year}"

    return textwrap.dedent(
        f"""
        drop table if exists {table_name};

        create table {table_name} as
        select *
        from {source_table}
        where below_alice_flag = 1
          and coalesce(student_heavy_flag, 0) = 0;
    """
    ).strip() + "\n"



def write_file(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")



def execute_sql_files(files: List[Path]) -> None:
    if not RUN_SQL:
        return

    if psycopg2 is None:
        raise ImportError("psycopg2 is required when RUN_SQL = True")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for file_path in files:
                sql_text = file_path.read_text(encoding="utf-8")
                print(f"Executing: {file_path.name}")
                cur.execute(sql_text)
                if cur.description is not None:
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()



def main() -> None:
    files: List[Path] = []

    for year in YEARS:
        file_path = OUTPUT_DIR / str(year) / f"11_create_pums_alice_nonstudent_households_{year}.sql"
        write_file(file_path, sql_create_nonstudent(year))
        files.append(file_path)

    print(f"Generated SQL files under: {OUTPUT_DIR}")

    if RUN_SQL:
        execute_sql_files(files)


if __name__ == "__main__":
    main()
