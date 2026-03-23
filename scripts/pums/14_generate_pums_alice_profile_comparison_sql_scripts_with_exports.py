from pathlib import Path
from typing import List, Tuple
import textwrap
import os

from dotenv import load_dotenv

try:
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None

try:
    import pandas as pd
except ImportError:
    pd = None

OUTPUT_DIR = Path(r"D:\Projects\Community-Pulse\sql\pums")
DATA_OUTPUT_DIR = Path(r"D:\Projects\Community-Pulse\outputs\pums\tableau-data")
YEARS = [2019, 2021, 2022, 2023]
RUN_SQL = True
EXPORT_CSV = True

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def sql_create_profile_comparison(year: int) -> str:
    return textwrap.dedent(
        f"""
        drop table if exists alice_nonstudent_vs_complete_profile_compare_{year};

        create table alice_nonstudent_vs_complete_profile_compare_{year} as
        with complete_profile as (
            select *
            from alice_below_alice_stat_profile_{year}
        ),
        nonstudent_profile as (
            select *
            from alice_nonstudent_stat_profile_{year}
        )
        select
            coalesce(c.year, n.year)::integer as year,
            coalesce(c.metric_group, n.metric_group)::text as metric_group,
            coalesce(c.metric_name, n.metric_name)::text as metric_name,
            c.metric_value::numeric as complete_metric_value,
            n.metric_value::numeric as nonstudent_metric_value,
            round(coalesce(n.metric_value, 0) - coalesce(c.metric_value, 0), 4)::numeric as value_diff_nonstudent_minus_complete,
            c.pct_of_households::numeric as complete_pct_of_households,
            n.pct_of_households::numeric as nonstudent_pct_of_households,
            round(coalesce(n.pct_of_households, 0) - coalesce(c.pct_of_households, 0), 4)::numeric as pct_point_diff,
            round(
                100.0 * coalesce(n.metric_value, 0) / nullif(c.metric_value, 0),
                4
            )::numeric as nonstudent_as_pct_of_complete,
            coalesce(c.sort_order, n.sort_order)::integer as sort_order
        from complete_profile c
        full join nonstudent_profile n
          on n.metric_group = c.metric_group
         and n.metric_name = c.metric_name
        order by sort_order, metric_group, metric_name;
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



def get_engine():
    if create_engine is None:
        raise ImportError(
            "sqlalchemy is required. Install with: pip install sqlalchemy psycopg2-binary"
        )
    validate_db_settings()
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )



def execute_sql_files(files: List[Path]) -> None:
    if not RUN_SQL:
        return

    engine = get_engine()
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



def export_tables_to_csv(exports: List[Tuple[int, str]]) -> None:
    if not EXPORT_CSV:
        return
    if pd is None:
        raise ImportError("pandas is required when EXPORT_CSV = True. Install with: pip install pandas")

    engine = get_engine()
    try:
        frames = []
        for year, table_name in exports:
            year_dir = DATA_OUTPUT_DIR / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)

            query = f"select * from {table_name} order by sort_order, metric_group, metric_name"
            print(f"Exporting CSV: {table_name}")
            df = pd.read_sql(query, engine)
            csv_path = year_dir / f"{table_name}.csv"
            df.to_csv(csv_path, index=False)
            frames.append(df)

        if frames:
            pd.concat(frames, ignore_index=True).to_csv(
                DATA_OUTPUT_DIR / "alice_nonstudent_vs_complete_profile_compare_all_years.csv",
                index=False,
            )
    finally:
        engine.dispose()



def main() -> None:
    files: List[Path] = []
    exports: List[Tuple[int, str]] = []

    for year in YEARS:
        file_path = OUTPUT_DIR / str(year) / f"13_create_pums_nonstudent_vs_complete_profile_compare_{year}.sql"
        table_name = f"alice_nonstudent_vs_complete_profile_compare_{year}"
        write_file(file_path, sql_create_profile_comparison(year))
        files.append(file_path)
        exports.append((year, table_name))

    print(f"Generated SQL files under: {OUTPUT_DIR}")
    execute_sql_files(files)
    export_tables_to_csv(exports)
    print(f"Done. Nonstudent vs complete comparison tables created. CSV exports saved under: {DATA_OUTPUT_DIR}")


if __name__ == "__main__":
    main()
