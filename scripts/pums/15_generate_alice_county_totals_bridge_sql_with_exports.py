from pathlib import Path
from typing import List
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
DATA_OUTPUT_DIR = Path(r"D:\Projects\Community-Pulse\outputs\pums\bridge")
YEARS = [2019, 2021, 2022, 2023]

# Set to False if you only want to generate the SQL worksheet.
RUN_SQL = True

# Set to False if you do not want CSV exports after SQL execution.
EXPORT_CSV = True

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def sql_create_county_totals_bridge() -> str:
    union_parts: List[str] = []

    for year in YEARS:
        union_parts.append(
            textwrap.dedent(
                f"""
                SELECT
                    {year}::integer AS year,
                    'complete_calibrated'::text AS source_variant,
                    (
                        SELECT metric_value::numeric
                        FROM alice_below_alice_stat_profile_{year}
                        WHERE metric_group = 'overall'
                          AND metric_name = 'weighted_households'
                        LIMIT 1
                    ) AS alice_households,
                    (
                        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
                        FROM alice_household_final_{year}
                    ) AS county_total_households
                """
            ).strip()
        )

        union_parts.append(
            textwrap.dedent(
                f"""
                SELECT
                    {year}::integer AS year,
                    'nonstudent_calibrated'::text AS source_variant,
                    (
                        SELECT metric_value::numeric
                        FROM alice_nonstudent_stat_profile_{year}
                        WHERE metric_group = 'overall'
                          AND metric_name = 'weighted_households'
                        LIMIT 1
                    ) AS alice_households,
                    (
                        SELECT COALESCE(SUM(analysis_weight::numeric), 0)
                        FROM alice_household_final_{year}
                    ) AS county_total_households
                """
            ).strip()
        )

    union_sql = "\nUNION ALL\n".join(union_parts)

    return textwrap.dedent(
        f"""
        DROP TABLE IF EXISTS alice_county_totals_bridge;

        CREATE TABLE alice_county_totals_bridge AS
        WITH base AS (
            {union_sql}
        )
        SELECT
            year,
            source_variant,
            alice_households,
            county_total_households,
            ROUND(
                100.0 * alice_households / NULLIF(county_total_households, 0),
                4
            )::numeric AS alice_rate_pct
        FROM base
        ORDER BY year, source_variant;
        """
    ).strip() + "\n"


def write_file(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")


def validate_db_settings() -> None:
    missing = [
        key
        for key, value in {
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


def export_tables_to_csv() -> None:
    if not EXPORT_CSV:
        return

    if pd is None:
        raise ImportError(
            "pandas is required when EXPORT_CSV = True. Install with: pip install pandas"
        )

    DATA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    engine = get_engine()
    try:
        combined = pd.read_sql(
            "SELECT * FROM alice_county_totals_bridge ORDER BY year, source_variant",
            engine,
        )
        combined.to_csv(DATA_OUTPUT_DIR / "alice_county_totals_bridge.csv", index=False)

        complete = combined[combined["source_variant"] == "complete_calibrated"].copy()
        complete.to_csv(
            DATA_OUTPUT_DIR / "alice_county_totals_complete_calibrated.csv",
            index=False,
        )

        nonstudent = combined[
            combined["source_variant"] == "nonstudent_calibrated"
        ].copy()
        nonstudent.to_csv(
            DATA_OUTPUT_DIR / "alice_county_totals_nonstudent_calibrated.csv",
            index=False,
        )

        print(f"CSV exports saved under: {DATA_OUTPUT_DIR}")
    finally:
        engine.dispose()


def main() -> None:
    sql_file = OUTPUT_DIR / "15_create_alice_county_totals_bridge.sql"
    write_file(sql_file, sql_create_county_totals_bridge())

    print(f"Generated SQL file: {sql_file}")

    execute_sql_files([sql_file])
    export_tables_to_csv()

    print("Done. Bridge county totals table and CSV exports created.")


if __name__ == "__main__":
    main()