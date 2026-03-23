from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

YEARS = [2019, 2021, 2022, 2023]
TABLE_CODES = ["s1501", "s2401", "b03002", "s0101", "s1601"]
TARGET_SCHEMA = "public"

OPTIONAL_META_COLUMNS = [
    "table_code",
    "table_family",
    "source_type",
]


def find_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_env_file(project_root: Path) -> None:
    env_file = project_root / ".env"
    if not env_file.exists():
        raise FileNotFoundError(f".env file not found at: {env_file}")
    load_dotenv(env_file)
    print(f"[ENV] Loaded .env from: {env_file}")


def get_db_config() -> dict:
    return {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }


def validate_db_config(db_config: dict) -> None:
    missing = [k for k, v in db_config.items() if v in {None, ""}]
    if missing:
        raise ValueError(f"Missing DB config values in .env: {missing}")


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def connect_db(db_config: dict):
    return psycopg2.connect(**db_config)


def qident(name: str) -> str:
    return f'"{name}"'


def num_expr(col_name: str) -> str:
    col = qident(col_name)
    return (
        f"CASE "
        f"WHEN NULLIF(BTRIM({col}), '') ~ '^-?\\d+(\\.\\d+)?$' "
        f"THEN BTRIM({col})::numeric "
        f"ELSE NULL END"
    )


def get_table_columns(conn, table_name: str, schema: str = TARGET_SCHEMA) -> list[str]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table_name))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def measure_col_pattern(table_code: str) -> re.Pattern:
    # Examples after sanitization:
    # s1501_c01_001e, s1601_c02_015m, b03002_003e
    return re.compile(rf"^{table_code}_.+[em]$", re.IGNORECASE)


def get_measure_columns(table_code: str, available_columns: list[str]) -> list[str]:
    pattern = measure_col_pattern(table_code)
    cols = [c for c in available_columns if pattern.match(c)]
    return sorted(cols)


def build_select_sql(year: int, table_code: str, available_columns: list[str]) -> str:
    select_lines: list[str] = []

    if "year" not in available_columns or "tract_geoid" not in available_columns:
        raise ValueError(
            f"stg_acs_{year}_{table_code}_raw is missing required keys 'year' and/or 'tract_geoid'"
        )

    select_lines.append(f"    {num_expr('year')}::int AS year")
    select_lines.append("    tract_geoid")

    for col in ["geo_id", "name", "statefp", "countyfp", "tractce"]:
        if col in available_columns:
            select_lines.append(f"    {qident(col)}")

    for col in OPTIONAL_META_COLUMNS:
        if col in available_columns:
            select_lines.append(f"    {qident(col)}")

    measure_columns = get_measure_columns(table_code, available_columns)
    if not measure_columns:
        raise ValueError(
            f"stg_acs_{year}_{table_code}_raw has no detected ACS measure columns "
            f"for pattern '{table_code}_...e/m'"
        )

    for col in measure_columns:
        select_lines.append(f"    {num_expr(col)} AS {col}")

    return ",\n".join(select_lines)


def build_int_table_sql(year: int, table_code: str, available_columns: list[str]) -> str:
    src = f"{TARGET_SCHEMA}.stg_acs_{year}_{table_code}_raw"
    tgt = f"{TARGET_SCHEMA}.int_acs_{year}_{table_code}"

    select_sql = build_select_sql(year, table_code, available_columns)

    return f"""
DROP TABLE IF EXISTS {tgt} CASCADE;

CREATE TABLE {tgt} AS
SELECT
{select_sql}
FROM {src};

CREATE INDEX idx_int_acs_{year}_{table_code}_key
    ON {tgt} (year, tract_geoid);
""".strip()


def build_union_view_sql(table_code: str) -> str:
    view_name = f"{TARGET_SCHEMA}.vw_int_acs_{table_code}_all_years"
    union_parts = [
        f"SELECT * FROM {TARGET_SCHEMA}.int_acs_{year}_{table_code}"
        for year in YEARS
    ]
    union_sql = "\nUNION ALL\n".join(union_parts)

    return f"""
DROP VIEW IF EXISTS {view_name};

CREATE VIEW {view_name} AS
{union_sql};
""".strip()


def drop_existing_union_views(conn, table_codes: list[str]) -> None:
    with conn.cursor() as cur:
        for table_code in table_codes:
            view_name = f"{TARGET_SCHEMA}.vw_int_acs_{table_code}_all_years"
            cur.execute(f"DROP VIEW IF EXISTS {view_name};")
    conn.commit()


def execute_sql(conn, sql_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def fetch_row_count(conn, relation_name: str) -> int:
    sql = f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{relation_name};"
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-sql", default="true")
    args = parser.parse_args()

    run_sql = parse_bool(args.run_sql)

    project_root = find_project_root()
    load_env_file(project_root)

    db_config = get_db_config()
    validate_db_config(db_config)

    sql_dir = project_root / "sql" / "acs" / "fact" / "int_demo_edu"
    out_dir = project_root / "outputs" / "acs" / "acs_tract"
    sql_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = connect_db(db_config)
    print(
        f"[DB] Connected to {db_config['dbname']} at "
        f"{db_config['host']}:{db_config['port']} as {db_config['user']}"
    )

    summary_rows: list[dict] = []
    master_sql_parts: list[str] = []

    try:
        if run_sql:
            drop_existing_union_views(conn, TABLE_CODES)
            print("[RUN] Existing all-years views dropped before rebuilding int tables")

        for year in YEARS:
            year_sql_dir = sql_dir / str(year)
            year_sql_dir.mkdir(parents=True, exist_ok=True)

            for table_code in TABLE_CODES:
                stg_table = f"stg_acs_{year}_{table_code}_raw"
                int_table = f"int_acs_{year}_{table_code}"

                available_columns = get_table_columns(conn, stg_table, TARGET_SCHEMA)
                if not available_columns:
                    raise ValueError(
                        f"Staging table not found or no columns returned: {TARGET_SCHEMA}.{stg_table}"
                    )

                measure_columns = get_measure_columns(table_code, available_columns)
                sql_text = build_int_table_sql(year, table_code, available_columns)

                sql_file = year_sql_dir / f"01_create_{int_table}.sql"
                sql_file.write_text(sql_text + "\n", encoding="utf-8")
                master_sql_parts.append(f"-- {int_table}\n{sql_text}\n\n")

                row_count = None
                status = "generated"
                error_message = None

                if run_sql:
                    try:
                        execute_sql(conn, sql_text)
                        row_count = fetch_row_count(conn, int_table)
                        status = "success"
                        print(
                            f"[RUN] {int_table}: row_count={row_count}, "
                            f"measure_columns={len(measure_columns)}"
                        )
                    except Exception as e:
                        conn.rollback()
                        status = "failed"
                        error_message = str(e)
                        print(f"[ERROR] {int_table}: {error_message}")

                summary_rows.append(
                    {
                        "year": year,
                        "table_code": table_code.upper(),
                        "staging_table": stg_table,
                        "int_table": int_table,
                        "measure_column_count": len(measure_columns),
                        "sql_file": str(sql_file),
                        "status": status,
                        "row_count": row_count,
                        "error_message": error_message,
                    }
                )

        for table_code in TABLE_CODES:
            view_sql = build_union_view_sql(table_code)
            view_file = sql_dir / f"02_create_vw_int_acs_{table_code}_all_years.sql"
            view_file.write_text(view_sql + "\n", encoding="utf-8")
            master_sql_parts.append(f"-- vw_int_acs_{table_code}_all_years\n{view_sql}\n\n")

            if run_sql:
                try:
                    execute_sql(conn, view_sql)
                    print(f"[RUN] vw_int_acs_{table_code}_all_years created")
                except Exception as e:
                    conn.rollback()
                    print(f"[ERROR] vw_int_acs_{table_code}_all_years: {e}")

    finally:
        conn.close()

    master_sql_file = sql_dir / "00_run_all_int_demo_edu.sql"
    master_sql_file.write_text("".join(master_sql_parts), encoding="utf-8")

    summary_df = pd.DataFrame(summary_rows).sort_values(["year", "table_code"]).reset_index(drop=True)
    summary_file = out_dir / "int_demo_edu_build_summary.csv"
    summary_df.to_csv(summary_file, index=False)

    print("\nDone.")
    print(f"[OK] Summary file: {summary_file}")
    print(f"[OK] Master SQL file: {master_sql_file}")


if __name__ == "__main__":
    main()