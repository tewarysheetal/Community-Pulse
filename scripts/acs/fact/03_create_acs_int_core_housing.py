from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

YEARS = [2019, 2021, 2022, 2023]
TARGET_SCHEMA = "public"


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


def require_columns(table_name: str, available: list[str], required: list[str]) -> None:
    missing = [c for c in required if c not in available]
    if missing:
        raise ValueError(f"{table_name} is missing required columns: {missing}")


def optional_metric_expr(available: list[str], col_name: str, alias: str) -> str:
    if col_name in available:
        return f"    {num_expr(col_name)} AS {alias}"
    return f"    NULL::numeric AS {alias}"


def build_sql_b19013(year: int, available: list[str]) -> str:
    src = f"{TARGET_SCHEMA}.stg_acs_{year}_b19013_raw"
    tgt = f"{TARGET_SCHEMA}.int_acs_{year}_b19013"

    require_columns(
        f"stg_acs_{year}_b19013_raw",
        available,
        ["tract_geoid", "year", "b19013_001e", "b19013_001m"],
    )

    return f"""
DROP TABLE IF EXISTS {tgt} CASCADE;

CREATE TABLE {tgt} AS
SELECT
    {num_expr("year")}::int AS year,
    tract_geoid,
    {num_expr("b19013_001e")} AS median_household_income,
    {num_expr("b19013_001m")} AS median_household_income_moe,
    CASE
        WHEN {num_expr("b19013_001e")} IS NOT NULL THEN 1
        ELSE 0
    END AS has_income_metric
FROM {src};

CREATE INDEX idx_int_acs_{year}_b19013_key
    ON {tgt} (year, tract_geoid);
""".strip()


def build_sql_b25003(year: int, available: list[str]) -> str:
    src = f"{TARGET_SCHEMA}.stg_acs_{year}_b25003_raw"
    tgt = f"{TARGET_SCHEMA}.int_acs_{year}_b25003"

    require_columns(
        f"stg_acs_{year}_b25003_raw",
        available,
        ["tract_geoid", "year", "b25003_001e", "b25003_002e", "b25003_003e"],
    )

    occ = num_expr("b25003_001e")
    owner = num_expr("b25003_002e")
    renter = num_expr("b25003_003e")

    return f"""
DROP TABLE IF EXISTS {tgt} CASCADE;

CREATE TABLE {tgt} AS
SELECT
    {num_expr("year")}::int AS year,
    tract_geoid,
    {occ} AS occupied_units,
    {owner} AS owner_occupied_units,
    {renter} AS renter_occupied_units,
    CASE
        WHEN {occ} > 0 THEN ROUND(100.0 * {owner} / {occ}, 2)
        ELSE NULL
    END AS pct_owner_occupied,
    CASE
        WHEN {occ} > 0 THEN ROUND(100.0 * {renter} / {occ}, 2)
        ELSE NULL
    END AS pct_renter_occupied,
    CASE
        WHEN {occ} IS NOT NULL THEN 1
        ELSE 0
    END AS has_tenure_metric
FROM {src};

CREATE INDEX idx_int_acs_{year}_b25003_key
    ON {tgt} (year, tract_geoid);
""".strip()


def build_sql_b25070(year: int, available: list[str]) -> str:
    src = f"{TARGET_SCHEMA}.stg_acs_{year}_b25070_raw"
    tgt = f"{TARGET_SCHEMA}.int_acs_{year}_b25070"

    required = [
        "tract_geoid",
        "year",
        "b25070_001e",
        "b25070_007e",
        "b25070_008e",
        "b25070_009e",
        "b25070_010e",
        "b25070_011e",
    ]
    require_columns(f"stg_acs_{year}_b25070_raw", available, required)

    base = num_expr("b25070_001e")
    rent_30_34 = num_expr("b25070_007e")
    rent_35_39 = num_expr("b25070_008e")
    rent_40_49 = num_expr("b25070_009e")
    rent_50_plus = num_expr("b25070_010e")
    rent_not_computed = num_expr("b25070_011e")
    rent_30_plus_sum = (
        f"(COALESCE({rent_30_34}, 0) + "
        f"COALESCE({rent_35_39}, 0) + "
        f"COALESCE({rent_40_49}, 0) + "
        f"COALESCE({rent_50_plus}, 0))"
    )

    return f"""
DROP TABLE IF EXISTS {tgt} CASCADE;

CREATE TABLE {tgt} AS
SELECT
    {num_expr("year")}::int AS year,
    tract_geoid,
    {base} AS renter_hh_rent_burden_base,
    {rent_30_34} AS rent_30_34,
    {rent_35_39} AS rent_35_39,
    {rent_40_49} AS rent_40_49,
    {rent_50_plus} AS rent_50_plus,
    {rent_not_computed} AS rent_not_computed,
    CASE
        WHEN {base} > 0
        THEN ROUND(100.0 * {rent_30_plus_sum} / {base}, 2)
        ELSE NULL
    END AS pct_rent_burden_30_plus,
    CASE
        WHEN {base} > 0
        THEN ROUND(100.0 * COALESCE({rent_50_plus}, 0) / {base}, 2)
        ELSE NULL
    END AS pct_rent_burden_50_plus,
    CASE
        WHEN {base} > 0
        THEN ROUND(100.0 * COALESCE({rent_not_computed}, 0) / {base}, 2)
        ELSE NULL
    END AS pct_rent_not_computed,
    CASE
        WHEN {base} IS NOT NULL THEN 1
        ELSE 0
    END AS has_rent_burden_metric
FROM {src};

CREATE INDEX idx_int_acs_{year}_b25070_key
    ON {tgt} (year, tract_geoid);
""".strip()


def build_sql_dp04(year: int, available: list[str]) -> str:
    src = f"{TARGET_SCHEMA}.stg_acs_{year}_dp04_raw"
    tgt = f"{TARGET_SCHEMA}.int_acs_{year}_dp04"

    require_columns(
        f"stg_acs_{year}_dp04_raw",
        available,
        ["tract_geoid", "year", "dp04_0001e", "dp04_0002e", "dp04_0003e"],
    )

    total_units = num_expr("dp04_0001e")
    occupied_units = num_expr("dp04_0002e")
    vacant_units = num_expr("dp04_0003e")

    # Common DP04 columns, kept optional because profile schemas can drift.
    optional_lines = [
        optional_metric_expr(available, "dp04_0004e", "for_rent_units"),
        optional_metric_expr(available, "dp04_0005e", "rented_not_occupied_units"),
        optional_metric_expr(available, "dp04_0006e", "for_sale_only_units"),
        optional_metric_expr(available, "dp04_0007e", "sold_not_occupied_units"),
        optional_metric_expr(available, "dp04_0008e", "seasonal_recreational_units"),
        optional_metric_expr(available, "dp04_0009e", "migrant_worker_units"),
        optional_metric_expr(available, "dp04_0010e", "other_vacant_units"),
    ]

    optional_sql = ",\n".join(optional_lines)

    return f"""
DROP TABLE IF EXISTS {tgt} CASCADE;

CREATE TABLE {tgt} AS
SELECT
    {num_expr("year")}::int AS year,
    tract_geoid,
    {total_units} AS housing_units_total,
    {occupied_units} AS occupied_housing_units_dp04,
    {vacant_units} AS vacant_housing_units_dp04,
    CASE
        WHEN {total_units} > 0
        THEN ROUND(100.0 * {occupied_units} / {total_units}, 2)
        ELSE NULL
    END AS pct_occupied_housing_units,
    CASE
        WHEN {total_units} > 0
        THEN ROUND(100.0 * {vacant_units} / {total_units}, 2)
        ELSE NULL
    END AS pct_vacant_housing_units,
{optional_sql},
    CASE
        WHEN {total_units} IS NOT NULL THEN 1
        ELSE 0
    END AS has_housing_profile_metric
FROM {src};

CREATE INDEX idx_int_acs_{year}_dp04_key
    ON {tgt} (year, tract_geoid);
""".strip()


def build_union_view_sql(table_code: str) -> str:
    view_name = f"{TARGET_SCHEMA}.vw_int_acs_{table_code}_all_years"
    union_parts = [
        f"SELECT * FROM {TARGET_SCHEMA}.int_acs_{year}_{table_code}" for year in YEARS
    ]
    union_sql = "\nUNION ALL\n".join(union_parts)

    return f"""
DROP VIEW IF EXISTS {view_name};

CREATE VIEW {view_name} AS
{union_sql};
""".strip()


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

    sql_dir = project_root / "sql" / "acs" / "fact" / "int_core_housing"
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

    builders = {
        "b19013": build_sql_b19013,
        "b25003": build_sql_b25003,
        "b25070": build_sql_b25070,
        "dp04": build_sql_dp04,
    }

    try:
        for year in YEARS:
            year_sql_dir = sql_dir / str(year)
            year_sql_dir.mkdir(parents=True, exist_ok=True)

            for table_code, builder in builders.items():
                stg_table = f"stg_acs_{year}_{table_code}_raw"
                int_table = f"int_acs_{year}_{table_code}"

                columns = get_table_columns(conn, stg_table, TARGET_SCHEMA)
                if not columns:
                    raise ValueError(f"Staging table not found or empty schema: {TARGET_SCHEMA}.{stg_table}")

                sql_text = builder(year, columns)
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
                        print(f"[RUN] {int_table}: row_count={row_count}")
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
                        "sql_file": str(sql_file),
                        "status": status,
                        "row_count": row_count,
                        "error_message": error_message,
                    }
                )

        # all-year union views
        for table_code in builders.keys():
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

    master_sql_file = sql_dir / "00_run_all_int_core_housing.sql"
    master_sql_file.write_text("".join(master_sql_parts), encoding="utf-8")

    summary_df = pd.DataFrame(summary_rows).sort_values(["year", "table_code"]).reset_index(drop=True)
    summary_file = out_dir / "int_core_housing_build_summary.csv"
    summary_df.to_csv(summary_file, index=False)

    print("\nDone.")
    print(f"[OK] Summary file: {summary_file}")
    print(f"[OK] Master SQL file: {master_sql_file}")


if __name__ == "__main__":
    main()