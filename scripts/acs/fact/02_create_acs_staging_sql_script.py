from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path
import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

PROJECT_STAGING_REL = Path("data/acs/processed/acs_tract/staging")
PROJECT_SQL_REL = Path("sql/acs/fact/staging_all")
PROJECT_OUTPUT_REL = Path("outputs/acs/acs_tract")


def find_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_env_file(project_root: Path) -> None:
    env_file = project_root / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file)
        print(f"[ENV] Loaded .env from: {env_file}")
    else:
        raise FileNotFoundError(f".env file not found at: {env_file}")


def get_db_config() -> dict:
    return {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def sanitize_sql_identifier(name: str) -> str:
    name = str(name).strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "col"
    if re.match(r"^\d", name):
        name = f"col_{name}"
    return name


def make_unique(names: list[str]) -> list[str]:
    counts = Counter()
    result = []
    for name in names:
        counts[name] += 1
        if counts[name] == 1:
            result.append(name)
        else:
            result.append(f"{name}_{counts[name]}")
    return result


def read_header(csv_file: Path) -> list[str]:
    with csv_file.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return next(reader)


def build_column_mapping(raw_header: list[str]) -> list[tuple[str, str]]:
    sanitized = [sanitize_sql_identifier(col) for col in raw_header]
    sanitized = make_unique(sanitized)
    return list(zip(raw_header, sanitized))


def generate_create_table_sql(table_name: str, sql_columns: list[str]) -> str:
    column_lines = [f'    "{col}" TEXT' for col in sql_columns]
    column_lines.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

    return (
        f"DROP TABLE IF EXISTS {table_name};\n\n"
        f"CREATE TABLE {table_name} (\n"
        + ",\n".join(column_lines)
        + "\n);\n"
    )


def generate_manual_copy_sql(table_name: str, sql_columns: list[str], csv_path: Path) -> str:
    csv_path_sql = csv_path.resolve().as_posix()
    column_list = ",\n    ".join(f'"{col}"' for col in sql_columns)

    return (
        "\n-- Manual load option\n"
        f"TRUNCATE TABLE {table_name};\n\n"
        f"COPY {table_name} (\n"
        f"    {column_list}\n"
        f")\n"
        f"FROM '{csv_path_sql}'\n"
        f"DELIMITER ','\n"
        f"CSV HEADER;\n"
    )


def generate_validation_sql(table_name: str) -> str:
    return (
        f"\n-- Validation\n"
        f"SELECT '{table_name}' AS table_name, COUNT(*) AS row_count\n"
        f"FROM {table_name};\n"
    )


def connect_db(db_config: dict):
    return psycopg2.connect(**db_config)


def execute_create_sql(conn, create_sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()


def load_csv_with_copy(conn, table_name: str, sql_columns: list[str], csv_file: Path) -> None:
    quoted_cols = ", ".join(f'"{col}"' for col in sql_columns)
    copy_sql = f"COPY {table_name} ({quoted_cols}) FROM STDIN WITH CSV HEADER DELIMITER ','"

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_name};")
        with csv_file.open("r", encoding="utf-8-sig", newline="") as f:
            cur.copy_expert(copy_sql, f)
    conn.commit()


def fetch_row_count(conn, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return int(cur.fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-sql", default="false")
    args = parser.parse_args()

    run_sql = parse_bool(args.run_sql)

    project_root = find_project_root()
    load_env_file(project_root)
    db_config = get_db_config()

    staging_root = project_root / PROJECT_STAGING_REL
    sql_root = project_root / PROJECT_SQL_REL
    output_root = project_root / PROJECT_OUTPUT_REL

    if not staging_root.exists():
        raise FileNotFoundError(f"Staging folder not found: {staging_root}")

    sql_root.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(staging_root.glob("*/*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No staging CSV files found under: {staging_root}")

    summary_rows = []
    execution_rows = []
    master_sql_parts = []

    conn = None
    if run_sql:
        conn = connect_db(db_config)
        print(
            f"[DB] Connected to {db_config['dbname']} at "
            f"{db_config['host']}:{db_config['port']} as {db_config['user']}"
        )

    try:
        for csv_file in csv_files:
            table_name = csv_file.stem.lower()

            parts = table_name.split("_")
            if len(parts) < 5:
                print(f"[SKIP] Unexpected filename format: {csv_file.name}")
                continue

            year = parts[2]
            table_code = parts[3].upper()

            raw_header = read_header(csv_file)
            mapping = build_column_mapping(raw_header)
            sql_columns = [x[1] for x in mapping]

            year_sql_dir = sql_root / year
            year_sql_dir.mkdir(parents=True, exist_ok=True)

            sql_file = year_sql_dir / f"01_create_{table_name}.sql"

            create_sql = generate_create_table_sql(table_name, sql_columns)
            manual_copy_sql = generate_manual_copy_sql(table_name, sql_columns, csv_file)
            validation_sql = generate_validation_sql(table_name)

            full_sql = create_sql + manual_copy_sql + validation_sql
            sql_file.write_text(full_sql, encoding="utf-8")

            master_sql_parts.append(f"-- {table_name}\n")
            master_sql_parts.append(full_sql)
            master_sql_parts.append("\n\n")

            summary_rows.append(
                {
                    "year": int(year),
                    "table_code": table_code,
                    "table_name": table_name,
                    "csv_file": str(csv_file.resolve()),
                    "sql_file": str(sql_file.resolve()),
                    "sql_column_count": len(sql_columns),
                }
            )

            print(f"[OK] SQL generated for {table_name} -> {sql_file}")

            if run_sql:
                status = "success"
                row_count = None
                error_message = None

                try:
                    execute_create_sql(conn, create_sql)
                    load_csv_with_copy(conn, table_name, sql_columns, csv_file)
                    row_count = fetch_row_count(conn, table_name)
                    print(f"[RUN] {table_name}: row_count={row_count}")
                except Exception as e:
                    if conn:
                        conn.rollback()
                    status = "failed"
                    error_message = str(e)
                    print(f"[ERROR] {table_name}: {error_message}")

                execution_rows.append(
                    {
                        "year": int(year),
                        "table_code": table_code,
                        "table_name": table_name,
                        "status": status,
                        "row_count": row_count,
                        "error_message": error_message,
                    }
                )
    finally:
        if conn is not None:
            conn.close()

    summary_df = pd.DataFrame(summary_rows).sort_values(["year", "table_code"]).reset_index(drop=True)
    summary_file = output_root / "all_acs_staging_sql_summary.csv"
    summary_df.to_csv(summary_file, index=False)

    master_sql_file = sql_root / "00_run_all_acs_staging.sql"
    master_sql_file.write_text("".join(master_sql_parts), encoding="utf-8")

    print("\nDone.")
    print(f"[OK] SQL summary file: {summary_file}")
    print(f"[OK] Master SQL file : {master_sql_file}")

    if run_sql:
        execution_df = pd.DataFrame(execution_rows).sort_values(["year", "table_code"]).reset_index(drop=True)
        execution_file = output_root / "all_acs_staging_execution_summary.csv"
        execution_df.to_csv(execution_file, index=False)

        print(f"[OK] Execution summary file: {execution_file}")
        print(f"[OK] Successful loads: {(execution_df['status'] == 'success').sum()}")
        print(f"[OK] Failed loads    : {(execution_df['status'] == 'failed').sum()}")


if __name__ == "__main__":
    main()